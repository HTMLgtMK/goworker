package agent

import (
	"context"
	"errors"
	"sync"
)

// 本文件是多会话执行内核的会话运行时:SessionRuntime 持有「每会话一个」的
// 独占执行权(gate)与当前运行记账(active)。gate 形态对齐
// daemon/internal/core/service/acp/coordinator.go 的 SessionGate——
// 容量 1 channel + select ctx.Done(),锁不可被卡死客户端攥住。

// SessionRuntime 持有一个会话的独占执行权,是 SessionManager 注册的最小单元。
// session 允许后置绑定(P0-2 的 daemon 在 startSession 前就要有 gate),
// 也可为 nil(纯 gate 场景)。
type SessionRuntime struct {
	ID string

	mu      sync.Mutex
	session *Session
	gate    chan struct{}
	active  *AgentRun
}

// NewSessionRuntime 创建运行时。s 允许 nil,由 BindSession 后置绑定。
func NewSessionRuntime(id string, s *Session) *SessionRuntime {
	return &SessionRuntime{
		ID:      id,
		session: s,
		gate:    make(chan struct{}, 1),
	}
}

// AcquireRun 获取本会话的独占执行权;ctx 取消(如连接断开)时立即失败返回,
// 不无限排队。成功时返回的 release 必须恰好调用一次(多还、少还都会破坏
// 互斥);err 非 nil 时 release 为 nil。
func (r *SessionRuntime) AcquireRun(ctx context.Context) (func(), error) {
	select {
	case r.gate <- struct{}{}:
		return func() { <-r.gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Session 返回当前绑定的会话;未绑定时返回 nil。
func (r *SessionRuntime) Session() *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.session
}

// BindSession 后置绑定会话;传 nil 解绑同样合法。
func (r *SessionRuntime) BindSession(s *Session) {
	r.mu.Lock()
	r.session = s
	r.mu.Unlock()
}

// Active 返回当前运行记账;无运行时返回 nil。
// 记账写入在 P0-3 的 RecordRun 接线(假定调用方已持 gate)。
func (r *SessionRuntime) Active() *AgentRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active
}

// RecordRun 在「调用方已持 gate」的前提下执行一轮会话并记账——withSession 的
// 临界区是唯一预期调用点,这里绝不再 AcquireRun(二次排队即死锁)。流程:创建
// AgentRun 存入 r.active → 执行 fn → 按 result/ctx 终态化 → 清 r.active → 返回结果。
// 终态映射:fn 返回的 RunResult 已带终态则原样记账;调用层 error(fn 未归类的
// 失败)按 ctx 语义分流——取消/超时记 RunCancelled,其余记 RunFailed。
//
// 这是 P0 阶段 daemon 同步调用形态的适配;「独占 + 记账 + 宿主 ctx」合一的完整
// 形态(StartRun)由 P2 的 RunSupervisor 提供。
func (r *SessionRuntime) RecordRun(fn func(*Session) (RunResult, error)) RunResult {
	// SessionID 暂以 runtime id 充当("current");store 的稳定 SessionID 在 P1-3 接线。
	run := newAgentRun(newRunID(), r.ID)
	run.transition(RunRunning)
	r.mu.Lock()
	r.active = run
	r.mu.Unlock()

	res, err := fn(r.session)
	switch {
	case err != nil:
		state := RunFailed
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			state = RunCancelled
		}
		res = RunResult{State: state, Err: err, Persisted: res.Persisted}
	case res.State != RunCompleted && res.State != RunFailed && res.State != RunCancelled:
		// 防御:fn 返回非终态且无错误时按完成记账,避免 active 永悬。
		res.State = RunCompleted
	}

	run.finish(res)
	r.mu.Lock()
	if r.active == run { // 防御:不覆盖后续记账(当前 gate 独占下不会发生)
		r.active = nil
	}
	r.mu.Unlock()
	return res
}
