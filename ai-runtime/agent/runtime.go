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
	// cwd/cwdErr 是本 runtime 的会话工作目录声明(P1-4 状态绑定收口):cwd 为
	// 最近一次有效声明的绝对路径(空 = 未声明,工具回退 cfg.AllowedWorkDir),
	// cwdErr 为最近一次声明的校验错误(非 nil 即无效标记,且 cwd 恒空——宿主
	// DeclareCWD 时保证)。每 runtime 一份,随会话走;/new 的会话边界上由宿主
	// 从旧 runtime 显式迁移有效声明(startSession),Register 与 NewSessionRuntime
	// 一律零值起步——新会话无声明,不继承任何旧值。
	cwd    string
	cwdErr error
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

// DeclaredCWD 返回本 runtime 的会话 cwd 声明快照(cwd, err):err 非 nil = 最近
// 一次声明未通过宿主校验(无效标记,此时 cwd 恒空);err == nil 且 cwd == "" =
// 未声明。声明是 runtime 级状态(P1-4):/usage 式的清单回显与 prompt 门都按
// current runtime 解析,会话边界(/new)上由宿主负责迁移与作废。
func (r *SessionRuntime) DeclaredCWD() (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cwd, r.cwdErr
}

// DeclareCWD 记录本 runtime 的会话 cwd 声明(宿主校验后调用):有效声明(err ==
// nil)携带 cwd(空 = 未声明重置);无效声明(err != nil)强制清空 cwd——无效
// 标记与有效声明互斥,读侧只需判 err 即可知道 cwd 是否可信。复用 runtime.mu,
// 与 Session()/Active() 同一把锁。导出:声明落点是宿主(daemon plugin)的
// SetSession/startSession,跨包不可用小写(Plan 文本写 declareCWD,编译器指路
// 后按包边界导出为 DeclareCWD)。
func (r *SessionRuntime) DeclareCWD(cwd string, err error) {
	if err != nil {
		cwd = ""
	}
	r.mu.Lock()
	r.cwd, r.cwdErr = cwd, err
	r.mu.Unlock()
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
		// call-layer 映射保留为冗余防御(P2-1):主映射已前移至 Session.Run
		// (session.go 的 agent.Run 失败分支)。这里兜底 fn 返回 error 而非带终态
		// RunResult 的调用层错误(现状恒 nil),同样按 ctx 语义分流。
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
