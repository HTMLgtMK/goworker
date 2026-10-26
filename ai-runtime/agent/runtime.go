package agent

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-runtime/session"
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

// journalRunState 将一条 Run 状态记录写入会话绑定的 Run journal(s.deps.RunJournal,
// P2-3):daemon 同步路径(RecordRun)与后台路径(P2-5 StartRun)共用同一持久化面。
// s 为 nil(未绑定会话)或 RunJournal 为 nil(不持久化)时直接跳过——接口字段必须
// 真 nil 的宿主约定(spec.go typed-nil 陷阱同 Store)保证零值 deps 的测试路径安全。
// AppendState 失败只 warn 不阻塞:journal 可用性低于执行可用性,缺终态记录由恢复
// 扫描显式标记 interrupted(缺口可观测,不静默)。调用点在 rt.mu 外(锁内不做文件
// I/O);同会话串行由调用方已持的 gate 保证,跨会话互不共享句柄(每 run 一文件)。
func journalRunState(s *Session, rec session.RunStateRecord) {
	if s == nil || s.deps.RunJournal == nil {
		return
	}
	if err := s.deps.RunJournal.AppendState(rec); err != nil {
		slog.Warn("agent: run journal append failed", "err", err, "run_id", rec.RunID, "state", rec.State)
	}
}

// finalizeRun 是 RecordRun(同步路径)与 StartRun/execute(P2-5 后台路径)共享
// 的终态化路径:终态归一防御 → journal 终态记录(durable-first)→ run.finish
// (关闭 done 唤醒等待者)→ 清 active(仅当仍为本 run,防御不覆盖后续记账)。
// 返回归一后的结果供调用方继续使用(RecordRun 的返回值即它)。次序即不变量:
// journal 落盘先于 done 关闭——等待者被唤醒时终态已持久(总方案第九节);
// journalRunState 在 rt.mu 外调用(P2-3:锁内不做文件 I/O)。
//
// 终态归一(P2-1 防御分支原样移自 RecordRun,主映射在 Session.Run,与 execute
// 层归一(supervisor.go)三层互补):res.Err 非 nil 时按 ctx 语义分流——取消/
// 超时记 RunCancelled,其余记 RunFailed;无错误但状态非终态时按完成记账,避免
// active 永悬。RecordRun 在调用前把 fn 的 call-layer error 并入 res.Err(见
// RecordRun),使两条路径经同一分支归一。
func finalizeRun(r *SessionRuntime, run *AgentRun, res RunResult) RunResult {
	switch {
	case res.Err != nil:
		state := RunFailed
		if errors.Is(res.Err, context.Canceled) || errors.Is(res.Err, context.DeadlineExceeded) {
			state = RunCancelled
		}
		res = RunResult{State: state, Err: res.Err, Persisted: res.Persisted}
	case res.State != RunCompleted && res.State != RunFailed && res.State != RunCancelled:
		res.State = RunCompleted
	}

	termErr := ""
	if res.Err != nil {
		termErr = res.Err.Error()
	}
	// 会话指针在 rt.mu 内快照(锁内不做 I/O):execute 失败出口未持 gate,不能
	// 像 RecordRun 那样依赖「调用方已持 gate」直读 r.session。
	r.mu.Lock()
	sess := r.session
	r.mu.Unlock()
	journalRunState(sess, session.RunStateRecord{
		Kind:      "state",
		SessionID: run.SessionID,
		RunID:     run.ID,
		State:     string(res.State),
		Err:       termErr, // nil 时留空,omitted 由 json tag 兜底(journal.go:42)
		Persisted: res.Persisted,
		At:        time.Now(),
	})

	run.finish(res)
	r.mu.Lock()
	if r.active == run { // 防御:不覆盖后续记账(当前 gate 独占下不会发生)
		r.active = nil
	}
	r.mu.Unlock()
	return res
}

// RecordRun 在「调用方已持 gate」的前提下执行一轮会话并记账——withSession 的
// 临界区是唯一预期调用点,这里绝不再 AcquireRun(二次排队即死锁)。流程:创建
// AgentRun 存入 r.active → 执行 fn → finalizeRun 终态化(归一+journal 终态+
// finish+清 active)→ 返回归一后结果。
//
// 这是 P0 阶段 daemon 同步调用形态的适配;后台形态(StartRun)由 P2-5 的
// RunSupervisor 提供,二者经 finalizeRun 共享终态化。
func (r *SessionRuntime) RecordRun(fn func(*Session) (RunResult, error)) RunResult {
	// SessionID 暂以 runtime id 充当("current");store 的稳定 SessionID 在 P1-3 接线。
	run := newAgentRun(newRunID(), r.ID)
	run.transition(RunRunning)
	r.mu.Lock()
	r.active = run
	r.mu.Unlock()

	// ① 起跑记录:每 run journal 文件的首条 state 记录。fn 尚未执行,取本体
	// r.session(与 fn(r.session) 同一指针,Session() 已绑定);nil 会话/nil
	// journal 由 journalRunState 跳过,不影响执行。
	journalRunState(r.session, session.RunStateRecord{
		Kind:      "state", // session.stateKind 未导出;恢复扫描只认 kind=="state"(journal.go:47)
		SessionID: run.SessionID,
		RunID:     run.ID,
		State:     string(RunRunning),
		At:        time.Now(),
	})

	res, err := fn(r.session)
	if err != nil {
		// call-layer 错误并入 res 后交 finalizeRun 归一(原 switch 的 err 分支
		// 语义保留:err 驱动状态映射,仅保留 res.Persisted)。Session.Run 恒以
		// 第二返回值 nil、错误在 RunResult.Err 内返回,此合并只兜底非常规 fn。
		res = RunResult{Err: err, Persisted: res.Persisted}
	}
	return finalizeRun(r, run, res)
}
