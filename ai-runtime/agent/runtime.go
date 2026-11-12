package agent

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-runtime/session"
)

// SessionRuntime 为单个会话持有 gate、当前运行记录及其会话状态。

// ErrSessionArchived prevents new runs after a session reaches its terminal state.
var ErrSessionArchived = errors.New("session: runtime is archived")

// ErrSessionRetired rejects runs on a runtime instance that has been removed from
// the SessionManager. Archived is a persisted terminal state; retired only ends
// this in-memory instance, so a later session/load may open a fresh runtime from
// the same store.
var ErrSessionRetired = errors.New("session: runtime retired")

// SessionRuntime 持有一个会话的独占执行权,是 SessionManager 注册的最小单元。
// session 允许后置绑定(daemon 在 startSession 前就要有 gate),
// 也可为 nil(纯 gate 场景)。
type SessionRuntime struct {
	ID string

	mu      sync.Mutex
	session *Session
	gate    chan struct{}
	active  *AgentRun
	// admissionMu serializes run reservation with the archived transition.
	admissionMu sync.Mutex
	archived    bool
	// retired marks this runtime instance as removed from the manager registry.
	// It shares admissionMu with archived so a StartRun that already holds the
	// pointer (but has not reserved yet) cannot execute against a closed hub/store.
	retired bool
	// Session working directory: cwd is the last valid absolute path, cwdErr marks an
	// invalid declaration (with cwd cleared). Declarations are per-runtime and never inherited.
	cwd    string
	cwdErr error
	// events 是本会话的事件枢纽(惰性):仅 StartRun 且 deps.RunEvents 非 nil
	// 时经 bindEvents 创建恰好一次,后续 run 复用同一实例(跨 run 的 Seq 游标
	// 连续不重置依赖此语义)。RecordRun 同步路径**不**创建、**不**写事件
	// ——同步路径行为零变化。读写全部在 rt.mu 内,见
	// bindEvents/eventHub 的并发论证。
	events *EventHub
	// onRunDone is injected at Register and called after finalizeRun clears active.
	// It is the shared completion hook for synchronous and background runs; invoke
	// it outside rt.mu because the host may close stores or inspect the manager.
	onRunDone func(sessionID string)
}

// NewSessionRuntime 创建运行时。s 允许 nil,由 BindSession 后置绑定。
func NewSessionRuntime(id string, s *Session) *SessionRuntime {
	return &SessionRuntime{
		ID:      id,
		session: s,
		gate:    make(chan struct{}, 1),
	}
}

// reserveRun atomically checks the admission state and records a queued run.
func (r *SessionRuntime) reserveRun(run *AgentRun) (*Session, error) {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.archived {
		return nil, ErrSessionArchived
	}
	if r.retired {
		return nil, ErrSessionRetired
	}
	if r.active != nil {
		return nil, ErrRunActive
	}
	if r.session == nil {
		return nil, ErrSessionNotBound
	}
	r.active = run
	return r.session, nil
}

// releaseRunReservation removes a run that failed before execution started.
func (r *SessionRuntime) releaseRunReservation(run *AgentRun) {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()

	r.mu.Lock()
	if r.active == run {
		r.active = nil
	}
	r.mu.Unlock()
}

// Retire irreversibly rejects new reservations. It shares admissionMu with
// reserveRun: reservation wins and returns ErrRunActive, or retirement wins and
// StartRun returns ErrSessionRetired. The caller closes hub/store after retirement.
func (r *SessionRuntime) Retire() error {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active != nil {
		return ErrRunActive
	}
	r.retired = true
	return nil
}

// Retired reports whether this instance has been removed from admission. A
// retired runtime must never be registered again; a persisted session reopens
// as a fresh instance instead.
func (r *SessionRuntime) Retired() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retired
}

// Archive transitions the runtime to read-only and rolls it back if commit fails.
func (r *SessionRuntime) Archive(commit func() error) error {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()

	r.mu.Lock()
	if r.active != nil {
		r.mu.Unlock()
		return ErrRunActive
	}
	r.archived = true
	r.mu.Unlock()

	if commit == nil {
		return nil
	}
	if err := commit(); err != nil {
		r.archived = false
		return err
	}
	return nil
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
// 记账写入在 RecordRun 接线(假定调用方已持 gate)。
func (r *SessionRuntime) Active() *AgentRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active
}

// DeclaredCWD 返回本 runtime 的会话 cwd 声明快照(cwd, err):err 非 nil = 最近
// 一次声明未通过宿主校验(无效标记,此时 cwd 恒空);err == nil 且 cwd == "" =
// 未声明。声明是 runtime 级状态:/usage 式的清单回显与 prompt 门都按
// current runtime 解析,会话边界(/new)上由宿主负责迁移与作废。
func (r *SessionRuntime) DeclaredCWD() (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cwd, r.cwdErr
}

// DeclareCWD records a validated working-directory declaration. A non-nil err clears cwd,
// so callers can trust cwd exactly when err is nil. Uses the shared runtime mutex.
func (r *SessionRuntime) DeclareCWD(cwd string, err error) {
	if err != nil {
		cwd = ""
	}
	r.mu.Lock()
	r.cwd, r.cwdErr = cwd, err
	r.mu.Unlock()
}

// bindEvents lazily creates and returns this runtime's EventHub; a nil journal returns
// (nil, nil). Exactly one hub is created and reused so the session-scoped sequence stays
// continuous. Disk seeding runs outside rt.mu with a double-checked write; construction
// errors are returned without caching, and all events-field access stays under rt.mu.
func (r *SessionRuntime) bindEvents(sessionID string, journal EventJournal) (*EventHub, error) {
	if journal == nil {
		return nil, nil
	}
	r.mu.Lock()
	if hub := r.events; hub != nil {
		r.mu.Unlock()
		return hub, nil
	}
	r.mu.Unlock()

	hub, err := NewEventHub(sessionID, journal) // SeqRange 扫描在 rt.mu 外(锁内不做文件 I/O)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	if r.events == nil {
		r.events = hub
	} else {
		hub = r.events // 双重检查:极端竞态下以先创建者为准,丢弃后造者
	}
	r.mu.Unlock()
	return hub, nil
}

// eventHub 返回本 runtime 的 EventHub(未启用或尚未有 StartRun 时为 nil)。读侧
// 访问器:closeEvents 与包内测试消费;读写与 bindEvents 同在 rt.mu 内。
func (r *SessionRuntime) eventHub() *EventHub {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.events
}

// Hub 返回本 runtime 的 EventHub(导出访问器,eventHub() 的导出形):
// daemon 侧 RunControlSource 的 RunStatus 经它读 LatestSeq。未启用 RunEvents
// 或尚无 StartRun 时为 nil——与 eventHub() 同语义,只导出不改行为。
func (r *SessionRuntime) Hub() *EventHub {
	return r.eventHub()
}

// CloseEvents ends and closes the runtime's event hub idempotently. Call only when no
// Publish can be in flight; closed hubs reject Subscribe/Publish, and a later runtime
// rebinds and seeds its sequence from the persistent journal. A nil hub is a no-op.
func (r *SessionRuntime) CloseEvents() {
	r.closeEvents()
}

// EnsureHub lazily creates the hub for attach paths that never ran StartRun. It snapshots
// the session under rt.mu, then delegates to bindEvents outside the lock. Unbound or
// unconfigured sessions return (nil, nil); construction errors propagate uncached.
func (r *SessionRuntime) EnsureHub() (*EventHub, error) {
	r.mu.Lock()
	sess := r.session
	r.mu.Unlock()
	if sess == nil {
		return nil, nil
	}
	return r.bindEvents(r.ID, sess.deps.RunEvents)
}

// closeEvents ends and closes the hub after all in-flight runs are done, so no Publish
// can reach a closed journal. Hub locks and file I/O run outside rt.mu; the locks never nest.
func (r *SessionRuntime) closeEvents() {
	if hub := r.eventHub(); hub != nil {
		hub.EndRun()
		if err := hub.Close(); err != nil {
			// 桥接 journal 的 Close 失败只 warn:与 journalRunState/teePublish 的
			// 降级语义一致(journal.go:493-513,RunJournalStore 不实现 io.Closer,
			// 此路仅自定义桥接可达),不阻断 manager 停机。
			slog.Warn("agent: event hub close failed", "err", err, "session", r.ID)
		}
	}
}

// appendRunState writes and returns errors so StartRun can reject an accepted-but-untracked
// run. Only the queued acceptance point uses it; nil session/journal is skipped. Call outside rt.mu.
func appendRunState(s *Session, rec session.RunStateRecord) error {
	if s == nil || s.deps.RunJournal == nil {
		return nil
	}
	return s.deps.RunJournal.AppendState(rec)
}

// journalRunState writes a Run state record and only logs failures. Nil session/journal is
// skipped; the queued acceptance write instead uses appendRunState. Call outside rt.mu.
func journalRunState(s *Session, rec session.RunStateRecord) {
	if err := appendRunState(s, rec); err != nil {
		slog.Warn("agent: run journal append failed", "err", err, "run_id", rec.RunID, "state", rec.State)
	}
}

// finalizeRun is shared terminalization: normalize the result, durably journal it, clear
// active only if still ours, then close done. Journal before done and active before done are
// invariants; journal I/O runs outside rt.mu. Cancellation maps to cancelled, other errors
// to failed, and a non-terminal success to completed.
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

	r.mu.Lock()
	if r.active == run { // 防御:不覆盖后续记账(当前 gate 独占下不会发生)
		r.active = nil
	}
	hook := r.onRunDone
	r.mu.Unlock()
	// 先清 active 再关 done:等待者被唤醒的瞬间,本会话已可提交下一笔 Run。
	// 反序会给「prompt 返回 → 立即下一次 prompt」的同步调用形态留一个
	// ErrRunActive 窗口(fast provider + race detector 下已实测命中)。
	run.finish(res)
	// Notify terminal state outside rt.mu after active and done are cleared, so the host
	// can re-evaluate deferred shutdown without blocking waiters. The hook must not block or
	// reacquire this runtime's gate.
	if hook != nil {
		hook(r.ID)
	}
	return res
}

// RecordRun executes one run while the caller already holds the gate; it never calls
// AcquireRun. It installs active, runs fn, then finalizes. Production uses StartRun; this
// remains for tests and shared finalization.
func (r *SessionRuntime) RecordRun(fn func(*Session) (RunResult, error)) RunResult {
	// SessionID 暂以 runtime id 充当("current");store 的稳定 SessionID 由 store 接线提供。
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
