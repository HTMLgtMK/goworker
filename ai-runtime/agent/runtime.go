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
	// events 是本会话的事件枢纽(P3-4,惰性):仅 StartRun 且 deps.RunEvents 非
	// nil 时经 bindEvents 创建恰好一次,后续 run 复用同一实例(跨 run 的 Seq
	// 游标连续不重置依赖此语义)。RecordRun 同步路径**不**创建、**不**写事件
	// ——同步路径行为零变化(plan P3-4 边界,§6)。读写全部在 rt.mu 内,见
	// bindEvents/eventHub 的并发论证。
	events *EventHub
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

// bindEvents 惰性创建并返回本 runtime 的 EventHub(P3-4):journal 非 nil 才创建,
// 否则返回 nil(未启用,SessionDeps.RunEvents=nil 的宿主/同步路径零事件)。创建
// 恰好一次,后续 StartRun 复用同一 hub——EventHub 的 Seq 是会话维度游标,跨 run
// 不重置(event_hub.go 语义契约),复用即游标连续的前提。调用点只有 StartRun
// 同步段;RecordRun 不经此处(同步路径行为零变化)。
//
// 并发论证(plan P3-4「hub 创建一次,后续复用」):
//   - 创建点唯一(StartRun 同步段),而同会话的第二 StartRun 在 active 占位检查
//     处被拒(ErrRunActive,supervisor.go ②)——同一 runtime 上不存在并发
//     bindEvents,「读-判-写」的竞态窗口按前提不可达;rt.mu 内判空互斥同时兜底
//     读侧(eventHub 与 P3-5 的 closeEvents 经同一把锁);
//   - NewEventHub 构造含 journal.SeqRange 磁盘扫描(播种游标),按 P2-3 起的
//     「锁内不做文件 I/O」纪律放在 rt.mu 外执行:锁内判空 → 锁外构造 → 锁内
//     二次确认写入(双重检查)。两把锁无嵌套:rt.mu 释放后才进 NewEventHub
//     (hub.mu 由 hub 自持,不回触 rt.mu),无锁环;极端竞态下(前提被未来改动
//     破坏)最坏结果是多造一个被丢弃的 hub——构造只读不写,丢弃无副作用;
//   - 与 Manager.Close 的 closeEvents(P3-5)的交集:Close 的次序是「先等全部
//     在途 run 终态,再逐 runtime closeEvents」,而 bindEvents 只发生在 StartRun
//     同步段——Close 置 closed 后 StartRun 在 ① 即被拒,二者无并发窗口;events
//     字段的全部读写都在 rt.mu 内。
func (r *SessionRuntime) bindEvents(sessionID string, journal EventJournal) *EventHub {
	if journal == nil {
		return nil
	}
	r.mu.Lock()
	if hub := r.events; hub != nil {
		r.mu.Unlock()
		return hub
	}
	r.mu.Unlock()

	hub := NewEventHub(sessionID, journal) // SeqRange 扫描在 rt.mu 外(锁内不做文件 I/O)

	r.mu.Lock()
	if r.events == nil {
		r.events = hub
	} else {
		hub = r.events // 双重检查:极端竞态下以先创建者为准,丢弃后造者
	}
	r.mu.Unlock()
	return hub
}

// eventHub 返回本 runtime 的 EventHub(未启用或尚未有 StartRun 时为 nil)。读侧
// 访问器:P3-5 的 closeEvents 与包内测试消费;读写与 bindEvents 同在 rt.mu 内。
func (r *SessionRuntime) eventHub() *EventHub {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.events
}

// Hub 返回本 runtime 的 EventHub(导出访问器,P5-6;eventHub() 的导出形):
// daemon 侧 RunControlSource 的 RunStatus 经它读 LatestSeq。未启用 RunEvents
// 或尚无 StartRun 时为 nil——与 eventHub() 同语义,只导出不改行为。
func (r *SessionRuntime) Hub() *EventHub {
	return r.eventHub()
}

// EnsureHub 惰性创建并返回本 runtime 的 EventHub(P5-6,遗留③「重启后 attach
// 时 hub 未建」的解法):bindEvents 只在 StartRun 同步段触发(supervisor.go ⑥),
// 重启后的 runtime 若从未 StartRun,events 恒 nil——attach 即建补上该缺口,
// NewEventHub 构造含 journal.SeqRange 磁盘播种(游标续接既有事件历史)。
// daemon 的 RunControlSource.Attach(P5-8 的 attach 面)是唯一预期调用点;
// StartRun 的既有路径不受影响(bindEvents 幂等,创建恰好一次,后续复用)。
//
// journal 来源:经 Session deps 读(本方法在 rt.mu 内快照 r.session,取
// sess.deps.RunEvents 后交 bindEvents)——与 StartRun ⑥ 的
// rt.bindEvents(sessionID, sess.deps.RunEvents) 同一来源、同一形状,不新增
// runtime 字段。会话未绑定(sess == nil)或 RunEvents 未接线(nil)时返回 nil,
// 由调用方诚实报错(P5-8:「RunEvents 未接线/纯内存 → 诚实错误」)。
//
// 并发论证:与 bindEvents 同构——rt.mu 内快照 session 后立即释放,构造(含
// SeqRange 磁盘扫描)在 rt.mu 外(锁内不做文件 I/O 纪律),bindEvents 内部
// 双重检查保证创建恰好一次;与 closeEvents 的交集由 Manager.Close 的次序
// (先等在途 run 终态、置 closed 拒绝后续 StartRun)覆盖,Attach 若在 Close
// 后到达,拿到的是已 Close 的 hub(Subscribe/Publish 一律 ErrHubClosed,可
// 观测)或 nil,无新锁面。
func (r *SessionRuntime) EnsureHub() *EventHub {
	r.mu.Lock()
	sess := r.session
	r.mu.Unlock()
	if sess == nil {
		return nil
	}
	return r.bindEvents(r.ID, sess.deps.RunEvents)
}

// closeEvents 收口本 runtime 的事件枢纽(P3-5 生命周期断线/关闭语义):events
// 非 nil 时依次 EndRun(幂等:flush 残留合并缓冲 + 清 curRunID)与 Close(幂等:
// 置 closed、断开全部订阅者、关 journal 桥接)——此后 hub 一律拒绝 Publish/
// Subscribe(ErrHubClosed)。「关掉的事件流不能再产出事件」。
//
// 调用点只有 Manager.Close(P3-5):「等全部在途 run 终态」之后、「清表」之前,
// 逐 runtime 调用。次序即论证:tee 的 Publish 只发生在 Session.Run 内(supervisor.go
// 的 cb 调用面,先于 finalizeRun 关闭 run.done),Close 等到全部 done 关闭后才到
// 这里 ⇒ 执行中的 Publish 不可能写已关闭的 journal;EndRun 先行兜底 flush(正常
// 路径 execute ⑤.5 已收口,此处到达时缓冲已空,幂等空操作)。Close 之后
// bindEvents 理论上仍可被极端竞态下的 StartRun 触发(见 bindEvents 注释),本方法
// 不做重入防御——只保证「Close 返回时快照 runtime 的 hub 已收口」,与 P2-4 Close
// 的「快照时在途 run 全部 done」语义对齐。
//
// 锁协调:events 经 eventHub() 在 rt.mu 内读取,EndRun/Close(hub.mu + journal
// 文件 I/O)在 rt.mu 外执行——「锁内不做文件 I/O」纪律(P2-3 起);rt.mu 与
// hub.mu 无嵌套(hub 不回触 runtime),无锁环。
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

// appendRunState 将一条 Run 状态记录写入会话绑定的 Run journal 并**上抛错误**
// (I1 修复:journalRunState 的错误返回变体)。唯一调用方是 StartRun 的 queued
// 接受记录写点(supervisor.go ⑤)——「run/start 被接受」以持久化登记成功为前提
// (总方案第七节),写失败必须拒绝启动而非降级;其余写点(RecordRun 起跑、
// execute 各状态迁移、finalizeRun 终态)仍走 journalRunState 的 warn 降级。
// s 为 nil(未绑定会话)或 RunJournal 为 nil(不持久化,接口字段必须真 nil 的
// typed-nil 约定)时跳过并返回 nil;调用点在 rt.mu 外(锁内不做文件 I/O)。
func appendRunState(s *Session, rec session.RunStateRecord) error {
	if s == nil || s.deps.RunJournal == nil {
		return nil
	}
	return s.deps.RunJournal.AppendState(rec)
}

// journalRunState 将一条 Run 状态记录写入会话绑定的 Run journal(s.deps.RunJournal,
// P2-3):daemon 同步路径(RecordRun)与后台路径(P2-5 StartRun)共用同一持久化面。
// s 为 nil(未绑定会话)或 RunJournal 为 nil(不持久化)时直接跳过——接口字段必须
// 真 nil 的宿主约定(spec.go typed-nil 陷阱同 Store)保证零值 deps 的测试路径安全。
// AppendState 失败只 warn 不阻塞:journal 可用性低于执行可用性,缺终态记录由恢复
// 扫描显式标记 interrupted(缺口可观测,不静默)。例外是 StartRun 的 queued 接受
// 记录(I1 修复改走 appendRunState 的错误上抛变体——「已接受但无法追踪」的 Run
// 绝不产生)。调用点在 rt.mu 外(锁内不做文件 I/O);同会话串行由调用方已持的
// gate 保证,跨会话互不共享句柄(每 run 一文件)。
func journalRunState(s *Session, rec session.RunStateRecord) {
	if err := appendRunState(s, rec); err != nil {
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
