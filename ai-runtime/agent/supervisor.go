// supervisor.go 是 P2-5 的后台执行调度(RunSupervisor):StartRun 立即返回
// RunHandle,执行在独立 goroutine(execute)上进行——与 RecordRun 同步路径共用
// runtime 记账(active/gate)与终态化(finalizeRun,runtime.go),并发控制两级:
// 每会话 gate(容量 1)+ 全局 runSlots(容量=最大并发),排队面 pending 有界
// (计数令牌,满即拒)。
//
// 锁/channel 次序(本步核心论证,plan P2-5「并发与锁论证」):
//   - m.mu 只护查找与 closed,持有期间不做任何 channel 等待;rt.mu 只护
//     active/session 指针,同理(总方案第四节「不建议在 Manager.mu 下等待执行槽」);
//   - 通道获取次序固定 gate → runSlots,释放逆序 runSlots → gate,两把「锁」
//     全序无环;pending 是计数令牌非锁:StartRun 同步段非阻塞获取(满即拒),
//     execute 各出口路径恰好归还一次,无阻塞、无泄漏;
//   - 同会话互斥:active 的检查与占位在同一 rt.mu 临界区内原子完成(C1 修复:
//     旧形态检查后放锁、pending 预留后再单独占位,是检查/占位 TOCTOU——并发
//     StartRun 可双双通过检查,后占位者覆盖先占位者的 active,CancelRun 定位
//     失败);占位 ⇒ 第二 StartRun 被拒(ErrRunActive;排队只发生在全局槽一级),
//     冲突拒绝锁内置标志、锁外归还 pending 令牌(rt.mu 内禁止 channel 操作,
//     归还接收经令牌计数守恒保证无阻塞);execute 的 AcquireRun 与同步路径
//     withSession 在 gate 上天然互斥——RecordRun 若撞上在途后台 run,由
//     finalizeRun 的 active 相等防御与 gate 串行保证记账不互踩;
//   - 不丢记账:状态迁移只经 transition(幂等);execute 各失败分支都收敛到
//     finalizeRun(active 必清、终态必写、令牌必释放)。
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	core "github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// RunHandle 是 StartRun 返回的后台运行句柄:调用方经它观察状态(State)与
// 等待终态(Wait),不触碰执行本身。SessionID/RunID 是总方案第三节的观察面
// (在其上加法扩展,不删字段);run 是包内记账指针,不导出。
type RunHandle struct {
	SessionID string
	RunID     string

	run *AgentRun
	// submitState 是接受时刻的状态快照(同步段内捕获,此后只读)——
	// SubmitState() 的取值来源;与 State() 的活快照语义区分。
	submitState RunState
}

// State 返回运行当前状态快照(queued/running/终态)。注意这是读取时刻的活
// 快照——execute goroutine 并发推进,fast provider 下读取时可能已是终态。
func (h RunHandle) State() RunState {
	return h.run.snapshot().State
}

// SubmitState 返回**接受时刻**的状态(总方案 §4.1 RunAccepted{State} 语义):
// 在 manager.StartRun 同步段内捕获(此时 run 恒为 RunQueued——transition 到
// running 发生在 execute ④),不接受后续推进,是确定性的提交响应值。查询
// 运行的当前进度应使用 State() 或 RunStatus,二者语义不同。
func (h RunHandle) SubmitState() RunState {
	return h.submitState
}

// Wait 阻塞至运行终态或 ctx 取消:终态时返回结果快照(done 关闭前 journal 终态
// 已落盘——finalizeRun 的次序不变量,等待者看到的结果即持久事实);ctx 取消时
// 返回 ctx.Err(),运行本身继续在后台执行(等待与运行生命周期解耦:断连的观察
// 者不影响后台 Run,见 TestRunSurvivesDisconnect)。
func (h RunHandle) Wait(ctx context.Context) (RunResult, error) {
	select {
	case <-h.run.done:
		return h.run.snapshot(), nil
	case <-ctx.Done():
		return RunResult{}, ctx.Err()
	}
}

// StartRun 提交一次后台运行并立即返回句柄(总方案 P2 第 2/3 条):同步段完成
// 记账占位、事件面接线(⑥,N1 修复:先于 queued 记录)与 queued 记录
// durable-first 落盘后即返回,执行由 supervisor goroutine
// (execute)接管,经「每会话 gate → 全局 runSlots」两级并发控制,排队面 pending
// 有界(满即拒 ErrTooManyPending)。
//
// 契约(调用方必须遵守,plan §5 风险 1):
//  1. cb 所有权自本方法返回起归 supervisor:execute goroutine 是 cb 的唯一后续
//     使用者,调用方不得再读写 cb 的任何字段/捕获变量,否则数据竞争;
//  2. run 的 runCtx 由本方法同步段以 manager 宿主 ctx(m.contextOf())为父派生
//     (StartRun ⑦,P2-6 修正)——调用方 ctx 不参与(API 形状即保证:连接断开
//     不影响后台 Run,TestRunSurvivesDisconnect 钉死)。P5-1 起 runCtx 携带
//     WithRunContext 注入的键值对:daemon 以 sessionGateKey 重入标记兑现 P2-5
//     契约——后台 Run 内 hub 工具嵌套 Eval 同会话命令(tools.go 的
//     p.hub.Eval → commands.go withSession)不再「持 gate 重复排队」死锁
//     (总方案 §6 风险 2,P5-6/P5-7 StartRun 接线的顺序硬前置);未配置注入项
//     时 ctx 链与 P2-5 完全一致(零注入零变化)。
//  3. 审批回退(P4-2):cb.Decide 为 nil 时,HITL 决策由 manager 的
//     ApprovalManager 承担(decideFunc 闭包,⑦.5 注入)——run 在等待决策期间
//     迁移 RunWaiting(waiting_input)并落 waiting_input journal 记录,决策经
//     ResolveApproval/Approvals().Resolve 跨连接投递,超时/取消默认拒绝;
//     调用方自带 Decide(CLI/测试形态)不受影响,本方法不覆盖。
//
// 拒绝语义:manager 已 Close → ErrManagerClosed;未知会话 → ErrSessionNotFound;
// 全局排队满 → ErrTooManyPending(③ 预留先于 ② 的占用检查:排队面满即拒,不
// 为注定拒绝的提交探测会话状态);同会话已有 Run(在跑或排队)→ ErrRunActive;
// 会话未绑定 → ErrSessionNotBound;queued 接受记录持久化失败 → ErrRunJournalWrite
// (I1 修复,包裹底层 journal 错误——「接受并持久化登记」是 run/start 返回的
// 前提,写失败即拒绝启动,pending/active 记账无痕清退,绝不返回句柄);
// 事件 hub 接线失败(EventJournal.SeqRange 播种 I/O 错误,I3 修复,包裹底层
// 错误)→ 同款无痕清退后显式拒绝——绝不返回携带「看似健康 hub」的句柄。
// N1 修复(第四轮评审):hub 接线(⑥)物理先于 queued 接受记录(⑤)——接线
// 失败时 journal 零记录,不留「看似已接受但从未执行、无终态」的孤儿 queued Run
// (旧形态接受点在先,残留 queued 只能靠恢复扫描标 interrupted 补救,与
// 「StartRun 返回失败不留已接受痕迹」冲突)。
func (m *SessionManager) StartRun(sessionID string, req RunRequest, cb RunCallbacks) (RunHandle, error) {
	// ① manager 查找:m.mu 内快照 closed 与 runtime 后立即释放(锁内不做等待)。
	m.mu.RLock()
	closed := m.closed
	rt := m.runtimes[sessionID]
	m.mu.RUnlock()
	if closed {
		return RunHandle{}, ErrManagerClosed
	}
	if rt == nil {
		return RunHandle{}, ErrSessionNotFound
	}

	// ③ pending 非阻塞预留(有界排队,满即拒——锁/通道都不等;先于 ②:排队面
	// 满即拒,不为注定拒绝的提交探测会话状态)。预留令牌由 execute 归还(进入
	// 运行前或失败出口),② 的拒绝路径亦原路归还(见下)。
	select {
	case m.pending <- struct{}{}:
	default:
		return RunHandle{}, ErrTooManyPending
	}

	// ② 同会话互斥检查 + ④ 记账占位,合并为同一 rt.mu 临界区(C1 修复:原形态
	// ② 读 active 后放锁、③ 预留后 ④ 再单独占位,是检查/占位 TOCTOU——并发
	// StartRun 可双双通过检查,后占位者覆盖先占位者的 rt.active,CancelRun 经
	// Active() 定位失败、「同会话至多一个 Run(含排队)」不变量被破坏)。锁内
	// 只做判定与指针写:active 非 nil → 置冲突标志(ErrRunActive);会话未绑定
	// → 同置标志(ErrSessionNotBound);否则占位 rt.active=run 并快照 sess——
	// 检查通过与占位同临界区完成,无窗口。rt.mu 内禁止 channel 操作:冲突/未
	// 绑定先置标志,解锁后归还 ③ 预留的令牌再返回;归还接收无阻塞(令牌计数
	// 守恒:每笔预留恰一次归还——execute 各出口或本冲突路径,任一归还者执行
	// 接收时其余接收者至多完成「预留总数-1」次,缓冲恒非空)。
	run := newAgentRun(newRunID(), sessionID) // state 保持 RunQueued(不 transition,queued 状态由此启用),execute 内再推进 running
	var (
		sess   *Session
		reject error // 冲突标志:锁内置非 nil,锁外归还令牌后以此返回
	)
	rt.mu.Lock()
	switch {
	case rt.active != nil: // 同会话第二 Run 不排队(总方案第四节「Session 在排队期间已经被占用,不能继续提交第二个 Run」)
		reject = ErrRunActive
	case rt.session == nil:
		reject = ErrSessionNotBound
	default:
		rt.active = run
		sess = rt.session
	}
	rt.mu.Unlock()
	if reject != nil {
		<-m.pending // ③ 预留的令牌原路归还(锁外——rt.mu 内不做 channel 操作)
		return RunHandle{}, reject
	}

	// ⑥ 事件面接线(P3-4,可选;N1 修复:**物理先于 ⑤** 的 queued 接受记录):
	// deps.RunEvents 非 nil 时惰性创建会话级 EventHub(runtime.bindEvents,创建
	// 一次后续 run 复用),本 run 的产生边界由 ⑤ 之后的 BeginRun 括起(设
	// AppendEvent 目标兜底 + 防御性 flush 上一 run 残留合并缓冲)。RunEvents 为
	// nil 时 hub 为 nil——零开销路径(未接线事件流的宿主/测试路径)。tee 包装
	// (cb = teeRunCallbacks)已下移到 ⑦.6:P4-2 的审批回退(⑦.5)
	// 须先于 tee——tee 检测 cb.Decide 非 nil 才包 permission.requested/resolved,
	// 次序颠倒会漏掉审批事件对。
	//
	// I3 修复:hub 构造失败(EventJournal.SeqRange 播种的 Journal I/O 错误)→
	// 显式上抛,pending/active 无痕清退(与 ⑤ 的 I1 清退同款:rt.mu 内身份校验
	// 清 active + 锁外归还 ③ 预留的 pending 令牌),绝不带着缺失/重起的 hub 进入
	// 执行(事件流从 Seq=1 重起会与既有记录冲突)。此刻 run 尚未 begin——未
	// transition、未派生 runCtx(cancel 未写入)、execute 未启动;接线本身只读
	// (SeqRange 磁盘扫描,构造在 rt.mu 外——锁内不做文件 I/O 纪律不变),不写
	// 任何 journal。
	//
	// N1 修复(第四轮评审):接线放在 ⑤ 的 queued 接受记录**之前**——失败路径
	// journal 零记录,不留「看似已接受但从未执行、无终态」的孤儿 queued Run
	// (旧形态接受点在先,失败后残留的 queued 只能由恢复扫描按「无终态」诚实标
	// interrupted 补救)。接线成功后即便 ⑤ 落败,会话级 hub 留在 runtime 上也是
	// 合法状态(创建恰好一次、后续 run 复用,与 EnsureHub 的 attach 先建同型,
	// bindEvents 不缓存失败、下次调用重试),本 run 未 BeginRun、未装 tee、
	// execute 未启动——零事件产出,无需回收。
	hub, err := rt.bindEvents(sessionID, sess.deps.RunEvents)
	if err != nil {
		rt.mu.Lock()
		if rt.active == run {
			rt.active = nil
		}
		rt.mu.Unlock()
		<-m.pending // ③ 预留的令牌原路归还(锁外,同 ② 的拒绝路径)
		return RunHandle{}, fmt.Errorf("agent: start run %s in session %s: %w", run.ID, sessionID, err)
	}

	// ⑤ queued 记录 durable-first:先落盘再返回(总方案第七节「run/start 响应
	// 在 Run 被接受并持久化登记后立即返回」)。I1 修复:接受登记是 StartRun 唯一
	// 的严格化写点——appendRunState 直接检查错误,失败即拒绝启动(返回
	// ErrRunJournalWrite 包裹底层 journal 错误),绝不降级为「已接受但无法追踪」
	// 的 Run(journalRunState 的 warn 语义只覆盖运行中/终态的非关键记录,其缺口
	// 由恢复扫描标记 interrupted;接受记录缺失则整个 Run 无处可寻)。queued 记录
	// 同时是 run 事件文件的首条记录(RunJournalStore 的句柄经它建立,后续事件走
	// 缓存命中)。N1 修复后本写点在 ⑥ 接线成功之后执行,失败路径同样 journal
	// 零记录(queued 未写成,且无更早记录)。
	//
	// 失败清理(无痕拒绝):此刻 run 尚未 begin——未 transition(状态恒 queued 但
	// 无观察者)、未派生 runCtx(cancel 未写入)、execute 未启动,返回的零值
	// RunHandle 是唯一凭据且已被丢弃——journal 无该 run 的任何记录(RunJournalStore
	// 建文件后首写失败时残留空/半行文件,恢复扫描对「无 state 记录」的 run 跳过,
	// 不标 interrupted),按无痕收敛:rt.mu 内身份校验清 active(防御式,与
	// finalizeRun 同款——此窗口内不可能有并发占位:active 非 nil 的第二 StartRun
	// 全部撞 ErrRunActive)+ 锁外归还 ③ 预留的 pending 令牌(rt.mu 内禁止
	// channel 操作的纪律不变)。
	if err := appendRunState(sess, session.RunStateRecord{
		Kind:      "state",
		SessionID: run.SessionID,
		RunID:     run.ID,
		State:     string(RunQueued),
		At:        time.Now(),
	}); err != nil {
		rt.mu.Lock()
		if rt.active == run {
			rt.active = nil
		}
		rt.mu.Unlock()
		<-m.pending // ③ 预留的令牌原路归还(锁外,同 ②/⑥ 的拒绝路径)
		return RunHandle{}, fmt.Errorf("agent: start run %s in session %s: %w: %w",
			run.ID, sessionID, ErrRunJournalWrite, err)
	}

	// BeginRun 括起本 run 的事件产生边界(N1 次序:在 ⑤ 的 queued 记录之后——
	// BeginRun 只设 curRunID + flush 上一 run 残留合并缓冲(event_hub.go:298),
	// 无任何 journal 写;本 run 首批事件晚于 queued 记录产生无碍——事件面与 run
	// 面是两本账:⑤ 是 Run journal 的首条记录,⑥ 的 Seq 播种读的是事件 journal,
	// 互不依赖)。
	if hub != nil {
		hub.BeginRun(run.ID)
	}

	// ⑦ 派生 runCtx 并写入 run.cancel——**同步段完成**(P2-6 修正):返回的
	// 句柄立即可取消。若推迟到 execute 内写入(P2-5 原形态),StartRun 返回后
	// 立即 CancelRun 会与 execute 的调度竞争,对排队中的 Run 误报
	// ErrRunNotCancellable(plan P2-6 的 TestCancelIsolation 实测 20 次复跑 6 次
	// 命中)。runCtx 以宿主 ctx 为父(契约②:调用方 ctx 不参与);P5-1 起先逐项
	// WithValue 注入 WithRunContext 声明的键值对(构造期写定、此处只读,无锁),
	// 再 WithCancel——取消传播不变(WithCancel 的父是注入链,根仍是宿主 ctx)。
	// RecordRun 记账形态(PR4 起仅测试/内部细节)不经此处,其 run.cancel 恒 nil
	// (ErrRunNotCancellable 的唯一产生面)。
	runCtx := m.contextOf()
	for _, kv := range m.runCtxValues {
		runCtx = context.WithValue(runCtx, kv.Key, kv.Value)
	}
	runCtx, cancel := context.WithCancel(runCtx)
	run.mu.Lock()
	run.cancel = cancel
	run.mu.Unlock()

	// ⑦.5 审批回退(P4-2,总方案 §6 Decide 适配):cb.Decide 为 nil 时注入
	// ApprovalManager 的 decideFunc 闭包——闭包捕获 runCtx/sess/run/sessionID
	// 与 hub(P5-2:hub 非 nil 时 waiting 两处迁移增发 run.state_changed 事件,
	// 从纯决策面扩展),等待期间 run 迁移 RunWaiting 并落 waiting_input journal
	// 记录(见 approval.go decideFunc 注释);取消经 runCtx.Done → Wait reject →
	// 工具中止 → 既有取消路径收敛。调用方自带 Decide(CLI/测试形态)不受影响。
	// 必须先于 ⑦.6 的 tee:tee 检测 cb.Decide 非 nil 才包审批事件对。
	if cb.Decide == nil {
		cb.Decide = m.approvals.decideFunc(runCtx, sess, run, sessionID, hub)
	}

	// ⑦.6 tee 包装(P3-4;C2 修复):审批回退已就位,tee 看到的是 decideFunc
	// 版本的 Decide——permission.requested/resolved 事件对围绕真实等待产生,
	// 回放面完整。hub 非 nil 时 tee 对 nil 的 EmitToken/Publish 主动装上事件
	// 采集回调(C2:daemon run/start 以 RunCallbacks{} 全 nil 提交,采集不再
	// 依赖调用方回调,真实 token/tool_call/最终消息与 usage 类事件照常进事件
	// 流);包装对象仍是 cb 本体(execute 是唯一后续使用者,契约①不变)。
	cb = teeRunCallbacks(cb, hub, run.ID)

	// ⑧ 执行转移给 supervisor goroutine(cb 所有权随之转移,契约①;tee 包装
	// 不改变所有权归属——execute 仍是 cb 的唯一使用者),立即返回。
	// 接受时刻状态快照:此刻 run 恒为 RunQueued(transition 到 running 在
	// execute ④),同步段内捕获后不接受 execute 的并发推进——StartRun 的
	// 响应状态(总方案 §4.1 RunAccepted)是确定性的提交值,而非与 fast
	// provider 竞争的活快照(runcontrol_test 断言 queued 即依赖本语义)。
	submitState := run.snapshot().State

	go m.execute(rt, run, runCtx, cancel, req, cb, hub)
	return RunHandle{SessionID: sessionID, RunID: run.ID, run: run, submitState: submitState}, nil
}

// execute 是后台 Run 的单 goroutine 执行体(cb 所有权自 StartRun 返回起归它,
// 契约①;cb 可能已被 teeRunCallbacks 包装,事件随执行流入会话 hub):gate →
// runSlots → 出队 → running → Session.Run → 事件收口 → 终态化。runCtx/cancel
// 由 StartRun 同步段派生并写入 run.cancel(P2-6 修正,见 StartRun ⑦),execute
// 负责在出口释放 cancel。每条出口路径上 pending/gate/runSlots/active 四项资源
// 各恰好释放一次(评审重点,plan §5.8 的释放矩阵)。
//
// hub 为 nil 表示会话未启用事件流(SessionDeps.RunEvents=nil):全部事件动作
// 跳过,行为与 P2 完全一致。
func (m *SessionManager) execute(rt *SessionRuntime, run *AgentRun, runCtx context.Context, cancel context.CancelFunc, req RunRequest, cb RunCallbacks, hub *EventHub) {
	defer cancel()

	// 事件收口兜底(P3-4):早退路径(①/② 失败)从未产生事件,EndRun 是幂等
	// 空操作;正常路径由 ⑤ 之后的显式 EndRun 先行收口,本 defer 到达时合并缓冲
	// 已空。次序铁律见显式调用处注释——defer 晚于 finalizeRun(晚于终态落盘),
	// 不能替代显式调用。
	if hub != nil {
		defer hub.EndRun()
	}

	// ① 每会话 gate:与同步路径(withSession)天然互斥。失败(runCtx 取消,如
	// 宿主 Close)→ 归还 pending 预留 → 终态化取消 → return(pending/gate/
	// runSlots/active 释放矩阵:pending 此处归还一次,gate/runSlots 未获取,
	// active 由 finalizeRun 清)。
	releaseGate, err := rt.AcquireRun(runCtx)
	if err != nil {
		<-m.pending
		// run.finished{cancelled}(P5-2,仅 hub 非 nil;execute ① 早退出口)——
		// 次序铁律:事件先于终态 state 记录(finalizeRun 的 AppendState 会 Close+
		// 逐出 journal 句柄,journal_store.go 的次序约束),也先于上方 defer 的
		// EndRun(显式调用,不依赖 defer)。
		emitRunEvent(hub, run.ID, session.EventRunFinished, map[string]any{"state": string(RunCancelled)})
		finalizeRun(rt, run, RunResult{State: RunCancelled, Err: err, Persisted: false})
		return
	}

	// ② 全局 runSlots:获取次序固定 gate → runSlots(全序无环)。失败(runCtx
	// 取消,如并发槽被占满时宿主停机)→ 释放 gate → 归还 pending → 终态化取消
	// → return(pending 归还恰一次,gate 内联释放恰一次,runSlots 未获取)。
	select {
	case m.runSlots <- struct{}{}:
	case <-runCtx.Done():
		releaseGate()
		<-m.pending
		// run.finished{cancelled}(P5-2,同 ① 早退出口的次序铁律)。
		emitRunEvent(hub, run.ID, session.EventRunFinished, map[string]any{"state": string(RunCancelled)})
		finalizeRun(rt, run, RunResult{State: RunCancelled, Err: runCtx.Err(), Persisted: false})
		return
	}
	// 注册次序即释放次序(defer LIFO):runSlots 先于 gate 释放——两把「锁」
	// 释放逆序(runSlots → gate,与获取次序互为逆),无环。
	defer releaseGate() // gate 由 defer 释放(⑦)
	defer func() { <-m.runSlots }()

	// ③ 离开排队:归还 pending 预留令牌(此后不再占用排队面)。
	<-m.pending

	// ④ 推进 running 并落盘 running 记录(与 RecordRun 同款次序:transition 先
	// 于 journal;失败 warn 不阻塞)。run.started{state:"running"}(P5-2,仅 hub
	// 非 nil)紧随 transition 产出——run 生命周期的起点事件。
	run.transition(RunRunning)
	emitRunEvent(hub, run.ID, session.EventRunStarted, map[string]any{"state": string(RunRunning)})
	journalRunState(rt.Session(), session.RunStateRecord{
		Kind:      "state",
		SessionID: run.SessionID,
		RunID:     run.ID,
		State:     string(RunRunning),
		At:        time.Now(),
	})

	// ④.5 能力等待观察者注入(P4-4):包装 req.ToolSource,给携带
	// capabilityWaitMetaKey 标记的代理工具注入 onWait 闭包——等待重绑期间 run
	// 迁移 RunWaitingCapability 并落 waiting_capability journal 记录(与
	// decideFunc 的 waiting_input 迁移同构:transition 在 run.mu 内、journal
	// 在 rt.mu 外),等待收敛回 RunRunning(仅迁移,journal 由 ④ 与终态记录
	// 夹注,不重复落 running;ctx 取消收敛时跳过回迁——P4 评审 issue4 的取消
	// 守卫)。onWait 经 ctx 注入代理 Execute(capability.go
	// 的 withCapabilityWaitObserver):ai-core 的 toolCtx 是本 Run agentCtx 的
	// 子 ctx,值随链传播。
	//
	// **RecordRun 同步路径不注入**(不包装 ToolSource):同步 Run 的代理工具
	// 拿不到观察者,等待照常发生(安全超时兜底)但 RunState 不迁移——
	// waiting_capability 仅后台 Run 可观测,同步路径记 running(limitation,
	// plan P4-4 行为变化声明的组成部分)。
	//
	// 契约前提:ToolSource 每 Run 恰被调用一次且返回当轮新构造的工具
	// (session.go:124-129 probe 一次;P5-9 的 binder 每 prompt 重建代理)——
	// 若未来缓存跨 Run 复用同一 Execute 闭包,重复包装会把旧 Run 的观察者叠进
	// 新 Run,该契约破坏须先修。
	if req.ToolSource != nil {
		orig := req.ToolSource
		sess := rt.Session()
		onWait := func(waiting bool) {
			if waiting {
				run.transition(RunWaitingCapability)
				journalRunState(sess, session.RunStateRecord{
					Kind:      "state",
					SessionID: run.SessionID,
					RunID:     run.ID,
					State:     string(RunWaitingCapability),
					At:        time.Now(),
				})
				return
			}
			// 取消收敛守卫(P4 评审 issue4):等待因 ctx 取消退出时不再瞬时
			// 回迁 running——transition 前查 runCtx.Err()==nil,取消路径的
			// RunHandle.State() 轮询者不看到与事实不符的 running(finalizeRun
			// 随后归一 cancelled);重绑成功/等待超时出口(runCtx 未取消)照常
			// 回迁,onWait(true/false) 的调用配对不受影响(守卫只收迁移)。
			if runCtx.Err() == nil {
				run.transition(RunRunning)
			}
		}
		req.ToolSource = func(ctx context.Context) []core.Tool {
			tools := orig(ctx)
			for i := range tools {
				if tools[i].Metadata[capabilityWaitMetaKey] == "" || tools[i].Execute == nil {
					continue
				}
				inner := tools[i].Execute
				tools[i].Execute = func(toolCtx context.Context, args map[string]any) (string, error) {
					return inner(withCapabilityWaitObserver(toolCtx, onWait), args)
				}
			}
			return tools
		}
	}

	// ⑤ 执行。Session.Run 的取消态归一(P2-1)在这里生效,但 ai-core 的
	// Agent.Run 把 provider 错误漏斗进 note token(P2-1 探针实测:Session.Run
	// 对 ctx 取消也可能返回 completed——错误经流式出口,不进返回值)。runCtx
	// 归 supervisor 所有,取消是它知道的权威事实,故在 ⑤ 与 ⑥ 之间做 execute
	// 层归一(与 session.go / finalizeRun 的归一互补):runCtx 已取消
	// (Canceled/DeadlineExceeded,含宿主 Close 传播)时覆盖为 RunCancelled,
	// 否则取消的 run 会被 journal 记成 completed。Err 取 res.Err 优先(保留
	// 原始失败原因),否则 runCtx.Err();Persisted 保留(若 Conversation Commit
	// 恰在取消信号前成功,已持久化是事实,不篡改)。
	res, _ := rt.Session().Run(runCtx, req, cb)
	if rerr := runCtx.Err(); rerr != nil {
		err := res.Err
		if err == nil {
			err = rerr
		}
		res = RunResult{State: RunCancelled, Err: err, Persisted: res.Persisted}
	}

	// ⑤.4 run.* 生命周期事件(P5-2,仅 hub 非 nil)——**次序铁律**(plan 表
	// 「execute ⑤ 与 ⑤.5 之间」):run.* 全是 key 事件(Publish 内 durable-first
	// fsync 落盘),必须落在该 run 的终态 state 记录**之前**(⑥ finalizeRun 的
	// AppendState 会 Close+逐出该 run 的 journal 句柄,其后的 AppendEvent 撞
	// ErrRunExists,journal_store.go 的次序约束),也先于 ⑤.5 的 EndRun。失败
	// 仅 warn 不阻塞终态化(emitRunEvent 内收敛,与 teePublish/journalRunState
	// 的降级语义一致)。res.State 已含 ⑤ 的取消归一,finished 无条件产出
	// (含 RunCancelled/RunCompleted);run.error 仅 RunFailed 产出。
	if res.State == RunFailed {
		runErrMsg := ""
		if res.Err != nil {
			runErrMsg = res.Err.Error()
		}
		emitRunEvent(hub, run.ID, session.EventRunError, map[string]any{"err": runErrMsg})
	}
	emitRunEvent(hub, run.ID, session.EventRunFinished, map[string]any{
		"state":     string(res.State),
		"persisted": res.Persisted,
	})

	// ⑤.5 事件收口——**次序铁律**(主 agent 补充约束一,P3-1 发现):事件必须
	// 落在该 run 的终态 state 记录**之前**。RunJournalStore.AppendState 写终态
	// 会 Close 并逐出该 run 的 journal 句柄(journal_store.go 句柄生命周期),
	// 此后再 flush 的合并 delta 只能经 OpenRunJournal 撞已存在文件 → ErrRunExists
	// 降级 warn → 该 run 的 delta 缺段。因此显式调用必须落在 ⑤ 取消覆盖检查
	// 之后、⑥ journal 终态之前;开头的 defer 只兜 ①/② 早退(仅 run.finished
	// key 事件,不进合并缓冲,到达时缓冲仍空,幂等空操作),不能替代本调用。
	// EndRun 失败(delta flush 落盘失败)仅 warn(event_hub.go 降级语义,
	// assistant.message_finalized 兜底定稿),不阻塞终态化。
	if hub != nil {
		hub.EndRun()
	}

	// ⑥ 终态化:finalizeRun 内部先 journal 终态(durable-first)再关闭 done——
	// 等待者被唤醒时终态记录已持久(总方案第九节)。
	finalizeRun(rt, run, res)

	// ⑦ runSlots/gate 由 defer 释放(LIFO:runSlots → gate)。
}

// ── teeRunCallbacks:RunCallbacks → SessionEvent 的事件分流(P3-4;C2 修复)──
//
// 把 StartRun 的 cb 三个事件入口(EmitToken/Decide/Publish)按 plan P3-4 表
// (P3-0 契约,源自 streamTokens 实际流 session.go:213-249)映射成 SessionEvent
// 投进会话 hub(journal+广播),非 nil 的原回调随后被转发——tee 先落事件、后
// 转发,消费方看到的事件流先于其输出被持久化/广播(与 hub 内 key 事件的
// durable-first 同向)。
//
// 采集与调用方回调解耦(C2 修复,GPT 评审):hub 非 nil 时 EmitToken/Publish 的
// 事件采集**主动供给**,不依赖调用方是否提供回调——daemon run/start 以
// RunCallbacks{} 全 nil 提交后台 Run(runcontrol.go),只包装非 nil 入口的旧
// 形态下 streamTokens/中间件对 nil 回调直接跳过(session.go 的 nil 检查),真实
// Token/tool_call/最终消息与 usage 类事件全部丢失,Re-Attach 只见 run.* 生命
// 周期事件。三个入口的形态:
//   - EmitToken/Publish:调用方非 nil → 包装(先发事件再转发原回调);nil → 装
//     纯事件映射版(只发事件,无原始转发——后台 Run 的输出只进事件流/journal,
//     不进任何终端)。装上的回调仍受 streamTokens/中间件的既有调用条件约束
//     (nil 检查变为命中而已),tee 不发明契约外事件;
//   - Decide 面不变(P4-2):审批回退已在 StartRun ⑦.5 先行(调用方未提供时
//     cb.Decide 已是 decideFunc),requested/resolved 事件对仍只围绕真实决策
//     等待产生;
//   - Write/WriteToken 不包装不采集:终端文本由 assistant.message_finalized
//     覆盖,不进事件 journal(plan P3-4 表「Write:不进 journal」)。
//
// 并发论证(plan P3-4):tee 的全部函数都在 supervisor goroutine(execute→
// Session.Run→streamTokens/中间件)上同步调用,Publish 的调用方即 execute——
// hub.mu 只包住 hub 内部单条事件的处理,不与 execute 持有的 gate/runSlots 嵌套
// (hub 不回触 runtime),慢订阅者满缓冲的断开路径不可能卡住 execute(P3-2 机制
// 在真实链路成立,TestSlowSubscriber 钉死)。特别地,Decide 的包装只在调用
// 前后各发一条事件,HITL 决策等待(inner 的阻塞)本身在 hub 锁外——等待期间
// 其他订阅者/发布者不受阻塞。
//
// cb 所有权:包装/供给后的 teed 只在 execute 内使用(StartRun 返回起归
// supervisor,契约①),tee 不引入新的跨 goroutine 共享。
func teeRunCallbacks(cb RunCallbacks, hub *EventHub, runID string) RunCallbacks {
	if hub == nil {
		return cb // RunEvents 未启用:原样透传,零开销路径
	}
	teed := cb
	// EmitToken(C2 修复):nil → 纯事件映射版(daemon 全 nil 提交形态);非 nil →
	// 包装(先发事件再转发原回调)。
	if cb.EmitToken != nil {
		inner := cb.EmitToken
		teed.EmitToken = func(tok core.Token) {
			teeToken(hub, runID, tok)
			inner(tok)
		}
	} else {
		teed.EmitToken = func(tok core.Token) {
			teeToken(hub, runID, tok)
		}
	}
	// Decide 面不变(P4-2,见函数头):包装只在调用前后各发一条事件。
	if cb.Decide != nil {
		inner := cb.Decide
		teed.Decide = func(req *hitl.InterruptRequest) hitl.Decision {
			// requested 先于决策等待落盘:回放侧在等待期间就能看到「审批挂起」
			// 这一事实(resolved 由返回前的下一行补上)。inner 的阻塞在 hub 锁外
			// (见函数头并发论证)。
			teePublish(hub, runID, session.EventPermissionRequested, req)
			decision := inner(req)
			teePublish(hub, runID, session.EventPermissionResolved, decision)
			return decision
		}
	}
	// Publish(C2 修复):nil → 纯映射版;非 nil → 包装。映射规则见
	// teePublishCallbackEvent(usage/iteration → EventUsageProgress)。
	if cb.Publish != nil {
		inner := cb.Publish
		teed.Publish = func(event string, data any) {
			teePublishCallbackEvent(hub, runID, event, data)
			inner(event, data)
		}
	} else {
		teed.Publish = func(event string, data any) {
			teePublishCallbackEvent(hub, runID, event, data)
		}
	}
	return teed
}

// teePublishCallbackEvent 把 cb.Publish 的中间件事件按 plan P3-4 表映射发布:
// usage/iteration 同型映射为 usage.progress 高频 delta(表「usage/iteration 类
// → EventUsageProgress(delta)」)。iteration 的 data 为 nil,marshal 为 null
// payload——合并聚合按「非字符串退化为窗内末条」处理,无语义损害。其余事件名
// 不在 P3-0 契约(现产生方只用上述两个),防御性跳过:未知类型进 journal 会让
// 回放方面对契约外语义。
func teePublishCallbackEvent(hub *EventHub, runID, event string, data any) {
	switch event {
	case runtimeconfig.EventUsage, runtimeconfig.EventIteration:
		teePublish(hub, runID, session.EventUsageProgress, data)
	}
}

// teeToken 把一个 core.Token 映射为事件类型并发布(payload=整个 Token 的 JSON,
// plan P3-4 表;回放侧可从 payload 还原 token 全貌,含 ToolCall/ToolCallID 的
// 结构化关联)。映射(与 streamTokens 的转发条件对齐,session.go:226-234):
//
//	TokenTypeToolCall        → tool_call(key)
//	TokenTypeToolResult      → tool_result(key)
//	文本 Done token          → assistant.message_finalized(key)(空 content 的
//	                           收尾 Done 也映射——前端靠它定稿流式渲染,
//	                           session.go:225;错误 note 的 Done 同样落此,
//	                           定稿即「本 run 的最终消息」)
//	TokenTypeThinking 增量   → thinking.delta(delta)
//	其余文本增量             → text.delta(delta)
//	TokenTypeEvent           → 跳过:HITL 中断等运行时事件不在 P3-0 事件契约,
//	                           中断的事件面由 Decide 的 requested/resolved 对覆盖
func teeToken(hub *EventHub, runID string, tok core.Token) {
	var typ string
	switch tok.Type {
	case core.TokenTypeToolCall:
		typ = session.EventToolCall
	case core.TokenTypeToolResult:
		typ = session.EventToolResult
	case core.TokenTypeText:
		if tok.Done {
			typ = session.EventAssistantMessageFinalized
		} else {
			typ = session.EventTextDelta
		}
	case core.TokenTypeThinking:
		// thinking token 在实际流中恒为增量(kernel 不发 Done thinking);
		// 若未来出现 Done thinking,仍按增量映射——payload 内的 Done 字段自明。
		typ = session.EventThinkingDelta
	default:
		return
	}
	teePublish(hub, runID, typ, tok)
}

// teePublish marshal 载荷并发布。marshal 失败的降级是**不对称**的(主 agent
// 补充约束五「null 或跳过,按你论证」):
//   - key 事件降级为 null payload 仍发布:边界标记(定稿/审批对)的价值在
//     「事件存在」——回放游标的推进锚点与生命周期边界不能因载荷损失而丢失;
//     null 是合法 JSON 载荷,EventRecord 落盘/广播语义完整;
//   - delta 直接跳过:该事件实时侧本就未广播(null payload 的 delta 无信息
//     量),跳过使实时与回放一致地不存在该事件——CR3-I1 起合并批次按 per-part
//     原样保留 payload,不存在聚合退化问题,宁缺勿噪。
//
// 实际上两条路径均不可达:Token/hitl 结构/UsageEvent 均为纯数据类型,marshal
// 恒成功;分支只为「未来字段引入不可 marshal 类型」兜底。Publish 失败(key 事件
// 落盘失败被 hub 拒绝广播,或 hub 已关)只 warn 不阻塞——事件可用性低于执行
// 可用性,与 journalRunState 的降级语义一致(runtime.go)。
func teePublish(hub *EventHub, runID, typ string, v any) {
	payload, err := json.Marshal(v)
	if err != nil {
		if !session.IsKeyEvent(typ) {
			return
		}
		payload = json.RawMessage("null")
	}
	if err := hub.Publish(IngressEvent{RunID: runID, Type: typ, Payload: payload}); err != nil {
		slog.Warn("agent: event tee publish failed", "err", err, "run", runID, "type", typ)
	}
}

// emitRunEvent 发布一条 run.* 生命周期事件(P5-2,plan §4:run.started/
// state_changed/finished/error 四类,常量见 journal.go:108-111,产生面仅
// StartRun 后台路径):hub 为 nil 直接跳过(RunEvents 未启用——daemon 现状不接
// deps.RunEvents,P5-4 才接线;同步 RecordRun 路径本就不建 hub——零行为变化);
// 非 nil 经 Publish 走 key 事件的 durable-first 语义(run.* 四类都在
// IsKeyEvent 的 9 类内:journal fsync 成功才广播,失败不广播返回 err)。payload
// 一次 marshal(map 值仅 string/bool 纯数据,实际不可失败;真失败按 teePublish
// 的 key 事件降级——null 载荷仍发布,生命周期边界标记的价值在「事件存在」)。
// Publish 失败(journal 落盘失败被 hub 拒绝广播、或 hub 已关)在此收敛为 warn
// 不上抛——事件可用性低于执行可用性,与 teePublish/journalRunState 的降级语义
// 一致;调用点(execute ①/②/④/⑤.4 与 decideFunc,全在 execute goroutine)不得
// 因事件失败改变终态化路径(事件失败绝不阻塞终态化)。
//
// 并发论证(plan P5-2):Publish 持 hub.mu(durable-first fsync)——P3-2 论证
// 原样;新调用点全在 execute goroutine / decideFunc(后者同在 execute goroutine
// 上被 streamTokens 同步调用),无新共享状态。
func emitRunEvent(hub *EventHub, runID, typ string, payload map[string]any) {
	if hub == nil {
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		if !session.IsKeyEvent(typ) {
			return
		}
		data = json.RawMessage("null")
	}
	if err := hub.Publish(IngressEvent{RunID: runID, Type: typ, Payload: data}); err != nil {
		slog.Warn("agent: run lifecycle event publish failed", "err", err, "run", runID, "type", typ)
	}
}
