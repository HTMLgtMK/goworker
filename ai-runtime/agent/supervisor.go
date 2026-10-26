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
//   - 同会话互斥:active 在同步段占位 ⇒ 第二 StartRun 被拒(ErrRunActive;排队
//     只发生在全局槽一级);execute 的 AcquireRun 与同步路径 withSession 在 gate
//     上天然互斥——RecordRun 若撞上在途后台 run,由 finalizeRun 的 active 相等
//     防御与 gate 串行保证记账不互踩;
//   - 不丢记账:状态迁移只经 transition(幂等);execute 各失败分支都收敛到
//     finalizeRun(active 必清、终态必写、令牌必释放)。
package agent

import (
	"context"
	"time"

	"github.com/tinguo/goworker/ai-runtime/session"
)

// RunHandle 是 StartRun 返回的后台运行句柄:调用方经它观察状态(State)与
// 等待终态(Wait),不触碰执行本身。SessionID/RunID 是总方案第三节的观察面
// (在其上加法扩展,不删字段);run 是包内记账指针,不导出。
type RunHandle struct {
	SessionID string
	RunID     string

	run *AgentRun
}

// State 返回运行当前状态快照(queued/running/终态)。
func (h RunHandle) State() RunState {
	return h.run.snapshot().State
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
// 记账占位与 queued 记录 durable-first 落盘后即返回,执行由 supervisor goroutine
// (execute)接管,经「每会话 gate → 全局 runSlots」两级并发控制,排队面 pending
// 有界(满即拒 ErrTooManyPending)。
//
// 契约(调用方必须遵守,plan §5 风险 1):
//  1. cb 所有权自本方法返回起归 supervisor:execute goroutine 是 cb 的唯一后续
//     使用者,调用方不得再读写 cb 的任何字段/捕获变量,否则数据竞争;
//  2. execute 的 runCtx 以 manager 宿主 ctx(m.contextOf())为父——调用方 ctx
//     不参与(API 形状即保证:连接断开不影响后台 Run,TestRunSurvivesDisconnect
//     钉死),且 runCtx **不带 sessionGateKey 重入标记**:后台 Run 与前端同会话
//     操作靠 gate 互斥,但 run 内经 hub.Eval 等路径嵌套发起同会话 prompt(需要
//     重入标记放行 SessionGate,commands.go:28-50)会死锁——P5 接前端时必须在
//     注入点补上该标记,本方法不做。
//
// 拒绝语义:manager 已 Close → ErrManagerClosed;未知会话 → ErrSessionNotFound;
// 同会话已有 Run(在跑或排队)→ ErrRunActive;会话未绑定 → ErrSessionNotBound;
// 全局排队满 → ErrTooManyPending。
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

	// ② 同会话互斥检查 + 会话快照:rt.mu 内记快照后先释放,再判拒绝(锁内不做
	// 等待)。active 非 nil 即拒——同会话第二 Run 不排队(总方案第四节「Session
	// 在排队期间已经被占用,不能继续提交第二个 Run」)。
	rt.mu.Lock()
	active := rt.active
	sess := rt.session
	rt.mu.Unlock()
	if active != nil {
		return RunHandle{}, ErrRunActive
	}
	if sess == nil {
		return RunHandle{}, ErrSessionNotBound
	}

	// ③ pending 非阻塞预留(有界排队,满即拒——锁/通道都不等)。预留令牌由
	// execute 归还(进入运行前或失败出口)。
	select {
	case m.pending <- struct{}{}:
	default:
		return RunHandle{}, ErrTooManyPending
	}

	// ④ 创建记账并占位:state 保持 RunQueued(不 transition,queued 状态由此
	// 启用),execute 内再推进 running。
	run := newAgentRun(newRunID(), sessionID)
	rt.mu.Lock()
	rt.active = run
	rt.mu.Unlock()

	// ⑤ queued 记录 durable-first:先落盘再返回(总方案第七节「run/start 响应
	// 在 Run 被接受并持久化登记后立即返回」);失败 warn 继续(journalRunState,
	// 缺口由恢复扫描显式标记 interrupted,不静默)。
	journalRunState(sess, session.RunStateRecord{
		Kind:      "state",
		SessionID: run.SessionID,
		RunID:     run.ID,
		State:     string(RunQueued),
		At:        time.Now(),
	})

	// ⑥ 执行转移给 supervisor goroutine(cb 所有权随之转移,契约①),立即返回。
	go m.execute(rt, run, req, cb)
	return RunHandle{SessionID: sessionID, RunID: run.ID, run: run}, nil
}

// execute 是后台 Run 的单 goroutine 执行体(cb 所有权自 StartRun 返回起归它,
// 契约①):派生 runCtx → gate → runSlots → 出队 → running → Session.Run →
// 终态化。每条出口路径上 pending/gate/runSlots/active 四项资源各恰好释放一次
// (评审重点,plan §5.8 的释放矩阵)。
func (m *SessionManager) execute(rt *SessionRuntime, run *AgentRun, req RunRequest, cb RunCallbacks) {
	// runCtx 以宿主 ctx 为父(契约②:调用方 ctx 不参与、无 sessionGateKey 重入
	// 标记);cancel 写入 run 供 CancelRun(P2-6)经 cancelFunc() 调用。
	runCtx, cancel := context.WithCancel(m.contextOf())
	run.mu.Lock()
	run.cancel = cancel
	run.mu.Unlock()
	defer cancel()

	// ① 每会话 gate:与同步路径(withSession)天然互斥。失败(runCtx 取消,如
	// 宿主 Close)→ 归还 pending 预留 → 终态化取消 → return(pending/gate/
	// runSlots/active 释放矩阵:pending 此处归还一次,gate/runSlots 未获取,
	// active 由 finalizeRun 清)。
	releaseGate, err := rt.AcquireRun(runCtx)
	if err != nil {
		<-m.pending
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
	// 于 journal;失败 warn 不阻塞)。
	run.transition(RunRunning)
	journalRunState(rt.Session(), session.RunStateRecord{
		Kind:      "state",
		SessionID: run.SessionID,
		RunID:     run.ID,
		State:     string(RunRunning),
		At:        time.Now(),
	})

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

	// ⑥ 终态化:finalizeRun 内部先 journal 终态(durable-first)再关闭 done——
	// 等待者被唤醒时终态记录已持久(总方案第九节)。
	finalizeRun(rt, run, res)

	// ⑦ runSlots/gate 由 defer 释放(LIFO:runSlots → gate)。
}
