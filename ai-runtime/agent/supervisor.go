// RunSupervisor runs accepted jobs asynchronously, bounded by per-session gates,
// global run slots, and a pending queue. It acquires gate before run slots and
// releases them in reverse; manager/runtime locks never guard channel waits.
// Reservation and active cleanup are atomic and cover every exit.
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
// 等待终态(Wait),不触碰执行本身。SessionID/RunID 是运行的观察面
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

// SubmitState 返回**接受时刻**的状态(RunAccepted{State} 语义):
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

// StartRun reserves the session and pending slot, binds events, durably records
// queued state when journaling is enabled, then launches execute. Failures roll back
// active/pending and return no handle. SubmitState is the acceptance snapshot;
// caller ctx does not control execution, and callback ownership transfers to execute.
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

	// reserveRun checks and installs active under the shared admission/runtime locks,
	// linearizing submission against Retire and preventing duplicate active Runs.
	// Do not touch channels while holding rt.mu; release the pending token after unlock.
	run := newAgentRun(newRunID(), sessionID) // state 保持 RunQueued(不 transition,queued 状态由此启用),execute 内再推进 running
	sess, reject := rt.reserveRun(run)
	if reject != nil {
		<-m.pending // ③ 预留的令牌原路归还(锁外——runtime admission 已完成)
		return RunHandle{}, reject
	}

	// Wire the optional session EventHub before the durable queued acceptance record.
	// Hub creation failure releases active/pending and aborts submission; it does not begin
	// a run or write journal state. Install the approval fallback before the callback tee.
	hub, err := rt.bindEvents(sessionID, sess.deps.RunEvents)
	if err != nil {
		rt.releaseRunReservation(run)
		<-m.pending // ③ 预留的令牌原路归还(锁外,同 ② 的拒绝路径)
		return RunHandle{}, fmt.Errorf("agent: start run %s in session %s: %w", run.ID, sessionID, err)
	}

	// Persist queued before accepting or returning a handle. This is StartRun's strict
	// write: on failure, release active/pending and return ErrRunJournalWrite without
	// starting execution; other state writes remain best-effort.
	if err := appendRunState(sess, session.RunStateRecord{
		Kind:      "state",
		SessionID: run.SessionID,
		RunID:     run.ID,
		State:     string(RunQueued),
		At:        time.Now(),
	}); err != nil {
		rt.releaseRunReservation(run)
		<-m.pending // ③ 预留的令牌原路归还(锁外,同 ②/⑥ 的拒绝路径)
		return RunHandle{}, fmt.Errorf("agent: start run %s in session %s: %w: %w",
			run.ID, sessionID, ErrRunJournalWrite, err)
	}

	// Begin the event scope after the queued record; it only sets curRunID and flushes
	// the previous run's delta buffer, so the two journals remain independent.
	if hub != nil {
		hub.BeginRun(run.ID)
	}

	// Derive runCtx from the host context and store cancel before returning, so queued
	// runs are cancellable immediately. Run context values are read-only here.
	runCtx := m.contextOf()
	for _, kv := range m.runCtxValues {
		runCtx = context.WithValue(runCtx, kv.Key, kv.Value)
	}
	runCtx, cancel := context.WithCancel(runCtx)
	run.mu.Lock()
	run.cancel = cancel
	run.mu.Unlock()

	// Install the cross-connection approval fallback before wrapping callbacks in the tee.
	// Caller-supplied Decide is left untouched; cancellation converges through runCtx.
	if cb.Decide == nil {
		cb.Decide = m.approvals.decideFunc(runCtx, sess, run, sessionID, hub)
	}

	// Wrap callbacks after the approval fallback; the tee supplies token/publish collectors
	// even for nil callbacks so background runs still produce events.
	cb = teeRunCallbacks(cb, hub, run.ID)

	// Transfer callback ownership to the execute goroutine and return immediately. The
	// accepted state is captured in the synchronous section, where it is deterministically queued.
	submitState := run.snapshot().State

	go m.execute(rt, run, runCtx, cancel, req, cb, hub)
	return RunHandle{SessionID: sessionID, RunID: run.ID, run: run, submitState: submitState}, nil
}

// execute runs one background Run on a dedicated goroutine: acquire gate, then runSlots,
// dequeue, mark running, run the session, close events, and finalize. Every exit releases
// pending/gate/runSlots/active exactly once; a nil hub disables all event work.
func (m *SessionManager) execute(rt *SessionRuntime, run *AgentRun, runCtx context.Context, cancel context.CancelFunc, req RunRequest, cb RunCallbacks, hub *EventHub) {
	defer cancel()

	// 事件收口兜底:早退路径(①/② 失败)从未产生事件,EndRun 是幂等
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
		// run.finished{cancelled}(仅 hub 非 nil;execute ① 早退出口)——
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
		// run.finished{cancelled}(同 ① 早退出口的次序铁律)。
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
	// 于 journal;失败 warn 不阻塞)。run.started{state:"running"}(仅 hub
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

	// Wrap the per-run ToolSource to inject capability-wait observers into proxy tools.
	// This only applies to background runs; the source must be built fresh per run, since
	// repeated wrapping would stack stale observers.
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

	// ai-core may return completed after context cancellation, so runCtx is authoritative:
	// override canceled results to RunCancelled while preserving Err and Persisted.
	res, _ := rt.Session().Run(runCtx, req, cb)
	if rerr := runCtx.Err(); rerr != nil {
		err := res.Err
		if err == nil {
			err = rerr
		}
		res = RunResult{State: RunCancelled, Err: err, Persisted: res.Persisted}
	}

	// Publish run lifecycle key events between execution and event teardown, before the
	// terminal state record evicts the run journal. Failures are logged, never fatal.
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

	// Close the run's event scope before finalization: AppendState evicts the journal handle,
	// after which flushed deltas would fail with ErrRunExists. The early defer cannot replace
	// this explicit call; flush failures are logged and do not block finalization.
	if hub != nil {
		hub.EndRun()
	}

	// ⑥ 终态化:finalizeRun 内部先 journal 终态(durable-first)再关闭 done——
	// 等待者被唤醒时终态记录已持久。
	finalizeRun(rt, run, res)

	// ⑦ runSlots/gate 由 defer 释放(LIFO:runSlots → gate)。
}

// teeRunCallbacks maps token, middleware and approval callbacks into hub events, publishing
// before forwarding any original callback. Token/Publish collectors are supplied even when
// nil; Decide is wrapped only when configured (the approval fallback is installed earlier).
// Calls remain synchronous on execute's goroutine; hub locks do not cover callback waits.
func teeRunCallbacks(cb RunCallbacks, hub *EventHub, runID string) RunCallbacks {
	if hub == nil {
		return cb // RunEvents 未启用:原样透传,零开销路径
	}
	teed := cb
	// EmitToken:nil → 纯事件映射版(daemon 全 nil 提交形态);非 nil →
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
	// Decide 面不变(见函数头):包装只在调用前后各发一条事件。
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
	// Publish:nil → 纯映射版;非 nil → 包装。映射规则见
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

// teePublishCallbackEvent maps usage and iteration events to usage.progress deltas; other
// names are outside the event contract and skipped.
func teePublishCallbackEvent(hub *EventHub, runID, event string, data any) {
	switch event {
	case runtimeconfig.EventUsage, runtimeconfig.EventIteration:
		teePublish(hub, runID, session.EventUsageProgress, data)
	}
}

// teeToken publishes a token as a per-token event with the full token JSON payload:
// tool calls/results map to key events, text Done finalizes a message, thinking/text
// deltas map to their delta events, and runtime events are skipped (Decide handles HITL).
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

// teePublish marshals and publishes an event. On marshal failure, key events retain their
// boundary with a null payload; deltas are skipped. Publish failures are logged, not fatal.
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

// emitRunEvent publishes a durable-first run lifecycle key event; nil hubs are a no-op.
// Marshal/Publish failures are logged without changing Run finalization.
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
