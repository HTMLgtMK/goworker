package service

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// syncrun_test.go 是 PR4(总方案 §九「同步与后台 Run 最终统一」)的插件侧验收:
// 同步 prompt 入口(handleAgent → StartRunSync → manager.StartRun)与后台路径
// (xgw run/start)进入同一套门禁与记账。复用 runcontrol_test.go 的 gatedProvider
// (开工/放行屏障代替计时)与 runjournal_wiring_test.go 的 readRunJournalFile。
//
//   - TestSyncRunSurvivesSubmitterExit:TestRunSurvivesDisconnect 的 daemon 委托
//     层等价物——提交 API 无 ctx 参数,runCtx 来自宿主;提交方 goroutine 不 Wait、
//     直接退出,Run 照常跑完并持久化;RunStatus 对在跑/已终态 run 的观测面可用;
//   - TestHandleAgentCallerCtxCancelCancelsRun:Legacy 取消语义——调用方请求 ctx
//     取消经 Adapter 显式 CancelRunSync 落地(不依赖请求 ctx 传播),prompt 返回
//     时终态已落盘(queued→running→cancelled),「prompt 返回 = run 完成」;
//   - TestHandleAgentBusyRejectedWhileRunInFlight:统一门禁——同会话第二提交
//     (同步 prompt 与 xgw 后台提交)在飞期间均被 ErrRunActive 拒绝,释放后后台
//     提交恢复(xgw 后台 Run 能力不受影响)。

// readStateRecords 读一个 run journal 文件并只保留 kind=="state" 的记录
// (事件记录与 state 记录同文件混存,journal_store.go P3-1)。
func readStateRecords(t *testing.T, path string) []session.RunStateRecord {
	t.Helper()
	var states []session.RunStateRecord
	for _, rec := range readRunJournalFile(t, path) {
		if rec.Kind == "state" {
			states = append(states, rec)
		}
	}
	return states
}

// waitActiveCleared 等待 runtime 的 active 记账清空(finalizeRun 在 done 关闭后
// 才清 active,等待面返回与记账清退之间是微秒级窗口;轮询至多 2s,屏障代替计时)。
func waitActiveCleared(t *testing.T, p *AgentPlugin) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if p.currentSession().Active() == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("active run not cleared after terminal state")
		}
		time.Sleep(time.Millisecond)
	}
}

// waitRunStatusCompleted 轮询 RunStatus 直到 run 报告 completed(journal 权威分支)。
func waitRunStatusCompleted(t *testing.T, p *AgentPlugin, sid, runID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, latest, integrity, err := p.RunStatus(sid, runID)
		if err == nil && state == string(runtimeagent.RunCompleted) {
			if latest == 0 {
				t.Fatal("RunStatus(completed) latest seq = 0, want > 0 (run journal events)")
			}
			if integrity != runtimeagent.ReplayIntegrityComplete {
				t.Fatalf("RunStatus integrity = %q, want complete", integrity)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("RunStatus(%s) never reported completed (last state=%q err=%v)", runID, state, err)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestSyncRunSurvivesSubmitterExit:提交方 goroutine 在 StartRunSync 返回后直接
// 退出、不 Wait(模拟断连——StartRunSync 无 ctx 参数,API 形状即保证调用方 ctx
// 不成 run 父,宿主 ctx 是 runCtx 的唯一父);Run 照常执行至终态并持久化,
// RunStatus 的观测面(active 分支 → journal 权威分支)全程可用。
func TestSyncRunSurvivesSubmitterExit(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()

	out := make(chan runtimeagent.RunHandle, 1)
	go func() {
		h, err := p.StartRunSync(sid, runtimeagent.RunRequest{Input: "q-sync"}, runtimeagent.RunCallbacks{
			Write: func(string) {},
		})
		if err != nil {
			t.Errorf("StartRunSync: %v", err)
			out <- runtimeagent.RunHandle{}
			return
		}
		out <- h
		// 提交方到此为止:不 Wait、不再触碰 cb(契约①)。
	}()
	var h runtimeagent.RunHandle
	select {
	case h = <-out:
	case <-time.After(5 * time.Second):
		t.Fatal("StartRunSync did not return in time")
	}
	if h.RunID == "" {
		t.Fatal("StartRunSync returned empty run id")
	}

	<-gated.started // run 真实开工(execute 已推进 running)

	// 在跑观测面:RunStatus 命中 active 分支 → running;事件流已建立(latest>0)。
	state, latest, integrity, err := p.RunStatus(sid, h.RunID)
	if err != nil || state != string(runtimeagent.RunRunning) {
		t.Fatalf("RunStatus(in-flight) = %q, %v, want running", state, err)
	}
	if latest == 0 {
		t.Fatal("RunStatus(in-flight) latest seq = 0, want > 0 (run.started event)")
	}
	if integrity != runtimeagent.ReplayIntegrityComplete {
		t.Fatalf("RunStatus integrity = %q, want complete", integrity)
	}

	close(gated.release)
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := h.Wait(waitCtx)
	if err != nil {
		t.Fatalf("Wait: %v (state=%s)", err, h.State())
	}
	if res.State != runtimeagent.RunCompleted || !res.Persisted {
		t.Fatalf("run result = %+v, want RunCompleted+Persisted=true", res)
	}

	// 已终态观测面:active 已清 → journal 权威分支(被路由会话自己的事实源)。
	waitRunStatusCompleted(t, p, sid, h.RunID)

	// journal 记账:state 记录序 queued → running → completed(与后台 Run 同型)。
	runsDir := filepath.Join(p.cfg.Session.Dir, sid, "runs")
	states := readStateRecords(t, filepath.Join(runsDir, h.RunID+".jsonl"))
	if len(states) != 3 ||
		states[0].State != string(runtimeagent.RunQueued) ||
		states[1].State != string(runtimeagent.RunRunning) ||
		states[2].State != string(runtimeagent.RunCompleted) {
		t.Fatalf("state records = %+v, want [queued running completed]", states)
	}
	if !states[2].Persisted {
		t.Fatalf("completed record Persisted = false, want true")
	}
}

// TestHandleAgentCallerCtxCancelCancelsRun:CLI 的「请求 ctx 取消 → 终止 Agent」
// Legacy 语义(总方案 §九 Adapter 形态)——StartRun 的 runCtx 来自宿主,取消必须
// 经 handleAgent 的等待面显式 CancelRunSync;prompt 返回时 run 已终态且 journal
// 落盘(「prompt 返回 = run 完成」在取消路径成立)。
func TestHandleAgentCallerCtxCancelCancelsRun(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mctx := &model.Context{
		Ctx:  reqCtx,
		Args: []string{"hello"},
		FrontendContext: model.FrontendContext{
			Writer: func(string) {},
		},
	}
	done := make(chan error, 1)
	go func() { done <- p.handleAgent(mctx) }()

	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("run never started")
	}
	run := p.currentSession().Active()
	if run == nil {
		t.Fatal("no active run while prompt in flight")
	}
	runID := run.ID

	// 调用方取消(模拟 CLI Ctrl-C 的 agentCancel):prompt 必须返回,run 收敛取消。
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleAgent(cancel): %v, want nil (错误面与迁移前同型)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handleAgent did not return after caller ctx cancel")
	}

	// journal:state 记录序 queued → running → cancelled(统一记账面 + 取消终态;
	// 取消不是失败,P2-1 归一)。
	runsDir := filepath.Join(p.cfg.Session.Dir, sid, "runs")
	states := readStateRecords(t, filepath.Join(runsDir, runID+".jsonl"))
	if len(states) != 3 ||
		states[0].State != string(runtimeagent.RunQueued) ||
		states[1].State != string(runtimeagent.RunRunning) ||
		states[2].State != string(runtimeagent.RunCancelled) {
		t.Fatalf("state records = %+v, want [queued running cancelled]", states)
	}
}

// TestHandleAgentBusyRejectedWhileRunInFlight:统一门禁(§二「每个 Session 至多
// 一个活动 Run」)——同步 run 在飞期间,同会话的第二同步 prompt 与 xgw 后台提交
// 均被 ErrRunActive 拒绝(同一 active 占位,不是两套门禁);释放后后台提交恢复,
// xgw 后台 Run 能力不受 PR4 迁移影响。
func TestHandleAgentBusyRejectedWhileRunInFlight(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()

	done := make(chan error, 1)
	go func() {
		done <- p.handleAgent(&model.Context{
			Ctx:             context.Background(),
			Args:            []string{"first"},
			FrontendContext: model.FrontendContext{Writer: func(string) {}},
		})
	}()
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("run never started")
	}

	// 同会话第二同步 prompt:统一门禁拒绝(active 占位)。
	err := p.handleAgent(&model.Context{
		Ctx:             context.Background(),
		Args:            []string{"second"},
		FrontendContext: model.FrontendContext{Writer: func(string) {}},
	})
	if !errors.Is(err, runtimeagent.ErrRunActive) {
		t.Fatalf("second prompt err = %v, want ErrRunActive", err)
	}

	// xgw 后台提交(持 lease):同一门禁,同样拒绝。
	p.AcquireControl(sid, "conn-x")
	_, _, err = p.StartRun(sid, "conn-x", "bg", "req-busy")
	if !errors.Is(err, runtimeagent.ErrRunActive) {
		t.Fatalf("xgw StartRun while sync run in flight = %v, want ErrRunActive", err)
	}

	close(gated.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first prompt: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first prompt did not finish after release")
	}
	waitActiveCleared(t, p)

	// 释放后 xgw 后台提交恢复:queued 接受,随后跑完(后台 Run 不受影响)。
	runID, state, err := p.StartRun(sid, "conn-x", "bg2", "req-busy-2")
	if err != nil {
		t.Fatalf("xgw StartRun after release: %v", err)
	}
	if state != string(runtimeagent.RunQueued) {
		t.Fatalf("xgw StartRun state = %q, want queued", state)
	}
	waitRunStatusCompleted(t, p, sid, runID)
}

// ─────────────────────────────────────────────────────────────────────────────
// 第四轮评审 R1/R2 修复验收:Legacy 取消契约——CancelRunSync 结果分类 + 有界
// 收尾等待。同步屏障/确定性编排,不用 time.Sleep 对齐:CancelRunSync 的失败
// 注入经 SessionRouter 窄接口的测试替身(stubRouter,agentcontrol.Service 持
// 接口、装配点 p.control 可换装)与 provider 闸门完成;换装 p.control 与
// handleAgent 的读取之间经 gated.started / reqCtx 取消的 channel happens-before
// 链同步(go execute → close(started) → 测试换装 → cancel() → close(done) →
// Wait 返回),race 检测下无数据竞争。
// ─────────────────────────────────────────────────────────────────────────────

// stubRouter 是 CancelRunSync 失败注入的确定性接缝(SessionRouter 单方法窄接口
// 的测试替身):包裹真实 router(插件自任),按模式改写 RouteSession 返回——
//   - miss 模式(allMiss):恒返 nil → CancelRunSync 路由失败,返回包裹
//     ErrSessionNotFound 的非预期错误(handleAgent 分类表的 default 分支);
//   - barrier 模式(proceed 非 nil):首个调用关闭 entered 并挂起在 proceed 上,
//     给测试一个「handleAgent 已进 CancelRunSync、尚未读 rt.Active()」的确定性
//     时序窗——测试在该窗内让 run 收敛终态后放行,manager.CancelRun 对已终态
//     Run 必报 ErrRunNotFound(竞争落败分支);close(proceed) 后透传真实 router。
type stubRouter struct {
	real    agentcontrol.SessionRouter
	allMiss bool
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (r *stubRouter) RouteSession(sessionID string) *runtimeagent.SessionRuntime {
	if r.proceed != nil {
		r.once.Do(func() { close(r.entered) })
		<-r.proceed
	}
	if r.allMiss {
		return nil
	}
	return r.real.RouteSession(sessionID)
}

// deafProvider 是非协作式 Provider 的测试替身(R1):与 gatedProvider 同形的
// 闸门,但 Chat 只等 release、**不 select ctx.Done**——模拟「执行卡在不支持
// 取消的外部调用中」:CancelRunSync 成功发出取消(runCtx 已就绪)后 run 依旧
// 不收敛。测试收尾 close(release) 放行,execute 层按 runCtx 归一取消终态。
type deafProvider struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *deafProvider) Name() string  { return "deaf" }
func (p *deafProvider) Model() string { return "m" }

func (p *deafProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	p.once.Do(func() { close(p.started) })
	<-p.release // 无视 ctx:取消后依旧阻塞(非协作式,不支持有界取消)
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "deaf"}}}}, nil
}

func (p *deafProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// waitActiveClearedOn 等待指定 runtime 的 active 记账清空(finalizeRun 先关 done
// 再清 active,active 清空 ⇒ 终态已可读;轮询至多 5s,屏障代替计时)。test ②/③
// 换装 stubRouter 后 RunStatus 的路由面被改写,终态等待直接观测 runtime 记账。
func waitActiveClearedOn(t *testing.T, rt *runtimeagent.SessionRuntime) {
	t.Helper()
	if rt == nil {
		t.Fatal("waitActiveClearedOn: nil runtime")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if rt.Active() == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("active run not cleared after terminal state")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestHandleAgentCancelBoundedWaitReturnsCancellationPending(R1):取消成功发出
// 但 run 不收敛(非协作式 Provider 无视 runCtx)→ prompt 在
// cancelCleanupTimeout+余量内以 cancellation_pending 错误返回,而非永久挂起;
// 返回时 run 仍在跑(诚实性:不伪装已完成),放行后后台照常收敛取消终态并落盘
// ——客户端可经 RunStatus(xgw run/get)查询真实终态。
func TestHandleAgentCancelBoundedWaitReturnsCancellationPending(t *testing.T) {
	deaf := &deafProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, deaf)
	sid := p.CurrentSessionID()

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- p.handleAgent(&model.Context{
			Ctx:             reqCtx,
			Args:            []string{"hello"},
			FrontendContext: model.FrontendContext{Writer: func(string) {}},
		})
	}()
	select {
	case <-deaf.started:
	case <-time.After(5 * time.Second):
		t.Fatal("run never started")
	}
	run := p.currentSession().Active()
	if run == nil {
		t.Fatal("no active run while prompt in flight")
	}
	runID := run.ID

	// 调用方取消:CancelRunSync 成功(取消已发出),但 provider 无视 runCtx——
	// 有界收尾等待必须超时并以 cancellation_pending 返回,绝不无限等待。
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("handleAgent returned nil while the run is still in flight (伪装完成,R1 禁止)")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handleAgent err = %v, want wrap of context.DeadlineExceeded", err)
		}
		if !strings.Contains(err.Error(), "cancellation pending") || !strings.Contains(err.Error(), runID) {
			t.Fatalf("handleAgent err = %v, want cancellation_pending error naming run %s", err, runID)
		}
	case <-time.After(cancelCleanupTimeout + 10*time.Second):
		t.Fatalf("handleAgent did not return within cancelCleanupTimeout+margin — unbounded-wait regression (R1)")
	}

	// 诚实性:prompt 返回时 run 仍未终态(provider 未放行,取消未收敛是事实)。
	if act := p.currentSession().Active(); act == nil {
		t.Fatal("run finalized before the provider was released; cancellation_pending must not be fabricated")
	}

	// 放行:provider 返回,execute 层按 runCtx 归一取消终态(后台收敛);
	// RunStatus(journal 权威)可查真实终态——取消早于 prompt 返回发出。
	close(deaf.release)
	waitForRunStatus(t, p, sid, runID, string(runtimeagent.RunCancelled))

	runsDir := filepath.Join(p.cfg.Session.Dir, sid, "runs")
	states := readStateRecords(t, filepath.Join(runsDir, runID+".jsonl"))
	if len(states) != 3 ||
		states[0].State != string(runtimeagent.RunQueued) ||
		states[1].State != string(runtimeagent.RunRunning) ||
		states[2].State != string(runtimeagent.RunCancelled) {
		t.Fatalf("state records = %+v, want [queued running cancelled]", states)
	}
	waitActiveCleared(t, p)
}

// TestHandleAgentCancelUnexpectedErrorPropagates(R2):CancelRunSync 返回非预期
// 失败(路由丢失 → 包裹 ErrSessionNotFound)→ handleAgent 不静默吞掉、不假设
// 取消已发出:显式上抛含 sessionID/runID/原因的错误;取消从未落地,放行后 run
// 自然完成(终态 completed 而非 cancelled)——证明失败未被当成「已取消」编造。
func TestHandleAgentCancelUnexpectedErrorPropagates(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- p.handleAgent(&model.Context{
			Ctx:             reqCtx,
			Args:            []string{"hello"},
			FrontendContext: model.FrontendContext{Writer: func(string) {}},
		})
	}()
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("run never started")
	}
	run := p.currentSession().Active()
	if run == nil {
		t.Fatal("no active run while prompt in flight")
	}
	runID := run.ID

	// 提交走真实 control 面,开工后换装 miss router:此后 CancelRunSync 的路由
	// 必失败(sessionNotFoundErr,R2 的非预期错误形态)。
	p.control = agentcontrol.NewService(p.manager, &stubRouter{real: p, allMiss: true}, p)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("handleAgent swallowed the unexpected cancel failure (R2: 非预期错误必须上抛)")
		}
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "cancellation pending") {
			t.Fatalf("handleAgent err = %v, want the cancel-failure error (非 cancellation_pending 语义)", err)
		}
		if !errors.Is(err, runtimeagent.ErrSessionNotFound) {
			t.Fatalf("handleAgent err = %v, want wrap of the routing failure (ErrSessionNotFound)", err)
		}
		if !strings.Contains(err.Error(), runID) || !strings.Contains(err.Error(), sid) {
			t.Fatalf("handleAgent err = %v, want sessionID(%s) and runID(%s) in the message", err, sid, runID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handleAgent did not return after the unexpected cancel failure")
	}

	// 取消从未落地(路由失败先于 manager.CancelRun):放行后 run 自然完成,
	// journal 终态是 completed 而非 cancelled——「未假设取消已发出」的事实证明。
	close(gated.release)
	waitActiveClearedOn(t, p.currentSession())
	runsDir := filepath.Join(p.cfg.Session.Dir, sid, "runs")
	states := readStateRecords(t, filepath.Join(runsDir, runID+".jsonl"))
	if len(states) != 3 ||
		states[0].State != string(runtimeagent.RunQueued) ||
		states[1].State != string(runtimeagent.RunRunning) ||
		states[2].State != string(runtimeagent.RunCompleted) {
		t.Fatalf("state records = %+v, want [queued running completed] (取消未落地,自然完成)", states)
	}
}

// TestHandleAgentCancelRunNotFoundRaceReadsRealTerminalState(R2 竞争分支):
// 取消与完成竞争落败——barrier router 把 handleAgent 挂在 CancelRunSync 的路由
// 点内,测试在该确定性窗口内让 run 自然收敛 completed 并等 active 清退,放行后
// manager.CancelRun 对已终态 Run 必报 ErrRunNotFound(幂等空操作)。断言:
// handleAgent 不报错、不虚构取消终态——收尾 Wait 读到真实终态 completed 并落盘,
// 「prompt 返回 = run 完成」与「CancelRun 竞争落败」交错同收敛。
func TestHandleAgentCancelRunNotFoundRaceReadsRealTerminalState(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- p.handleAgent(&model.Context{
			Ctx:             reqCtx,
			Args:            []string{"hello"},
			FrontendContext: model.FrontendContext{Writer: func(string) {}},
		})
	}()
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("run never started")
	}
	run := p.currentSession().Active()
	if run == nil {
		t.Fatal("no active run while prompt in flight")
	}
	runID := run.ID

	// barrier router:首个 RouteSession(即取消路径的路由点)挂起,给测试
	// 「Wait(reqCtx) 已因取消返回、CancelRunSync 尚未读 Active()」的时序窗。
	router := &stubRouter{real: p, entered: make(chan struct{}), proceed: make(chan struct{})}
	p.control = agentcontrol.NewService(p.manager, router, p)

	cancel() // Wait(reqCtx) → Canceled → CancelRunSync → RouteSession 挂起
	select {
	case <-router.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("CancelRunSync never reached the routing point")
	}

	// 窗口内让 run 自然完成(取消从未发出):completed 终态落盘、active 清退
	// (finalizeRun 先 run.finish 关 done 再清 active,active 清空 ⇒ 终态可读)。
	close(gated.release)
	waitActiveClearedOn(t, p.currentSession())

	// 放行:manager.CancelRun 对已终态 Run 报 ErrRunNotFound(竞争落败)→
	// handleAgent 按幂等空操作处理,收尾 Wait 立即返回真实终态。
	close(router.proceed)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleAgent(ErrRunNotFound race): %v, want nil (读真实终态,不虚构取消)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handleAgent did not return after the ErrRunNotFound race")
	}

	// 真实终态是 completed(不是虚构的 cancelled):journal 与 RunStatus 双源一致。
	runsDir := filepath.Join(p.cfg.Session.Dir, sid, "runs")
	states := readStateRecords(t, filepath.Join(runsDir, runID+".jsonl"))
	if len(states) != 3 ||
		states[0].State != string(runtimeagent.RunQueued) ||
		states[1].State != string(runtimeagent.RunRunning) ||
		states[2].State != string(runtimeagent.RunCompleted) {
		t.Fatalf("state records = %+v, want [queued running completed] (真实终态,非虚构取消)", states)
	}
	waitRunStatusCompleted(t, p, sid, runID)
	waitActiveCleared(t, p)
}
