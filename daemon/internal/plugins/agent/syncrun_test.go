package service

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
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
