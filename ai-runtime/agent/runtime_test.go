package agent

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSessionRuntimeExclusiveRun:同一 runtime 的独占执行权——A 持 gate 时
// B 阻塞,B ctx 取消后立即返回 ctx.Err(),A release 后 B 成功
// (对应 daemon/internal/plugins/agent/gate_test.go:112 的语义,搬到 SDK 层)。
func TestSessionRuntimeExclusiveRun(t *testing.T) {
	rt := NewSessionRuntime("s1", nil)

	relA, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	acquired := make(chan error, 1)
	go func() {
		rel, err := rt.AcquireRun(ctx)
		if err == nil {
			rel() // 防御:不该发生,若 gate 泄漏也照常归还
		}
		acquired <- err
	}()
	time.Sleep(20 * time.Millisecond) // 给排队者时间进入阻塞

	select {
	case err := <-acquired:
		t.Fatalf("second acquire returned while gate held: %v", err)
	default:
	}

	cancel()
	select {
	case err := <-acquired:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled acquire error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled acquirer did not return")
	}

	relA()

	relB, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	relB()

	// release 恰好一次后 gate 应已归还:再次获取立即可得。
	relC, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("re-acquire after归还: %v", err)
	}
	relC()
}

// TestSessionRuntimeSessionBinding:session 允许 nil 构造 + BindSession 后置
// 绑定/解绑;Active 在无运行记账时为 nil。
func TestSessionRuntimeSessionBinding(t *testing.T) {
	rt := NewSessionRuntime("s1", nil)
	if got := rt.Session(); got != nil {
		t.Fatalf("fresh runtime session = %v, want nil", got)
	}
	if got := rt.Active(); got != nil {
		t.Fatalf("fresh runtime active = %v, want nil", got)
	}

	s := NewSession(SessionDeps{}) // 纯内存会话(store 为 nil)
	rt.BindSession(s)
	if got := rt.Session(); got != s {
		t.Fatal("BindSession did not bind")
	}
	rt.BindSession(nil)
	if got := rt.Session(); got != nil {
		t.Fatal("BindSession(nil) did not unbind")
	}
}

// TestAgentRunTransitions:初始 RunQueued;RunRunning 盖 startedAt;终态关闭
// done(幂等);snapshot 汇总 state 与 result。
func TestAgentRunTransitions(t *testing.T) {
	run := newAgentRun("run-1", "s1")
	if got := run.snapshot().State; got != RunQueued {
		t.Fatalf("initial state = %q, want %q", got, RunQueued)
	}
	select {
	case <-run.done:
		t.Fatal("done closed before terminal state")
	default:
	}

	run.transition(RunRunning)
	if got := run.snapshot().State; got != RunRunning {
		t.Fatalf("state after transition = %q, want %q", got, RunRunning)
	}
	if run.startedAt.IsZero() {
		t.Fatal("startedAt not stamped on RunRunning")
	}

	run.result = RunResult{Err: errors.New("boom")} // 终态前记录 result(包内直写)
	run.transition(RunFailed)
	select {
	case <-run.done:
	default:
		t.Fatal("done not closed on terminal state")
	}
	snap := run.snapshot()
	if snap.State != RunFailed || snap.Err == nil || snap.Persisted {
		t.Fatalf("snapshot = %+v, want state=RunFailed with err, not persisted", snap)
	}

	run.transition(RunFailed) // 重复终态不得重复关闭 done(panic 即失败)
}

// TestRunDoneHookFiresAfterTerminal(Plan A2 生命周期半边/A7 设计点 2):run 终态
// 通知面——WithRunDoneHook 声明的回调经 Register 注入 runtime,finalizeRun 在
// 清 active 后以 runtime id 触发恰好一次;触发点不变量:此刻 active 已清且
// run.done 已关(等待面已放行),宿主的延迟收口据此安全重判。同步(RecordRun)
// 与后台(StartRun)形态共享 finalizeRun,本测试覆盖同步形态的触发契约。
func TestRunDoneHookFiresAfterTerminal(t *testing.T) {
	hookCalls := make(chan string, 4)
	m := NewSessionManager(WithRunDoneHook(func(sessionID string) {
		hookCalls <- sessionID
	}))
	defer m.Close()

	rt := NewSessionRuntime("s-hook", NewSession(SessionDeps{}))
	if err := m.Register(rt); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var observedRun *AgentRun
	relGate, err := rt.AcquireRun(context.Background()) // RecordRun 约定调用方已持 gate
	if err != nil {
		t.Fatalf("AcquireRun: %v", err)
	}
	res := rt.RecordRun(func(*Session) (RunResult, error) {
		observedRun = rt.Active() // 执行中的 active 记账,供终态后对照
		return RunResult{State: RunCompleted}, nil
	})
	relGate()
	if res.State != RunCompleted {
		t.Fatalf("RecordRun state = %q, want completed", res.State)
	}

	select {
	case got := <-hookCalls:
		if got != "s-hook" {
			t.Fatalf("hook sessionID = %q, want s-hook", got)
		}
	case <-time.After(time.Second):
		t.Fatal("run done hook did not fire")
	}

	// 触发点不变量:active 已清、done 已关——宿主在回调里看到的是「无在飞
	// run」的干净终态,重判引用计数不会误判在飞。
	if got := rt.Active(); got != nil {
		t.Fatalf("Active() inside/after hook window = %v, want nil (cleared before hook)", got)
	}
	if observedRun == nil {
		t.Fatal("setup: active was not set during RecordRun")
	}
	select {
	case <-observedRun.done:
	default:
		t.Fatal("run.done not closed when hook fired (ordering invariant broken)")
	}

	// 无 hook 的 runtime(直接构造、不经 Register)零行为变化:finalizeRun 照常
	// 完成、不 panic(hook 为 nil 的既有路径回归)。
	bare := NewSessionRuntime("s-bare", NewSession(SessionDeps{}))
	relBare, err := bare.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("bare AcquireRun: %v", err)
	}
	if res := bare.RecordRun(func(*Session) (RunResult, error) {
		return RunResult{State: RunCompleted}, nil
	}); res.State != RunCompleted {
		t.Fatalf("bare RecordRun state = %q, want completed", res.State)
	}
	relBare()
}
