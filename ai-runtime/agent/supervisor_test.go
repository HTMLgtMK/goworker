package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-sandbox"
)

// 本文件是 P2-5 的后台执行验收测试(总方案第十一节 → 本步),全部模拟 Provider
// (复用 gatedProvider/newFakeStoreSession/fakeConversationStore,run_lifecycle_test.go):
//   - TestRunSurvivesDisconnect:提交方 goroutine 不 Wait、直接返回(模拟断连),
//     后台 Run 不受影响跑完并持久化——宿主 ctx 是 runCtx 的父,调用方 ctx 不参与;
//   - TestStartRunQueuedOnGlobalSlot:全局槽占满时后续 StartRun 立即返回且保持
//     RunQueued(同步段零阻塞、排队发生在全局槽一级);
//   - TestStartRunPendingBounded:pending 有界,排队满即拒(ErrTooManyPending);
//   - TestStartRunSecondRunRejected:同会话第二 StartRun 被拒(ErrRunActive);
//   - TestStartRunJournalsQueuedBeforeReturn:StartRun 返回时 queued 记录已在
//     (durable-first,总方案第七节)。
//
// 并行性断言沿用 run_lifecycle_test.go 的「屏障代替计时」风格:唯一挂钟等待是
// waitStarted(开工宣告)与终态 Wait 的超时上界,其余为确定性断言。

// startOutcome 承载提交方 goroutine 的 StartRun 结果(提交方不 Wait 的形态)。
type startOutcome struct {
	h   RunHandle
	err error
}

// newSupRuntime 构造带 gatedProvider 的 runtime 并注册进 manager:starts[0] 在
// provider 开工时关闭;hang 为真时第 1 轮挂起至 release 通道关闭,否则 provider
// 被调用即返回(release 返回 nil,调用方不得关闭)。
func newSupRuntime(t *testing.T, m *SessionManager, id, reply string, hang bool) (st *fakeConversationStore, started, release chan struct{}) {
	t.Helper()
	started = make(chan struct{})
	release = make(chan struct{})
	pv := &gatedProvider{
		replies: []string{reply},
		starts:  []chan struct{}{started},
		release: []chan struct{}{release},
	}
	if !hang {
		pv.release = nil
		release = nil
	}
	st = &fakeConversationStore{}
	rt := NewSessionRuntime(id, newFakeStoreSession(st, nil, pv))
	if err := m.Register(rt); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
	return st, started, release
}

// waitHandle 等待句柄到达终态并断言 RunCompleted+Persisted=true。
func waitHandle(t *testing.T, h RunHandle) RunResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := h.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v (state=%s)", err, h.State())
	}
	if res.State != RunCompleted || !res.Persisted {
		t.Fatalf("run result = %+v, want RunCompleted+Persisted=true", res)
	}
	return res
}

// assertHostCtxAlive 断言 manager 宿主 ctx 未被取消(when 说明断言时机)。
func assertHostCtxAlive(t *testing.T, m *SessionManager, when string) {
	t.Helper()
	select {
	case <-m.contextOf().Done():
		t.Fatalf("manager host context canceled %s", when)
	default:
	}
}

// TestRunSurvivesDisconnect(总方案第十一节 → 本步):提交方 goroutine 在
// StartRun 返回后直接退出、不 Wait(模拟连接断开——StartRun 无 ctx 参数,API
// 形状即保证调用方 ctx 不成 run 父);provider 挂起期间宿主 ctx 未取消;release
// 后 Wait 得 RunCompleted+Persisted,store 提交完整。
func TestRunSurvivesDisconnect(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	st, started, release := newSupRuntime(t, m, "disc", "reply-A", true)

	// 提交方 goroutine:StartRun 返回即退出——不 Wait、不再触碰 cb(契约①)。
	out := make(chan startOutcome, 1)
	go func() {
		h, err := m.StartRun("disc", RunRequest{Input: "q-A"}, testRunCallbacks(&strings.Builder{}))
		out <- startOutcome{h: h, err: err}
	}()
	var h RunHandle
	select {
	case got := <-out:
		if got.err != nil {
			t.Fatalf("StartRun: %v", got.err)
		}
		h = got.h
	case <-time.After(5 * time.Second):
		t.Fatal("StartRun did not return in time")
	}
	if h.SessionID != "disc" || h.RunID == "" {
		t.Fatalf("handle = {SessionID:%q RunID:%q}, want session id + non-empty run id", h.SessionID, h.RunID)
	}

	waitStarted(t, started) // provider 挂起中:run 在跑,提交方 goroutine 已返回
	assertHostCtxAlive(t, m, "while the submitter is gone and the run is in flight")

	close(release) // 放行 provider → run 走到终态
	waitHandle(t, h)
	assertHostCtxAlive(t, m, "after the run completed")
	assertSingleCommit(t, st, "q-A", "reply-A")
}

// TestStartRunQueuedOnGlobalSlot:WithRunLimits(1, 8)——A 占住唯一全局槽挂起
// 中,B StartRun 立即返回(同步段零阻塞)且 State()==RunQueued(排队发生在全局
// 槽一级);release A 后 B 执行完成,两 fake store 各自提交完整。
func TestStartRunQueuedOnGlobalSlot(t *testing.T) {
	m := NewSessionManager(WithRunLimits(1, 8))
	defer m.Close()
	stA, startedA, releaseA := newSupRuntime(t, m, "session-A", "reply-A", true)
	stB, startedB, _ := newSupRuntime(t, m, "session-B", "reply-B", false)

	hA, err := m.StartRun("session-A", RunRequest{Input: "q-A"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun A: %v", err)
	}
	waitStarted(t, startedA) // A 占住唯一 runSlots 槽

	hB, err := m.StartRun("session-B", RunRequest{Input: "q-B"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun B while slot held: %v", err)
	}
	if got := hB.State(); got != RunQueued {
		t.Fatalf("B state right after StartRun = %q, want RunQueued (同步段零阻塞,排队在全局槽一级)", got)
	}
	// 确定性断言:槽容量 1 且被 A 持有,B 不得开工。
	select {
	case <-startedB:
		t.Fatal("B started while A still holds the only global slot")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseA) // 放行 A → B 获得槽执行
	waitHandle(t, hA)
	waitStarted(t, startedB)
	waitHandle(t, hB)

	assertSingleCommit(t, stA, "q-A", "reply-A")
	assertSingleCommit(t, stB, "q-B", "reply-B")
}

// TestStartRunPendingBounded:槽 1、pending 2——A running + B/C queued(不同
// 会话)后,第 4 个 StartRun 因排队满被拒(ErrTooManyPending,有界排队不无限
// 堆积);放行 A 后 B/C 依次完成。
func TestStartRunPendingBounded(t *testing.T) {
	m := NewSessionManager(WithRunLimits(1, 2))
	defer m.Close()
	_, startedA, releaseA := newSupRuntime(t, m, "s-A", "reply-A", true)
	_, startedB, _ := newSupRuntime(t, m, "s-B", "reply-B", false)
	_, startedC, _ := newSupRuntime(t, m, "s-C", "reply-C", false)
	newSupRuntime(t, m, "s-D", "reply-D", false)

	hA, err := m.StartRun("s-A", RunRequest{Input: "q-A"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun A: %v", err)
	}
	waitStarted(t, startedA)

	hB, err := m.StartRun("s-B", RunRequest{Input: "q-B"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun B: %v", err)
	}
	hC, err := m.StartRun("s-C", RunRequest{Input: "q-C"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun C: %v", err)
	}
	if got := hB.State(); got != RunQueued {
		t.Fatalf("B state = %q, want RunQueued", got)
	}
	if got := hC.State(); got != RunQueued {
		t.Fatalf("C state = %q, want RunQueued", got)
	}

	// pending 容量 2 已被 B/C 的预留占满(预留发生在 StartRun 同步段):第 4 个
	// 提交立即拒绝。
	if _, err := m.StartRun("s-D", RunRequest{Input: "q-D"}, testRunCallbacks(&strings.Builder{})); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("4th StartRun = %v, want ErrTooManyPending", err)
	}
	// 被拒的 D 不得占位/开工(拒绝发生在记账占位之前)。
	if rt := m.Get("s-D"); rt.Active() != nil {
		t.Fatalf("rejected StartRun left active accounting on s-D: %+v", rt.Active())
	}

	close(releaseA)
	waitHandle(t, hA)
	waitStarted(t, startedB)
	waitHandle(t, hB)
	waitStarted(t, startedC)
	waitHandle(t, hC)
}

// TestStartRunSecondRunRejected:同会话第二 StartRun 被拒(ErrRunActive)——
// 会话级不排队,排队只发生在全局槽一级(总方案第四节)。
func TestStartRunSecondRunRejected(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	_, started, release := newSupRuntime(t, m, "solo", "reply", true)

	h1, err := m.StartRun("solo", RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("first StartRun: %v", err)
	}
	waitStarted(t, started)

	if _, err := m.StartRun("solo", RunRequest{Input: "q2"}, testRunCallbacks(&strings.Builder{})); !errors.Is(err, ErrRunActive) {
		t.Fatalf("second StartRun = %v, want ErrRunActive", err)
	}

	close(release)
	waitHandle(t, h1)
}

// TestStartRunJournalsQueuedBeforeReturn:sink stub 断言 StartRun 返回时 queued
// 记录已在(durable-first:「run/start 响应在 Run 被接受并持久化登记后立即返回」,
// 总方案第七节);完成后记录流为 queued → running → completed,RunID 同源。
func TestStartRunJournalsQueuedBeforeReturn(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	jr := &stubRunJournal{}
	pv := &gatedProvider{replies: []string{"reply"}} // 不挂起、不宣告开工
	s := NewSession(SessionDeps{
		Config:   runtimeconfig.Default(),
		AuditDir: "",
		CollectTools: func(*sandbox.Config) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store:      &fakeConversationStore{},
		RunJournal: jr,
	})
	rt := NewSessionRuntime("jq", s)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}

	h, err := m.StartRun("jq", RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// StartRun 已返回:queued 记录必须在(同步段写,先于 go execute)。
	recs := jr.snapshot()
	if len(recs) < 1 {
		t.Fatal("no journal record at StartRun return, want queued record (durable-first)")
	}
	first := recs[0]
	if first.Kind != "state" || first.State != string(RunQueued) {
		t.Fatalf("first record = {kind:%q state:%q}, want kind=state state=queued", first.Kind, first.State)
	}
	if first.RunID != h.RunID || first.SessionID != "jq" {
		t.Fatalf("first record = {run:%q session:%q}, want run=%q session=jq", first.RunID, first.SessionID, h.RunID)
	}

	waitHandle(t, h)

	recs = jr.snapshot()
	wantStates := []string{string(RunQueued), string(RunRunning), string(RunCompleted)}
	if len(recs) != len(wantStates) {
		t.Fatalf("journal records = %d, want %d (queued/running/completed)", len(recs), len(wantStates))
	}
	for i, rec := range recs {
		if rec.RunID != h.RunID {
			t.Fatalf("record %d RunID = %q, want %q", i, rec.RunID, h.RunID)
		}
		if rec.State != wantStates[i] {
			t.Fatalf("record %d state = %q, want %q", i, rec.State, wantStates[i])
		}
	}
}
