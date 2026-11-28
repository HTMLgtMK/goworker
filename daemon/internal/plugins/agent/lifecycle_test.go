package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// lifecycle_test.go 验证引用归零时的会话收口、run 在途时延迟收口，以及新增绑定
// 对挂起收口的取消。多连接存活和绑定记账分别由 binding_test.go 与 agentcontrol
// 测试覆盖。挂起 run 通过 runtime active 状态直接构造，避免依赖 provider 时序。

// waitActive 等待 rt 的 active 记账就位(RecordRun 在独立 goroutine 上推进)。
func waitActive(t *testing.T, rt *runtimeagent.SessionRuntime) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rt.Active() != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("run did not occupy rt.active in time")
}

// newBoundSession 复用 newPersistedPlugin 的装配（真实 NewPlugin/Init、manager
// 带 WithRunDoneHook、control 已接线），再经 newPersistedSession 建一个独立目录
// 新会话并登记连接绑定，返回插件与会话 id。不直接用 Init 的首会话，因为
// refreshSession 的同 id 重建会在 manager 里留下 current 不在册的旧条目，那是
// 纯内存降级夹具，与本文件断言的持久化拓扑无关。
func newBoundSession(t *testing.T, connID string) (*AgentPlugin, string) {
	t.Helper()
	p := newPersistedPlugin(t, fixedProvider{content: "x"})
	sid := newPersistedSession(t, p, fixedProvider{content: "x"})
	if rt := p.manager.Get(sid); rt == nil || rt != p.currentSession() {
		t.Fatal("setup: current runtime must be the registered manager entry")
	}
	p.BindConnectionSessionReference(connID, sid)
	if got := p.control.SessionBindings(sid); len(got) != 1 || got[0] != connID {
		t.Fatalf("setup: bindings(%s) = %v, want [%s]", sid, got, connID)
	}
	return p, sid
}

// TestDisconnectReleasesSessionWhenLastBindingDrops verifies that disconnecting the
// final bound connection unregisters the runtime and releases its store cache entry.
func TestDisconnectReleasesSessionWhenLastBindingDrops(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")

	// 旧会话先落一条历史,重开断言数据不丢。
	if st := p.currentStore(); st == nil {
		t.Fatal("setup: no current store")
	} else if _, err := st.Commit([]core.Message{{Role: "user", Content: "旧会话历史", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("setup commit: %v", err)
	}

	// Current session has an implicit live reference; move current away before
	// releasing the final explicit connection binding.
	sidB := newPersistedSession(t, p, fixedProvider{content: "x"})
	if err := p.swapCurrentRuntime(p.manager.Get(sidB)); err != nil {
		t.Fatalf("select replacement current session: %v", err)
	}
	p.ReleaseConnection("conn-1")
	if p.manager.Get(sidA) != nil {
		t.Fatal("session A runtime still registered after final connection release")
	}
	if got := p.control.SessionBindings(sidA); len(got) != 0 {
		t.Fatalf("bindings after disconnect = %v, want empty", got)
	}

	// 重开:flock 已释放,慢路径拿活实例(A2.1①),旧历史可读、可继续写。
	st, err := p.repo.Open(sidA)
	if err != nil {
		t.Fatalf("repo.Open(released session): %v", err)
	}
	view := st.ActiveView()
	if len(view) != 1 || view[0].Content != "旧会话历史" {
		t.Fatalf("reopened view = %d msgs, want the seeded history (数据不丢)", len(view))
	}
	if _, err := st.Commit([]core.Message{{Role: "user", Content: "reopened", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit on reopened store: %v", err)
	}
}

// TestRunInFlightDelaysReleaseUntilTerminal(run 在飞解绑 → 延迟到 run 终态才收口,
// 定案需求 #8 的收口时点半边):引用归零时有 run 在飞 → 挂起(runtime/store 保留,
// A3 的 state=active 语义依赖此刻仍可观测);run 终态通知(WithRunDoneHook →
// onSessionRunDone)触发重判 → 收口。
func TestRunInFlightDelaysReleaseUntilTerminal(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")
	// current 换到 B:被评估的 A 不是 current(否则触发 current 隐式引用守卫,
	// 那是 CLI 形态的保护,与本场景无关)。
	sidB := newPersistedSession(t, p, fixedProvider{content: "x"})
	if sidB == sidA {
		t.Fatal("setup: distinct sessions required")
	}

	rt := p.manager.Get(sidA)
	if rt == nil {
		t.Fatal("setup: session A runtime missing")
	}
	// 在飞 run:RecordRun 阻塞在 fn(测试持 gate,fn 释放后终态化触发 hook)。
	gate, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("AcquireRun: %v", err)
	}
	block := make(chan struct{})
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		rt.RecordRun(func(*runtimeagent.Session) (runtimeagent.RunResult, error) {
			<-block
			return runtimeagent.RunResult{State: runtimeagent.RunCompleted}, nil
		})
	}()
	waitActive(t, rt)

	// 解绑(断连同款):引用归零但 run 在飞 → 挂起,不收口。
	p.ReleaseConnection("conn-1")
	if p.manager.Get(sidA) == nil {
		t.Fatal("session A must stay registered while its run is in flight (挂起收口)")
	}

	// run 终态:finalizeRun 清 active 后触发 hook → 重判 → 收口;hook 在
	// RecordRun 返回前同步执行,runDone 关闭即收口已完成。
	close(block)
	<-runDone
	gate()
	if p.manager.Get(sidA) != nil {
		t.Fatal("session A must be released after its run reached terminal state (延迟收口)")
	}
	// store 数据保留:重开活实例。
	if _, err := p.repo.Open(sidA); err != nil {
		t.Fatalf("repo.Open after delayed release: %v", err)
	}
}

// TestSuspendedReleaseCancelledByRebind(挂起期间的新绑定 → 取消挂起收口):
// 收口执行点的原子复查——run 终态触发的重判在执行点读最新绑定集合,挂起期间
// session/load 重开把连接绑回该会话(BindConnectionSession 登记)时,重判读到
// 非空集合,挂起收口被取消,绝不关闭仍有引用的会话。
func TestSuspendedReleaseCancelledByRebind(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")
	sidB := newPersistedSession(t, p, fixedProvider{content: "x"})
	if sidB == sidA {
		t.Fatal("setup: distinct sessions required")
	}

	rt := p.manager.Get(sidA)
	gate, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("AcquireRun: %v", err)
	}
	block := make(chan struct{})
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		rt.RecordRun(func(*runtimeagent.Session) (runtimeagent.RunResult, error) {
			<-block
			return runtimeagent.RunResult{State: runtimeagent.RunCompleted}, nil
		})
	}()
	waitActive(t, rt)

	// 解绑 → 挂起;挂起期间重开路径把新连接绑回该会话。
	p.ReleaseConnection("conn-1")
	if p.manager.Get(sidA) == nil {
		t.Fatal("session A must stay registered while its run is in flight (挂起收口)")
	}
	p.BindConnectionSessionReference("conn-2", sidA)

	// run 终态 → 重判读到非空绑定 → 取消收口,会话存活。
	close(block)
	<-runDone
	gate()
	if p.manager.Get(sidA) == nil {
		t.Fatal("session A must survive: a new binding arrived during suspension (原子复查取消收口)")
	}
	if st := p.storeFor(p.manager.Get(sidA)); st == nil {
		t.Fatal("session A store must stay alive after suspension cancellation")
	} else if _, err := st.Commit([]core.Message{{Role: "user", Content: "still bound", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit on surviving store: %v", err)
	}
}

// TestRunDoneHookIgnoresUnsuspendedSessions(钩子消费闸门):普通 run 收尾(无挂起
// 台账)零动作——零绑定的 current 会话(CLI 形态)不能被 run 终态通知误收口,
// current 隐式引用守卫与台账闸门双保险。
func TestRunDoneHookIgnoresUnsuspendedSessions(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")
	p.ReleaseConnection("conn-1") // 先解除登记,制造「零绑定 current」形态

	rt := p.currentSession()
	if rt == nil || rt.ID != sidA {
		t.Fatal("setup: current session must be A")
	}
	gate, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("AcquireRun: %v", err)
	}
	rt.RecordRun(func(*runtimeagent.Session) (runtimeagent.RunResult, error) {
		return runtimeagent.RunResult{State: runtimeagent.RunCompleted}, nil
	})
	gate()

	// run 终态已发生(hook 同步触发),但 A 从未挂起 → 不收口。
	if p.manager.Get(sidA) == nil {
		t.Fatal("zero-binding current session must not be reaped by run-done hook (便利指针隐式引用)")
	}
	if st := p.currentStore(); st == nil {
		t.Fatal("current store must stay alive for the CLI fallback path")
	}
}

func TestRunDoneDuringSuspendRegistration(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")
	_ = newPersistedSession(t, p, fixedProvider{content: "x"})
	rt := p.manager.Get(sidA)
	gate, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("AcquireRun: %v", err)
	}
	block := make(chan struct{})
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		rt.RecordRun(func(*runtimeagent.Session) (runtimeagent.RunResult, error) {
			<-block
			return runtimeagent.RunResult{State: runtimeagent.RunCompleted}, nil
		})
	}()
	waitActive(t, rt)

	releaseDone := make(chan struct{})
	go func() {
		p.ReleaseConnection("conn-1")
		close(releaseDone)
	}()
	close(block)
	<-runDone
	<-releaseDone
	gate()

	if p.manager.Get(sidA) != nil {
		t.Fatal("session should be released after the last binding and run completion")
	}
	if p.isSuspendedRelease(sidA) {
		t.Fatal("run completion must consume the suspended release marker")
	}
}

func TestLoadWithoutCWDPreservesWorkspace(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "x"})
	sid := p.CurrentSessionID()
	rt := p.manager.Get(sid)
	cwd := t.TempDir()
	p.applySessionCWD(rt, cwd, true)

	if err := p.LoadBoundSession(sid, ""); err != nil {
		t.Fatalf("LoadBoundSession: %v", err)
	}
	got, cwdErr := rt.DeclaredCWD()
	if cwdErr != nil || got != cwd {
		t.Fatalf("runtime cwd = %q, err=%v, want %q", got, cwdErr, cwd)
	}
	if got := p.storeFor(rt).Workspace(); got != cwd {
		t.Fatalf("stored workspace = %q, want %q", got, cwd)
	}
}

func TestConcurrentLoadAndRelease(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")
	_ = newPersistedSession(t, p, fixedProvider{content: "x"})

	start := make(chan struct{})
	loadErr := make(chan error, 1)
	releaseDone := make(chan struct{})
	go func() {
		<-start
		_, err := p.LoadBoundSessionForConnection(sidA, "", "conn-2")
		loadErr <- err
	}()
	go func() {
		<-start
		p.ReleaseConnection("conn-1")
		close(releaseDone)
	}()
	close(start)

	if err := <-loadErr; err != nil {
		t.Fatalf("LoadBoundSessionForConnection: %v", err)
	}
	<-releaseDone

	rt := p.manager.Get(sidA)
	if rt == nil {
		t.Fatal("load must keep the runtime registered after concurrent release")
	}
	if got := p.control.SessionBindings(sidA); len(got) != 1 || got[0] != "conn-2" {
		t.Fatalf("bindings after concurrent load/release = %v, want [conn-2]", got)
	}
	if _, err := p.storeFor(rt).Commit([]core.Message{{Role: "user", Content: "still usable", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit after concurrent load/release: %v", err)
	}
}

var _ model.ConnectionBinder = (*AgentPlugin)(nil)
var _ model.MultiSessionBinder = (*AgentPlugin)(nil)
var _ model.GuardedConnectionBinder = (*AgentPlugin)(nil)
var _ model.GuardedMultiSessionBinder = (*AgentPlugin)(nil)

func lifecycleTestPlugin(t *testing.T) (*AgentPlugin, *runtimeagent.SessionRuntime) {
	t.Helper()
	manager := runtimeagent.NewSessionManager()
	p := &AgentPlugin{manager: manager}
	p.control = agentcontrol.NewService(manager, p, p)
	current := runtimeagent.NewSessionRuntime("session-a", runtimeagent.NewSession(runtimeagent.SessionDeps{}))
	if err := manager.Register(current); err != nil {
		t.Fatalf("register initial runtime: %v", err)
	}
	p.current.Store(current)
	return p, current
}

// TestCurrentSwapRechecksDisplacedRuntimeReproducesLeak deterministically reproduces the
// disconnect-before-swap interleaving: disconnect sees the runtime as current and keeps it;
// if the subsequent current replacement does not recheck lifecycle, the manager retains it.
func TestCurrentSwapRechecksDisplacedRuntimeReproducesLeak(t *testing.T) {
	p, old := lifecycleTestPlugin(t)
	p.BindConnectionSessionReference("conn", old.ID)
	p.UnbindConnectionSession("conn", old.ID) // current implicit reference prevents release

	next := runtimeagent.NewSessionRuntime("session-b", runtimeagent.NewSession(runtimeagent.SessionDeps{}))
	if err := p.manager.Register(next); err != nil {
		t.Fatalf("register replacement runtime: %v", err)
	}
	p.swapCurrentRuntime(next)

	if got := p.manager.Get(old.ID); got != nil {
		t.Fatalf("displaced runtime %q remains registered after its last reference vanished", got.ID)
	}
}

// TestCurrentSwapAndDisconnectRace leaves only the current runtime registered regardless of
// whether a disconnect or a current swap wins lifecycleMu for each session.
func TestCurrentSwapAndDisconnectRace(t *testing.T) {
	p, initial := lifecycleTestPlugin(t)
	const count = 32
	runtimes := make([]*runtimeagent.SessionRuntime, 0, count)
	runtimes = append(runtimes, initial)
	for i := 1; i < count; i++ {
		rt := runtimeagent.NewSessionRuntime(fmt.Sprintf("session-%d", i), runtimeagent.NewSession(runtimeagent.SessionDeps{}))
		if err := p.manager.Register(rt); err != nil {
			t.Fatalf("register runtime %q: %v", rt.ID, err)
		}
		runtimes = append(runtimes, rt)
	}
	for i, rt := range runtimes {
		p.BindConnectionSessionReference(fmt.Sprintf("conn-%d", i), rt.ID)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, rt := range runtimes {
		i, rt := i, rt
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			p.swapCurrentRuntime(rt)
		}()
		go func() {
			defer wg.Done()
			<-start
			p.UnbindConnectionSession(fmt.Sprintf("conn-%d", i), rt.ID)
		}()
	}
	close(start)
	wg.Wait()

	current := p.currentSession()
	if current == nil {
		t.Fatal("current runtime is nil")
	}
	for _, rt := range runtimes {
		got := p.manager.Get(rt.ID)
		if rt == current {
			if got != rt {
				t.Errorf("current runtime %q is not registered", rt.ID)
			}
			continue
		}
		if got != nil {
			t.Errorf("displaced runtime %q leaked in manager", rt.ID)
		}
	}
}

func TestGuardedBinderInterfacesPreserveReplaceAndAdditiveContracts(t *testing.T) {
	p, _ := lifecycleTestPlugin(t)
	first := runtimeagent.NewSessionRuntime("session-b", runtimeagent.NewSession(runtimeagent.SessionDeps{}))
	second := runtimeagent.NewSessionRuntime("session-c", runtimeagent.NewSession(runtimeagent.SessionDeps{}))
	for _, rt := range []*runtimeagent.SessionRuntime{first, second} {
		if err := p.manager.Register(rt); err != nil {
			t.Fatalf("register runtime %q: %v", rt.ID, err)
		}
	}
	allow := func() bool { return true }

	if !p.BindConnectionSessionReferenceGuarded("multi", first.ID, allow) ||
		!p.BindConnectionSessionReferenceGuarded("multi", second.ID, allow) {
		t.Fatal("guarded multi-session bind rejected an open connection")
	}
	if !p.BindConnectionSessionGuarded("legacy", first.ID, allow) ||
		!p.BindConnectionSessionGuarded("legacy", second.ID, allow) {
		t.Fatal("guarded legacy bind rejected an open connection")
	}

	if got := p.control.SessionBindings(first.ID); !reflect.DeepEqual(got, []string{"multi"}) {
		t.Fatalf("first session bindings = %v, want [multi] after legacy replacement", got)
	}
	if got := p.control.SessionBindings(second.ID); !reflect.DeepEqual(got, []string{"legacy", "multi"}) {
		t.Fatalf("second session bindings = %v, want [legacy multi]", got)
	}
}

type runtimeCleanupJournal struct {
	closed chan struct{}
	once   sync.Once
}

func (j *runtimeCleanupJournal) AppendEvent(session.EventRecord) error { return nil }
func (j *runtimeCleanupJournal) ReadEvents(uint64) ([]session.EventRecord, error) {
	return nil, nil
}
func (j *runtimeCleanupJournal) SeqRange() (uint64, uint64, bool, error) {
	return 0, 0, false, nil
}
func (j *runtimeCleanupJournal) Close() error {
	j.once.Do(func() { close(j.closed) })
	return nil
}

func TestSameIDSwapDefersDisplacedRuntimeCleanupUntilRunDone(t *testing.T) {
	p := newGatedPlugin()
	t.Cleanup(func() { _ = p.Stop() })
	old := p.currentSession()
	journal := &runtimeCleanupJournal{closed: make(chan struct{})}
	old.BindSession(runtimeagent.NewSession(runtimeagent.SessionDeps{RunEvents: journal}))
	hub, err := old.EnsureHub()
	if err != nil {
		t.Fatalf("ensure old runtime hub: %v", err)
	}

	runEntered := make(chan struct{})
	releaseRun := make(chan struct{})
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		old.RecordRun(func(*runtimeagent.Session) (runtimeagent.RunResult, error) {
			close(runEntered)
			<-releaseRun
			return runtimeagent.RunResult{State: runtimeagent.RunCompleted}, nil
		})
	}()
	<-runEntered

	next := runtimeagent.NewSessionRuntime(old.ID, nil)
	if err := p.swapCurrentRuntime(next); err != nil {
		t.Fatalf("swap current runtime: %v", err)
	}
	if p.manager.Get(next.ID) != next || p.currentSession() != next {
		t.Fatal("same-ID swap did not publish the replacement runtime")
	}
	if !old.Retired() {
		t.Fatal("displaced runtime still admits work after same-ID replacement")
	}
	select {
	case <-journal.closed:
		t.Fatal("displaced runtime journal closed while its run was active")
	default:
	}

	close(releaseRun)
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("displaced runtime run did not finish")
	}
	select {
	case <-journal.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("displaced runtime journal was not closed after its run finished")
	}
	if _, err := hub.Subscribe(context.Background(), 0); !errors.Is(err, runtimeagent.ErrHubClosed) {
		t.Fatalf("subscribe to displaced runtime hub = %v, want ErrHubClosed", err)
	}
}

func TestConcurrentStartSessionDoesNotLeakDisplacedRuntimes(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "x"})
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p.startSession()
		}()
	}
	close(start)
	wg.Wait()

	current := p.currentSession()
	if current == nil || p.manager.Get(current.ID) != current {
		t.Fatal("concurrent startSession left current runtime unregistered")
	}
	if runtimes := p.manager.List(); len(runtimes) != 1 || runtimes[0] != current {
		t.Fatalf("registered runtimes = %v, want only current %q", runtimes, current.ID)
	}
}

func TestLegacyConnectionBinderPreservesReplacementSemantics(t *testing.T) {
	p, current := lifecycleTestPlugin(t)
	first := runtimeagent.NewSessionRuntime("session-b", runtimeagent.NewSession(runtimeagent.SessionDeps{}))
	second := runtimeagent.NewSessionRuntime("session-c", runtimeagent.NewSession(runtimeagent.SessionDeps{}))
	for _, rt := range []*runtimeagent.SessionRuntime{first, second} {
		if err := p.manager.Register(rt); err != nil {
			t.Fatalf("register runtime %q: %v", rt.ID, err)
		}
	}
	p.BindConnectionSession("conn", first.ID)
	p.BindConnectionSession("conn", second.ID)

	if got := p.control.SessionBindings(first.ID); len(got) != 0 {
		t.Fatalf("legacy bind left previous session referenced: %v", got)
	}
	if got := p.control.SessionBindings(second.ID); len(got) != 1 || got[0] != "conn" {
		t.Fatalf("replacement reference = %v, want [conn]", got)
	}
	if got := p.manager.Get(first.ID); got != nil {
		t.Fatalf("replaced unreferenced runtime %q was not released", got.ID)
	}
	if p.manager.Get(current.ID) != current {
		t.Fatal("legacy replacement disturbed current runtime")
	}
}

// TestStartRunConcurrentWithRelease 覆盖「Run 提交」与「最后解绑收口」的竞态：
// StartRunSync 可能已路由到 rt，随后 ReleaseConnection 完成 Retire+Remove。
// 合法结果只有两种——Run 先占位（收口挂起，终态后注销），或收口先 Retire
// （提交在 reserveRun 被拒，绝不落到已关闭的 hub/store）。核心断言：被拒的提交
// 不能复活已注销的 runtime；旧 ensureRegistered 指针曾把该窗口放大成写坏 store。
type blockedReservationFailure struct {
	entered chan struct{}
	release chan struct{}
	err     error
	once    sync.Once
}

func (j *blockedReservationFailure) fail() error {
	j.once.Do(func() { close(j.entered) })
	<-j.release
	return j.err
}

type blockedEventSeedJournal struct {
	blockedReservationFailure
	closed chan struct{}
}

func (j *blockedEventSeedJournal) AppendEvent(session.EventRecord) error { return nil }
func (j *blockedEventSeedJournal) ReadEvents(uint64) ([]session.EventRecord, error) {
	return nil, nil
}
func (j *blockedEventSeedJournal) SeqRange() (uint64, uint64, bool, error) {
	return 0, 0, false, j.fail()
}
func (j *blockedEventSeedJournal) Close() error {
	close(j.closed)
	return nil
}

func (j *blockedReservationFailure) AppendState(session.RunStateRecord) error {
	return j.fail()
}

func TestSameIDReplacementDoesNotWaitForeverAfterStartRunReservationFailure(t *testing.T) {
	failure := errors.New("injected reservation failure")
	cases := []struct {
		name       string
		blockStage string
	}{
		{name: "bind events", blockStage: "bind"},
		{name: "queued journal", blockStage: "queued"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, old := lifecycleTestPlugin(t)
			var eventJournalClosed <-chan struct{}
			var eventSeedFailure *blockedEventSeedJournal
			deps := runtimeagent.SessionDeps{}
			var stateJournal *blockedReservationFailure
			if tc.blockStage == "bind" {
				journal := &blockedEventSeedJournal{
					blockedReservationFailure: blockedReservationFailure{
						entered: make(chan struct{}),
						release: make(chan struct{}),
						err:     failure,
					},
					closed: make(chan struct{}),
				}
				eventJournalClosed = journal.closed
				eventSeedFailure = journal
				deps.RunEvents = journal
			} else {
				journal := &runtimeCleanupJournal{closed: make(chan struct{})}
				eventJournalClosed = journal.closed
				deps.RunEvents = journal
				stateJournal = &blockedReservationFailure{
					entered: make(chan struct{}),
					release: make(chan struct{}),
					err:     failure,
				}
				deps.RunJournal = stateJournal
			}
			old.BindSession(runtimeagent.NewSession(deps))

			type startResult struct {
				err error
			}
			started := make(chan startResult, 1)
			var callbackCalls atomic.Int32
			go func() {
				_, err := p.manager.StartRun(old.ID, runtimeagent.RunRequest{Input: "never executed"}, runtimeagent.RunCallbacks{
					Write: func(string) { callbackCalls.Add(1) },
				})
				started <- startResult{err: err}
			}()

			var blocked <-chan struct{}
			if stateJournal != nil {
				blocked = stateJournal.entered
			} else {
				blocked = eventSeedFailure.entered
			}
			select {
			case <-blocked:
			case <-time.After(5 * time.Second):
				t.Fatal("StartRun did not reach the injected failure point")
			}
			run := old.Active()
			if run == nil {
				t.Fatal("StartRun must reserve the run before the injected failure")
			}

			next := runtimeagent.NewSessionRuntime(old.ID, nil)
			if err := p.swapCurrentRuntime(next); err != nil {
				t.Fatalf("same-ID replacement: %v", err)
			}
			select {
			case <-eventJournalClosed:
				t.Fatal("replacement closed event journal while StartRun was unwinding")
			default:
			}

			if stateJournal != nil {
				close(stateJournal.release)
			} else {
				close(eventSeedFailure.release)
			}
			select {
			case result := <-started:
				if !errors.Is(result.err, failure) {
					t.Fatalf("StartRun error = %v, want injected failure", result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("StartRun did not return after injected failure")
			}

			var runDone bool
			select {
			case <-run.Done():
				runDone = true
			default:
			}
			if old.Active() != nil {
				t.Fatalf("old runtime active = %v, want nil after reservation rollback", old.Active())
			}
			if callbackCalls.Load() != 0 {
				t.Fatalf("run callback calls = %d, want zero for rejected submission", callbackCalls.Load())
			}

			stopResult := make(chan error, 1)
			go func() { stopResult <- p.Stop() }()
			select {
			case err := <-stopResult:
				if err != nil {
					t.Fatalf("Stop: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Stop blocked on a run whose StartRun reservation was rejected")
			}
			if !runDone {
				t.Fatal("rejected reserved run Done channel was never closed")
			}
			if got := run.State(); got != runtimeagent.RunCancelled {
				t.Fatalf("rejected reserved run state = %q, want %q", got, runtimeagent.RunCancelled)
			}
			select {
			case <-eventJournalClosed:
			default:
				t.Fatal("displaced runtime event journal was not closed")
			}
		})
	}
}

func TestReservationFailureRechecksSuspendedReleaseWithoutLeaseLifecycleDeadlock(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "unused"})
	rt := p.currentSession()
	if rt == nil {
		t.Fatal("setup: current runtime is nil")
	}
	const connectionID = "conn"
	sessionID := rt.ID
	p.BindConnectionSessionReference(connectionID, sessionID)
	if nextID := newPersistedSession(t, p, fixedProvider{content: "next"}); nextID == sessionID {
		t.Fatal("setup: expected a distinct current session")
	}

	failure := errors.New("injected queued journal failure")
	stateJournal := &blockedReservationFailure{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		err:     failure,
	}
	eventJournal := &runtimeCleanupJournal{closed: make(chan struct{})}
	rt.BindSession(runtimeagent.NewSession(runtimeagent.SessionDeps{
		RunEvents:  eventJournal,
		RunJournal: stateJournal,
	}))
	if _, ok := p.repo.Peek(sessionID); !ok {
		t.Fatal("setup: persistent session store is not open")
	}
	p.control.AcquireControl(sessionID, connectionID)

	startResult := make(chan error, 1)
	go func() {
		_, _, err := p.StartRun(sessionID, connectionID, "never executed", "request", model.RunToolOptions{})
		startResult <- err
	}()
	select {
	case <-stateJournal.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("StartRun did not reach queued journal failure point")
	}
	if rt.Active() == nil {
		t.Fatal("StartRun must reserve the run before the queued journal write")
	}

	// Drop the final binding while the reservation is still active. The session becomes
	// suspended and must be rechecked by the reservation rollback notification.
	p.UnbindConnectionSession(connectionID, sessionID)
	if !p.isSuspendedRelease(sessionID) {
		t.Fatal("session release was not suspended for the active reservation")
	}

	// Recreate the production lock inversion deterministically: disconnect owns lifecycleMu
	// and blocks acquiring the lease held by StartRun's WithControl callback.
	releaseConnectionDone := make(chan struct{})
	go func() {
		p.ReleaseConnection(connectionID)
		close(releaseConnectionDone)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for p.lifecycleMu.TryLock() {
		p.lifecycleMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("ReleaseConnection did not acquire lifecycleMu")
		}
		time.Sleep(time.Millisecond)
	}

	close(stateJournal.release)
	select {
	case err := <-startResult:
		if !errors.Is(err, failure) {
			t.Fatalf("StartRun error = %v, want injected failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StartRun rollback waited for lifecycleMu while holding the control lease")
	}
	select {
	case <-releaseConnectionDone:
	case <-time.After(5 * time.Second):
		t.Fatal("ReleaseConnection remained blocked after StartRun released the lease")
	}

	select {
	case <-eventJournal.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("reaped runtime event journal was not closed")
	}
	// Runtime removal and suspended-state clearing happen before CloseSession. The
	// close notification proves cleanup reached that phase; lifecycleMu is the fence
	// that proves CloseSession has returned before inspecting the repository cache.
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if p.manager.Get(sessionID) != nil {
		t.Fatal("reaped session runtime remains registered after lifecycle cleanup")
	}
	if p.isSuspendedRelease(sessionID) {
		t.Fatal("reaped session remains marked suspended after lifecycle cleanup")
	}
	if _, ok := p.repo.Peek(sessionID); ok {
		t.Fatal("reaped session store remains cached in repository after lifecycle cleanup")
	}
}

func TestStartRunConcurrentWithRelease(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, fixedProvider{content: "x"})
	// A 必须是 newPersistedSession 建出来的会话:newPersistedPlugin 的
	// refreshSession 同 id 重注册会撞 Init 留下的旧条目,manager 里仍是真实
	// provider 的 runtime——A 变成非 current 后路由走 manager.Get,会拿到它。
	sidA := newPersistedSession(t, p, gated)
	if rt := p.manager.Get(sidA); rt == nil || rt != p.currentSession() {
		t.Fatal("setup: gated runtime must be registered for session A")
	}
	// current 换到 B:被释放的 A 不在 current 隐式引用保护下,收口可真实执行。
	if sidB := newPersistedSession(t, p, fixedProvider{content: "x"}); sidB == sidA {
		t.Fatal("setup: distinct sessions required")
	}
	p.BindConnectionSessionReference("conn-1", sidA)

	start := make(chan struct{})
	type outcome struct {
		h   runtimeagent.RunHandle
		err error
	}
	runCh := make(chan outcome, 1)
	go func() {
		<-start
		h, err := p.StartRunSync(sidA, runtimeagent.RunRequest{Input: "q"}, runtimeagent.RunCallbacks{})
		runCh <- outcome{h: h, err: err}
	}()
	releaseDone := make(chan struct{})
	go func() {
		<-start
		p.ReleaseConnection("conn-1")
		close(releaseDone)
	}()
	close(start)

	run := <-runCh
	<-releaseDone

	if run.err != nil {
		// 收口先 Retire:提交必须在 admission 处被拒(still-registered → retired,
		// 已 Remove → not found),且不得复活 runtime。
		if !errors.Is(run.err, runtimeagent.ErrSessionRetired) && !errors.Is(run.err, runtimeagent.ErrSessionNotFound) {
			t.Fatalf("StartRun concurrent with release = %v, want ErrSessionRetired/ErrSessionNotFound", run.err)
		}
		if p.manager.Get(sidA) != nil {
			t.Fatal("retired runtime was resurrected into the registry by a rejected StartRun")
		}
		return
	}

	// Run 先占位:收口被挂起,run 终态后由 run-done 通知重判注销。
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not start in time")
	}
	close(gated.release)
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := run.h.Wait(waitCtx); err != nil {
		t.Fatalf("run Wait: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for p.manager.Get(sidA) != nil {
		if time.Now().After(deadline) {
			t.Fatal("session not reaped after run terminal state")
		}
		time.Sleep(time.Millisecond)
	}
}
