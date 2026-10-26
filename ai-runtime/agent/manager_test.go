package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestManagerIndependentSessions:manager 内两个 runtime 同时持有各自 gate,
// 互不阻塞(TestConcurrentSessions 的 manager 层雏形)。
func TestManagerIndependentSessions(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	runtimes := []*SessionRuntime{
		NewSessionRuntime("a", nil),
		NewSessionRuntime("b", nil),
	}
	for _, rt := range runtimes {
		if err := m.Register(rt); err != nil {
			t.Fatalf("register %s: %v", rt.ID, err)
		}
	}

	acquired := make(chan error, len(runtimes))
	for _, rt := range runtimes {
		rt := rt
		go func() {
			rel, err := rt.AcquireRun(context.Background())
			acquired <- err // 先宣告获取结果,再谈释放
			if err == nil {
				time.Sleep(50 * time.Millisecond) // 各持自己的 gate 一小段
				rel()
			}
		}()
	}

	// 双方都尚未释放时,两个获取都必须成功——互不阻塞的直接证明;
	// 若任一方被对方卡住,将在 deadline 处失败。
	deadline := time.After(time.Second)
	for range runtimes {
		select {
		case err := <-acquired:
			if err != nil {
				t.Fatalf("acquire failed while gates independent: %v", err)
			}
		case <-deadline:
			t.Fatal("independent runtimes blocked each other")
		}
	}
}

// TestSessionManagerLifecycle:Register/Get/List/Remove/Close 行为,含同 ID
// 冲突报错与幂等 Remove/Close。
func TestSessionManagerLifecycle(t *testing.T) {
	m := NewSessionManager()

	a := NewSessionRuntime("a", nil)
	if err := m.Register(a); err != nil {
		t.Fatalf("register a: %v", err)
	}
	if err := m.Register(NewSessionRuntime("a", nil)); err == nil {
		t.Fatal("duplicate id accepted, want error")
	}
	if err := m.Register(nil); err == nil {
		t.Fatal("nil runtime accepted, want error")
	}
	if got := m.Get("a"); got != a {
		t.Fatalf("Get(\"a\") = %v, want the registered runtime", got)
	}
	if got := m.Get("missing"); got != nil {
		t.Fatalf("Get(\"missing\") = %v, want nil", got)
	}

	if err := m.Register(NewSessionRuntime("b", nil)); err != nil {
		t.Fatalf("register b: %v", err)
	}
	list := m.List()
	if len(list) != 2 || list[0].ID != "a" || list[1].ID != "b" {
		t.Fatalf("List = %s, want sorted [a b]", runtimeIDs(list))
	}

	m.Remove("a")
	if got := m.Get("a"); got != nil {
		t.Fatal("Remove did not drop a")
	}
	m.Remove("a") // 幂等
	if list := m.List(); len(list) != 1 || list[0].ID != "b" {
		t.Fatalf("List after remove = %s, want [b]", runtimeIDs(list))
	}

	m.Close()
	if list := m.List(); len(list) != 0 {
		t.Fatalf("List after close = %s, want empty", runtimeIDs(list))
	}
	if got := m.Get("b"); got != nil {
		t.Fatal("Get after close returned runtime")
	}
}

func runtimeIDs(list []*SessionRuntime) []string {
	ids := make([]string, 0, len(list))
	for _, rt := range list {
		ids = append(ids, rt.ID)
	}
	return ids
}

// TestManagerCloseCancelsContextAndRejectsRegister:P2-4 的宿主停机语义——
// Close 后宿主生命周期 ctx 取消、Register 拒绝(ErrManagerClosed)、Close 幂等。
func TestManagerCloseCancelsContextAndRejectsRegister(t *testing.T) {
	m := NewSessionManager()
	rt := NewSessionRuntime("a", nil)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register a: %v", err)
	}

	ctx := m.contextOf()
	select {
	case <-ctx.Done():
		t.Fatal("host context canceled before Close")
	default:
	}

	m.Close()

	select {
	case <-m.contextOf().Done():
	default:
		t.Fatal("contextOf().Done() not closed after Close")
	}
	if err := m.Register(NewSessionRuntime("b", nil)); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Register after Close = %v, want ErrManagerClosed", err)
	}

	m.Close() // 幂等:第二次 Close 不阻塞、不 panic
	if list := m.List(); len(list) != 0 {
		t.Fatalf("List after close = %s, want empty", runtimeIDs(list))
	}
	if got := m.Get("a"); got != nil {
		t.Fatal("Get after close returned runtime")
	}
}

// TestManagerCloseWaitsActiveRun:P2-4 的 Shutdown 等待语义——在途同步 Run
// (RecordRun,fn 用调用方 ctx)未终态前 Close 阻塞;run 终态后 Close 返回,
// run 正常 RunCompleted(证明 Close 的 cancel 不影响在途同步 prompt)。
func TestManagerCloseWaitsActiveRun(t *testing.T) {
	m := NewSessionManager()
	st := &fakeConversationStore{}
	started := make(chan struct{})
	release := make(chan struct{})
	pv := &gatedProvider{
		replies: []string{"reply"},
		starts:  []chan struct{}{started},
		release: []chan struct{}{release},
	}
	rt := NewSessionRuntime("close-wait", newFakeStoreSession(st, nil, pv))
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}

	done := make(chan RunResult, 1)
	go func() {
		rel, err := rt.AcquireRun(context.Background())
		if err != nil {
			done <- RunResult{State: RunFailed, Err: err}
			return
		}
		defer rel()
		done <- rt.RecordRun(func(s *Session) (RunResult, error) {
			return s.Run(context.Background(), RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
		})
	}()
	waitStarted(t, started) // provider 挂起中:run 在途,active 已记账

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()

	// Close 必须阻塞到 run 终态:provider 释放前不得返回。
	select {
	case <-closed:
		t.Fatal("Close returned while the in-flight run was still active")
	case <-time.After(100 * time.Millisecond):
	}

	close(release) // 放行 provider → run 走到终态 → Close 解除阻塞
	res := waitRunResult(t, done)
	// run.done 关闭后 Close 仍需走清表收尾,与主 goroutine 无先后保证——
	// 有界等待其返回,而非非阻塞抢跑。
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the run reached a terminal state")
	}
	if res.State != RunCompleted || !res.Persisted {
		t.Fatalf("run result = %+v, want RunCompleted+Persisted (Close cancel must not reach sync-path run)", res)
	}
	if got := rt.Active(); got != nil {
		t.Fatalf("Active after Close = %+v, want nil (run 已终态并清理)", got)
	}
}
