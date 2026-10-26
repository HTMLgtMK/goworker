package agent

import (
	"context"
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
