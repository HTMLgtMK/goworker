package agent

import (
	"context"
	"strings"
	"testing"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

// 本文件验证多个 runtime 的 CWD 与 usage 状态相互隔离。

func TestUsageContextWindowTracksAcceptedRunConfig(t *testing.T) {
	s, _ := testSession(&captureProvider{})
	windowConfig := func(window int) *runtimeconfig.Config {
		cfg := runtimeconfig.Default()
		provider := cfg.LLM.Providers["openai"]
		model := provider.Models["gpt-4o"]
		model.ContextWindow = window
		provider.Models["gpt-4o"] = model
		cfg.LLM.Providers["openai"] = provider
		return cfg
	}

	for _, window := range []int{32 * 1024, 64 * 1024} {
		result, err := s.Run(context.Background(), RunRequest{
			Input:  "question",
			Config: windowConfig(window),
		}, testRunCallbacks(&strings.Builder{}))
		if err != nil {
			t.Fatalf("Run with %d-token window: %v", window, err)
		}
		if result.State != RunCompleted {
			t.Fatalf("Run with %d-token window = %+v, want completed", window, result)
		}
		if got := s.LastUsageContextWindow(); got != window {
			t.Errorf("last usage context window = %d after Run configured with %d", got, window)
		}
	}

	// A run that exits before reaching a provider must not replace the last
	// actual-call window with whichever config happened to be selected.
	result, err := s.Run(context.Background(), RunRequest{
		Config: windowConfig(96 * 1024),
	}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("empty Run: %v", err)
	}
	if result.State != RunCancelled {
		t.Fatalf("empty Run state = %s, want cancelled", result.State)
	}
	if got := s.LastUsageContextWindow(); got != 64*1024 {
		t.Errorf("context window after call-free Run = %d, want 65536", got)
	}
}

// TestSessionStateIsolation：manager 注册 A/B 两个 runtime，各自声明不同 cwd、
// 各跑一轮 → 双方 DeclaredCWD / Session.cwd 互不覆盖；A 再跑一轮 → usage 各自
// 累计（A 两次调用、B 仍一次），互不串账。
func TestSessionStateIsolation(t *testing.T) {
	mgr := NewSessionManager()

	newRuntime := func(id, cwd string) *SessionRuntime {
		rt := NewSessionRuntime(id, newFakeStoreSession(&fakeConversationStore{}, nil, &captureProvider{}))
		if err := mgr.Register(rt); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		rt.DeclareCWD(cwd, nil)
		if s := rt.Session(); s != nil {
			s.SetCWD(cwd)
		}
		return rt
	}

	a := newRuntime("session-A", "/workspace/a")
	b := newRuntime("session-B", "/workspace/b")

	runOne := func(rt *SessionRuntime, input string) RunResult {
		rel, err := rt.AcquireRun(context.Background())
		if err != nil {
			return RunResult{State: RunFailed, Err: err}
		}
		defer rel()
		return rt.RecordRun(func(s *Session) (RunResult, error) {
			return s.Run(context.Background(), RunRequest{Input: input}, testRunCallbacks(&strings.Builder{}))
		})
	}

	if res := runOne(a, "q-A"); res.State != RunCompleted {
		t.Fatalf("session A run = %+v, want RunCompleted", res)
	}
	if res := runOne(b, "q-B"); res.State != RunCompleted {
		t.Fatalf("session B run = %+v, want RunCompleted", res)
	}

	// 声明隔离：双方 runtime 各持各的 cwd 声明，互不覆盖
	if cwd, err := a.DeclaredCWD(); err != nil || cwd != "/workspace/a" {
		t.Errorf("A DeclaredCWD = (%q, %v), want (/workspace/a, nil)", cwd, err)
	}
	if cwd, err := b.DeclaredCWD(); err != nil || cwd != "/workspace/b" {
		t.Errorf("B DeclaredCWD = (%q, %v), want (/workspace/b, nil)", cwd, err)
	}

	// 会话级隔离：cwd 随各自 Session 走，互不污染
	sa, sb := a.Session(), b.Session()
	if got := sa.cwd(); got != "/workspace/a" {
		t.Errorf("A session cwd = %q, want /workspace/a (不被 B 污染)", got)
	}
	if got := sb.cwd(); got != "/workspace/b" {
		t.Errorf("B session cwd = %q, want /workspace/b (不被 A 污染)", got)
	}

	// usage 隔离：A 累计到 2 次模型调用，B 停在 1 次——账本随会话走，互不串账
	if res := runOne(a, "q-A-again"); res.State != RunCompleted {
		t.Fatalf("session A second run = %+v, want RunCompleted", res)
	}
	ua, _, _ := sa.UsageSnapshot()
	ub, _, _ := sb.UsageSnapshot()
	if len(ua) != 2 {
		t.Errorf("A usage calls = %d, want 2 (会话内跨轮累计)", len(ua))
	}
	if len(ub) != 1 {
		t.Errorf("B usage calls = %d, want 1 (不被 A 的第二轮污染)", len(ub))
	}
}
