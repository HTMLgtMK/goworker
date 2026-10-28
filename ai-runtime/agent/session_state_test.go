package agent

import (
	"context"
	"strings"
	"testing"
)

// 本文件是 P1-4 状态绑定收口的 SDK 层验收（总方案第十一节「CWD/Usage 会话隔离」）：
// CWD 声明（P1-4 起 runtime 级）与 usage 账本（Session 级，spec.go usage
// *core.UsageTracker 随会话实例走、跨轮累计、/new 换实例清零）都随会话绑定，
// manager 里并存的多个 runtime 互不污染。

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
