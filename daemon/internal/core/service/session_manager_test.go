package service

import (
	"context"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	runtimesession "github.com/tinguo/goworker/ai-runtime/session"
	sandbox "github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

func TestSessionManager_AllowsDifferentSessionsToOverlap(t *testing.T) {
	cfg := runtimeconfig.Default()
	manager, err := newSessionManager(sessionManagerDeps{
		BuildDeps: func(_ string, cwd string, store *runtimesession.Store) runtimeagent.SessionDeps {
			return runtimeagent.SessionDeps{
				Config: cfg,
				CWD:    cwd,
				Store:  store,
				CollectTools: func(*sandbox.Config) []core.Tool {
					return nil
				},
			}
		},
	})
	if err != nil {
		t.Fatalf("newSessionManager: %v", err)
	}

	a, err := manager.Create("/tmp/a")
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	b, err := manager.Create("/tmp/b")
	if err != nil {
		t.Fatalf("create B: %v", err)
	}

	started := make(chan string, 2)
	release := make(chan struct{})
	for _, managed := range []*managedSession{a, b} {
		managed := managed
		go func() {
			if err := managed.run(context.Background(), func(*runtimeagent.Session) error {
				started <- managed.id
				<-release
				return nil
			}); err != nil {
				t.Errorf("run %s: %v", managed.id, err)
			}
		}()
	}

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case id := <-started:
			seen[id] = true
		case <-time.After(time.Second):
			t.Fatalf("sessions did not overlap; started=%v", seen)
		}
	}
	close(release)
}

func TestManagedSession_SameSessionSingleFlight(t *testing.T) {
	cfg := runtimeconfig.Default()
	manager, err := newSessionManager(sessionManagerDeps{
		BuildDeps: func(_ string, _ string, _ *runtimesession.Store) runtimeagent.SessionDeps {
			return runtimeagent.SessionDeps{Config: cfg}
		},
	})
	if err != nil {
		t.Fatalf("newSessionManager: %v", err)
	}
	managed, err := manager.Create("")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = managed.run(context.Background(), func(*runtimeagent.Session) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := managed.run(ctx, func(*runtimeagent.Session) error { return nil }); err == nil {
		t.Fatal("second same-session run succeeded while first was active")
	}
	close(release)
}

func TestAgentPluginWithSelectedSessionRoutesByID(t *testing.T) {
	cfg := runtimeconfig.Default()
	manager, err := newSessionManager(sessionManagerDeps{
		BuildDeps: func(_ string, _ string, _ *runtimesession.Store) runtimeagent.SessionDeps {
			return runtimeagent.SessionDeps{Config: cfg}
		},
	})
	if err != nil {
		t.Fatalf("newSessionManager: %v", err)
	}
	a, err := manager.Create("")
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	b, err := manager.Create("")
	if err != nil {
		t.Fatalf("create B: %v", err)
	}

	plugin := &AgentPlugin{manager: manager}
	ctx := &model.Context{Ctx: context.Background(), SessionID: b.id}
	var selected *runtimeagent.Session
	if err := plugin.withSelectedSession(ctx, func(_ context.Context, agent *runtimeagent.Session, _ *runtimesession.Store) error {
		selected = agent
		return nil
	}); err != nil {
		t.Fatalf("withSelectedSession: %v", err)
	}
	if selected != b.agent {
		t.Fatalf("selected session = %p, want B %p", selected, b.agent)
	}
	if selected == a.agent {
		t.Fatal("selected session unexpectedly resolved to A")
	}
}

func TestCollectToolsForSessionPropagatesSessionID(t *testing.T) {
	var gotSessionID string
	plugin := &AgentPlugin{hub: &model.Hub{
		Tools: func() []model.Tool {
			return []model.Tool{{Name: "compact"}}
		},
		Eval: func(ctx *model.Context, _ string) error {
			gotSessionID = ctx.SessionID
			return nil
		},
	}}

	tools := plugin.collectToolsForSession("sess-b", &sandbox.Config{})
	for _, tool := range tools {
		if tool.Name != "compact" {
			continue
		}
		if _, err := tool.Execute(context.Background(), nil); err != nil {
			t.Fatalf("execute command tool: %v", err)
		}
		if gotSessionID != "sess-b" {
			t.Fatalf("tool session id = %q, want sess-b", gotSessionID)
		}
		return
	}

	t.Fatal("compact tool was not collected")
}

func TestAgentPluginWithSelectedSessionAllowsNestedRun(t *testing.T) {
	cfg := runtimeconfig.Default()
	manager, err := newSessionManager(sessionManagerDeps{
		BuildDeps: func(_ string, _ string, _ *runtimesession.Store) runtimeagent.SessionDeps {
			return runtimeagent.SessionDeps{Config: cfg}
		},
	})
	if err != nil {
		t.Fatalf("newSessionManager: %v", err)
	}
	managed, err := manager.Create("")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	plugin := &AgentPlugin{manager: manager}
	base := context.WithValue(context.Background(), managedSessionGateKey{plugin: plugin, sessionID: managed.id}, struct{}{})
	ctx := &model.Context{Ctx: base, SessionID: managed.id}

	managed.runGate <- struct{}{}
	defer func() { <-managed.runGate }()
	done := make(chan error, 1)
	go func() {
		done <- plugin.withSelectedSession(ctx, func(context.Context, *runtimeagent.Session, *runtimesession.Store) error {
			return nil
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("nested session run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("nested session run waited on its own gate")
	}
}

func TestSessionManager_RestartDiscoversIndependentSessions(t *testing.T) {
	root := t.TempDir()
	cfg := runtimeconfig.Default()
	buildDeps := func(_ string, cwd string, store *runtimesession.Store) runtimeagent.SessionDeps {
		return runtimeagent.SessionDeps{Config: cfg, CWD: cwd, Store: store}
	}
	manager, err := newSessionManager(sessionManagerDeps{Root: root, Persistent: true, BuildDeps: buildDeps})
	if err != nil {
		t.Fatalf("newSessionManager: %v", err)
	}
	a, err := manager.CreateWithID("session-a", "/tmp/a")
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	b, err := manager.CreateWithID("session-b", "/tmp/b")
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	if _, err := a.store.Commit([]core.Message{{MsgID: "msg-a", Role: "user", Content: "A"}}); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	if _, err := b.store.Commit([]core.Message{{MsgID: "msg-b", Role: "user", Content: "B"}}); err != nil {
		t.Fatalf("commit B: %v", err)
	}
	if err := manager.Archive(context.Background(), b.id); err != nil {
		t.Fatalf("archive B: %v", err)
	}
	if err := manager.CloseAll(); err != nil {
		t.Fatalf("close first manager: %v", err)
	}

	restarted, err := newSessionManager(sessionManagerDeps{Root: root, Persistent: true, BuildDeps: buildDeps})
	if err != nil {
		t.Fatalf("restart manager: %v", err)
	}
	defer restarted.CloseAll()
	if len(restarted.Sessions()) != 2 {
		t.Fatalf("discovered sessions = %d, want 2", len(restarted.Sessions()))
	}
	loadedA, err := restarted.Load("session-a", "")
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	loadedB, err := restarted.Load("session-b", "")
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	if loadedA.statusSnapshot() != runtimesession.SessionStatusLive {
		t.Fatalf("A status = %q, want live", loadedA.statusSnapshot())
	}
	if loadedB.statusSnapshot() != runtimesession.SessionStatusArchived {
		t.Fatalf("B status = %q, want archived", loadedB.statusSnapshot())
	}
	if _, err := restarted.Load("session-b", "/tmp/should-not-apply"); err != nil {
		t.Fatalf("load archived B with cwd: %v", err)
	}
	if got := loadedB.cwdSnapshot(); got != "" {
		t.Fatalf("archived B cwd = %q, want empty", got)
	}
	if got := loadedA.store.ActiveView(); len(got) != 1 || got[0].Content != "A" {
		t.Fatalf("A view = %+v", got)
	}
	if got := loadedB.store.ActiveView(); len(got) != 1 || got[0].Content != "B" {
		t.Fatalf("B view = %+v", got)
	}
}

func TestAgentPlugin_ManagerKeepsCWDPerSession(t *testing.T) {
	cfg := runtimeconfig.Default()
	manager, err := newSessionManager(sessionManagerDeps{
		BuildDeps: func(_ string, cwd string, _ *runtimesession.Store) runtimeagent.SessionDeps {
			return runtimeagent.SessionDeps{Config: cfg, CWD: cwd}
		},
	})
	if err != nil {
		t.Fatalf("newSessionManager: %v", err)
	}
	a, err := manager.CreateWithID("session-a", "")
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	b, err := manager.CreateWithID("session-b", "")
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	plugin := &AgentPlugin{cfg: cfg, manager: manager}

	plugin.SetSession(a.id, "/tmp/a")
	plugin.SetSession(b.id, "/tmp/b")

	if got := a.cwdSnapshot(); got != "/tmp/a" {
		t.Errorf("session A cwd = %q, want /tmp/a", got)
	}
	if got := b.cwdSnapshot(); got != "/tmp/b" {
		t.Errorf("session B cwd = %q, want /tmp/b", got)
	}
	if got := plugin.currentSessionCwd(); got != "" {
		t.Errorf("plugin-global cwd = %q, want empty in manager mode", got)
	}
}

func TestSessionManager_RejectsUnknownUnsafeIDWithoutFallback(t *testing.T) {
	root := t.TempDir()
	cfg := runtimeconfig.Default()
	manager, err := newSessionManager(sessionManagerDeps{
		Root: root, Persistent: true,
		BuildDeps: func(_ string, _ string, store *runtimesession.Store) runtimeagent.SessionDeps {
			return runtimeagent.SessionDeps{Config: cfg, Store: store}
		},
	})
	if err != nil {
		t.Fatalf("newSessionManager: %v", err)
	}
	defer manager.CloseAll()
	if _, err := manager.CreateWithID("safe", ""); err != nil {
		t.Fatalf("create safe: %v", err)
	}
	for _, id := range []string{"../safe", "unknown"} {
		if _, err := manager.Load(id, ""); err == nil {
			t.Fatalf("Load(%q) unexpectedly succeeded", id)
		}
	}
	if _, ok := manager.lookup("safe"); !ok {
		t.Fatal("safe session disappeared after invalid loads")
	}
}
