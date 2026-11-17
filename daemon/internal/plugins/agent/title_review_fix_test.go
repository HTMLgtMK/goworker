package service

import (
	"context"
	"strings"
	"testing"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
)

func TestTitleStoreForDoesNotCloseBorrowedStore(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	p.manager = runtimeagent.NewSessionManager()
	borrowed, release, err := p.titleStoreFor(store.SessionID())
	if err != nil {
		t.Fatal(err)
	}
	release()
	if _, err := borrowed.AppendTitle("still open"); err != nil {
		t.Fatalf("borrowed store was closed: %v", err)
	}
}

func TestListSessionsPreservesExplicitTitle(t *testing.T) {
	dir := t.TempDir()
	repo, err := session.OpenRepository(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	store, err := repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	title := strings.Repeat("界", 70)
	if _, err := store.AppendTitle(title); err != nil {
		t.Fatal(err)
	}
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = dir
	p := &AgentPlugin{cfg: cfg, manager: runtimeagent.NewSessionManager()}
	got := p.ListSessions()
	if len(got) != 1 || got[0].Title != title {
		t.Fatalf("ListSessions() = %+v, want full title %q", got, title)
	}
}
