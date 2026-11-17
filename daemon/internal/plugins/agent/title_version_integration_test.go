package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/middlewares"
	"github.com/tinguo/goworker/ai-runtime/session"
)

type pausedTitleStore struct {
	*generatedTitleStore
	written chan struct{}
	resume  chan struct{}
}

func (s *pausedTitleStore) AppendTitle(title string) (session.TitleState, error) {
	state, err := s.generatedTitleStore.AppendTitle(title)
	close(s.written)
	<-s.resume
	return state, err
}

func TestTitleVersionSurvivesOutOfOrderPublication(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	id := store.SessionID()
	p.AcquireControl(id, "client")
	listener, stop, err := p.sessionEvents.subscribe(id)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	pause := &pausedTitleStore{generatedTitleStore: &generatedTitleStore{plugin: p, sessionID: id}, written: make(chan struct{}), resume: make(chan struct{})}
	defer func() {
		select {
		case <-pause.resume:
		default:
			close(pause.resume)
		}
	}()
	provider := &titleTestProvider{response: &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Content: "automatic"}}}}}
	var once sync.Once
	middleware := middlewares.NewTitleMiddleware(pause, &once, provider, runtimeconfig.Default(), p.sessionEventPublisher(id))
	middleware.OnAfterAgent(&core.AfterAgentEvent{History: []core.Message{{Role: "user", Content: "question"}}})
	select {
	case <-pause.written:
	case <-time.After(time.Second):
		t.Fatal("automatic write did not finish")
	}
	if err := p.SetSessionTitle(id, "client", "manual"); err != nil {
		t.Fatal(err)
	}
	close(pause.resume)
	var events []session.TitleUpdated
	for len(events) < 2 {
		select {
		case ev := <-listener.events:
			var update session.TitleUpdated
			if err := json.Unmarshal(ev.Payload, &update); err != nil {
				t.Fatal(err)
			}
			events = append(events, update)
		case <-time.After(time.Second):
			t.Fatal("title events not delivered")
		}
	}
	if events[0].Title != "manual" || events[0].TitleVersion != 2 || events[1].Title != "automatic" || events[1].TitleVersion != 1 {
		t.Fatalf("updates = %+v", events)
	}
	snapshot, err := p.GetSessionTitle(id)
	if err != nil || snapshot.Title != "manual" || snapshot.TitleVersion != 2 {
		t.Fatalf("snapshot = %+v, %v", snapshot, err)
	}
}

func TestTitleSnapshotOutsideSessionList(t *testing.T) {
	dir := t.TempDir()
	repo, err := session.OpenRepository(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	old, err := repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.AppendTitle("older"); err != nil {
		t.Fatal(err)
	}
	id := old.SessionID()
	oldTime := time.Unix(1, 0)
	if err := os.Chtimes(filepath.Join(dir, id, id+".jsonl"), oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxListSessions; i++ {
		if _, err := repo.Create(context.Background(), session.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = dir
	p := &AgentPlugin{repo: repo, cfg: cfg}
	wireTitleControl(t, p, old)
	for _, item := range p.ListSessions() {
		if item.SessionID == id {
			t.Fatal("old session unexpectedly in limited list")
		}
	}
	got, err := p.GetSessionTitle(id)
	if err != nil || got.Title != "older" || got.TitleVersion != 1 {
		t.Fatalf("snapshot = %+v, %v", got, err)
	}
}

func TestTitleSnapshotFallbackAndLastWins(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	id := store.SessionID()
	snapshot, err := p.GetSessionTitle(id)
	if err != nil || snapshot.Title != "What is the answer?" || snapshot.TitleVersion != 0 {
		t.Fatalf("fallback = %+v, %v", snapshot, err)
	}
	p.AcquireControl(id, "client")
	if err := p.SetSessionTitle(id, "client", "manual"); err != nil {
		t.Fatal(err)
	}
	state, err := (&generatedTitleStore{plugin: p, sessionID: id}).AppendTitle("late automatic")
	if err != nil || state.Version != 2 {
		t.Fatalf("append = %+v, %v", state, err)
	}
	snapshot, err = p.GetSessionTitle(id)
	if err != nil || snapshot.Title != "late automatic" || snapshot.TitleVersion != 2 {
		t.Fatalf("snapshot = %+v, %v", snapshot, err)
	}
}
