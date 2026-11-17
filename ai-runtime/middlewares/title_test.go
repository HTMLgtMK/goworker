package middlewares

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
)

type titleMemoryStore struct {
	mu     sync.Mutex
	title  string
	writes []string
	err    error
}

func (s *titleMemoryStore) Title() session.TitleState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return session.TitleState{Title: s.title}
}
func (s *titleMemoryStore) AppendTitle(title string) (session.TitleState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return session.TitleState{}, s.err
	}
	s.title = title
	s.writes = append(s.writes, title)
	return session.TitleState{Title: title, Version: uint64(len(s.writes))}, nil
}
func (s *titleMemoryStore) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.writes...)
}

type titleMiddlewareProvider struct {
	calls chan *core.ChatRequest
	title string
	err   error
}

func (*titleMiddlewareProvider) Name() string  { return "title-test" }
func (*titleMiddlewareProvider) Model() string { return "title-test" }
func (p *titleMiddlewareProvider) Chat(_ context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	p.calls <- req
	if p.err != nil {
		return nil, p.err
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Content: p.title}}}}, nil
}
func (*titleMiddlewareProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not used")
}

func TestTitleMiddlewareGeneratesOnceAndPublishesAfterAppend(t *testing.T) {
	store := &titleMemoryStore{}
	provider := &titleMiddlewareProvider{calls: make(chan *core.ChatRequest, 2), title: "  A concise\n title  "}
	published := make(chan string, 1)
	var once sync.Once
	m := NewTitleMiddleware(store, &once, provider, runtimeconfig.Default(), func(_ string, data any) { published <- data.(session.TitleUpdated).Title })
	event := &core.AfterAgentEvent{History: []core.Message{{Role: "user", Content: "first question"}, {Role: "assistant", Content: "answer"}}}
	m.OnAfterAgent(event)
	m.OnAfterAgent(event)
	select {
	case <-provider.calls:
	case <-time.After(time.Second):
		t.Fatal("provider was not called")
	}
	select {
	case got := <-published:
		if got != "A concise title" {
			t.Fatalf("published %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("title was not published")
	}
	if got := store.snapshot(); len(got) != 1 || got[0] != "A concise title" {
		t.Fatalf("stored titles=%v", got)
	}
	select {
	case <-provider.calls:
		t.Fatal("provider called more than once")
	default:
	}
}

func TestTitleMiddlewarePublishesExactVersionOnlyAfterSuccess(t *testing.T) {
	store := &titleMemoryStore{err: errors.New("write failed")}
	provider := &titleMiddlewareProvider{calls: make(chan *core.ChatRequest, 2), title: "generated"}
	var updates []session.TitleUpdated
	m := NewTitleMiddleware(store, &sync.Once{}, provider, runtimeconfig.Default(), func(_ string, data any) {
		updates = append(updates, data.(session.TitleUpdated))
	})
	m.generate("question", "answer")
	if len(updates) != 0 {
		t.Fatalf("published after failure: %+v", updates)
	}
	store.err = nil
	m.generate("question", "answer")
	if len(updates) != 1 || updates[0].Title != "generated" || updates[0].TitleVersion != 1 || updates[0].Source != "generated" {
		t.Fatalf("updates = %+v", updates)
	}
}

func TestTitleMiddlewareDoesNotConsumeOnceWithoutUser(t *testing.T) {
	store := &titleMemoryStore{}
	provider := &titleMiddlewareProvider{calls: make(chan *core.ChatRequest, 1), title: "title"}
	var once sync.Once
	m := NewTitleMiddleware(store, &once, provider, runtimeconfig.Default(), nil)
	m.OnAfterAgent(&core.AfterAgentEvent{History: []core.Message{{Role: "assistant", Content: "orphan"}}})
	m.OnAfterAgent(&core.AfterAgentEvent{History: []core.Message{{Role: "user", Content: "first"}}})
	select {
	case req := <-provider.calls:
		if !strings.Contains(req.Messages[1].Content, "first") {
			t.Fatalf("request=%+v", req)
		}
	case <-time.After(time.Second):
		t.Fatal("later user turn did not generate")
	}
}

func TestTitleMiddlewareSkipsExistingTitleAndFailedAttemptIsOnce(t *testing.T) {
	store := &titleMemoryStore{title: "persisted"}
	provider := &titleMiddlewareProvider{calls: make(chan *core.ChatRequest, 2), title: "new", err: errors.New("provider unavailable")}
	var once sync.Once
	m := NewTitleMiddleware(store, &once, provider, runtimeconfig.Default(), nil)
	event := &core.AfterAgentEvent{History: []core.Message{{Role: "user", Content: "q"}, {Role: "assistant", Content: "a"}}}
	m.OnAfterAgent(event)
	if len(provider.calls) != 0 {
		t.Fatal("existing title did not suppress generation")
	}
	store.mu.Lock()
	store.title = ""
	store.mu.Unlock()
	m.OnAfterAgent(event)
	select {
	case <-provider.calls:
	case <-time.After(time.Second):
		t.Fatal("provider was not called")
	}
	m.OnAfterAgent(event)
	select {
	case <-provider.calls:
		t.Fatal("failed attempt was retried")
	case <-time.After(20 * time.Millisecond):
	}
	if len(store.snapshot()) != 0 {
		t.Fatal("failure persisted a fallback title")
	}
}

func TestTitleMiddlewareFirstTurnMaterial(t *testing.T) {
	user, assistant, ok := firstTurnMaterial([]core.Message{{Role: "system", Content: "prompt"}, {Role: "user", Content: "one"}, {Role: "assistant", Content: "answer one"}, {Role: "tool", Content: "tool"}, {Role: "assistant", Content: "answer two"}, {Role: "user", Content: "follow-up"}})
	if !ok || user != "one" || assistant != "answer one\nanswer two" {
		t.Fatalf("material=(%q,%q,%t)", user, assistant, ok)
	}
}
