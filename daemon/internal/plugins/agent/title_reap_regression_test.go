package service

import (
	"context"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-sandbox"
)

type reapTitleProvider struct {
	calls   chan struct{}
	release chan struct{}
}

func (*reapTitleProvider) Name() string  { return "title-reap" }
func (*reapTitleProvider) Model() string { return "title-reap" }
func (p *reapTitleProvider) Chat(ctx context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.calls <- struct{}{}
	select {
	case <-p.release:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (*reapTitleProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	ch := make(chan core.Token, 1)
	ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "answer"}}}}}
	close(ch)
	return ch, nil
}
func TestAutomaticTitleOnceAcrossRuntimeReap(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	id := store.SessionID()
	p.manager = runtimeagent.NewSessionManager()
	p.current.Store(runtimeagent.NewSessionRuntime("other", nil))
	provider := &reapTitleProvider{calls: make(chan struct{}, 2), release: make(chan struct{})}
	defer close(provider.release)
	p.deps = runtimeagent.SessionDeps{Config: runtimeconfig.Default(), CollectTools: func(*sandbox.Config, runtimeagent.ToolCollectionOptions) []core.Tool { return nil }, NewProvider: func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil }}
	if _, err := p.mountRuntime(store); err != nil {
		t.Fatal(err)
	}
	run, err := p.manager.StartRun(id, runtimeagent.RunRequest{Input: "first"}, runtimeagent.RunCallbacks{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.calls:
	case <-time.After(time.Second):
		t.Fatal("title call missing")
	}
	p.tryReleaseSession(id)
	if p.manager.Get(id) != nil {
		t.Fatal("runtime was not reaped")
	}
	reopened, err := p.repo.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.mountRuntime(reopened); err != nil {
		t.Fatal(err)
	}
	run, err = p.manager.StartRun(id, runtimeagent.RunRequest{Input: "second"}, runtimeagent.RunCallbacks{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.calls:
		t.Fatal("title generated again after reap")
	case <-time.After(100 * time.Millisecond):
	}
}
