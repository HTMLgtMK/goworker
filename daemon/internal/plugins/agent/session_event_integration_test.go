package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
)

func TestGeneratedTitleReachesReattachedPluginSubscriber(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	id := store.SessionID()
	p.manager = runtimeagent.NewSessionManager()
	p.control = agentcontrol.NewService(p.manager, p, p)
	p.current.Store(runtimeagent.NewSessionRuntime("current", nil))
	provider := &runAndBlockingTitleProvider{blockingTitleProvider: blockingTitleProvider{
		entered: make(chan struct{}), release: make(chan struct{}),
		response: &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Content: "new title"}}}},
	}}
	t.Cleanup(func() {
		select {
		case <-provider.release:
		default:
			close(provider.release)
		}
	})
	p.deps = runtimeagent.SessionDeps{
		Config:       runtimeconfig.Default(),
		CollectTools: func(*sandbox.Config, runtimeagent.ToolCollectionOptions) []core.Tool { return nil },
		NewProvider:  func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil },
	}
	if _, err := p.mountRuntime(store); err != nil {
		t.Fatal(err)
	}
	run, err := p.manager.StartRun(id, runtimeagent.RunRequest{Input: "hello"}, runtimeagent.RunCallbacks{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.entered:
	case <-time.After(time.Second):
		t.Fatal("title generation did not start")
	}
	if _, err := run.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.tryReleaseSession(id)
	if p.RouteSession(id) != nil {
		t.Fatal("runtime was not released")
	}
	reopened, err := p.repo.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.mountRuntime(reopened); err != nil {
		t.Fatal(err)
	}
	attached, err := p.SubscribeEvents(id, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer attached.Subscription.Close()
	close(provider.release)
	deadline := time.After(2 * time.Second)
	var lastSeq uint64
	titleSeen := false
	if attached.LatestSeq == 0 {
		t.Fatal("run did not produce replayable events")
	}
	for {
		select {
		case ev, ok := <-attached.Subscription.Events():
			if !ok {
				t.Fatal("subscription closed before title update")
			}
			if ev.Type != session.EventSessionTitleUpdated {
				if ev.Seq != lastSeq+1 {
					t.Fatalf("run event seq=%d after %d", ev.Seq, lastSeq)
				}
				lastSeq = ev.Seq
			} else {
				if ev.SessionID != id || ev.Seq != 0 || titleSeen {
					t.Fatalf("unexpected event: %+v", ev)
				}
				var payload session.TitleUpdated
				if err := json.Unmarshal(ev.Payload, &payload); err != nil || payload.Title != "new title" || payload.TitleVersion != 1 || payload.Source != "generated" {
					t.Fatalf("payload=%s err=%v", ev.Payload, err)
				}
				if got := reopened.Title().Title; got != "new title" {
					t.Fatalf("stored title=%q", got)
				}
				titleSeen = true
			}
			if titleSeen && lastSeq >= attached.LatestSeq {
				return
			}
		case <-deadline:
			t.Fatal("reattached subscriber missed title update or run replay")
		}
	}
}

func TestFailedAttachReleasesPluginSubscription(t *testing.T) {
	p := &AgentPlugin{manager: runtimeagent.NewSessionManager()}
	p.control = agentcontrol.NewService(p.manager, p, p)
	if _, err := p.SubscribeEvents("missing", 0); err == nil {
		t.Fatal("unknown session unexpectedly subscribed")
	}
	p.sessionEvents.mu.Lock()
	defer p.sessionEvents.mu.Unlock()
	if n := len(p.sessionEvents.subs); n != 0 {
		t.Fatalf("failed attach left %d plugin subscriptions", n)
	}
}

func TestManualTitleUsesPluginEventSubscription(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	id := store.SessionID()
	p.manager = runtimeagent.NewSessionManager()
	p.control = agentcontrol.NewService(p.manager, p, p)
	if _, err := p.mountRuntime(store); err != nil {
		t.Fatal(err)
	}
	attached, err := p.SubscribeEvents(id, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer attached.Subscription.Close()
	p.AcquireControl(id, "client")
	if err := p.SetSessionTitle(id, "client", "manual title"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev, ok := <-attached.Subscription.Events():
		if !ok || ev.SessionID != id || ev.Seq != 0 || ev.Type != session.EventSessionTitleUpdated {
			t.Fatalf("unexpected title event: %+v, open=%v", ev, ok)
		}
		var payload session.TitleUpdated
		if err := json.Unmarshal(ev.Payload, &payload); err != nil || payload.Title != "manual title" || payload.Source != "manual" || payload.TitleVersion != 1 {
			t.Fatalf("payload=%s err=%v", ev.Payload, err)
		}
	case <-time.After(time.Second):
		t.Fatal("manual title event not delivered")
	}
	if latest := p.RouteSession(id).Hub().LatestSeq(); latest != 0 {
		t.Fatalf("title update advanced run cursor to %d", latest)
	}
}

type runAndBlockingTitleProvider struct{ blockingTitleProvider }

func (*runAndBlockingTitleProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	ch := make(chan core.Token, 1)
	ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
		Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "reply"}}},
	}}
	close(ch)
	return ch, nil
}
