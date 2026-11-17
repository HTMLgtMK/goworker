package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/middlewares"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

func TestGeneratedTitleStoreDoesNotReopenAfterStopBegins(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	if err := p.repo.CloseSession(store.SessionID()); err != nil {
		t.Fatal(err)
	}

	p.inFlight.closeAdmission()
	adapter := &generatedTitleStore{plugin: p, sessionID: store.SessionID()}
	if _, err := adapter.AppendTitle("too late"); err == nil {
		t.Fatal("title append succeeded after Stop closed admission")
	}
	if _, ok := p.repo.Peek(store.SessionID()); ok {
		t.Fatal("title adapter reopened the session store after Stop began")
	}
}

func TestAutomaticTitlePersistsAfterSessionReap(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	p.manager = runtimeagent.NewSessionManager()
	p.current.Store(runtimeagent.NewSessionRuntime("current", nil))
	if _, err := p.mountRuntime(store); err != nil {
		t.Fatal(err)
	}

	provider := &blockingTitleProvider{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		response: &core.ChatResponse{Choices: []core.ResponseChoice{{
			Message: core.Message{Content: "Generated after detach"},
		}}},
	}
	published := make(chan struct{}, 1)
	var once sync.Once
	middleware := middlewares.NewTitleMiddleware(
		&generatedTitleStore{plugin: p, sessionID: store.SessionID()},
		&once,
		provider,
		runtimeconfig.Default(),
		func(string, any) { published <- struct{}{} },
	)
	middleware.OnAfterAgent(&core.AfterAgentEvent{History: []core.Message{
		{Role: "user", Content: "first question"},
		{Role: "assistant", Content: "first answer"},
	}})
	select {
	case <-provider.entered:
	case <-time.After(time.Second):
		t.Fatal("title provider was not called")
	}

	p.tryReleaseSession(store.SessionID())
	if p.manager.Get(store.SessionID()) != nil {
		t.Fatal("session runtime was not reaped")
	}
	if _, ok := p.repo.Peek(store.SessionID()); ok {
		t.Fatal("reaped session store is still cached")
	}

	close(provider.release)
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("generated title was not persisted and published after reap")
	}

	reopened, err := p.repo.Open(store.SessionID())
	if err != nil {
		t.Fatalf("reopen session after generated title: %v", err)
	}
	if reopened == store {
		t.Fatal("reopen returned the retired store instance")
	}
	if got := reopened.Title().Title; got != "Generated after detach" {
		t.Fatalf("reopened title = %q, want generated title", got)
	}
	if err := p.repo.CloseSession(store.SessionID()); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}

type blockingTitleProvider struct {
	entered  chan struct{}
	release  chan struct{}
	response *core.ChatResponse
}

func (*blockingTitleProvider) Name() string  { return "blocking-title-test" }
func (*blockingTitleProvider) Model() string { return "blocking-title-test" }
func (p *blockingTitleProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	close(p.entered)
	<-p.release
	return p.response, nil
}
func (*blockingTitleProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("streaming is not used for title generation")
}

func TestGenerateSessionTitleUsesSharedGenerator(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	p.AcquireControl(store.SessionID(), "client-1")
	provider := &titleTestProvider{response: &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Content: " Generated title "}}}}}
	p.cfg = runtimeconfig.Default()
	p.providerFactory = func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil }
	title, err := p.GenerateSessionTitle(store.SessionID(), "client-1")
	if err != nil {
		t.Fatal(err)
	}
	if title != "Generated title" || store.Title().Title != title {
		t.Fatalf("generated=%q stored=%+v", title, store.Title())
	}
}

func TestGenerateSessionTitleIgnoresConcurrentSessionDeps(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	p.AcquireControl(store.SessionID(), "client-1")
	p.cfg = runtimeconfig.Default()
	provider := &titleTestProvider{response: &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Content: "stable title"}}}}}
	p.providerFactory = func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil }

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			p.deps.NewProvider = nil
		}
	}()
	for i := 0; i < 20; i++ {
		title, err := p.GenerateSessionTitle(store.SessionID(), "client-1")
		if err != nil || title != "stable title" {
			<-done
			t.Fatalf("GenerateSessionTitle = %q, %v", title, err)
		}
	}
	<-done
}

func TestGenerateSessionTitleDuringSessionRebuild(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	defer p.manager.Close()
	p.AcquireControl(store.SessionID(), "client-1")
	p.cfg = runtimeconfig.Default()
	p.cfg.Memory.Enabled = false
	p.current.Store(runtimeagent.NewSessionRuntime("current", nil))
	provider := &titleTestProvider{response: &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Content: "stable title"}}}}}
	p.providerFactory = func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil }

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.startSession()
	}()
	for i := 0; i < 20; i++ {
		title, err := p.GenerateSessionTitle(store.SessionID(), "client-1")
		if err != nil || title != "stable title" {
			<-done
			t.Fatalf("GenerateSessionTitle = %q, %v", title, err)
		}
	}
	<-done
}

func TestGenerateSessionTitleUsesOneConfigSnapshot(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	p.AcquireControl(store.SessionID(), "client-1")
	first, second := runtimeconfig.Default(), runtimeconfig.Default()
	first.LLM.Current.Model, second.LLM.Current.Model = "before", "after"
	var current atomic.Pointer[runtimeconfig.Config]
	current.Store(first)
	p.hub = &model.Hub{ConfigSnapshot: current.Load}
	provider := &snapshotTitleProvider{model: make(chan string, 1), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-provider.release:
		default:
			close(provider.release)
		}
	})
	factoryModel := make(chan string, 1)
	p.providerFactory = func(cfg *runtimeconfig.Config) (core.Provider, error) {
		factoryModel <- cfg.LLM.Current.Model
		return provider, nil
	}
	result := make(chan error, 1)
	go func() {
		_, err := p.GenerateSessionTitle(store.SessionID(), "client-1")
		result <- err
	}()
	select {
	case got := <-provider.model:
		if got != "before" || <-factoryModel != "before" {
			t.Fatalf("generation did not use the original model: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("provider was not called")
	}
	current.Store(second)
	close(provider.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

type snapshotTitleProvider struct {
	model   chan string
	release chan struct{}
}

func (*snapshotTitleProvider) Name() string  { return "title-snapshot" }
func (*snapshotTitleProvider) Model() string { return "title-snapshot" }
func (p *snapshotTitleProvider) Chat(_ context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	p.model <- req.Model
	<-p.release
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Content: "title"}}}}, nil
}
func (*snapshotTitleProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("streaming is not used for title generation")
}

func TestGenerateSessionTitleRechecksLeaseAfterGeneration(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	p.AcquireControl(store.SessionID(), "client-1")
	p.cfg = runtimeconfig.Default()
	provider := &snapshotTitleProvider{model: make(chan string, 1), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-provider.release:
		default:
			close(provider.release)
		}
	})
	p.providerFactory = func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil }
	result := make(chan error, 1)
	go func() {
		_, err := p.GenerateSessionTitle(store.SessionID(), "client-1")
		result <- err
	}()
	select {
	case <-provider.model:
	case <-time.After(time.Second):
		t.Fatal("provider was not called")
	}
	p.AcquireControl(store.SessionID(), "client-2")
	close(provider.release)
	if err := <-result; !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("GenerateSessionTitle error = %v, want lease lost", err)
	}
	if got := store.Title().Title; got != "" {
		t.Fatalf("title after losing lease = %q, want empty", got)
	}
}

func TestTitleLeaseAndValidation(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	if err := p.SetSessionTitle(store.SessionID(), "stale", "No"); err == nil {
		t.Fatal("stale lease accepted")
	}
	if err := p.SetSessionTitle(store.SessionID(), "client-1", "  "); err == nil {
		t.Fatal("empty title accepted")
	}
	if err := p.SetSessionTitle(store.SessionID(), "client-1", strings.Repeat("界", 81)); err == nil {
		t.Fatal("oversized title accepted")
	}
	p.AcquireControl(store.SessionID(), "client-1")
	if err := p.SetSessionTitle(store.SessionID(), "client-1", " Manual title "); err != nil {
		t.Fatal(err)
	}
	if got := store.Title().Title; got != "Manual title" {
		t.Fatalf("title = %q", got)
	}
}

func TestGenerateSessionTitleRequiresLeaseAndMaterial(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	if _, err := p.GenerateSessionTitle(store.SessionID(), "stale"); err == nil {
		t.Fatal("generate without lease succeeded")
	}
	p.AcquireControl(store.SessionID(), "client-1")
	providerCalled := false
	p.cfg = runtimeconfig.Default()
	p.providerFactory = func(*runtimeconfig.Config) (core.Provider, error) {
		providerCalled = true
		return &titleTestProvider{}, nil
	}
	empty, err := p.repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p.AcquireControl(empty.SessionID(), "client-1")
	if _, err := p.GenerateSessionTitle(empty.SessionID(), "client-1"); err == nil {
		t.Fatal("generate without material succeeded")
	}
	if providerCalled {
		t.Fatal("provider created without material")
	}
}

func TestStoreTitleIsPureLastWins(t *testing.T) {
	_, store := newTitleTestPlugin(t)
	for _, tc := range []struct {
		writes []string
		want   string
	}{
		{writes: []string{"generated", "manual"}, want: "manual"},
		{writes: []string{"manual", "late generated"}, want: "late generated"},
	} {
		for _, title := range tc.writes {
			if _, err := store.AppendTitle(title); err != nil {
				t.Fatal(err)
			}
		}
		if got := store.Title().Title; got != tc.want {
			t.Fatalf("Title() = %q, want %q", got, tc.want)
		}
	}
}

func wireTitleControl(t *testing.T, p *AgentPlugin, store *session.Store) {
	t.Helper()
	p.manager = runtimeagent.NewSessionManager()
	if err := p.manager.Register(runtimeagent.NewSessionRuntime(store.SessionID(), nil)); err != nil {
		t.Fatal(err)
	}
	p.control = agentcontrol.NewService(p.manager, p, p)
}

type titleTestProvider struct {
	request  *core.ChatRequest
	response *core.ChatResponse
}

func (*titleTestProvider) Name() string  { return "title-test" }
func (*titleTestProvider) Model() string { return "title-test-model" }
func (p *titleTestProvider) Chat(_ context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	p.request = req
	if p.response == nil {
		return nil, errors.New("no configured response")
	}
	return p.response, nil
}
func (*titleTestProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("streaming is not used for title generation")
}

func newTitleTestPlugin(t *testing.T) (*AgentPlugin, *session.Store) {
	t.Helper()
	repo, err := session.OpenRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := repo.Close(); err != nil {
			t.Errorf("close repository: %v", err)
		}
	})
	store, err := repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit([]core.Message{{MsgID: "title-user-1", Role: "user", Content: "What is the answer?"}, {MsgID: "title-assistant-1", Role: "assistant", Content: "Here it is."}}); err != nil {
		t.Fatal(err)
	}
	return &AgentPlugin{repo: repo}, store
}

func TestGenerateTitleBoundsInputAndOutput(t *testing.T) {
	provider := &titleTestProvider{response: &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Content: " `" + strings.Repeat("界", 85) + "` "}}}}}
	cfg := runtimeconfig.Default()
	cfg.LLM.Current.Model = "snapshot-model"
	title, err := middlewares.GenerateTitle(context.Background(), provider, cfg, strings.Repeat("用", 2004), strings.Repeat("答", 2004))
	if err != nil {
		t.Fatal(err)
	}
	if len([]rune(title)) != 80 {
		t.Fatalf("rune length=%d", len([]rune(title)))
	}
	if provider.request.Model != "snapshot-model" {
		t.Fatalf("model=%q", provider.request.Model)
	}
	prompt := provider.request.Messages[1].Content
	if strings.Count(prompt, "用") != 2000 || strings.Count(prompt, "答") != 2000 {
		t.Fatal("input was not truncated to 2000 runes")
	}
}
