package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

func TestSaveConfig_RollsBackOnWriteFailure(t *testing.T) {
	cfg := config.Default()
	runtimeCfg := cfg.ToRuntime()
	engine := NewEngine(cfg, runtimeCfg, nil)
	candidate := *runtimeCfg
	candidate.LLM = runtimeCfg.LLM.Clone()
	candidate.LLM.Providers["openai"] = runtimeconfig.ProviderConfig{
		Type:          runtimeconfig.ProviderTypeOpenAI,
		Endpoint:      "http://localhost:8000/v1",
		Model:         "changed",
		ContextWindow: 128000,
		Thinking: runtimeconfig.ProviderThinkingConfig{
			RequestMode: runtimeconfig.ThinkingRequestAuto,
			Effort:      runtimeconfig.ThinkingEffortMedium,
		},
	}

	beforeRuntime := *runtimeCfg
	beforeRuntime.LLM = runtimeCfg.LLM.Clone()
	beforeDaemon := cfg.LLM.Clone()
	if err := engine.saveConfig(&candidate, func(*config.Config, string) error { return errors.New("disk full") }); err == nil {
		t.Fatal("saveConfig should return write error")
	}
	if !reflect.DeepEqual(runtimeCfg.LLM, beforeRuntime.LLM) {
		t.Errorf("runtime config mutated after failed write: %#v", runtimeCfg.LLM)
	}
	if !reflect.DeepEqual(cfg.LLM, beforeDaemon) {
		t.Errorf("daemon config mutated after failed write: %#v", cfg.LLM)
	}
}

func TestRegisterPluginInitializesAndRejectsInvalidRegistrations(t *testing.T) {
	e := newTestEngine()
	missingDep := &testPlugin{name: "child", deps: []string{"base"}}
	if err := e.Register(missingDep); err == nil {
		t.Fatalf("Register missing dependency should fail")
	}

	base := &testPlugin{name: "base"}
	if err := e.Register(base); err != nil {
		t.Fatalf("Register base: %v", err)
	}
	if base.initCount != 1 || base.hub == nil {
		t.Fatalf("plugin initCount=%d hub nil=%v", base.initCount, base.hub == nil)
	}
	if got := e.Plugin("base"); got != base {
		t.Fatalf("Plugin(base) = %#v, want base", got)
	}
	if !e.HasPlugin("base") {
		t.Fatalf("HasPlugin(base) = false")
	}
	if !containsString(e.Plugins(), "base") {
		t.Fatalf("Plugins() does not contain base: %#v", e.Plugins())
	}
	if err := e.Register(base); err == nil {
		t.Fatalf("duplicate plugin should fail")
	}

	child := &testPlugin{name: "child", deps: []string{"base"}}
	if err := e.Register(child); err != nil {
		t.Fatalf("Register child: %v", err)
	}
}

func TestRegisterCommandAliasFallbackAndInterceptors(t *testing.T) {
	e := newTestEngine()
	calls := make([]string, 0)

	if err := e.RegisterCommand(model.Command{
		Name:    "/taken",
		Handler: func(ctx *model.Context) error { calls = append(calls, "taken"); return nil },
	}); err != nil {
		t.Fatalf("RegisterCommand taken: %v", err)
	}
	if err := e.RegisterCommand(model.Command{
		Name:    "/with-conflict",
		Aliases: []string{"/taken"},
		Handler: func(ctx *model.Context) error { calls = append(calls, "conflict"); return nil },
	}); err != nil {
		t.Fatalf("RegisterCommand with conflict: %v", err)
	}
	if err := e.RegisterCommand(model.Command{Name: "/taken"}); err == nil {
		t.Fatalf("duplicate command should fail")
	}

	e.Use(func(ctx *model.Context, next func() error) error {
		calls = append(calls, "a-before")
		err := next()
		calls = append(calls, "a-after")
		return err
	})
	e.Use(func(ctx *model.Context, next func() error) error {
		calls = append(calls, "b-before")
		err := next()
		calls = append(calls, "b-after")
		return err
	})
	if err := e.RegisterCommand(model.Command{
		Name:    "/hello",
		Aliases: []string{"/h"},
		Handler: func(ctx *model.Context) error {
			calls = append(calls, "handler")
			if !reflect.DeepEqual(ctx.Args, []string{"one", "two"}) {
				t.Fatalf("Args = %#v", ctx.Args)
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("RegisterCommand hello: %v", err)
	}

	ctx := model.NewContext(context.Background(), nil, nil, nil)
	if err := e.Eval(ctx, "/h one two"); err != nil {
		t.Fatalf("Eval alias: %v", err)
	}
	want := []string{"a-before", "b-before", "handler", "b-after", "a-after"}
	if !reflect.DeepEqual(calls[len(calls)-len(want):], want) {
		t.Fatalf("interceptor calls = %#v, want %#v", calls, want)
	}

	calls = calls[:0]
	if err := e.Eval(model.NewContext(context.Background(), nil, nil, nil), "/taken"); err != nil {
		t.Fatalf("Eval taken: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"a-before", "b-before", "taken", "b-after", "a-after"}) {
		t.Fatalf("alias conflict changed existing command: %#v", calls)
	}

	var fallbackArgs [][]string
	e.SetFallbackHandler(func(ctx *model.Context) error {
		fallbackArgs = append(fallbackArgs, append([]string(nil), ctx.Args...))
		return nil
	})
	if err := e.Eval(model.NewContext(context.Background(), nil, nil, nil), "natural language"); err != nil {
		t.Fatalf("Eval natural fallback: %v", err)
	}
	if err := e.Eval(model.NewContext(context.Background(), nil, nil, nil), "/missing arg"); err != nil {
		t.Fatalf("Eval slash fallback: %v", err)
	}
	if !reflect.DeepEqual(fallbackArgs, [][]string{{"natural", "language"}, {"arg"}}) {
		t.Fatalf("fallback args = %#v", fallbackArgs)
	}
}

func TestLifecycleEventsAndToolSorting(t *testing.T) {
	e := newTestEngine()
	base := &testPlugin{name: "base"}
	child := &testPlugin{name: "child", deps: []string{"base"}}
	if err := e.Register(base); err != nil {
		t.Fatalf("Register base: %v", err)
	}
	if err := e.Register(child); err != nil {
		t.Fatalf("Register child: %v", err)
	}

	var listenerEvents []model.Event
	base.hub.AddEventListener(func(event model.Event) {
		listenerEvents = append(listenerEvents, event)
	})
	if err := base.hub.RegisterTool(model.Tool{Name: "z"}); err != nil {
		t.Fatalf("RegisterTool z: %v", err)
	}
	if err := base.hub.RegisterTool(model.Tool{Name: "a"}); err != nil {
		t.Fatalf("RegisterTool a: %v", err)
	}
	if err := base.hub.RegisterTool(model.Tool{Name: "z"}); err == nil {
		t.Fatalf("duplicate tool should fail")
	}

	tools := e.Tools()
	if got := []string{tools[0].Name, tools[1].Name}; !reflect.DeepEqual(got, []string{"a", "z"}) {
		t.Fatalf("tool order = %#v", got)
	}
	if err := e.StartAll(); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	e.StopAll()
	if base.startCount != 1 || base.stopCount != 1 || child.startCount != 1 || child.stopCount != 1 {
		t.Fatalf("base start=%d stop=%d child start=%d stop=%d", base.startCount, base.stopCount, child.startCount, child.stopCount)
	}
	if eventCount(listenerEvents, model.EventPluginStarted) != 2 || eventCount(listenerEvents, model.EventPluginStopped) != 2 {
		t.Fatalf("listener events = %#v", listenerEvents)
	}
	for _, registeredPlugin := range []*testPlugin{base, child} {
		if eventCount(registeredPlugin.events, model.EventPluginStarted) != 2 || eventCount(registeredPlugin.events, model.EventPluginStopped) != 2 {
			t.Fatalf("%s events = %#v", registeredPlugin.name, registeredPlugin.events)
		}
	}
}

func newTestEngine() *Engine {
	cfg := config.Default()
	return NewEngine(cfg, cfg.ToRuntime(), nil)
}

type testPlugin struct {
	name       string
	deps       []string
	hub        *model.Hub
	initCount  int
	startCount int
	stopCount  int
	events     []model.Event
}

func (p *testPlugin) Name() string { return p.name }

func (p *testPlugin) Init(h *model.Hub) error {
	p.initCount++
	p.hub = h
	return nil
}

func (p *testPlugin) Start() error {
	p.startCount++
	return nil
}

func (p *testPlugin) Stop() error {
	p.stopCount++
	return nil
}

func (p *testPlugin) Dependencies() []string { return p.deps }

func (p *testPlugin) OnEvent(event model.Event) error {
	p.events = append(p.events, event)
	return nil
}

func eventCount(events []model.Event, typ model.EventType) int {
	count := 0
	for _, event := range events {
		if event.Type == typ {
			count++
		}
	}
	return count
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
