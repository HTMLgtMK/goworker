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

func TestCapabilityRegistrationAndLookup(t *testing.T) {
	e := newTestEngine()
	plugin := &testPlugin{name: "provider"}
	if err := e.Register(plugin); err != nil {
		t.Fatalf("Register: %v", err)
	}

	capability := &struct{ ready bool }{ready: true}
	if err := plugin.hub.RegisterCapability("session.source", capability); err != nil {
		t.Fatalf("RegisterCapability: %v", err)
	}
	if err := plugin.hub.RegisterCapability("session.source", capability); err == nil {
		t.Fatal("duplicate capability should fail")
	}
	got, ok := plugin.hub.Capability("session.source")
	if !ok || got != capability {
		t.Fatalf("Capability() = (%#v, %v), want (%#v, true)", got, ok, capability)
	}
	plugin.hub.UnregisterCapability("session.source")
	if _, ok := e.Capability("session.source"); ok {
		t.Fatal("unregistered capability is still available")
	}
}

func TestLifecycleUsesRegistrationOrderAndRollsBack(t *testing.T) {
	e := newTestEngine()
	var starts, stops []string
	base := &lifecyclePlugin{name: "base", starts: &starts, stops: &stops}
	child := &lifecyclePlugin{name: "child", starts: &starts, stops: &stops}
	if err := e.Register(base); err != nil {
		t.Fatalf("Register base: %v", err)
	}
	if err := e.Register(child); err != nil {
		t.Fatalf("Register child: %v", err)
	}
	if err := e.StartAll(); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	if !reflect.DeepEqual(starts, []string{"base", "child"}) {
		t.Fatalf("start order = %#v, want [base child]", starts)
	}
	e.StopAll()
	if !reflect.DeepEqual(stops, []string{"child", "base"}) {
		t.Fatalf("stop order = %#v, want [child base]", stops)
	}
	e.StopAll()
	if !reflect.DeepEqual(stops, []string{"child", "base"}) {
		t.Fatalf("StopAll is not idempotent: %#v", stops)
	}
	if err := e.StartAll(); err == nil {
		t.Fatal("StartAll after StopAll should fail")
	}

	e = newTestEngine()
	starts = nil
	stops = nil
	good := &lifecyclePlugin{name: "good", starts: &starts, stops: &stops}
	failed := &lifecyclePlugin{name: "failed", failStart: true, starts: &starts, stops: &stops}
	var stoppedEvents []string
	e.AddEventListener(func(event model.Event) {
		if event.Type == model.EventPluginStopped {
			stoppedEvents = append(stoppedEvents, event.Payload.(string))
		}
	})
	if err := e.Register(good); err != nil {
		t.Fatalf("Register good: %v", err)
	}
	if err := e.Register(failed); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if err := e.StartAll(); err == nil {
		t.Fatal("StartAll should return start failure")
	}
	if !reflect.DeepEqual(stops, []string{"failed", "good"}) {
		t.Fatalf("rollback stop order = %#v, want [failed good]", stops)
	}
	if !reflect.DeepEqual(stoppedEvents, []string{"good"}) {
		t.Fatalf("rollback stopped events = %#v, want [good]", stoppedEvents)
	}
	if err := e.StartAll(); err == nil {
		t.Fatal("StartAll after rollback should fail")
	}

	e = newTestEngine()
	starts = nil
	stops = nil
	retry := &lifecyclePlugin{name: "retry", failStop: true, starts: &starts, stops: &stops}
	if err := e.Register(retry); err != nil {
		t.Fatalf("Register retry: %v", err)
	}
	if err := e.StartAll(); err != nil {
		t.Fatalf("StartAll retry: %v", err)
	}
	e.StopAll()
	e.StopAll()
	if !reflect.DeepEqual(stops, []string{"retry", "retry"}) {
		t.Fatalf("failed stop should retry: %#v", stops)
	}
	retry.failStop = false
	e.StopAll()
	if !reflect.DeepEqual(stops, []string{"retry", "retry", "retry"}) {
		t.Fatalf("successful stop should be retained: %#v", stops)
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

type lifecyclePlugin struct {
	name      string
	failStart bool
	failStop  bool
	starts    *[]string
	stops     *[]string
}

func (p *lifecyclePlugin) Name() string { return p.name }

func (p *lifecyclePlugin) Init(*model.Hub) error { return nil }

func (p *lifecyclePlugin) Start() error {
	*p.starts = append(*p.starts, p.name)
	if p.failStart {
		return errors.New("start failed")
	}
	return nil
}

func (p *lifecyclePlugin) Stop() error {
	*p.stops = append(*p.stops, p.name)
	if p.failStop {
		return errors.New("stop failed")
	}
	return nil
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
