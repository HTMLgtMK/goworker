package core

import (
	"context"
	"reflect"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

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

	if err := e.RegisterCommand(plugin.Command{
		Name:    "/taken",
		Handler: func(ctx *plugin.Context) error { calls = append(calls, "taken"); return nil },
	}); err != nil {
		t.Fatalf("RegisterCommand taken: %v", err)
	}
	if err := e.RegisterCommand(plugin.Command{
		Name:    "/with-conflict",
		Aliases: []string{"/taken"},
		Handler: func(ctx *plugin.Context) error { calls = append(calls, "conflict"); return nil },
	}); err != nil {
		t.Fatalf("RegisterCommand with conflict: %v", err)
	}
	if err := e.RegisterCommand(plugin.Command{Name: "/taken"}); err == nil {
		t.Fatalf("duplicate command should fail")
	}

	e.Use(func(ctx *plugin.Context, next func() error) error {
		calls = append(calls, "a-before")
		err := next()
		calls = append(calls, "a-after")
		return err
	})
	e.Use(func(ctx *plugin.Context, next func() error) error {
		calls = append(calls, "b-before")
		err := next()
		calls = append(calls, "b-after")
		return err
	})
	if err := e.RegisterCommand(plugin.Command{
		Name:    "/hello",
		Aliases: []string{"/h"},
		Handler: func(ctx *plugin.Context) error {
			calls = append(calls, "handler")
			if !reflect.DeepEqual(ctx.Args, []string{"one", "two"}) {
				t.Fatalf("Args = %#v", ctx.Args)
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("RegisterCommand hello: %v", err)
	}

	ctx := plugin.NewContext(context.Background(), nil, nil, nil)
	if err := e.Eval(ctx, "/h one two"); err != nil {
		t.Fatalf("Eval alias: %v", err)
	}
	want := []string{"a-before", "b-before", "handler", "b-after", "a-after"}
	if !reflect.DeepEqual(calls[len(calls)-len(want):], want) {
		t.Fatalf("interceptor calls = %#v, want %#v", calls, want)
	}

	calls = calls[:0]
	if err := e.Eval(plugin.NewContext(context.Background(), nil, nil, nil), "/taken"); err != nil {
		t.Fatalf("Eval taken: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"a-before", "b-before", "taken", "b-after", "a-after"}) {
		t.Fatalf("alias conflict changed existing command: %#v", calls)
	}

	var fallbackArgs [][]string
	e.SetFallbackHandler(func(ctx *plugin.Context) error {
		fallbackArgs = append(fallbackArgs, append([]string(nil), ctx.Args...))
		return nil
	})
	if err := e.Eval(plugin.NewContext(context.Background(), nil, nil, nil), "natural language"); err != nil {
		t.Fatalf("Eval natural fallback: %v", err)
	}
	if err := e.Eval(plugin.NewContext(context.Background(), nil, nil, nil), "/missing arg"); err != nil {
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

	var listenerEvents []plugin.Event
	base.hub.AddEventListener(func(event plugin.Event) {
		listenerEvents = append(listenerEvents, event)
	})
	if err := base.hub.RegisterTool(plugin.Tool{Name: "z"}); err != nil {
		t.Fatalf("RegisterTool z: %v", err)
	}
	if err := base.hub.RegisterTool(plugin.Tool{Name: "a"}); err != nil {
		t.Fatalf("RegisterTool a: %v", err)
	}
	if err := base.hub.RegisterTool(plugin.Tool{Name: "z"}); err == nil {
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
	if eventCount(listenerEvents, plugin.EventPluginStarted) != 2 || eventCount(listenerEvents, plugin.EventPluginStopped) != 2 {
		t.Fatalf("listener events = %#v", listenerEvents)
	}
	for _, registeredPlugin := range []*testPlugin{base, child} {
		if eventCount(registeredPlugin.events, plugin.EventPluginStarted) != 2 || eventCount(registeredPlugin.events, plugin.EventPluginStopped) != 2 {
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
	hub        *plugin.Hub
	initCount  int
	startCount int
	stopCount  int
	events     []plugin.Event
}

func (p *testPlugin) Name() string { return p.name }

func (p *testPlugin) Init(h *plugin.Hub) error {
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

func (p *testPlugin) OnEvent(event plugin.Event) error {
	p.events = append(p.events, event)
	return nil
}

func eventCount(events []plugin.Event, typ plugin.EventType) int {
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
