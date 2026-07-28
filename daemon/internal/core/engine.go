package core

import (
	"fmt"
	"log"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// Engine 是 goworker 的核心引擎。
//
// 职责：
//   - 插件注册与生命周期管理
//   - 命令注册与路由
//   - Tool 注册（MCP / AI）
//   - 中间件链
//   - 事件广播
type Engine struct {
	config       *config.Config
	plugins      map[string]spec.Plugin
	commands     map[string]spec.Command
	tools        map[string]spec.Tool
	interceptors []spec.PluginInterceptor
	listeners    []func(spec.Event)
}

// NewEngine 创建一个引擎并关联全局配置。
func NewEngine(cfg *config.Config) *Engine {
	return &Engine{
		config:       cfg,
		plugins:      make(map[string]spec.Plugin),
		commands:     make(map[string]spec.Command),
		tools:        make(map[string]spec.Tool),
		interceptors: make([]spec.PluginInterceptor, 0),
		listeners:    make([]func(spec.Event), 0),
	}
}

// Config 返回全局配置。
func (e *Engine) Config() *config.Config { return e.config }

// ---- Middleware ----

// Use 注册一个中间件，按注册顺序依次执行。
func (e *Engine) Use(interceptor spec.PluginInterceptor) {
	e.interceptors = append(e.interceptors, interceptor)
}

// ---- Plugin ----

// Register 注册一个插件并初始化。
func (e *Engine) Register(p spec.Plugin) error {
	name := p.Name()
	if _, exists := e.plugins[name]; exists {
		return fmt.Errorf("plugin %q already registered", name)
	}

	// 依赖检查
	if dp, ok := p.(spec.DependentPlugin); ok {
		for _, dep := range dp.Dependencies() {
			if _, exists := e.plugins[dep]; !exists {
				return fmt.Errorf("plugin %q depends on %q, not registered", name, dep)
			}
		}
	}

	// 构造 Hub 适配器，暴露有限 API 给插件
	hub := e.pluginHub()

	// Init
	if err := p.Init(hub); err != nil {
		return fmt.Errorf("plugin %q init: %w", name, err)
	}

	e.plugins[name] = p
	log.Printf("[core] plugin %q registered", name)
	return nil
}

// pluginHub 构造 *spec.Hub 适配器，将 Engine 的方法暴露给插件。
func (e *Engine) pluginHub() *spec.Hub {
	return &spec.Hub{
		RegisterCommand: func(cmd spec.Command) error {
			return e.RegisterCommand(cmd)
		},
		RegisterTool: func(tool spec.Tool) error {
			return e.RegisterTool(tool)
		},
		AddEventListener: func(fn func(spec.Event)) {
			e.listeners = append(e.listeners, fn)
		},
		Plugin: func(name string) spec.Plugin {
			return e.plugins[name]
		},
		Plugins: func() []string {
			names := make([]string, 0, len(e.plugins))
			for n := range e.plugins {
				names = append(names, n)
			}
			return names
		},
		Tools: func() []spec.Tool {
			return e.Tools()
		},
		Notify: func(event spec.Event) {
			e.Notify(event)
		},
		Eval: func(ctx *spec.Context, input string) error {
			return e.Eval(ctx, input)
		},
		Config: e.config,
		SaveConfig: func(cfg *config.Config) error {
			*e.config = *cfg // 同步内存
			return config.Save(cfg, config.DefaultPath())
		},
	}
}

// Command 注册

// RegisterCommand 注册一个命令，支持别名。
func (e *Engine) RegisterCommand(cmd spec.Command) error {
	if _, exists := e.commands[cmd.Name]; exists {
		return fmt.Errorf("command %q already registered", cmd.Name)
	}
	e.commands[cmd.Name] = cmd

	for _, alias := range cmd.Aliases {
		if _, exists := e.commands[alias]; exists {
			log.Printf("[core] alias %q conflicts, skipping", alias)
			continue
		}
		e.commands[alias] = cmd
	}

	log.Printf("[core] command %q registered", cmd.Name)
	return nil
}

// Commands 返回所有已注册的命令（去重）。
func (e *Engine) Commands() []spec.Command {
	seen := make(map[string]bool)
	out := make([]spec.Command, 0, len(e.commands))
	for _, cmd := range e.commands {
		if !seen[cmd.Name] {
			seen[cmd.Name] = true
			out = append(out, cmd)
		}
	}
	return out
}

// ---- Tool 注册 ----

func (e *Engine) RegisterTool(tool spec.Tool) error {
	if _, exists := e.tools[tool.Name]; exists {
		return fmt.Errorf("tool %q already registered", tool.Name)
	}
	e.tools[tool.Name] = tool
	log.Printf("[core] tool %q registered", tool.Name)
	return nil
}

func (e *Engine) Tools() []spec.Tool {
	out := make([]spec.Tool, 0, len(e.tools))
	for _, tool := range e.tools {
		out = append(out, tool)
	}
	return out
}

// ---- 核心执行 ----

// Eval 解析输入并执行命令，经过中间件链。
func (e *Engine) Eval(ctx *spec.Context, input string) error {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return fmt.Errorf("empty input")
	}

	name := parts[0]
	cmd, exists := e.commands[name]
	if !exists {
		return fmt.Errorf("unknown command: %q", name)
	}

	ctx.Args = parts[1:]

	// 洋葱模型：从最后一个 interceptor 开始往前层层包裹。
	// 执行时从最外层往里剥，每个 interceptor 调 next() 进入下一层，
	// next() 返回后执行收尾逻辑（剥洋葱）。
	//
	// 注册 [A, B, C] → 构建 [A → B → C → handler]
	// 执行 A → B → C → handler → C → B → A
	var next func() error

	// 最内层：curry 住 ctx，把 Handler 转换成 func() error
	next = func() error { return cmd.Handler(ctx) }

	for i := len(e.interceptors) - 1; i >= 0; i-- {
		interceptor := e.interceptors[i]
		// 用中间变量固定住 "当前 next"，防止闭包捕获循环变量
		handler := next
		next = func() error {
			return interceptor(ctx, handler)
		}
	}

	// 从最外层 interceptor 开始执行
	return next()
}

// ---- 生命周期 ----

func (e *Engine) StartAll() error {
	for name, p := range e.plugins {
		if err := p.Start(); err != nil {
			return fmt.Errorf("plugin %q start: %w", name, err)
		}
		e.Notify(spec.Event{Type: spec.EventPluginStarted, Payload: name})
		log.Printf("[core] plugin %q started", name)
	}
	return nil
}

func (e *Engine) StopAll() {
	// 逆序关闭
	order := make([]string, 0, len(e.plugins))
	for name := range e.plugins {
		order = append(order, name)
	}
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		if err := e.plugins[name].Stop(); err != nil {
			log.Printf("[core] plugin %q stop error: %v", name, err)
		}
		e.Notify(spec.Event{Type: spec.EventPluginStopped, Payload: name})
		log.Printf("[core] plugin %q stopped", name)
	}
}

// Notify 向所有 EventAwarePlugin 和外部监听者广播事件。
func (e *Engine) Notify(event spec.Event) {
	for name, p := range e.plugins {
		if ep, ok := p.(spec.EventAwarePlugin); ok {
			if err := ep.OnEvent(event); err != nil {
				log.Printf("[core] plugin %q OnEvent(%s): %v", name, event.Type, err)
			}
		}
	}
	for _, fn := range e.listeners {
		fn(event)
	}
}

// ---- 查询 ----

func (e *Engine) Plugin(name string) spec.Plugin {
	return e.plugins[name]
}

func (e *Engine) Plugins() []string {
	names := make([]string, 0, len(e.plugins))
	for n := range e.plugins {
		names = append(names, n)
	}
	return names
}

func (e *Engine) HasPlugin(name string) bool {
	_, ok := e.plugins[name]
	return ok
}
