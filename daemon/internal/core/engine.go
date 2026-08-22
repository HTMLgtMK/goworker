package core

import (
	"fmt"
	"sort"
	"strings"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/plugin"
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
	config          *config.Config
	runtimeCfg      *runtimeconfig.Config
	log             *logger.Logger
	plugins         map[string]plugin.Plugin
	commands        map[string]plugin.Command
	tools           map[string]plugin.Tool
	interceptors    []plugin.PluginInterceptor
	listeners       []func(plugin.Event)
	fallbackHandler func(ctx *plugin.Context) error // 未匹配命令的兜底处理器
}

// NewEngine 创建一个引擎并关联全局配置与日志器。
// l 为 nil 时回落到 stderr logger，避免调用方误传 nil 导致内部 panic。
func NewEngine(cfg *config.Config, runtimeCfg *runtimeconfig.Config, l *logger.Logger) *Engine {
	if l == nil {
		l, _ = logger.Setup(logger.Default()) // 空 File 配置必然成功
	}
	if runtimeCfg == nil {
		runtimeCfg = cfg.ToRuntime()
	}
	return &Engine{
		config:       cfg,
		runtimeCfg:   runtimeCfg,
		log:          l,
		plugins:      make(map[string]plugin.Plugin),
		commands:     make(map[string]plugin.Command),
		tools:        make(map[string]plugin.Tool),
		interceptors: make([]plugin.PluginInterceptor, 0),
		listeners:    make([]func(plugin.Event), 0),
	}
}

// Config 返回全局配置。
func (e *Engine) Config() *config.Config { return e.config }

// SaveConfig 持久化配置到 YAML 文件并同步内存。
// cfg 是插件注入的 ai-runtime 配置（plugin.Hub.Config 已 any 化），
// 回写解析层后整份落盘，保证 runtime→daemon→磁盘三处一致。
func (e *Engine) SaveConfig(cfg any) error {
	rc, ok := cfg.(*runtimeconfig.Config)
	if !ok {
		return fmt.Errorf("SaveConfig: unexpected config type %T", cfg)
	}
	*e.runtimeCfg = *rc
	e.config.ApplyRuntime(rc)
	return config.Save(e.config, config.DefaultPath())
}

// SetFallbackHandler 设置未匹配命令的兜底处理器。
// 当用户输入不是任何已注册命令时，引擎会调用此 handler 而非返回错误。
func (e *Engine) SetFallbackHandler(fn func(ctx *plugin.Context) error) {
	e.fallbackHandler = fn
}

// ---- Middleware ----

// Use 注册一个中间件，按注册顺序依次执行。
func (e *Engine) Use(interceptor plugin.PluginInterceptor) {
	e.interceptors = append(e.interceptors, interceptor)
}

// ---- Plugin ----

// Register 注册一个插件并初始化。
func (e *Engine) Register(p plugin.Plugin) error {
	name := p.Name()
	if _, exists := e.plugins[name]; exists {
		return fmt.Errorf("plugin %q already registered", name)
	}

	// 依赖检查
	if dp, ok := p.(plugin.DependentPlugin); ok {
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
	e.log.Info("plugin registered", "name", name)
	return nil
}

// pluginHub 构造 *plugin.Hub 适配器，将 Engine 的方法暴露给插件。
func (e *Engine) pluginHub() *plugin.Hub {
	return &plugin.Hub{
		RegisterCommand: func(cmd plugin.Command) error {
			return e.RegisterCommand(cmd)
		},
		RegisterTool: func(tool plugin.Tool) error {
			return e.RegisterTool(tool)
		},
		AddEventListener: func(fn func(plugin.Event)) {
			e.listeners = append(e.listeners, fn)
		},
		Plugin: func(name string) plugin.Plugin {
			return e.plugins[name]
		},
		Plugins: func() []string {
			names := make([]string, 0, len(e.plugins))
			for n := range e.plugins {
				names = append(names, n)
			}
			return names
		},
		Tools: func() []plugin.Tool {
			return e.Tools()
		},
		Notify: func(event plugin.Event) {
			e.Notify(event)
		},
		Eval: func(ctx *plugin.Context, input string) error {
			return e.Eval(ctx, input)
		},
		Config:     e.runtimeCfg,
		SaveConfig: e.SaveConfig,
		SetFallbackHandler: func(fn func(ctx *plugin.Context) error) {
			e.SetFallbackHandler(fn)
		},
	}
}

// Command 注册

// RegisterCommand 注册一个命令，支持别名。
func (e *Engine) RegisterCommand(cmd plugin.Command) error {
	if _, exists := e.commands[cmd.Name]; exists {
		return fmt.Errorf("command %q already registered", cmd.Name)
	}
	e.commands[cmd.Name] = cmd

	for _, alias := range cmd.Aliases {
		if _, exists := e.commands[alias]; exists {
			e.log.Warn("alias conflicts, skipping", "alias", alias)
			continue
		}
		e.commands[alias] = cmd
	}

	e.log.Info("command registered", "command", cmd.Name)
	return nil
}

// Commands 返回所有已注册的命令（去重）。
func (e *Engine) Commands() []plugin.Command {
	seen := make(map[string]bool)
	out := make([]plugin.Command, 0, len(e.commands))
	for _, cmd := range e.commands {
		if !seen[cmd.Name] {
			seen[cmd.Name] = true
			out = append(out, cmd)
		}
	}
	return out
}

// ---- Tool 注册 ----

func (e *Engine) RegisterTool(tool plugin.Tool) error {
	if _, exists := e.tools[tool.Name]; exists {
		return fmt.Errorf("tool %q already registered", tool.Name)
	}
	e.tools[tool.Name] = tool
	e.log.Info("tool registered", "tool", tool.Name)
	return nil
}

func (e *Engine) Tools() []plugin.Tool {
	out := make([]plugin.Tool, 0, len(e.tools))
	for _, tool := range e.tools {
		out = append(out, tool)
	}
	// map 迭代顺序随机 → 排序保证稳定。工具清单会进 system prompt 与请求 tools 字段，
	// 顺序一旦抖动，整条前缀就变，LLM 前缀缓存全部击穿 —— 排序的代价远小于此。
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ---- 核心执行 ----

// Eval 解析输入并执行命令，经过中间件链。
func (e *Engine) Eval(ctx *plugin.Context, input string) error {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return fmt.Errorf("empty input")
	}

	name := parts[0]
	cmd, exists := e.commands[name]
	if !exists {
		if e.fallbackHandler != nil {
			// 不以 / 开头的是自然语言输入，把全部内容传给 fallback
			if strings.HasPrefix(input, "/") {
				ctx.Args = parts[1:]
			} else {
				ctx.Args = parts
			}
			return e.fallbackHandler(ctx)
		}
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
		e.Notify(plugin.Event{Type: plugin.EventPluginStarted, Payload: name})
		e.log.Info("plugin started", "name", name)
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
			e.log.Error("plugin stop error", "name", name, "error", err)
		}
		e.Notify(plugin.Event{Type: plugin.EventPluginStopped, Payload: name})
		e.log.Info("plugin stopped", "name", name)
	}
}

// Notify 向所有 EventAwarePlugin 和外部监听者广播事件。
func (e *Engine) Notify(event plugin.Event) {
	for name, p := range e.plugins {
		if ep, ok := p.(plugin.EventAwarePlugin); ok {
			if err := ep.OnEvent(event); err != nil {
				e.log.Error("plugin OnEvent error", "name", name, "event", event.Type, "error", err)
			}
		}
	}
	for _, fn := range e.listeners {
		fn(event)
	}
}

// ---- 查询 ----

func (e *Engine) Plugin(name string) plugin.Plugin {
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
