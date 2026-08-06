// Package statusbar 提供可插拔的状态栏编排器。
//
// 用法：
//
//	bar := statusbar.New()
//	bar.Use(addon1, addon2)
//	bar.Start()
//	go bar.Run(ctx, pauseFn)
//	// ... agent 运行 ...
//	bar.Stop()
//
// 各 frontend 可以自行实现 Addon，也可以复用 statusbar 包下的内置 addon。
// 当前内置 addon 在 internal/frontend/stdin/ 下：ProgressAddon、IterationAddon。
package statusbar

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// 内置事件名。
const (
	EventIteration = "iteration" // 迭代事件：agent 完成一轮 ReAct 循环
	EventUsage     = "usage"     // 用量事件：agent 完成一次 Chat 调用，data 为最新累计快照（Usage）
)

// Usage 是 agent token 用量的累计快照，随 EventUsage 事件广播。
// 独立于 plugin 的类型定义，避免 frontend 反向依赖插件内部实现。
type Usage struct {
	EstimateTokens        int
	PromptTokens          int
	PromptCacheHitTokens  int
	PromptCacheMissTokens int
	CompletionTokens      int
	TotalTokens           int
	LastPromptTokens      int // 最近一次 Chat 的输入 token，上下文占用百分比计算用
	ContextWindow         int // 模型上下文窗口（token），0 = 未知（显示 fallback）
}

// CacheHitRate 返回上下文缓存命中率百分比（0-100），模型没返回 cache 字段时返回 (0, false)。
// 与 core.Usage.CacheHitRate 保持同一公式——frontend 不反向依赖 plugin 内部实现，
// 跨包重复这一个 3 行公式，是架构隔离的代价。
func (u Usage) CacheHitRate() (float64, bool) {
	if u.PromptCacheHitTokens+u.PromptCacheMissTokens == 0 {
		return 0, false
	}
	return float64(u.PromptCacheHitTokens) / float64(u.PromptCacheHitTokens+u.PromptCacheMissTokens) * 100, true
}

// Addon 定义状态栏的一个可插拔段。
//
// 每个 Addon 只负责产出一段字符串（不含尾部分隔符），
// 由 Bar 编排器合并渲染。Render() 返回空字符串表示该段不显示。
//
// 可选附加接口：
//   - Registerer：注册时订阅事件
//   - Stopper：收到停用通知时切换显示状态
//
// 实现示例：
//
//	type MyAddon struct {
//	    value atomic.Int64
//	}
//	func (a *MyAddon) Name() string             { return "my" }
//	func (a *MyAddon) Tick(ctx context.Context)  {}
//	func (a *MyAddon) Render() string {
//	    if n := a.value.Load(); n > 0 { return fmt.Sprintf("v: %d", n) }
//	    return ""
//	}
//	func (a *MyAddon) Reset() { a.value.Store(0) }
type Addon interface {
	// Name 返回唯一标识符，用于日志/调试。
	Name() string

	// Tick 在每次刷新周期（200ms）被调用。
	// 不要在此方法中做重活——它在 mutex 临界区内被调用。
	Tick(ctx context.Context)

	// Render 返回当前的显示内容。空字符串表示隐藏该段。
	// 注意：Tick 和 Render 是分开调用，Render 应无副作用，保持幂等。
	Render() string

	// Reset 清除 addon 状态，准备新一轮 agent 运行。
	Reset()
}

// PauseFunc 由外部决定是否暂停状态栏刷新（如 HITL 期间）。
// atomic.Bool.Load 可直接作为 PauseFunc 使用。
type PauseFunc func() bool

// Registerer 是 Addon 的可选接口。实现了 Registerer 的 Addon 在注册时会回调
// OnRegister，拿到 Bar 引用后可以订阅事件。
type Registerer interface {
	OnRegister(b *Bar)
}

// Stopper 是 Addon 的可选接口。Stop 时 Bar 会在最终渲染前回调 OnStop，
// addon 可以借此切换显示状态（如 spinner 改为完成标识）。
type Stopper interface {
	OnStop()
}

// Bar 是状态栏编排器。
//
// 职责：
//   - 管理 addon 列表
//   - 提供 Run 循环（定时刷新）
//   - 提供 Clear/Draw 供 WithLock 回调内操作
//   - 提供 WithLock 与外部共享同一把互斥锁
//   - 提供 Publish/Subscribe 事件总线，addon 和外部代码通过事件解耦
//
// 典型生命周期：
//
//	bar.Use(addon1, addon2)    // 构造时注册（触发 Registerer.OnRegister）
//	bar.Start()                 // agent 运行前
//	go bar.Run(ctx, pause)      // 启动刷新循环
//	bar.Stop()                  // agent 结束后
type Bar struct {
	mu     sync.Mutex
	active bool
	addons []Addon

	// 事件总线：Publish/Subscribe
	subs  map[string][]func(any)
	subMu sync.Mutex
}

// New 创建一个空的状态栏编排器。addons 通过 Use 方法注册。
func New() *Bar {
	return &Bar{
		addons: make([]Addon, 0),
		subs:   make(map[string][]func(any)),
	}
}

// Use 注册一个或多个 addon。应在 Start 前调用。
// 如果 addon 实现了 Registerer 接口，会回调 OnRegister。
func (b *Bar) Use(addons ...Addon) {
	for _, a := range addons {
		if r, ok := a.(Registerer); ok {
			r.OnRegister(b)
		}
	}
	b.addons = append(b.addons, addons...)
}

// Subscribe 注册一个事件处理器。线程安全。
func (b *Bar) Subscribe(event string, fn func(any)) {
	b.subMu.Lock()
	defer b.subMu.Unlock()
	b.subs[event] = append(b.subs[event], fn)
}

// Publish 向所有已注册的处理器广播事件。线程安全，可从任意 goroutine 调用。
func (b *Bar) Publish(event string, data any) {
	b.subMu.Lock()
	fns := b.subs[event]
	b.subMu.Unlock()
	// 在锁外调用 handler，避免 handler 内的锁操作导致死锁
	for _, fn := range fns {
		fn(data)
	}
}

// WithLock 在互斥锁保护下执行 fn。外部代码通过此方法与 Bar 共享锁，
// 避免直接暴露 mutex 引用。
//
// fn 内部可以调用 Clear、Draw 等需要持有锁的方法。
func (b *Bar) WithLock(fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fn()
}

// Active 返回状态栏是否处于活跃状态。调用者须在 WithLock 回调内调用。
func (b *Bar) Active() bool { return b.active }

// ---- 生命周期 ----

// Start 激活状态栏并重置所有 addon。通常在 agent 运行前调用。
func (b *Bar) Start() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.active = true
	for _, a := range b.addons {
		a.Reset()
	}
}

// Stop 停用状态栏，将最终状态写入终端作为永久行，然后换行。
// 停用前会通知所有实现了 Stopper 接口的 addon，以便切换显示。
func (b *Bar) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.active = false

	// 通知 addon 即将停用
	for _, a := range b.addons {
		if s, ok := a.(Stopper); ok {
			s.OnStop()
		}
	}

	// 把最终状态栏提交为永久输出行
	if line := b.render(); line != "" {
		fmt.Fprintf(os.Stderr, "\r%s\r\n", line)
	} else {
		fmt.Fprint(os.Stderr, "\r\n")
	}
}

// Run 启动状态栏刷新循环。
//
// 每隔 200ms 执行一次：Tick 所有 addon → draw 合并渲染。
// pause 可选，返回 true 时跳过本轮刷新（如 HITL 激活）。
// ctx 取消时退出。
func (b *Bar) Run(ctx context.Context, pause PauseFunc) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if pause != nil && pause() {
				continue
			}
			b.mu.Lock()
			if !b.active {
				b.mu.Unlock()
				continue
			}
			for _, a := range b.addons {
				a.Tick(ctx)
			}
			b.draw()
			b.mu.Unlock()
		}
	}
}

// ---- 绘制（调用者须在 WithLock 回调内调用） ----

// Clear 清除当前终端行（状态栏所在行）。须在 WithLock 回调内调用。
func (b *Bar) Clear() {
	fmt.Fprint(os.Stderr, "\033[2K\r")
}

// Draw 清除当前行并绘制组合后的状态栏。须在 WithLock 回调内调用。
func (b *Bar) Draw() {
	if line := b.render(); line != "" {
		fmt.Fprintf(os.Stderr, "\033[2K\r%s", line)
	}
}

// draw 内部版本，与 Draw 相同但假设调用者已持有锁。
func (b *Bar) draw() {
	if line := b.render(); line != "" {
		fmt.Fprintf(os.Stderr, "\033[2K\r%s", line)
	}
}

// render 遍历 addon 合并渲染内容，各段之间用 " | " 隔开。
func (b *Bar) render() string {
	var buf strings.Builder
	for _, a := range b.addons {
		if s := a.Render(); s != "" {
			if buf.Len() > 0 {
				buf.WriteString(" | ")
			}
			buf.WriteString(s)
		}
	}
	return buf.String()
}
