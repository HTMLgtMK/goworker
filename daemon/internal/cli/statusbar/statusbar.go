// Package statusbar provides a pluggable status bar for CLI frontends.
package statusbar

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// Addon supplies an optional status-bar segment. Empty Render output hides it.
// Registerer and Stopper are optional lifecycle hooks.
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

// Bar coordinates addons, refresh cycles, event delivery, and terminal rendering.
type Bar struct {
	mu     sync.Mutex
	active bool
	ticked bool // Start 以来是否刷新过至少一轮；瞬时命令 Stop 时据此静默
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

// Start 激活状态栏并重置所有 addon。通常在命令运行前调用。
func (b *Bar) Start() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.active = true
	b.ticked = false
	for _, a := range b.addons {
		a.Reset()
	}
}

// Stop 停用状态栏，将最终状态写入终端作为永久行，然后换行。
// 停用前会通知所有实现了 Stopper 接口的 addon，以便切换显示。
// Start 以来从未 Tick 过（瞬时命令，/help 等）则静默停用，不落任何行，
// 避免每条快命令都残留一行 "✓ ⏱ 3ms"。
func (b *Bar) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.active = false
	if !b.ticked {
		return
	}

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
			b.ticked = true
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
