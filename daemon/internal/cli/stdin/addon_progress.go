package stdin

import (
	"context"
	"fmt"
	"time"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/cli/statusbar"
)

// compile-time checks
var (
	_ statusbar.Addon      = (*ProgressAddon)(nil)
	_ statusbar.Registerer = (*ProgressAddon)(nil)
	_ statusbar.Stopper    = (*ProgressAddon)(nil)
)

// ProgressAddon 合并 spinner + 阶段 + 耗时：命令运行期间的"活动 + 进度"指示。
// 合成一个 segment（中间不加 | 分隔符），elapsed 右填充固定宽度，
// 避免 0.1s→59.9s→1m0.0s 这种长度变化导致状态栏宽度来回跳动。
//
// 生命周期完全事件驱动：frontend 的 Eval 包装器发 PhaseBegin/PhaseEnd，
// 命令内部（/compact 的固化→压缩、/new 的固化）发 PhaseStage 切换阶段文本。
// addon 据此驱动 Bar 的 Start/Stop，frontend 不感知具体命令。
// 事件全部从主 goroutine 发布（命令在主 goroutine 串行执行），started 标记无并发问题。
type ProgressAddon struct {
	bar     *statusbar.Bar
	frames  []string
	idx     int
	start   time.Time
	label   string // 当前阶段文本，空 = 通用运行指示
	stopped bool
	started bool // bar 是否因 phase 事件激活（防 begin/end 错配）
}

// NewProgressAddon 创建一个合并的进度 addon。
func NewProgressAddon() *ProgressAddon {
	return &ProgressAddon{
		frames: []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"},
		start:  time.Now(),
	}
}

// OnRegister 实现 statusbar.Registerer：拿到 Bar 引用并订阅阶段事件。
func (a *ProgressAddon) OnRegister(b *statusbar.Bar) {
	a.bar = b
	b.Subscribe(runtimeconfig.EventPhase, func(v any) {
		ev, ok := v.(runtimeconfig.PhaseEvent)
		if !ok {
			return
		}
		a.handlePhase(ev)
	})
}

// handlePhase 处理阶段事件，驱动 Bar 生命周期。
// label 会被 Run 刷新 goroutine 的 Render 读取，写入须经 WithLock（started 仅主 goroutine 触碰）。
func (a *ProgressAddon) handlePhase(ev runtimeconfig.PhaseEvent) {
	switch ev.Kind {
	case runtimeconfig.PhaseBegin:
		if a.started {
			return
		}
		a.bar.Start() // Reset 归零 spinner/计时/label（自身持锁）
		a.bar.WithLock(func() {
			a.label = ev.Label
		})
		a.started = true
	case runtimeconfig.PhaseStage:
		if a.started {
			a.bar.WithLock(func() {
				a.label = ev.Label
			})
		}
	case runtimeconfig.PhaseEnd:
		if a.started {
			a.bar.Stop() // 定格 ✓ + 耗时为永久行（未 Tick 过则 Bar 静默）
			a.started = false
		}
	}
}

func (a *ProgressAddon) Name() string             { return "progress" }
func (a *ProgressAddon) Tick(ctx context.Context) { a.idx = (a.idx + 1) % len(a.frames) }
func (a *ProgressAddon) Reset() {
	a.idx = 0
	a.stopped = false
	a.label = ""
	a.start = time.Now()
}
func (a *ProgressAddon) OnStop() { a.stopped = true }

// Render 组合图标 + 可选阶段 + 固定宽度耗时。
// elapsed 用 %-8s 右填充：agent 5 分钟超时内（最长 ~7 字符）宽度恒定，刷新不跳动。
func (a *ProgressAddon) Render() string {
	icon := a.frames[a.idx]
	if a.stopped {
		icon = "✓"
	}
	elapsed := time.Since(a.start).Round(100 * time.Millisecond)
	if a.label != "" {
		return fmt.Sprintf("%s %s ⏱ %-8s", icon, a.label, a.formatDuration(elapsed))
	}
	return fmt.Sprintf("%s ⏱ %s", icon, a.formatDuration(elapsed))
}

func (a *ProgressAddon) formatDuration(d time.Duration) string {
	d = d.Round(time.Millisecond)

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60
	millis := int(d.Milliseconds()) % 1000

	// 根据不同时间长度返回不同格式
	if hours > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%02d:%02d", minutes, seconds)
	}
	if seconds > 0 {
		return fmt.Sprintf("%.1fs", float64(seconds)+float64(millis)/1000)
	}
	return fmt.Sprintf("%dms", millis)
}
