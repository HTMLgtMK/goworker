package stdin

import (
	"context"
	"fmt"
	"time"

	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
)

// compile-time checks
var (
	_ statusbar.Addon   = (*ProgressAddon)(nil)
	_ statusbar.Stopper = (*ProgressAddon)(nil)
)

// ProgressAddon 合并 spinner + elapsed：agent 运行期间的"活动 + 耗时"指示。
// 合成一个 segment（中间不加 | 分隔符），elapsed 右填充固定宽度，
// 避免 0.1s→59.9s→1m0.0s 这种长度变化导致状态栏宽度来回跳动。
type ProgressAddon struct {
	frames  []string
	idx     int
	start   time.Time
	stopped bool
}

// NewProgressAddon 创建一个合并的进度 addon。
func NewProgressAddon() *ProgressAddon {
	return &ProgressAddon{
		frames: []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"},
		start:  time.Now(),
	}
}

func (a *ProgressAddon) Name() string             { return "progress" }
func (a *ProgressAddon) Tick(ctx context.Context) { a.idx = (a.idx + 1) % len(a.frames) }
func (a *ProgressAddon) Reset() {
	a.idx = 0
	a.stopped = false
	a.start = time.Now()
}
func (a *ProgressAddon) OnStop() { a.stopped = true }

// Render 组合图标 + 固定宽度耗时。
// elapsed 用 %-8s 右填充：agent 5 分钟超时内（最长 ~7 字符）宽度恒定，刷新不跳动。
func (a *ProgressAddon) Render() string {
	icon := a.frames[a.idx]
	if a.stopped {
		icon = "✓"
	}
	elapsed := time.Since(a.start).Round(100 * time.Millisecond)
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
