package stdin

import (
	"context"

	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
)

// compile-time checks
var (
	_ statusbar.Addon   = (*SpinnerAddon)(nil)
	_ statusbar.Stopper = (*SpinnerAddon)(nil)
)

// SpinnerAddon 显示一个旋转的 throbber，表示 agent 正在运行。
//
// 收到 OnStop 后切换为 ✓，表示已完成。
// 无额外状态依赖，开箱即用。
type SpinnerAddon struct {
	frames  []string
	idx     int
	stopped bool
}

// NewSpinnerAddon 创建一个 10 帧的旋转器。
func NewSpinnerAddon() *SpinnerAddon {
	return &SpinnerAddon{
		frames: []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"},
	}
}

func (a *SpinnerAddon) Name() string             { return "spinner" }
func (a *SpinnerAddon) Tick(ctx context.Context) { a.idx = (a.idx + 1) % len(a.frames) }
func (a *SpinnerAddon) Reset()                   { a.idx = 0; a.stopped = false }
func (a *SpinnerAddon) OnStop()                  { a.stopped = true }
func (a *SpinnerAddon) Render() string {
	if a.stopped {
		return "✓"
	}
	return a.frames[a.idx]
}
