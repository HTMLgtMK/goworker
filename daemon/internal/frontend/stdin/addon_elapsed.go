package stdin

import (
	"context"
	"fmt"
	"time"

	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
)

// compile-time check
var _ statusbar.Addon = (*ElapsedAddon)(nil)

// ElapsedAddon 显示 agent 已运行时长。
//
// Reset 时记录开始时间，Render 时计算当前耗时。
type ElapsedAddon struct {
	start time.Time
}

func NewElapsedAddon() *ElapsedAddon {
	return &ElapsedAddon{}
}

func (a *ElapsedAddon) Name() string            { return "elapsed" }
func (a *ElapsedAddon) Tick(ctx context.Context) {}
func (a *ElapsedAddon) Render() string {
	elapsed := time.Since(a.start).Round(100 * time.Millisecond)
	return fmt.Sprintf("⏱ %s", elapsed)
}
func (a *ElapsedAddon) Reset() { a.start = time.Now() }
