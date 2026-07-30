package stdin

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
)

// compile-time checks
var (
	_ statusbar.Addon      = (*IterationAddon)(nil)
	_ statusbar.Registerer = (*IterationAddon)(nil)
)

// IterationAddon 显示 agent 的 ReAct 循环轮次。
//
// 通过事件总线订阅 statusbar.EventIteration 事件：
// agent plugin 发 `Publish(statusbar.EventIteration, nil)`，
// addon 在 Tick 时从 channel 消费事件，更新计数。
// frontend 不持有 addon 引用，完全解耦。
type IterationAddon struct {
	count  atomic.Int64
	events chan struct{}
}

// NewIterationAddon 创建一个迭代计数 addon。
// events channel 缓冲设为 maxIterations（15）以防积压。
func NewIterationAddon() *IterationAddon {
	return &IterationAddon{
		events: make(chan struct{}, 15),
	}
}

// OnRegister 实现 statusbar.Registerer，注册时订阅 iteration 事件。
func (a *IterationAddon) OnRegister(b *statusbar.Bar) {
	b.Subscribe(statusbar.EventIteration, func(any) {
		select {
		case a.events <- struct{}{}:
		default:
		}
	})
}

func (a *IterationAddon) Name() string { return "iteration" }

// Tick 消费所有积压的迭代事件。
func (a *IterationAddon) Tick(ctx context.Context) {
	for {
		select {
		case <-a.events:
			a.count.Add(1)
		default:
			return
		}
	}
}

func (a *IterationAddon) Render() string {
	if n := a.count.Load(); n > 0 {
		return fmt.Sprintf("iter %d", n)
	}
	return ""
}

// Reset 清零计数并清空 events channel，防止上一轮残留事件泄漏到下一轮。
func (a *IterationAddon) Reset() {
	a.count.Store(0)
	for {
		select {
		case <-a.events:
		default:
			return
		}
	}
}
