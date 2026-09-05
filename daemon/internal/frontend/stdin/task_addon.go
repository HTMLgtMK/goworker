package stdin

import (
	"context"
	"fmt"
	"sync"

	"github.com/tinguo/goworker/ai-dispatch/task"
	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
)

// compile-time checks
var (
	_ statusbar.Addon      = (*TaskAddon)(nil)
	_ statusbar.Registerer = (*TaskAddon)(nil)
)

// TaskAddon 显示 dispatcher 的任务概况：运行中/待审批计数 + 最新进度摘要。
//
// dispatcher 任务跑在后台 goroutine（无命令 Context），事件经 Engine 事件总线
// 到达前端：dispatcher 插件 hub.Notify(task_status/task_progress) →
// stdin 前端桥接 listener → sb.Publish → 本 addon 订阅。
// Bar 未运行时事件积压在 channel（满则丢弃），不阻塞 dispatcher。
type TaskAddon struct {
	mu      sync.Mutex
	running int
	review  int
	latest  string // 最新进度摘要
	events  chan struct{}
}

// NewTaskAddon 创建 dispatcher 任务 addon。
// events 为 Tick 帧间的平滑队列，缓冲 16 + 满则丢（进度高频可丢，计数由
// status 事件补偿对齐：丢弃中间迁移时下一个到达的 status 事件仍携带完整状态）。
func NewTaskAddon() *TaskAddon {
	return &TaskAddon{events: make(chan struct{}, 16)}
}

// OnRegister 实现 statusbar.Registerer，订阅 dispatcher 的两类事件。
func (a *TaskAddon) OnRegister(b *statusbar.Bar) {
	b.Subscribe(task.EventTaskStatus, func(payload any) {
		if e, ok := payload.(task.StatusEvent); ok {
			a.applyStatus(e)
		}
		select {
		case a.events <- struct{}{}:
		default:
		}
	})
	b.Subscribe(task.EventTaskProgress, func(payload any) {
		if e, ok := payload.(task.ProgressEvent); ok {
			a.applyProgress(e)
		}
	})
}

// applyStatus 按迁移方向维护计数：
//
//	进入 working → running++；working 迁出且落终态 → running--
//	进入 awaiting_review → review++；迁出 awaiting_review（含 merging）→ review--
func (a *TaskAddon) applyStatus(e task.StatusEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e.To == task.StatusWorking && e.From != task.StatusWorking {
		a.running++
	}
	if e.From == task.StatusWorking && e.To != task.StatusWorking {
		if a.running > 0 {
			a.running--
		}
	}
	if e.To == task.StatusAwaitingReview && e.From != task.StatusAwaitingReview {
		a.review++
	}
	if e.From == task.StatusAwaitingReview && e.To != task.StatusAwaitingReview {
		if a.review > 0 {
			a.review--
		}
	}
}

// applyProgress 更新最新进度摘要。
func (a *TaskAddon) applyProgress(e task.ProgressEvent) {
	a.mu.Lock()
	a.latest = e.Summary
	a.mu.Unlock()
}

func (a *TaskAddon) Name() string { return "dispatch" }

// Tick 消费积压的 status 事件通知（状态本身已在 applyStatus 原子更新，
// 排水只为触发下一帧重绘）。
func (a *TaskAddon) Tick(ctx context.Context) {
	for {
		select {
		case <-a.events:
		default:
			return
		}
	}
}

// Render 输出任务概况；无任务时返回空不占位。
// review 段用 "!N" 前缀强调——待审批是用户必须看到的。
func (a *TaskAddon) Render() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running == 0 && a.review == 0 && a.latest == "" {
		return ""
	}
	var out string
	if a.running > 0 {
		out += fmt.Sprintf("dispatch %d run", a.running)
	}
	if a.review > 0 {
		if out != "" {
			out += " "
		}
		out += fmt.Sprintf("!%d review", a.review)
	}
	if a.latest != "" && a.running > 0 {
		out += " · " + a.latest
	}
	return out
}

// Reset 清空进度摘要（dispatcher 常驻，计数保留跨 /new 生命周期）。
func (a *TaskAddon) Reset() {
	a.mu.Lock()
	a.latest = ""
	a.mu.Unlock()
}
