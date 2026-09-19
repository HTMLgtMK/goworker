package stdin

import (
	"context"
	"fmt"
	"sync"

	"github.com/tinguo/goworker/ai-dispatch/task"
	"github.com/tinguo/goworker/daemon/internal/cli/statusbar"
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
	// summaries 按 taskID 记录各运行中任务的最新进度摘要，
	// 多任务并跑时 Render 轮换显示（Tick 每帧推进一轮）。
	summaries map[string]string
	order     []string // 任务首次汇报顺序，保证轮换稳定
	rotate    int
	events    chan struct{}
}

// NewTaskAddon 创建 dispatcher 任务 addon。
// events 为 Tick 帧间的平滑队列，缓冲 16 + 满则丢（进度高频可丢，计数由
// status 事件补偿对齐：丢弃中间迁移时下一个到达的 status 事件仍携带完整状态）。
func NewTaskAddon() *TaskAddon {
	return &TaskAddon{
		events:    make(chan struct{}, 16),
		summaries: make(map[string]string),
	}
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
	// 任务终态：移出摘要轮换
	if e.To.Terminal() {
		if _, ok := a.summaries[e.TaskID]; ok {
			delete(a.summaries, e.TaskID)
			for i, id := range a.order {
				if id == e.TaskID {
					a.order = append(a.order[:i], a.order[i+1:]...)
					break
				}
			}
			if a.rotate >= len(a.order) {
				a.rotate = 0
			}
		}
	}
}

// applyProgress 更新任务进度摘要；新任务进入轮换序列。
func (a *TaskAddon) applyProgress(e task.ProgressEvent) {
	a.mu.Lock()
	if _, ok := a.summaries[e.TaskID]; !ok {
		a.order = append(a.order, e.TaskID)
	}
	a.summaries[e.TaskID] = e.Summary
	a.mu.Unlock()
}

func (a *TaskAddon) Name() string { return "dispatch" }

// Tick 消费积压的 status 事件通知并推进摘要轮换
// （状态本身已在 applyStatus 原子更新，排水只为触发下一帧重绘）。
func (a *TaskAddon) Tick(ctx context.Context) {
	a.mu.Lock()
	if len(a.order) > 1 {
		a.rotate = (a.rotate + 1) % len(a.order)
	}
	a.mu.Unlock()
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
// 多任务并跑时摘要按 Tick 轮换显示。
func (a *TaskAddon) Render() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running == 0 && a.review == 0 && len(a.summaries) == 0 {
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
	if a.running > 0 && len(a.order) > 0 {
		// 从当前轮换位起找第一个仍有摘要的任务
		for i := 0; i < len(a.order); i++ {
			id := a.order[(a.rotate+i)%len(a.order)]
			if summary, ok := a.summaries[id]; ok && summary != "" {
				out += " · " + summary
				break
			}
		}
	}
	return out
}

// Reset 清空摘要轮换（dispatcher 常驻，计数保留跨 /new 生命周期）。
func (a *TaskAddon) Reset() {
	a.mu.Lock()
	a.summaries = make(map[string]string)
	a.order = nil
	a.rotate = 0
	a.mu.Unlock()
}
