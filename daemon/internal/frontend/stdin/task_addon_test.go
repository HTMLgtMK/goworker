package stdin

import (
	"testing"

	"github.com/tinguo/goworker/ai-dispatch/task"
)

func statusEvent(from, to task.Status) task.StatusEvent {
	return task.StatusEvent{TaskID: "task_1", From: from, To: to, Kind: task.KindCode, Worker: "fake"}
}

func TestTaskAddon_RenderSequence(t *testing.T) {
	a := NewTaskAddon()

	if got := a.Render(); got != "" {
		t.Fatalf("initial render = %q, want empty", got)
	}

	// queued → dispatching → working：显示 1 run
	a.applyStatus(statusEvent(task.StatusQueued, task.StatusDispatching))
	a.applyStatus(statusEvent(task.StatusDispatching, task.StatusWorking))
	a.Tick(t.Context())
	if got := a.Render(); got != "dispatch 1 run" {
		t.Fatalf("render = %q, want dispatch 1 run", got)
	}

	// 进度摘要滚动
	a.applyProgress(task.ProgressEvent{TaskID: "task_1", Kind: "agent_message_chunk", Summary: "committing"})
	if got := a.Render(); got != "dispatch 1 run · committing" {
		t.Fatalf("render = %q, want summary appended", got)
	}

	// 进入 awaiting_review：run 归零、review 高亮
	a.applyStatus(statusEvent(task.StatusWorking, task.StatusAwaitingReview))
	a.Tick(t.Context())
	if got := a.Render(); got != "!1 review" {
		t.Fatalf("render = %q, want !1 review", got)
	}

	// approve → merging → done：计数全清
	a.applyStatus(statusEvent(task.StatusAwaitingReview, task.StatusMerging))
	a.applyStatus(statusEvent(task.StatusMerging, task.StatusDone))
	a.Tick(t.Context())
	if got := a.Render(); got != "" {
		t.Fatalf("render = %q, want empty after done", got)
	}
}

func TestTaskAddon_MixedCounts(t *testing.T) {
	a := NewTaskAddon()

	// 任务 A 运行中；任务 B 走完 work 后进入待审批
	a.applyStatus(statusEvent(task.StatusQueued, task.StatusWorking))
	a.applyStatus(task.StatusEvent{TaskID: "task_2", From: task.StatusQueued, To: task.StatusWorking})
	a.applyStatus(task.StatusEvent{TaskID: "task_2", From: task.StatusWorking, To: task.StatusAwaitingReview})
	a.Tick(t.Context())
	if got := a.Render(); got != "dispatch 1 run !1 review" {
		t.Fatalf("render = %q, want mixed", got)
	}

	// 任务 A 完成、任务 B 被拒：归零
	a.applyStatus(statusEvent(task.StatusWorking, task.StatusCancelled))
	a.applyStatus(task.StatusEvent{TaskID: "task_2", From: task.StatusAwaitingReview, To: task.StatusRejected})
	a.Tick(t.Context())
	if got := a.Render(); got != "" {
		t.Fatalf("render = %q, want empty", got)
	}
}

func TestTaskAddon_ResetKeepsCounts(t *testing.T) {
	a := NewTaskAddon()
	a.applyStatus(statusEvent(task.StatusQueued, task.StatusWorking))
	a.applyProgress(task.ProgressEvent{TaskID: "task_1", Kind: "agent_message_chunk", Summary: "step 1"})

	a.Reset() // /agent 启动新一轮：摘要清空，dispatcher 计数保留

	a.Tick(t.Context())
	if got := a.Render(); got != "dispatch 1 run" {
		t.Fatalf("render = %q, want counts preserved without summary", got)
	}
}

// 非法载荷不 panic。
func TestTaskAddon_IgnoresForeignPayloads(t *testing.T) {
	a := NewTaskAddon()
	a.applyStatus(statusEvent(task.StatusQueued, task.StatusWorking))
	a.Tick(t.Context())
	if got := a.Render(); got != "dispatch 1 run" {
		t.Fatalf("render = %q", got)
	}
}
