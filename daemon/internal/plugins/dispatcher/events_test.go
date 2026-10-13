package dispatcher

import (
	"strings"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-dispatch/task"
)

// TestPlugin_EmitsStatusAndProgressEvents 任务全生命周期应广播 status 事件，
// worker 进度应广播 progress 事件。
func TestPlugin_EmitsStatusAndProgressEvents(t *testing.T) {
	p, hub := newTestPlugin(t)
	repo := initGitRepo(t)
	changeCwd(t, repo)

	addOut := runCmd(t, p, hub, "/dispatch", "event flow test")
	id := extractTaskID(t, addOut)
	waitForStatus(t, p, id, task.StatusAwaitingReview)

	// 允许事件 channel 异步排空
	deadline := time.After(2 * time.Second)
	for {
		statuses := hub.eventsOfType(task.EventTaskStatus)
		if len(statuses) >= 3 { // queued, dispatching, working, awaiting_review 至少 4 条
			break
		}
		select {
		case <-deadline:
			for _, e := range hub.events {
				t.Logf("event: %s payload=%+v", e.Type, e.Payload)
			}
			t.Fatalf("status events = %d, want >= 4", len(statuses))
		case <-time.After(10 * time.Millisecond):
		}
	}

	// 迁移序列完整且有序
	var froms, tos []task.Status
	for _, payload := range hub.eventsOfType(task.EventTaskStatus) {
		e := payload.(task.StatusEvent)
		if e.TaskID != id {
			continue
		}
		froms, tos = append(froms, e.From), append(tos, e.To)
	}
	want := []task.Status{"", task.StatusQueued, task.StatusDispatching, task.StatusWorking, task.StatusAwaitingReview}
	if len(tos) != len(want)-1 {
		t.Fatalf("tos = %v, want %d transitions", tos, len(want)-1)
	}
	for i, to := range tos {
		if to != want[i+1] || froms[i] != want[i] {
			t.Errorf("transition %d: %s -> %s, want %s -> %s", i, froms[i], to, want[i], want[i+1])
		}
	}

	// progress 事件：fake worker 发出 "committing"
	var progressSummaries []string
	for _, payload := range hub.eventsOfType(task.EventTaskProgress) {
		progressSummaries = append(progressSummaries, payload.(task.ProgressEvent).Summary)
	}
	joined := strings.Join(progressSummaries, "")
	if !strings.Contains(joined, "committing") {
		t.Errorf("progress summaries = %q, want committing", joined)
	}

	// approve 后应再有 merging、done 两条
	runCmd(t, p, hub, "/dispatch", "approve", id)
	waitForStatus(t, p, id, task.StatusDone)
	tos = tos[:0]
	for _, payload := range hub.eventsOfType(task.EventTaskStatus) {
		e := payload.(task.StatusEvent)
		if e.TaskID == id {
			tos = append(tos, e.To)
		}
	}
	last := tos[len(tos)-1]
	if last != task.StatusDone {
		t.Errorf("last transition = %s, want done", last)
	}
}
