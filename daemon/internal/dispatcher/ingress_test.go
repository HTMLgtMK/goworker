package dispatcher

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-dispatch/task"
)

// TestIngress_ACPClientSubmitsTask 外部 ACP Client 连 socket 提交任务：
// initialize → session/new(git 仓库 cwd) → session/prompt，
// 进度经 session/update 回流，任务落 awaiting_review。
func TestIngress_ACPClientSubmitsTask(t *testing.T) {
	p, _ := newTestPlugin(t)
	repo := initGitRepo(t)
	// macOS unix socket 路径上限 104 字符，测试改用短路径
	short, err := os.MkdirTemp("", "gwd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(short) })
	p.paths.DispatchDir = short
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	conn, err := net.Dial("unix", filepath.Join(p.paths.DispatchDir, "acp.sock"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	fc := newFakeACPTaskClient(conn)
	t.Cleanup(func() { _ = fc.conn.Close() })

	var initResp protocol.InitializeResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodInitialize,
		protocol.InitializeRequest{ProtocolVersion: protocol.Version}, &initResp); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	var newResp protocol.NewSessionResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodSessionNew,
		protocol.NewSessionRequest{Cwd: repo}, &newResp); err != nil {
		t.Fatalf("session/new: %v", err)
	}

	var promptResp protocol.PromptResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: newResp.SessionID,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("add a feature")},
	}, &promptResp); err != nil {
		t.Fatalf("session/prompt: %v", err)
	}
	if promptResp.StopReason != protocol.StopEndTurn {
		t.Errorf("stopReason = %q", promptResp.StopReason)
	}

	// 进度（worker commit 提示 + 收尾 message chunk）应已回流；通知与响应异步，轮询等待
	joined := ""
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		var texts []string
		for _, u := range ingressUpdates {
			if u.Update.Content != nil {
				texts = append(texts, u.Update.Content.Text)
			}
		}
		mu.Unlock()
		joined = strings.Join(texts, "")
		if strings.Contains(joined, "committing") && strings.Contains(joined, "等待审批") {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("streamed texts = %q, want worker progress + review notice", joined)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// 任务与 REPL 侧同一 store，状态一致
	tasks := p.store.List()
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d", len(tasks))
	}
	if tasks[0].Status != task.StatusAwaitingReview || tasks[0].Source != "acp" || tasks[0].Kind != task.KindCode {
		t.Errorf("task = %+v", tasks[0])
	}
	if len(tasks[0].Commits) != 1 || !strings.Contains(tasks[0].Commits[0], "worker commit") {
		t.Errorf("commits = %v", tasks[0].Commits)
	}
	_ = waitForStatus(t, p, tasks[0].ID, task.StatusAwaitingReview)
}

func TestIngress_ReviewReturnsStructuredArtifact(t *testing.T) {
	p, _ := newTestPlugin(t)
	now := time.Now()
	tk := &task.Task{
		ID: "task_ingress_review", Source: "acp", Kind: task.KindGeneral, Prompt: "summarize", Worker: "fake",
		Status: task.StatusQueued, CreatedAt: now, UpdatedAt: now,
	}
	if err := p.store.Add(tk); err != nil {
		t.Fatalf("store.Add: %v", err)
	}
	setReviewStatus(t, p, tk, task.StatusDispatching)
	setReviewStatus(t, p, tk, task.StatusWorking)
	setReviewStatus(t, p, tk, task.StatusAwaitingReview)
	appendReviewEvent(t, p, tk.ID, `{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"# Final\n\n- delivered"}}`)
	appendReviewEvent(t, p, tk.ID, `{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"private thought"}}`)

	fc, sessionID := newIngressSession(t, p, t.TempDir())
	clearIngressUpdates()
	var promptResp protocol.PromptResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: sessionID,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("--review " + tk.ID)},
	}, &promptResp); err != nil {
		t.Fatalf("review prompt: %v", err)
	}

	envelope := waitIngressEnvelope(t, "review")
	if envelope.Version != 1 || envelope.Task.ID != tk.ID || envelope.Task.Status != string(task.StatusAwaitingReview) {
		t.Fatalf("review envelope = %+v", envelope)
	}
	if envelope.Review.Artifact != "# Final\n\n- delivered" {
		t.Errorf("artifact = %q", envelope.Review.Artifact)
	}
	if strings.Contains(envelope.Review.Artifact, "private thought") {
		t.Fatalf("review leaked thought: %+v", envelope)
	}
}

func TestIngress_EventsReplayFromCursorAndComplete(t *testing.T) {
	p, _ := newTestPlugin(t)
	now := time.Now()
	tk := &task.Task{
		ID: "task_ingress_events", Source: "acp", Kind: task.KindGeneral, Prompt: "observe", Worker: "fake",
		Status: task.StatusQueued, CreatedAt: now, UpdatedAt: now,
	}
	if err := p.store.Add(tk); err != nil {
		t.Fatalf("store.Add: %v", err)
	}
	setReviewStatus(t, p, tk, task.StatusDispatching)
	setReviewStatus(t, p, tk, task.StatusWorking)
	appendReviewEvent(t, p, tk.ID, `{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"history"}}`)

	fc, sessionID := newIngressSession(t, p, t.TempDir())
	clearIngressUpdates()
	result := make(chan error, 1)
	go func() {
		var promptResp protocol.PromptResponse
		result <- fc.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    []protocol.ContentBlock{protocol.TextBlock("--events " + tk.ID + " 0")},
		}, &promptResp)
	}()

	history := waitIngressUpdateEvent(t, "history")
	if history.Cursor == 0 || history.Event.Update == nil {
		t.Fatalf("history envelope = %+v", history)
	}
	appendReviewEvent(t, p, tk.ID, `{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"live"}}`)
	setReviewStatus(t, p, tk, task.StatusAwaitingReview)

	live := waitIngressUpdateEvent(t, "live")
	completion := waitIngressEnvelope(t, "complete")
	if live.Cursor <= history.Cursor || completion.NextCursor <= live.Cursor || completion.Status != string(task.StatusAwaitingReview) {
		t.Fatalf("event cursors = history:%d live:%d completion:%+v", history.Cursor, live.Cursor, completion)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("events prompt: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("events prompt did not complete")
	}
}

func TestIngress_EventsStayOpenWhileCodeTaskMerges(t *testing.T) {
	p, _ := newTestPlugin(t)
	now := time.Now()
	tk := &task.Task{
		ID: "task_ingress_merging", Source: "acp", Kind: task.KindCode, Prompt: "merge", Worker: "fake",
		Status: task.StatusQueued, CreatedAt: now, UpdatedAt: now,
	}
	if err := p.store.Add(tk); err != nil {
		t.Fatalf("store.Add: %v", err)
	}
	setReviewStatus(t, p, tk, task.StatusDispatching)
	setReviewStatus(t, p, tk, task.StatusWorking)
	setReviewStatus(t, p, tk, task.StatusAwaitingReview)
	setReviewStatus(t, p, tk, task.StatusMerging)

	fc, sessionID := newIngressSession(t, p, t.TempDir())
	clearIngressUpdates()
	result := make(chan error, 1)
	go func() {
		var promptResp protocol.PromptResponse
		result <- fc.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    []protocol.ContentBlock{protocol.TextBlock("--events " + tk.ID + " 0")},
		}, &promptResp)
	}()

	waitIngressStatusEvent(t, task.StatusMerging)
	select {
	case err := <-result:
		t.Fatalf("events prompt finished while merging: %v", err)
	default:
	}

	setReviewStatus(t, p, tk, task.StatusDone)
	completion := waitIngressEnvelope(t, "complete")
	if completion.Status != string(task.StatusDone) {
		t.Fatalf("completion status = %q, want done", completion.Status)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("events prompt: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("events prompt did not complete")
	}
}

func TestIngress_ObservationDirectivesReturnRPCErrors(t *testing.T) {
	p, _ := newTestPlugin(t)
	fc, sessionID := newIngressSession(t, p, t.TempDir())

	for _, prompt := range []string{
		"--review task_missing",
		"--events task_missing 0",
		"--events task_missing not-a-cursor",
	} {
		var response protocol.PromptResponse
		err := fc.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    []protocol.ContentBlock{protocol.TextBlock(prompt)},
		}, &response)
		if err == nil || !strings.Contains(err.Error(), "rpc -32000") {
			t.Errorf("prompt %q error = %v, want RPC error", prompt, err)
		}
	}
}

type observedIngressEnvelope struct {
	Version    int    `json:"version"`
	Type       string `json:"type"`
	Cursor     int    `json:"cursor"`
	NextCursor int    `json:"next_cursor"`
	Status     string `json:"status"`
	Task       struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	} `json:"task"`
	Review struct {
		Artifact string `json:"artifact"`
	} `json:"review"`
	Event struct {
		Update json.RawMessage `json:"update"`
	} `json:"event"`
}

func newIngressSession(t *testing.T, p *DispatcherPlugin, cwd string) (*fakeACPTaskClient, string) {
	t.Helper()
	short, err := os.MkdirTemp("", "gwd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(short) })
	p.paths.DispatchDir = short
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	conn, err := net.Dial("unix", filepath.Join(short, "acp.sock"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	fc := newFakeACPTaskClient(conn)
	t.Cleanup(func() { _ = fc.conn.Close() })

	var newResp protocol.NewSessionResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodSessionNew,
		protocol.NewSessionRequest{Cwd: cwd}, &newResp); err != nil {
		t.Fatalf("session/new: %v", err)
	}
	return fc, newResp.SessionID
}

func clearIngressUpdates() {
	mu.Lock()
	defer mu.Unlock()
	ingressUpdates = nil
}

func waitIngressStatusEvent(t *testing.T, status task.Status) observedIngressEnvelope {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		updates := append([]protocol.SessionUpdate(nil), ingressUpdates...)
		mu.Unlock()
		for _, update := range updates {
			if update.Update.Content == nil {
				continue
			}
			var envelope observedIngressEnvelope
			if err := json.Unmarshal([]byte(update.Update.Content.Text), &envelope); err != nil || envelope.Type != "event" {
				continue
			}
			var event task.TaskEvent
			if err := json.Unmarshal([]byte(update.Update.Content.Text), &struct {
				Event *task.TaskEvent `json:"event"`
			}{Event: &event}); err == nil && event.Type == task.EventStatus && event.To == status {
				return envelope
			}
		}
		select {
		case <-deadline:
			t.Fatalf("did not receive status event %q", status)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func waitIngressUpdateEvent(t *testing.T, text string) observedIngressEnvelope {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		updates := append([]protocol.SessionUpdate(nil), ingressUpdates...)
		mu.Unlock()
		for _, update := range updates {
			if update.Update.Content == nil {
				continue
			}
			var envelope observedIngressEnvelope
			if err := json.Unmarshal([]byte(update.Update.Content.Text), &envelope); err != nil || envelope.Type != "event" || envelope.Event.Update == nil {
				continue
			}
			var eventUpdate protocol.SessionUpdateBody
			if err := json.Unmarshal(envelope.Event.Update, &eventUpdate); err == nil && eventUpdate.Content != nil && eventUpdate.Content.Text == text {
				return envelope
			}
		}
		select {
		case <-deadline:
			t.Fatalf("did not receive event containing %q", text)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func waitIngressEnvelope(t *testing.T, kind string) observedIngressEnvelope {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		updates := append([]protocol.SessionUpdate(nil), ingressUpdates...)
		mu.Unlock()
		for _, update := range updates {
			if update.Update.Content == nil {
				continue
			}
			var envelope observedIngressEnvelope
			if err := json.Unmarshal([]byte(update.Update.Content.Text), &envelope); err == nil && envelope.Type == kind {
				return envelope
			}
		}
		select {
		case <-deadline:
			t.Fatalf("did not receive %q envelope", kind)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// pollUpdatesText 轮询累积所有 update 文本直到条件满足（通知与响应异步到达）。
func pollUpdatesText(t *testing.T, done func(string) bool) string {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		var texts string
		for _, u := range ingressUpdates {
			if u.Update.Content != nil {
				texts += u.Update.Content.Text
			}
		}
		mu.Unlock()
		if done(texts) {
			return texts
		}
		select {
		case <-deadline:
			return texts
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// fakeACPTaskClient 是带 update 收集的 ACP 客户端。
type fakeACPTaskClient struct {
	conn *protocol.Conn
}

var (
	ingressUpdates []protocol.SessionUpdate
	mu             sync.Mutex
)

func newFakeACPTaskClient(conn net.Conn) *fakeACPTaskClient {
	fc := &fakeACPTaskClient{conn: protocol.NewConn(conn)}
	fc.conn.HandleNotification(protocol.MethodSessionUpdate, func(params json.RawMessage) {
		var u protocol.SessionUpdate
		if err := json.Unmarshal(params, &u); err == nil {
			mu.Lock()
			ingressUpdates = append(ingressUpdates, u)
			mu.Unlock()
		}
	})
	go func() { _ = fc.conn.Serve() }()
	return fc
}

// TestIngress_DetachAndCompensation 异步入队即返回 → 断连安全 →
// 新连接 --status 查询 → --approve 审批（补偿接口全链路）。
func TestIngress_DetachAndCompensation(t *testing.T) {
	p, _ := newTestPlugin(t)
	repo := initGitRepo(t)
	short, err := os.MkdirTemp("", "gwd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(short) })
	p.paths.DispatchDir = short
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	dial := func() *fakeACPTaskClient {
		conn, err := net.Dial("unix", filepath.Join(short, "acp.sock"))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		fc := newFakeACPTaskClient(conn)
		t.Cleanup(func() { _ = fc.conn.Close() })
		return fc
	}

	// 第一条连接：--detach 提交，立即返回 task ID，随后主动断连
	fc := dial()
	var newResp protocol.NewSessionResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodSessionNew,
		protocol.NewSessionRequest{Cwd: repo}, &newResp); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	ingressUpdates = nil // 丢弃此前测试的通知残留
	mu.Unlock()
	var promptResp protocol.PromptResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: newResp.SessionID,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("--detach add a feature")},
	}, &promptResp); err != nil {
		t.Fatalf("detach prompt: %v", err)
	}
	if promptResp.StopReason != protocol.StopEndTurn {
		t.Fatalf("detach stop = %q", promptResp.StopReason)
	}
	detachTexts := pollUpdatesText(t, func(text string) bool { return strings.Contains(text, "已异步入队") })
	taskID := extractTaskID(t, detachTexts)
	_ = fc.conn.Close() // 模拟提交方断连

	// 任务应继续执行到 awaiting_review（不受断连影响）
	tk := waitForStatus(t, p, taskID, task.StatusAwaitingReview)
	if len(tk.Commits) != 1 {
		t.Fatalf("commits = %v", tk.Commits)
	}

	// 新连接：--status 查询快照
	fc2 := dial()
	if err := fc2.conn.Call(context.Background(), protocol.MethodSessionNew,
		protocol.NewSessionRequest{Cwd: repo}, &newResp); err != nil {
		t.Fatal(err)
	}
	if err := fc2.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: newResp.SessionID,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("--status " + taskID)},
	}, &promptResp); err != nil {
		t.Fatalf("status prompt: %v", err)
	}
	mu.Lock()
	ingressUpdates = nil
	mu.Unlock()
	if err := fc2.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: newResp.SessionID,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("--status " + taskID)},
	}, &promptResp); err != nil {
		t.Fatal(err)
	}
	_ = promptResp
	snapshotText := pollUpdatesText(t, func(text string) bool {
		return strings.Contains(text, `"status": "awaiting_review"`) && strings.Contains(text, taskID)
	})
	if !strings.Contains(snapshotText, `"status": "awaiting_review"`) || !strings.Contains(snapshotText, taskID) {
		t.Errorf("status snapshot = %q", snapshotText)
	}

	// 远程审批：--approve → done
	if err := fc2.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: newResp.SessionID,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("--approve " + taskID)},
	}, &promptResp); err != nil {
		t.Fatalf("approve prompt: %v", err)
	}
	waitForStatus(t, p, taskID, task.StatusDone)
}
