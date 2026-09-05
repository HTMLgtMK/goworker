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
