package dispatcher

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-dispatch"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-dispatch/task"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// recordingHub 用真实 plugin.Hub 结构（函数字段）记录注册的命令与事件。
type recordingHub struct {
	mu       sync.Mutex
	commands map[string]plugin.Command
	events   []plugin.Event
}

func newRecordingHub() (*plugin.Hub, *recordingHub) {
	rh := &recordingHub{commands: map[string]plugin.Command{}}
	return &plugin.Hub{
		RegisterCommand: func(cmd plugin.Command) error {
			rh.commands[cmd.Name] = cmd
			return nil
		},
		Notify: func(event plugin.Event) {
			rh.mu.Lock()
			defer rh.mu.Unlock()
			rh.events = append(rh.events, event)
		},
	}, rh
}

// eventsOfType 返回指定类型的事件载荷。
func (h *recordingHub) eventsOfType(t string) []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []any
	for _, e := range h.events {
		if string(e.Type) == t {
			out = append(out, e.Payload)
		}
	}
	return out
}

// commitWorker 模拟干活的 ACP worker：在 cwd 是 git 仓库时产生一个空 commit。
type commitWorker struct {
	conn *protocol.Conn

	mu      sync.Mutex
	workdir string
}

func newCommitWorker(rwc io.ReadWriteCloser) *commitWorker {
	w := &commitWorker{conn: protocol.NewConn(rwc)}
	w.conn.Handle(protocol.MethodInitialize, func(context.Context, json.RawMessage) (any, error) {
		return protocol.InitializeResponse{ProtocolVersion: protocol.Version}, nil
	})
	w.conn.Handle(protocol.MethodSessionNew, func(_ context.Context, params json.RawMessage) (any, error) {
		var req protocol.NewSessionRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, err
		}
		w.mu.Lock()
		w.workdir = req.Cwd
		w.mu.Unlock()
		return protocol.NewSessionResponse{SessionID: "sess_fake"}, nil
	})
	w.conn.Handle(protocol.MethodSessionPrompt, func(_ context.Context, params json.RawMessage) (any, error) {
		var req protocol.PromptRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, err
		}
		_ = w.conn.Notify(protocol.MethodSessionUpdate, protocol.SessionUpdate{
			SessionID: req.SessionID,
			Update: protocol.SessionUpdateBody{
				SessionUpdate: protocol.UpdateAgentMessageChunk,
				Content:       &protocol.ContentBlock{Type: "text", Text: "committing"},
			},
		})
		w.mu.Lock()
		dir := w.workdir
		w.mu.Unlock()
		if dispatch.DetectGit(context.Background(), dir) {
			cmd := exec.Command("git", "-C", dir, "commit", "--allow-empty", "-m", "worker commit")
			if err := cmd.Run(); err != nil {
				return nil, err
			}
		}
		return protocol.PromptResponse{StopReason: protocol.StopEndTurn}, nil
	})
	go func() { _ = w.conn.Serve() }()
	return w
}

// ---- fixtures ----

func newTestPlugin(t *testing.T) (*DispatcherPlugin, *recordingHub) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	cfg := runtimeconfig.DefaultLLM()
	_ = cfg
	runtimeCfg := &runtimeconfig.Config{
		Dispatch: runtimeconfig.DispatchConfig{
			Enabled:       true,
			DefaultWorker: "fake",
			Workers:       []runtimeconfig.WorkerConfig{{Name: "fake", Command: "fake"}},
		},
	}
	paths := runtimeconfig.Paths{
		AuditDir:    filepath.Join(dir, "audit"),
		DispatchDir: filepath.Join(dir, "dispatch"),
	}
	p := NewPlugin(runtimeCfg, paths)
	hub, rh := newRecordingHub()
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}
	p.SetOpener(func(context.Context, dispatch.WorkerSpec) (io.ReadWriteCloser, io.Closer, error) {
		clientEnd, agentEnd := net.Pipe()
		w := newCommitWorker(agentEnd)
		t.Cleanup(func() { _ = w.conn.Close() })
		return clientEnd, clientEnd, nil
	})
	t.Cleanup(func() { _ = p.Stop() })
	return p, rh
}

func runCmd(t *testing.T, p *DispatcherPlugin, hub *recordingHub, name string, args ...string) string {
	t.Helper()
	var out strings.Builder
	cmd, ok := hub.commands[name]
	if !ok {
		t.Fatalf("command %s not registered", name)
	}
	ctx := plugin.NewContext(context.Background(), func(s string) { out.WriteString(s) }, nil, args)
	if err := cmd.Handler(ctx); err != nil {
		t.Fatalf("%s %v: %v", name, args, err)
	}
	return out.String()
}

func waitForStatus(t *testing.T, p *DispatcherPlugin, id string, want task.Status) task.Task {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		tk, ok := p.store.Get(id)
		if ok && tk.Status == want {
			return tk
		}
		select {
		case <-deadline:
			tk, _ := p.store.Get(id)
			t.Fatalf("task %s status = %+v, want %s", id, tk, want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// ---- tests ----

func TestPlugin_CodeTaskApproveMergesAndCleansUp(t *testing.T) {
	p, hub := newTestPlugin(t)
	repo := initGitRepo(t)
	changeCwd(t, repo)

	out := runCmd(t, p, hub, "/dispatch", "add a feature")
	if !strings.Contains(out, "已入队") {
		t.Fatalf("add output = %q", out)
	}
	id := extractTaskID(t, out)

	tk := waitForStatus(t, p, id, task.StatusAwaitingReview)
	if len(tk.Commits) != 1 || !strings.Contains(tk.Commits[0], "worker commit") {
		t.Errorf("commits = %v", tk.Commits)
	}
	if _, err := exec.Command("git", "-C", repo, "rev-parse", "--verify", "dispatch/"+id).CombinedOutput(); err != nil {
		t.Fatalf("task branch should exist: %v", err)
	}

	// worktree 里还有未合并 commit；主分支 HEAD 未动
	baseOut, _ := exec.Command("git", "-C", repo, "rev-parse", "HEAD").CombinedOutput()
	baseHead := strings.TrimSpace(string(baseOut))

	out = runCmd(t, p, hub, "/dispatch", "approve", id)
	if !strings.Contains(out, "已合并并完成") {
		t.Fatalf("approve output = %q", out)
	}
	tk = waitForStatus(t, p, id, task.StatusDone)

	// ff 合并后 HEAD 前进，worktree 与分支清理
	newHeadOut, _ := exec.Command("git", "-C", repo, "rev-parse", "HEAD").CombinedOutput()
	if strings.TrimSpace(string(newHeadOut)) == baseHead {
		t.Error("HEAD should advance after ff merge")
	}
	if _, err := exec.Command("git", "-C", repo, "rev-parse", "--verify", "dispatch/"+id).CombinedOutput(); err == nil {
		t.Error("task branch should be deleted after merge")
	}
	wtList, _ := exec.Command("git", "-C", repo, "worktree", "list", "--porcelain").CombinedOutput()
	if strings.Contains(string(wtList), id) {
		t.Errorf("worktree should be removed:\n%s", wtList)
	}
}

func TestPlugin_GeneralTaskFullFlow(t *testing.T) {
	p, hub := newTestPlugin(t)
	dir := t.TempDir() // 非 git
	changeCwd(t, dir)

	out := runCmd(t, p, hub, "/dispatch", "--general", "write a note")
	id := extractTaskID(t, out)

	tk := waitForStatus(t, p, id, task.StatusAwaitingReview)
	if tk.Kind != task.KindGeneral || tk.Commits != nil {
		t.Errorf("task = %+v", tk)
	}

	out = runCmd(t, p, hub, "/dispatch", "tail", id)
	if !strings.Contains(out, "committing") {
		t.Fatalf("tail output = %q, want worker message", out)
	}

	out = runCmd(t, p, hub, "/dispatch", "approve", id)
	if !strings.Contains(out, "已完成") {
		t.Fatalf("approve output = %q", out)
	}
	waitForStatus(t, p, id, task.StatusDone)
}

func TestPlugin_TailStreamsRunningTaskUpdates(t *testing.T) {
	p, _ := newTestPlugin(t)
	now := time.Now()
	tk := &task.Task{
		ID:        "task_live",
		Source:    "test",
		Kind:      task.KindGeneral,
		Prompt:    "stream",
		Repo:      t.TempDir(),
		Worker:    "fake",
		Status:    task.StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := p.store.Add(tk); err != nil {
		t.Fatalf("store.Add: %v", err)
	}
	if err := tk.Transition(task.StatusDispatching); err != nil {
		t.Fatal(err)
	}
	if err := tk.Transition(task.StatusWorking); err != nil {
		t.Fatal(err)
	}
	if err := p.store.Update(tk); err != nil {
		t.Fatalf("store.Update: %v", err)
	}

	if err := p.eventLog.Append(task.TaskEvent{
		TaskID: tk.ID,
		Type:   task.EventUpdate,
		Update: []byte(`{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"history"}}`),
	}); err != nil {
		t.Fatalf("append history: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var mu sync.Mutex
	var output strings.Builder
	attached := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- p.handleTail(plugin.NewContext(ctx, func(text string) {
			mu.Lock()
			output.WriteString(text)
			mu.Unlock()
			if strings.Contains(text, "history") {
				select {
				case attached <- struct{}{}:
				default:
				}
			}
		}, nil, nil), tk.ID)
	}()

	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Fatal("tail did not replay history")
	}

	if err := p.eventLog.Append(task.TaskEvent{
		TaskID: tk.ID,
		Type:   task.EventUpdate,
		Update: []byte(`{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"live update"}}`),
	}); err != nil {
		t.Fatalf("append update: %v", err)
	}
	if err := p.eventLog.Append(task.TaskEvent{TaskID: tk.ID, Type: task.EventStatus, From: task.StatusWorking, To: task.StatusAwaitingReview}); err != nil {
		t.Fatalf("append status: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleTail: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("tail did not stop at awaiting_review")
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(output.String(), "live update") {
		t.Fatalf("tail output = %q, want live update", output.String())
	}
}

func TestPlugin_RejectCleansUpWorktree(t *testing.T) {
	p, hub := newTestPlugin(t)
	repo := initGitRepo(t)
	changeCwd(t, repo)

	addOut := runCmd(t, p, hub, "/dispatch", "some task")
	id := extractTaskID(t, addOut)
	waitForStatus(t, p, id, task.StatusAwaitingReview)

	out := runCmd(t, p, hub, "/dispatch", "reject", id)
	if !strings.Contains(out, "已拒绝") {
		t.Fatalf("reject output = %q", out)
	}
	tk := waitForStatus(t, p, id, task.StatusRejected)
	if _, err := exec.Command("git", "-C", repo, "rev-parse", "--verify", tk.Branch).CombinedOutput(); err == nil {
		t.Error("branch should be deleted on reject")
	}
}

func TestPlugin_UnknownWorkerRejected(t *testing.T) {
	p, hub := newTestPlugin(t)
	changeCwd(t, t.TempDir())
	out := runCmd(t, p, hub, "/dispatch", "@nosuch", "task")
	if !strings.Contains(out, "未配置") {
		t.Fatalf("output = %q", out)
	}
}

func TestPlugin_WorkersListing(t *testing.T) {
	p, hub := newTestPlugin(t)
	out := runCmd(t, p, hub, "/workers")
	if !strings.Contains(out, "fake") || !strings.Contains(out, "→") {
		t.Errorf("workers output = %q", out)
	}
}

// ---- git fixtures ----

func initGitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	git := func(args ...string) {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	git("init", "-q")
	git("config", "user.email", "dispatch@test")
	git("config", "user.name", "dispatch")
	git("commit", "--allow-empty", "-q", "-m", "init")
	return dir
}

func changeCwd(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })
}

func extractTaskID(t *testing.T, out string) string {
	t.Helper()
	re := regexp.MustCompile(`task_[0-9a-f]+`)
	if id := re.FindString(out); id != "" {
		return id
	}
	t.Fatalf("no task id in output: %q", out)
	return ""
}
