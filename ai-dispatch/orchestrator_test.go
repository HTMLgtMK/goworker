package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-dispatch/task"
)

// commitWorker 模拟一个「干活」的 worker：在 session/new 给的 cwd 里产生一个空 commit。
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
	w.conn.Handle(protocol.MethodSessionPrompt, w.handlePrompt)
	go func() { _ = w.conn.Serve() }()
	return w
}

func (w *commitWorker) handlePrompt(ctx context.Context, params json.RawMessage) (any, error) {
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
	// git 仓库里才产生 commit（general 任务在非 git 目录跑）
	if DetectGit(ctx, dir) {
		cmd := exec.Command("git", "-C", dir, "commit", "--allow-empty", "-m", "worker commit")
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("worker commit: %w: %s", err, out)
		}
	}
	return protocol.PromptResponse{StopReason: protocol.StopEndTurn}, nil
}

// failingWorker 在 initialize 阶段直接报错。
func newFailingWorker(rwc io.ReadWriteCloser) *commitWorker {
	w := &commitWorker{conn: protocol.NewConn(rwc)}
	w.conn.Handle(protocol.MethodInitialize, func(context.Context, json.RawMessage) (any, error) {
		return nil, &protocol.RPCError{Code: -32000, Message: "auth required"}
	})
	go func() { _ = w.conn.Serve() }()
	return w
}

func newCodeTask(repo string) *task.Task {
	now := time.Now()
	return &task.Task{
		ID:        task.NewID(),
		Source:    "repl",
		Kind:      task.KindCode,
		Prompt:    "add a feature",
		Repo:      repo,
		Worker:    "fake",
		Status:    task.StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// workerOpener 把 fake worker 接进 Orchestrator 的连接建立环节。
func workerOpener(t *testing.T, newWorker func(io.ReadWriteCloser) *commitWorker) Opener {
	t.Helper()
	return func(context.Context, WorkerSpec) (io.ReadWriteCloser, io.Closer, error) {
		clientEnd, agentEnd := net.Pipe()
		w := newWorker(agentEnd)
		t.Cleanup(func() { _ = w.conn.Close() })
		return clientEnd, clientEnd, nil
	}
}

func TestOrchestrator_CodeTaskHappyPath(t *testing.T) {
	repo := initRepo(t)
	store, err := task.Open(filepath.Join(t.TempDir(), "tasks.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	orch := NewOrchestrator(store)
	var mu sync.Mutex
	var progressTexts []string
	orch.SetProgress(func(taskID string, u protocol.SessionUpdateBody) {
		if u.Content != nil {
			mu.Lock()
			progressTexts = append(progressTexts, u.Content.Text)
			mu.Unlock()
		}
	})
	orch.SetOpener(workerOpener(t, newCommitWorker))

	tk := newCodeTask(repo)
	outcome, err := orch.Run(context.Background(), tk, WorkerSpec{Name: "fake", Command: "fake"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcome.StopReason != protocol.StopEndTurn {
		t.Errorf("stopReason = %q", outcome.StopReason)
	}
	if outcome.Task.Status != task.StatusAwaitingReview {
		t.Errorf("status = %q, want awaiting_review", outcome.Task.Status)
	}
	if outcome.Task.BaseCommit == "" || outcome.Task.Worktree == "" || outcome.Task.Branch == "" {
		t.Errorf("code task workspace fields incomplete: %+v", outcome.Task)
	}
	if len(outcome.Task.Commits) != 1 || !strings.Contains(outcome.Task.Commits[0], "worker commit") {
		t.Errorf("commits = %v, want the worker commit", outcome.Task.Commits)
	}

	mu.Lock()
	if len(progressTexts) == 0 || progressTexts[0] != "committing" {
		mu.Unlock()
		t.Errorf("progress = %v, want [committing]", progressTexts)
	} else {
		mu.Unlock()
	}

	// awaiting_review 保留 worktree 供审批
	if !DetectGit(context.Background(), outcome.Task.Worktree) {
		t.Error("worktree should survive until review")
	}
	// store 快照与内存一致
	saved, ok := store.Get(tk.ID)
	if !ok || saved.Status != task.StatusAwaitingReview || len(saved.Commits) != 1 {
		t.Errorf("stored task = %+v ok=%v", saved, ok)
	}

	// 测试收尾：清 worktree
	_ = RemoveWorktree(context.Background(), repo, outcome.Task.Worktree, outcome.Task.Branch, true)
}

func TestOrchestrator_GeneralTaskSkipsWorktree(t *testing.T) {
	dir := t.TempDir() // 非 git 目录
	store, err := task.Open(filepath.Join(t.TempDir(), "tasks.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	orch := NewOrchestrator(store)
	orch.SetOpener(workerOpener(t, newCommitWorker))

	now := time.Now()
	tk := &task.Task{
		ID:        task.NewID(),
		Source:    "repl",
		Kind:      task.KindGeneral,
		Prompt:    "write a research note",
		Repo:      dir,
		Worker:    "fake",
		Status:    task.StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
	outcome, err := orch.Run(context.Background(), tk, WorkerSpec{Name: "fake", Command: "fake"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcome.Task.Status != task.StatusAwaitingReview {
		t.Errorf("status = %q", outcome.Task.Status)
	}
	if outcome.Task.Worktree != "" || outcome.Task.BaseCommit != "" || outcome.Task.Commits != nil {
		t.Errorf("general task should not touch git artifacts: %+v", outcome.Task)
	}
}

func TestOrchestrator_CodeTaskRequiresGitRepo(t *testing.T) {
	store, err := task.Open(filepath.Join(t.TempDir(), "tasks.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	orch := NewOrchestrator(store)
	orch.SetOpener(workerOpener(t, newCommitWorker))

	tk := newCodeTask(t.TempDir()) // 非 git
	if _, err := orch.Run(context.Background(), tk, WorkerSpec{Name: "fake", Command: "fake"}); err == nil {
		t.Fatal("expected error for non-git repo")
	}
	saved, _ := store.Get(tk.ID)
	if saved.Status != task.StatusFailed {
		t.Errorf("status = %q, want failed", saved.Status)
	}
}

func TestOrchestrator_WorkerFailureFailsTaskAndCleansWorktree(t *testing.T) {
	repo := initRepo(t)
	store, err := task.Open(filepath.Join(t.TempDir(), "tasks.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	orch := NewOrchestrator(store)
	orch.SetOpener(workerOpener(t, newFailingWorker))

	tk := newCodeTask(repo)
	if _, err := orch.Run(context.Background(), tk, WorkerSpec{Name: "fake", Command: "fake"}); err == nil {
		t.Fatal("expected error from failing worker")
	}
	saved, _ := store.Get(tk.ID)
	if saved.Status != task.StatusFailed {
		t.Errorf("status = %q, want failed", saved.Status)
	}
	if saved.Error == "" {
		t.Error("failed task should record the cause")
	}
	if _, err := exec.Command("git", "-C", repo, "worktree", "list").CombinedOutput(); err != nil {
		t.Fatalf("worktree list: %v", err)
	}
	out, _ := exec.Command("git", "-C", repo, "worktree", "list", "--porcelain").CombinedOutput()
	_ = out
	if strings.Contains(string(out), "dispatch/"+tk.ID) {
		t.Errorf("worktree should be cleaned up on failure:\n%s", out)
	}
}
