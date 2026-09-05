// Package dispatcher 是 commit dispatcher 的 daemon 插件适配器：
// 把 ai-dispatch 的编排器（Task 状态机 × ACP worker）装进 plugin.Plugin，
// 提供 /dispatch /workers 命令族与审批入口。设计见 docs/dispatcher.md。
package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"os/exec"

	"github.com/tinguo/goworker/ai-dispatch"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-dispatch/task"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// DispatcherPlugin 是 dispatcher 插件入口：持有任务 store、编排器与运行中任务表。
type DispatcherPlugin struct {
	cfg   *runtimeconfig.Config
	paths runtimeconfig.Paths
	hub   *plugin.Hub

	store *task.Store
	orch  *dispatch.Orchestrator

	// running 追踪在跑的任务：taskID → cancel。Stop 时统一取消并等待退出。
	mu      sync.Mutex
	running map[string]context.CancelFunc
	wg      sync.WaitGroup
}

// NewPlugin 构造 dispatcher 插件。cfg.Dispatch.Enabled=false 时 main 不会注册本插件。
func NewPlugin(cfg *runtimeconfig.Config, paths runtimeconfig.Paths) *DispatcherPlugin {
	return &DispatcherPlugin{cfg: cfg, paths: paths, running: make(map[string]context.CancelFunc)}
}

func (p *DispatcherPlugin) Name() string { return "dispatcher" }

func (p *DispatcherPlugin) Init(h *plugin.Hub) error {
	p.hub = h

	store, err := task.Open(filepath.Join(p.paths.DispatchDir, "tasks.jsonl"))
	if err != nil {
		return fmt.Errorf("dispatcher: open task store: %w", err)
	}
	p.store = store
	p.orch = dispatch.NewOrchestrator(store)
	p.orch.SetProgress(func(taskID string, u protocol.SessionUpdateBody) {
		slog.Debug("dispatch progress", "task", taskID, "kind", u.SessionUpdate)
	})

	if err := h.RegisterCommand(plugin.Command{
		Name:        "/dispatch",
		Description: "commit dispatcher：加任务/查任务/审批，用法见 /dispatch help",
		Handler:     p.handleDispatch,
	}); err != nil {
		return err
	}
	return h.RegisterCommand(plugin.Command{
		Name:        "/workers",
		Description: "查看已配置的 ACP worker 清单",
		Handler:     p.handleWorkers,
	})
}

// SetOpener 注入 worker 连接方式（测试用）。
func (p *DispatcherPlugin) SetOpener(op dispatch.Opener) { p.orch.SetOpener(op) }

func (p *DispatcherPlugin) Start() error { return nil }

func (p *DispatcherPlugin) Stop() error {
	p.mu.Lock()
	for _, cancel := range p.running {
		cancel()
	}
	p.mu.Unlock()
	p.wg.Wait()
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// ---- /dispatch ----

func (p *DispatcherPlugin) handleDispatch(ctx *plugin.Context) error {
	args := ctx.Args
	if len(args) == 0 || args[0] == "help" {
		p.writeUsage(ctx)
		return nil
	}

	switch args[0] {
	case "ls":
		return p.handleList(ctx, args[1:])
	case "show":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch show <task_id>\n")
			return nil
		}
		return p.handleShow(ctx, args[1])
	case "approve":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch approve <task_id>\n")
			return nil
		}
		return p.handleApprove(ctx, args[1])
	case "complete":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch complete <task_id>   ← 人工合并完成后收尾\n")
			return nil
		}
		return p.handleComplete(ctx, args[1])
	case "reject":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch reject <task_id>\n")
			return nil
		}
		return p.handleReject(ctx, args[1])
	case "cancel":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch cancel <task_id>\n")
			return nil
		}
		return p.handleCancel(ctx, args[1])
	}

	// 加任务：[/worker|--general] 前缀 + prompt
	worker, err := p.cfg.Dispatch.ResolveDefaultWorker()
	if err != nil {
		ctx.Writer("✘ " + err.Error() + "\n")
		return nil
	}
	general := false
	promptArgs := args
	for len(promptArgs) > 0 {
		arg := promptArgs[0]
		if strings.HasPrefix(arg, "@") && len(arg) > 1 {
			worker = arg[1:]
			promptArgs = promptArgs[1:]
			continue
		}
		if arg == "--general" {
			general = true
			promptArgs = promptArgs[1:]
			continue
		}
		break
	}
	if len(promptArgs) == 0 {
		p.writeUsage(ctx)
		return nil
	}
	if _, ok := p.cfg.Dispatch.Worker(worker); !ok {
		ctx.Writer(fmt.Sprintf("✘ worker %q 未配置（/workers 查看）\n", worker))
		return nil
	}
	return p.addTask(ctx, worker, general, strings.Join(promptArgs, " "))
}

func (p *DispatcherPlugin) writeUsage(ctx *plugin.Context) {
	ctx.Writer(`用法:
  /dispatch <prompt>              ← 当前目录是 git 仓库 → code 任务（worktree 隔离）；否则 general 任务
  /dispatch --general <prompt>    ← 强制 general 任务
  /dispatch @claude <prompt>      ← 指定 worker
  /dispatch ls [status]           ← 任务列表
  /dispatch show <id>             ← 任务详情
  /dispatch approve <id>          ← 审批通过（code 任务尝试 ff 合并）
  /dispatch complete <id>         ← 人工合并完成后收尾（清 worktree、标记 done）
  /dispatch reject <id>           ← 拒绝（弃置任务产物）
  /dispatch cancel <id>           ← 取消（运行中任务会中断 worker）
`)
}

func (p *DispatcherPlugin) addTask(ctx *plugin.Context, worker string, general bool, prompt string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	kind := task.KindCode
	if general || !dispatch.DetectGit(ctx.Ctx, cwd) {
		kind = task.KindGeneral
	}

	p.mu.Lock()
	maxParallel := p.cfg.Dispatch.MaxParallel
	if maxParallel <= 0 {
		maxParallel = 1
	}
	if len(p.running) >= maxParallel {
		p.mu.Unlock()
		ctx.Writer(fmt.Sprintf("✘ 已有 %d 个任务在运行（max_parallel=%d），稍后再派\n", len(p.running), maxParallel))
		return nil
	}
	p.mu.Unlock()

	now := time.Now()
	t := &task.Task{
		ID:        task.NewID(),
		Source:    "repl",
		Kind:      kind,
		Prompt:    prompt,
		Repo:      cwd,
		Worker:    worker,
		Status:    task.StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := p.store.Add(t); err != nil {
		return err
	}
	p.launch(t)
	ctx.Writer(fmt.Sprintf("⏳ 任务 %s 已入队（%s → %s worker）\n", t.ID, kind, worker))
	return nil
}

// launch 后台执行任务。orchestrator 负责状态机流转与 worktree 清理。
func (p *DispatcherPlugin) launch(t *task.Task) {
	runCtx, cancel := context.WithCancel(context.Background())
	p.mu.Lock()
	p.running[t.ID] = cancel
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			p.mu.Lock()
			delete(p.running, t.ID)
			p.mu.Unlock()
		}()

		spec := p.workerSpec(t.Worker)
		outcome, err := p.orch.Run(runCtx, t, spec)
		if err != nil {
			slog.Warn("dispatch task ended", "task", t.ID, "err", err)
			return
		}
		slog.Info("dispatch task awaiting review", "task", t.ID, "stop", outcome.StopReason,
			"commits", len(outcome.Task.Commits))
	}()
}

func (p *DispatcherPlugin) workerSpec(name string) dispatch.WorkerSpec {
	if w, ok := p.cfg.Dispatch.Worker(name); ok {
		return dispatch.WorkerSpec{Name: w.Name, Command: w.Command, Args: w.Args}
	}
	return dispatch.WorkerSpec{Name: name}
}

func (p *DispatcherPlugin) handleList(ctx *plugin.Context, args []string) error {
	filter := ""
	if len(args) > 0 {
		filter = args[0]
	}
	tasks := p.store.List()
	if len(tasks) == 0 {
		ctx.Writer("（暂无任务）\n")
		return nil
	}
	for _, t := range tasks {
		if filter != "" && string(t.Status) != filter {
			continue
		}
		line := fmt.Sprintf("%s  %-15s %-8s %-8s %s\n", t.ID, t.Status, t.Kind, t.Worker, truncate(t.Prompt, 48))
		ctx.Writer(line)
	}
	return nil
}

func (p *DispatcherPlugin) handleShow(ctx *plugin.Context, id string) error {
	t, ok := p.store.Get(id)
	if !ok {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 不存在\n", id))
		return nil
	}
	data, _ := json.MarshalIndent(t, "", "  ")
	ctx.Writer(string(data) + "\n")
	return nil
}

// handleApprove 审批通过：code 任务尝试 ff 合并（失败则提示走人工 complete），
// general 任务直接标记完成。
func (p *DispatcherPlugin) handleApprove(ctx *plugin.Context, id string) error {
	t, ok := p.store.Get(id)
	if !ok {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 不存在\n", id))
		return nil
	}
	if t.Status != task.StatusAwaitingReview {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 状态为 %s，仅 awaiting_review 可审批\n", id, t.Status))
		return nil
	}
	p.audit("approve", t.ID, "")

	if t.Kind == task.KindGeneral {
		if err := t.Transition(task.StatusDone); err != nil {
			return err
		}
		if err := p.store.Update(&t); err != nil {
			return err
		}
		ctx.Writer(fmt.Sprintf("✔ 任务 %s 已完成\n", id))
		return nil
	}

	// code 任务：ff 合并进派发时的分支头
	if out, err := gitMergeFF(t.Repo, t.Branch); err != nil {
		ctx.Writer(fmt.Sprintf("✘ ff 合并失败（可能有分叉或冲突）：\n%s\n请人工合并后执行 /dispatch complete %s\n", strings.TrimSpace(out), id))
		return nil
	}
	if err := p.finishCodeTask(ctx, &t, "approved"); err != nil {
		return err
	}
	ctx.Writer(fmt.Sprintf("✔ 任务 %s 已合并并完成\n", id))
	return nil
}

// handleComplete 人工合并后的收尾：清 worktree + 分支，标记 done。
func (p *DispatcherPlugin) handleComplete(ctx *plugin.Context, id string) error {
	snapshot, ok := p.store.Get(id)
	if !ok || snapshot.Kind != task.KindCode {
		ctx.Writer(fmt.Sprintf("✘ code 任务 %s 不存在\n", id))
		return nil
	}
	t := snapshot
	if t.Status != task.StatusAwaitingReview {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 状态为 %s，仅 awaiting_review 可收尾\n", id, t.Status))
		return nil
	}
	p.audit("complete", t.ID, "")
	if err := p.finishCodeTask(ctx, &t, "merged-manually"); err != nil {
		return err
	}
	ctx.Writer(fmt.Sprintf("✔ 任务 %s 已收尾完成\n", id))
	return nil
}

func (p *DispatcherPlugin) finishCodeTask(ctx *plugin.Context, t *task.Task, via string) error {
	if err := t.Transition(task.StatusMerging); err != nil {
		return err
	}
	if err := p.store.Update(t); err != nil {
		return err
	}
	_ = dispatch.RemoveWorktree(ctx.Ctx, t.Repo, t.Worktree, t.Branch, true)
	if err := t.Transition(task.StatusDone); err != nil {
		return err
	}
	p.audit("done", t.ID, via)
	return p.store.Update(t)
}

// handleReject 拒绝任务：code 任务弃置 worktree 与分支。
func (p *DispatcherPlugin) handleReject(ctx *plugin.Context, id string) error {
	t, ok := p.store.Get(id)
	if !ok {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 不存在\n", id))
		return nil
	}
	if t.Status != task.StatusAwaitingReview {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 状态为 %s，仅 awaiting_review 可拒绝\n", id, t.Status))
		return nil
	}
	if err := t.Transition(task.StatusRejected); err != nil {
		return err
	}
	if err := p.store.Update(&t); err != nil {
		return err
	}
	if t.Kind == task.KindCode && t.Worktree != "" {
		_ = dispatch.RemoveWorktree(ctx.Ctx, t.Repo, t.Worktree, t.Branch, true)
	}
	p.audit("reject", t.ID, "")
	ctx.Writer(fmt.Sprintf("✘ 任务 %s 已拒绝，产物已清理\n", id))
	return nil
}

// handleCancel 取消任务：运行中的取消 ctx（orchestrator 落 cancelled），
// 排队/待审的直接流转。
func (p *DispatcherPlugin) handleCancel(ctx *plugin.Context, id string) error {
	t, ok := p.store.Get(id)
	if !ok {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 不存在\n", id))
		return nil
	}
	if t.Status.Terminal() {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 已是终态 %s\n", id, t.Status))
		return nil
	}

	p.mu.Lock()
	cancel, running := p.running[id]
	p.mu.Unlock()
	if running {
		p.audit("cancel", t.ID, "running")
		cancel()
		ctx.Writer(fmt.Sprintf("⏳ 任务 %s 取消中（worker 中断后落 cancelled）\n", id))
		return nil
	}
	if err := t.Transition(task.StatusCancelled); err != nil {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 无法取消: %v\n", id, err))
		return nil
	}
	if err := p.store.Update(&t); err != nil {
		return err
	}
	if t.Kind == task.KindCode && t.Worktree != "" {
		_ = dispatch.RemoveWorktree(ctx.Ctx, t.Repo, t.Worktree, t.Branch, true)
	}
	p.audit("cancel", t.ID, "queued")
	ctx.Writer(fmt.Sprintf("✘ 任务 %s 已取消\n", id))
	return nil
}

// ---- /workers ----

func (p *DispatcherPlugin) handleWorkers(ctx *plugin.Context) error {
	cfg := p.cfg.Dispatch
	ctx.Writer(fmt.Sprintf("dispatcher: enabled=%v max_parallel=%d\n", cfg.Enabled, orOne(cfg.MaxParallel)))
	defaultWorker, _ := cfg.ResolveDefaultWorker()
	for _, w := range cfg.Workers {
		marker := "  "
		if w.Name == defaultWorker {
			marker = "→ "
		}
		ctx.Writer(fmt.Sprintf("%s%-12s %s %s\n", marker, w.Name, w.Command, strings.Join(w.Args, " ")))
	}
	return nil
}

// ---- 审计 ----

// audit 追加一条 dispatcher 决策记录到 audit/dispatch.jsonl。
func (p *DispatcherPlugin) audit(action, taskID, detail string) {
	if p.paths.AuditDir == "" {
		return
	}
	if err := os.MkdirAll(p.paths.AuditDir, 0o755); err != nil {
		return
	}
	f, err := os.OpenFile(filepath.Join(p.paths.AuditDir, "dispatch.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	record, _ := json.Marshal(map[string]string{
		"ts": time.Now().UTC().Format(time.RFC3339), "action": action, "task": taskID, "detail": detail,
	})
	_, _ = f.Write(append(record, '\n'))
}

// ---- helpers ----

// gitMergeFF 在 repo 中 fast-forward 合并分支。
func gitMergeFF(repo, branch string) (string, error) {
	cmd := exec.Command("git", "-C", repo, "merge", "--ff-only", branch)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func truncate(s string, n int) string {
	r := []rune(strings.TrimSpace(s))
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n]) + "…"
}

func orOne(n int) int {
	if n <= 0 {
		return 1
	}
	return n
}
