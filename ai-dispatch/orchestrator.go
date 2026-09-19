package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-dispatch/task"
)

// ProgressFunc 任务进度回调（worker 的 session/update 原样透出，
// 频控由订阅方决定）。
type ProgressFunc func(taskID string, update protocol.SessionUpdateBody)

// Orchestrator 把 Task 状态机与 ACP Client 串成派发链路：
// 准备隔离工作区 → spawn worker → 驱动回合 → 收集产物 → awaiting_review。
type Orchestrator struct {
	store    *task.Store
	opener   Opener // nil 时用 ProcessOpener
	progress ProgressFunc
	// onStatus 状态迁移回调（save 落盘时 diff 触发），宿主用于广播事件
	onStatus func(t *task.Task, from, to task.Status)
}

func NewOrchestrator(store *task.Store) *Orchestrator {
	return &Orchestrator{store: store}
}

// SetOpener 注入连接建立方式（测试用）。
func (o *Orchestrator) SetOpener(op Opener) { o.opener = op }

// SetProgress 注册进度回调。
func (o *Orchestrator) SetProgress(fn ProgressFunc) { o.progress = fn }

// SetStatusListener 注册状态迁移回调（含首次入队 queued）。
func (o *Orchestrator) SetStatusListener(fn func(t *task.Task, from, to task.Status)) {
	o.onStatus = fn
}

// Outcome 是一个任务跑到终点的结果。
type Outcome struct {
	Task       task.Task
	StopReason string
}

// RunOption 定制单次派发行为。
type RunOption func(*runConfig)

type runConfig struct {
	permission func(context.Context, protocol.PermissionRequest) (string, error)
}

// WithPermissionPolicy 覆盖本次派发的权限应答策略；nil = 拒绝（无人值守默认）。
func WithPermissionPolicy(fn func(context.Context, protocol.PermissionRequest) (string, error)) RunOption {
	return func(rc *runConfig) { rc.permission = fn }
}

// Run 派发单个任务到 awaiting_review / failed / cancelled。
// awaiting_review 返回 nil error；其余情况返回 error 且任务已落终态并清理。
func (o *Orchestrator) Run(ctx context.Context, t *task.Task, spec WorkerSpec, opts ...RunOption) (Outcome, error) {
	rc := runConfig{}
	for _, opt := range opts {
		opt(&rc)
	}
	// 崩溃恢复：任务从 store 重放出来时可能停在 dispatching/working（worker 会话
	// 曾建立）。此时跳过入队/建工作区，按能力协商走 session/load 续接。
	resuming := t.Status == task.StatusDispatching || t.Status == task.StatusWorking

	if !resuming {
		// 先落 queued 快照，后续任何失败路径都能以 Update 记录终态
		if err := o.save(t); err != nil {
			return Outcome{}, err
		}
		if err := o.prepare(ctx, t); err != nil {
			return Outcome{}, o.fail(ctx, t, err)
		}
		if err := o.save(t); err != nil {
			return Outcome{}, o.fail(ctx, t, err)
		}
		if err := t.Transition(task.StatusDispatching); err != nil {
			return Outcome{}, o.fail(ctx, t, err)
		}
		if err := o.save(t); err != nil {
			return Outcome{}, o.fail(ctx, t, err)
		}
	} else if err := o.verifyWorkspace(ctx, t); err != nil {
		return Outcome{}, o.fail(ctx, t, err)
	}

	client, err := o.startWorker(ctx, spec)
	if err != nil {
		return Outcome{}, o.fail(ctx, t, err)
	}
	defer func() {
		// processConn.Close 在 worker 卡死时会 5s 强杀并返回诊断错误 ——
		// 吞掉它，强杀这件事就永远无迹可循
		if err := client.Close(); err != nil {
			slog.Warn("dispatch: close worker", "worker", spec.Name, "err", err)
		}
	}()

	init, err := client.Initialize(ctx)
	if err != nil {
		return Outcome{}, o.fail(ctx, t, fmt.Errorf("initialize worker %q: %w", spec.Name, err))
	}
	cwd := t.Worktree
	if cwd == "" {
		cwd = t.Repo
	}
	sessionID, err := o.establishSession(ctx, client, init, t, cwd, resuming)
	if err != nil {
		return Outcome{}, o.fail(ctx, t, err)
	}

	if t.Status != task.StatusWorking {
		if err := t.Transition(task.StatusWorking); err != nil {
			return Outcome{}, o.fail(ctx, t, err)
		}
	}
	if err := o.save(t); err != nil {
		return Outcome{}, o.fail(ctx, t, err)
	}

	client.SetCallbacks(ClientCallbacks{
		OnUpdate: func(u protocol.SessionUpdate) {
			if o.progress != nil {
				o.progress(t.ID, u.Update)
			}
		},
		OnPermission: rc.permission, // nil = 无人值守拒绝
	})

	stop, err := client.Prompt(ctx, sessionID, []protocol.ContentBlock{protocol.TextBlock(t.Prompt)})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// 用户取消：落 cancelled 终态（区别于失败），错误随 ctx 返回
			o.cancelInto(ctx, t, ctxErr)
			return Outcome{}, ctxErr
		}
		return Outcome{}, o.fail(ctx, t, fmt.Errorf("prompt: %w", err))
	}

	if t.Kind == task.KindCode {
		commits, err := CollectCommits(ctx, t.Worktree, t.BaseCommit)
		if err != nil {
			return Outcome{}, o.fail(ctx, t, fmt.Errorf("collect commits: %w", err))
		}
		t.Commits = commits
	}
	if err := t.Transition(task.StatusAwaitingReview); err != nil {
		return Outcome{}, o.fail(ctx, t, err)
	}
	if err := o.save(t); err != nil {
		return Outcome{}, o.fail(ctx, t, err)
	}
	return Outcome{Task: *t, StopReason: stop}, nil
}

// establishSession 建立或恢复 worker 会话：崩溃恢复且 worker 声明 loadSession
// 能力时走 session/load 续接上下文；否则开新会话（任务从头执行，优雅降级）。
// 会话 ID 持久化到 Task.WorkerSession 供下次恢复。
func (o *Orchestrator) establishSession(
	ctx context.Context, client *Client, init protocol.InitializeResponse,
	t *task.Task, cwd string, resuming bool,
) (string, error) {
	if resuming && t.WorkerSession != "" && init.AgentCapabilities.LoadSession {
		sessionID, err := client.SessionLoad(ctx, t.WorkerSession, cwd)
		if err != nil {
			// load 失败不致命：降级为全新会话重跑任务
			sessionID, err = client.NewSession(ctx, cwd)
			if err != nil {
				return "", fmt.Errorf("new session after failed load: %w", err)
			}
		}
		t.WorkerSession = sessionID
		return sessionID, nil
	}
	sessionID, err := client.NewSession(ctx, cwd)
	if err != nil {
		return "", fmt.Errorf("new session: %w", err)
	}
	t.WorkerSession = sessionID
	return sessionID, nil
}

// verifyWorkspace 崩溃恢复前置校验：code 任务的 worktree 应仍存在且可用。
// ctx 透传给 git 子进程：大仓库/网络挂载上 git 变慢时，用户取消能即时生效。
func (o *Orchestrator) verifyWorkspace(ctx context.Context, t *task.Task) error {
	if t.Kind != task.KindCode || t.Worktree == "" {
		return nil
	}
	if !DetectGit(ctx, t.Worktree) {
		return fmt.Errorf("worktree %s is gone or not a git work tree", t.Worktree)
	}
	return nil
}

// prepare 按 Kind 做派发前置：code 任务校验 git 仓库并建 worktree；
// general 任务仅补齐默认工作目录。成功后把结果写回 t（BaseCommit/Worktree/Branch）。
// git 调用全部吃调用方 ctx（见 verifyWorkspace 注释）。
func (o *Orchestrator) prepare(ctx context.Context, t *task.Task) error {
	switch t.Kind {
	case task.KindCode:
		if t.Repo == "" {
			return errors.New("code task requires repo")
		}
		if !DetectGit(ctx, t.Repo) {
			return fmt.Errorf("repo %s is not a git work tree", t.Repo)
		}
		base, err := HeadCommit(ctx, t.Repo)
		if err != nil {
			return fmt.Errorf("resolve HEAD: %w", err)
		}
		t.BaseCommit = base
		t.Branch = "dispatch/" + t.ID
		t.Worktree = filepath.Join(t.Repo, ".goworker", "dispatch", t.ID)
		if err := AddWorktree(ctx, t.Repo, t.Worktree, t.Branch); err != nil {
			return fmt.Errorf("add worktree: %w", err)
		}
	case task.KindGeneral:
		if t.Repo == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return fmt.Errorf("general task without repo requires home dir: %w", err)
			}
			t.Repo = home
		}
	default:
		return fmt.Errorf("invalid kind %q", t.Kind)
	}
	return nil
}

// save 把任务快照写入 store；新任务 Add，已存在 Update。
// 状态变化时触发 onStatus（含首次入队，from 为空）。
func (o *Orchestrator) save(t *task.Task) error {
	old, exists := o.store.Get(t.ID)
	var err error
	if exists {
		err = o.store.Update(t)
	} else {
		err = o.store.Add(t)
	}
	if err == nil && o.onStatus != nil && old.Status != t.Status {
		o.onStatus(t, old.Status, t.Status)
	}
	return err
}

func (o *Orchestrator) startWorker(ctx context.Context, spec WorkerSpec) (*Client, error) {
	opener := o.opener
	if opener == nil {
		opener = ProcessOpener
	}
	return StartWorkerWithOpener(ctx, spec, opener)
}

// cancelInto 把任务落 cancelled 并清理 code 任务的 worktree；迁移不了（如已终态）回退 failed。
func (o *Orchestrator) cancelInto(ctx context.Context, t *task.Task, cause error) {
	if err := t.Transition(task.StatusCancelled); err != nil {
		_ = t.FailInto(cause.Error())
	}
	_ = o.save(t)
	o.cleanupCodeWorkspace(ctx, t)
}

// fail 把任务落 failed 并清理 code 任务的 worktree；已在终态则只保快照。
func (o *Orchestrator) fail(ctx context.Context, t *task.Task, cause error) error {
	if err := t.FailInto(cause.Error()); err != nil {
		// 已在终态（如 cancelled），仅落盘
		_ = o.save(t)
		return cause
	}
	_ = o.save(t)
	o.cleanupCodeWorkspace(ctx, t)
	return cause
}

// cleanupCodeWorkspace 清理失败/取消 code 任务的 worktree（保留分支便于排查）。
func (o *Orchestrator) cleanupCodeWorkspace(ctx context.Context, t *task.Task) {
	if t.Kind != task.KindCode || t.Worktree == "" {
		return
	}
	_ = RemoveWorktree(ctx, t.Repo, t.Worktree, t.Branch, false)
}
