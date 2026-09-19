// Package task 定义 dispatcher 的任务模型与状态机。
package task

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// Kind 任务类型：隔离要求与产物形态由它决定。
type Kind string

const (
	// KindCode 代码任务：必须在 git 仓库中派发，worktree 硬性隔离，
	// 产物为 BaseCommit..HEAD 的 commit 清单。
	KindCode Kind = "code"
	// KindGeneral 非代码任务（文档/调研/分析）：免 git、免 worktree，
	// 产物为 worker 自述/产出文件。
	KindGeneral Kind = "general"
)

func ValidKind(k Kind) bool { return k == KindCode || k == KindGeneral }

// Status 任务生命周期状态。
type Status string

const (
	StatusQueued         Status = "queued"
	StatusDispatching    Status = "dispatching"
	StatusWorking        Status = "working"
	StatusAwaitingReview Status = "awaiting_review"
	StatusMerging        Status = "merging"
	StatusDone           Status = "done"
	StatusRejected       Status = "rejected"
	StatusFailed         Status = "failed"
	StatusCancelled      Status = "cancelled"
)

// Terminal 终态不再迁移。
func (s Status) Terminal() bool {
	return s == StatusDone || s == StatusRejected || s == StatusFailed || s == StatusCancelled
}

// transitions 合法的状态迁移表。
var transitions = map[Status][]Status{
	StatusQueued:      {StatusDispatching, StatusFailed, StatusCancelled},
	StatusDispatching: {StatusWorking, StatusFailed, StatusCancelled},
	StatusWorking:     {StatusAwaitingReview, StatusFailed, StatusCancelled},
	// awaiting_review → done 供 general 任务使用：产物确认通过即完成，无合并步。
	StatusAwaitingReview: {StatusMerging, StatusDone, StatusRejected, StatusFailed, StatusCancelled},
	StatusMerging:        {StatusDone, StatusFailed, StatusCancelled},
}

// ErrInvalidTransition 非法状态迁移。
var ErrInvalidTransition = errors.New("task: invalid status transition")

// CanTransition 报告 from → to 是否合法。
func CanTransition(from, to Status) bool {
	for _, next := range transitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

// Task 是一次委派执行的全量快照。
type Task struct {
	ID            string    `json:"id"`
	Source        string    `json:"source"` // "repl" | "acp:<client>"
	Kind          Kind      `json:"kind"`
	Prompt        string    `json:"prompt"`
	Repo          string    `json:"repo"` // code 任务必须；general 任务可选（workdir）
	Worker        string    `json:"worker"`
	Status        Status    `json:"status"`
	BaseCommit    string    `json:"base_commit,omitempty"`    // 仅 code 任务
	Worktree      string    `json:"worktree,omitempty"`       // 仅 code 任务
	Branch        string    `json:"branch,omitempty"`         // 仅 code 任务
	WorkerSession string    `json:"worker_session,omitempty"` // worker 侧 ACP 会话 ID（崩溃恢复用）
	Commits       []string  `json:"commits,omitempty"`        // 仅 code 任务
	Error         string    `json:"error,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"` // 预留：审批/提问超时策略落地前无人赋值（暂无超时，决策 2026-09-19）
}

// NewID 生成 task_<16hex>。
func NewID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("task: rand: " + err.Error())
	}
	return "task_" + hex.EncodeToString(b[:])
}

// Transition 迁移状态并刷新 UpdatedAt；终态与非法迁移报错。
func (t *Task) Transition(to Status) error {
	if t.Status.Terminal() {
		return fmt.Errorf("%w: %s is terminal", ErrInvalidTransition, t.Status)
	}
	if !CanTransition(t.Status, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.Status, to)
	}
	t.Status = to
	t.UpdatedAt = time.Now()
	return nil
}

// FailInto 迁移到 failed 并记录原因；已在终态时只补记错误，便于崩溃恢复路径复用。
func (t *Task) FailInto(reason string) error {
	if !t.Status.Terminal() {
		if err := t.Transition(StatusFailed); err != nil {
			return err
		}
	}
	t.Error = reason
	t.UpdatedAt = time.Now()
	return nil
}
