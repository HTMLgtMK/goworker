package agent

import (
	"sync"
	"time"
)

// 本文件是多会话执行内核的运行记账类型:AgentRun 记账一次会话执行
// (总方案 docs/multi-session-runtime-plan.md 第三节)。P0 阶段仅骨架:
// 记账接线(RecordRun)在 P0-3,后台运行与宿主生命周期 ctx 属 P2(RunSupervisor)。

// RunState 是一次会话执行的生命周期状态(对齐总方案第三节枚举)。
type RunState string

const (
	RunQueued    RunState = "queued"
	RunRunning   RunState = "running"
	RunCompleted RunState = "completed"
	RunFailed    RunState = "failed"
	RunCancelled RunState = "cancelled"
	// RunWaiting(waiting_input)属 P4 ApprovalManager(审批等待),本步不定义。
)

// RunResult 是一次会话执行的结果快照。Persisted 语义见 P0-3「模型成功 ≠
// 持久化成功」:Conversation Commit 失败时 State=RunFailed 且 Persisted=false,
// 绝不记 RunCompleted。
type RunResult struct {
	State     RunState
	Err       error
	Persisted bool
}

// AgentRun 记账一次会话执行:使用独立 RunID 而非 SessionID 代表一次执行,
// 为后续事件回放、Trace、性能统计与取消指定 Run 留空间(总方案第三节)。
type AgentRun struct {
	ID        string
	SessionID string

	mu        sync.RWMutex
	state     RunState
	startedAt time.Time
	done      chan struct{}
	result    RunResult
}

// newAgentRun 创建一条运行记录:初始态 RunQueued,done 未关闭。
func newAgentRun(id, sessionID string) *AgentRun {
	return &AgentRun{
		ID:        id,
		SessionID: sessionID,
		state:     RunQueued,
		done:      make(chan struct{}),
	}
}

// transition 推进运行状态。首次进入 RunRunning 时盖 startedAt 时间戳;
// 进入终态(RunCompleted/RunFailed/RunCancelled)时幂等关闭 done,唤醒等待者。
// 调用方负责状态推进顺序,并应在进入终态前先记录 result,保证 done 关闭后
// 观察到的 snapshot 即为最终结果。
func (run *AgentRun) transition(next RunState) {
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.state == next {
		return
	}
	run.state = next
	if next == RunRunning && run.startedAt.IsZero() {
		run.startedAt = time.Now()
	}
	switch next {
	case RunCompleted, RunFailed, RunCancelled:
		select {
		case <-run.done:
		default:
			close(run.done)
		}
	}
}

// snapshot 返回结果快照:当前状态 + 已记录的结果(Err/Persisted)。
func (run *AgentRun) snapshot() RunResult {
	run.mu.RLock()
	defer run.mu.RUnlock()
	res := run.result
	res.State = run.state
	return res
}
