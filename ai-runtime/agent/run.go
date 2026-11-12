package agent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// 本文件定义运行记账类型。

// RunState 是一次会话执行的生命周期状态。
type RunState string

const (
	RunQueued    RunState = "queued"
	RunRunning   RunState = "running"
	RunCompleted RunState = "completed"
	RunFailed    RunState = "failed"
	RunCancelled RunState = "cancelled"
	// RunWaiting(waiting_input)由审批等待产生(HITL 决策挂起期间 Run 停留
	// 此态)。非终态:journal 恢复扫描(isTerminalRunState)不视其为收尾,重启后
	// 照旧标 interrupted(V1 审批在内存、不跨重启,自动续跑不做)。
	RunWaiting RunState = "waiting_input"
	// Client-tool rebinding wait state. It is non-terminal and restores as interrupted.
	RunWaitingCapability RunState = "waiting_capability"
)

// RunResult 是一次会话执行的结果快照。仅模型成功且持久化成功才标记为
// RunCompleted；持久化失败时 State 为 RunFailed、Persisted 为 false。
type RunResult struct {
	State     RunState
	Err       error
	Persisted bool
}

// AgentRun 记录一次会话执行，以独立 RunID 标识，可用于查询、等待和取消。
type AgentRun struct {
	ID        string
	SessionID string

	mu        sync.RWMutex
	state     RunState
	startedAt time.Time
	done      chan struct{}
	result    RunResult
	// cancel 是后台 Run 的取消函数(mu 保护):StartRun 同步段在派生 runCtx 后
	// 写入(先于 go execute,返回的句柄立即可取消),CancelRun 经 cancelFunc()
	// 读取并调用。RecordRun 同步路径恒 nil——同步 Run 的取消
	// 入口是调用方请求 ctx,不经 run 记账。
	cancel context.CancelFunc
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

// State 返回当前运行状态快照。
func (run *AgentRun) State() RunState {
	return run.snapshot().State
}

// cancelFunc 返回运行取消函数;未设置(RecordRun 同步路径)返回 nil。
// 一次写入(execute 派生 runCtx 后)多个读者,读写都在 run.mu 内。
func (run *AgentRun) cancelFunc() context.CancelFunc {
	run.mu.RLock()
	defer run.mu.RUnlock()
	return run.cancel
}

// Cancel invokes the run context's cancel function, if this is a cancellable background run.
// It returns false for synchronous RecordRun entries and is safe to call repeatedly.
func (run *AgentRun) Cancel() bool {
	cancel := run.cancelFunc()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

// finish 记录最终结果并推进到终态。result 必须先于 done 关闭写入(与 transition
// 的契约一致):done 关闭后等待者观察到的 snapshot 即为完整终态结果。
func (run *AgentRun) finish(res RunResult) {
	run.mu.Lock()
	run.result = res
	run.mu.Unlock()
	run.transition(res.State)
}

// runSeq 是 RunID 的进程内自增序列,作 UnixNano 时间戳后的同刻消歧后缀:
// 进程内 seq 保唯一(UnixNano 单调性不做强假设);跨重启同会话目录同 nano+同
// seq 概率可忽略,且由 RunJournal 建文件 O_EXCL 兜底——碰撞变成可观测
// 错误(ErrRunExists)而非静默损坏。文件名字符序=创建序(nano 定宽至 2262 年),
// 跨文件 seq 拼接依赖此序。
var runSeq atomic.Uint64

// newRunID 返回新的 run id(run-<unixnano>-<seq>,跨重启唯一)。
func newRunID() string {
	return fmt.Sprintf("run-%d-%d", time.Now().UnixNano(), runSeq.Add(1))
}
