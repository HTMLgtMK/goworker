package agent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	// RunWaiting(waiting_input)P4-1 定义,产生方 P4-2 审批等待(HITL 决策
	// 挂起期间 Run 停留此态)。非终态:journal 恢复扫描(isTerminalRunState)
	// 不视其为收尾,重启后照旧标 interrupted(总方案 §9;V1 审批在内存、
	// 不跨重启,自动续跑不做)。
	RunWaiting RunState = "waiting_input"
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
	// cancel 是后台 Run 的取消函数(mu 保护):StartRun 同步段在派生 runCtx 后
	// 写入(P2-6 修正:先于 go execute,返回的句柄立即可取消),CancelRun(P2-6)
	// 经 cancelFunc() 读取并调用。RecordRun 同步路径恒 nil——同步 Run 的取消
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

// cancelFunc 返回运行取消函数;未设置(RecordRun 同步路径)返回 nil。
// 一次写入(execute 派生 runCtx 后)多个读者,读写都在 run.mu 内。
func (run *AgentRun) cancelFunc() context.CancelFunc {
	run.mu.RLock()
	defer run.mu.RUnlock()
	return run.cancel
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
// seq 概率可忽略,且由 P2-2 RunJournal 建文件 O_EXCL 兜底——碰撞变成可观测
// 错误(ErrRunExists)而非静默损坏。文件名字符序=创建序(nano 定宽至 2262 年),
// P3-1 的跨文件 seq 拼接依赖此序。
var runSeq atomic.Uint64

// newRunID 返回新的 run id(run-<unixnano>-<seq>,跨重启唯一)。
func newRunID() string {
	return fmt.Sprintf("run-%d-%d", time.Now().UnixNano(), runSeq.Add(1))
}
