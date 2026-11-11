package agent

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// ApprovalManager 以 requestID 管理待决审批，并将其归属到 SessionID 和 RunID，
// 而非连接。决策只接受一次；超时或取消默认拒绝。审批仅驻留内存，重启后清空。

// PendingApproval 是一笔等待用户决策的审批快照(Pending/协议面的读取形态)。
type PendingApproval struct {
	RequestID        string                // = hitl.InterruptRequest.ID(hitl.go:129 等 "req-N")
	SessionID, RunID string                // 审批归属:会话 + 本次执行(非连接)
	Request          hitl.InterruptRequest // 原始中断(含 ExpiresAt/Command/RiskLevel/Effects)
}

// 审批决策面的拒绝语义 sentinel:ErrApprovalUnknown 的产生方是 Resolve
// (未知 requestID);ErrApprovalResolved 的产生方是 Resolve(迟到/重复决策,
// 已有首个有效决策获胜)。
var (
	ErrApprovalUnknown  = errors.New("approval: request not found")
	ErrApprovalResolved = errors.New("approval: already resolved")
)

// approvalResolvedRetention 保留已决且过期的审批一段时间，使迟到决策返回
// ErrApprovalResolved；宽限期后由 Wait/Resolve 惰性清理。无后台清理 goroutine。
const approvalResolvedRetention = 30 * time.Second

// ApprovalManager 是审批的进程内注册表。mu 保护 pending 与 entry 状态；等待发生在锁外，
// 不与 AgentRun、runtime 或 EventHub 的锁嵌套。
type ApprovalManager struct {
	mu      sync.Mutex
	pending map[string]*approvalEntry
}

// approvalEntry stores the request, a one-slot decision channel, and the resolved
// guard. Resolved entries remain briefly so late decisions return ErrApprovalResolved;
// Pending and lazy Wait/Resolve sweeps apply their documented expiry rules.
type approvalEntry struct {
	pa       PendingApproval
	resolved bool
	decide   chan hitl.Decision
}

// NewApprovalManager 创建空注册表。生命周期与宿主进程一致(V1 不持久化,
// 重启即清空),不随 SessionManager 的 runtime 增删联动。
func NewApprovalManager() *ApprovalManager {
	return &ApprovalManager{pending: make(map[string]*approvalEntry)}
}

// Wait blocks until a decision, expiry, or run cancellation; every timeout/cancel
// path rejects. ExpiresAt is authoritative (DefaultTimeout is the zero-value
// fallback); duplicate request IDs are rejected, and the first Resolve wins.
func (m *ApprovalManager) Wait(runCtx context.Context, sessionID, runID string, req *hitl.InterruptRequest) hitl.Decision {
	if req == nil {
		return hitl.Decision{Type: hitl.DecisionReject}
	}
	reject := func() hitl.Decision {
		return hitl.Decision{InterruptID: req.ID, Type: hitl.DecisionReject}
	}

	entry := &approvalEntry{
		pa: PendingApproval{
			RequestID: req.ID,
			SessionID: sessionID,
			RunID:     runID,
			Request:   *req,
		},
		decide: make(chan hitl.Decision, 1),
	}

	// 收口超时/取消 entry，使迟到 Resolve 返回 ErrApprovalResolved。

	markResolved := func() {
		m.mu.Lock()
		entry.resolved = true
		m.mu.Unlock()
	}

	m.mu.Lock()
	// 入口顺带清扫(P4 评审 issue3):逐出「已 resolved 且已过期超过宽限窗」
	// 的滞留条目——生产中 Pending 可能从不被调,该清扫是 Wait 自身附带的
	// 确定性回收时机;同锁内 O(n),审批频率低开销可忽略。先于重复检查:被
	// 清扫条目已过宽限窗,重复 Wait 落到全新注册(其 Wait 早已返回,无竞争)。
	m.sweepResolvedExpiredLocked(time.Now())
	if _, exists := m.pending[req.ID]; exists {
		// 相同 requestID 只接受一次有效决策:重复 Wait 不复用、不覆盖既有
		// entry(其决策归属首次 Wait),立即拒绝。
		m.mu.Unlock()
		return reject()
	}
	m.pending[req.ID] = entry
	m.mu.Unlock()

	var timeout *time.Timer
	switch {
	case req.ExpiresAt.IsZero():
		timeout = time.NewTimer(hitl.DefaultTimeout)
	case time.Until(req.ExpiresAt) <= 0:
		// 已过期:不再等,按宪法直接拒绝(默认拒绝,不隐式通过)。
		markResolved()
		return reject()
	default:
		timeout = time.NewTimer(time.Until(req.ExpiresAt))
	}
	defer timeout.Stop()

	select {
	case <-runCtx.Done():
		markResolved()
		return reject()
	case <-timeout.C:
		markResolved()
		return reject()
	case d := <-entry.decide:
		return d
	}
}

// Resolve 投递决策:第一个有效决策获胜,恰好投递一次(decide 容量 1 + resolved
// 标志);未知 requestID → ErrApprovalUnknown,迟到/重复 → ErrApprovalResolved。
// decision.InterruptID 统一归一为 requestID(= req.ID,见 PendingApproval 契约),
// 决策回填中间件时与原始中断严格对应。
func (m *ApprovalManager) Resolve(requestID string, decision hitl.Decision) error {
	m.mu.Lock()
	// 入口顺带清扫(P4 评审 issue3,同 Wait 入口):宽限窗外的已 resolved 且
	// 已过期条目在此逐出——迟到决策因此对宽限窗外落 ErrApprovalUnknown(对
	// 宽限窗内仍 ErrApprovalResolved,TestApprovalLateResolveRejected 钉死)。
	m.sweepResolvedExpiredLocked(time.Now())
	entry, ok := m.pending[requestID]
	if !ok {
		m.mu.Unlock()
		return ErrApprovalUnknown
	}
	if entry.resolved {
		m.mu.Unlock()
		return ErrApprovalResolved
	}
	entry.resolved = true
	decision.InterruptID = requestID
	entry.decide <- decision // 容量 1:resolved 置位后至多一次投递,不阻塞
	m.mu.Unlock()
	return nil
}

// Owner returns the server-recorded session for a request, including recently
// resolved entries. It is read-only; Wait/Resolve/Pending perform lazy cleanup,
// so an absent ID matches Resolve's ErrApprovalUnknown semantics.
func (m *ApprovalManager) Owner(requestID string) (sessionID string, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.pending[requestID]
	if !ok {
		return "", false
	}
	return entry.pa.SessionID, true
}

// sweepResolvedExpiredLocked removes resolved entries past their request expiry plus
// the retention window. The caller holds m.mu; zero-expiry requests are retained.
func (m *ApprovalManager) sweepResolvedExpiredLocked(now time.Time) {
	for id, entry := range m.pending {
		if !entry.resolved || entry.pa.Request.ExpiresAt.IsZero() {
			continue
		}
		if entry.pa.Request.ExpiresAt.Add(approvalResolvedRetention).After(now) {
			continue
		}
		delete(m.pending, id)
	}
}

// Pending 返回会话的未过期审批快照(ExpiresAt 未到且未 resolved),按
// RequestID 排序保证输出稳定。读时顺带清理过期条目(含已 resolved 的残留)——
// V1 无独立清扫 goroutine,过期 entry 的逐出全靠本读路径。
func (m *ApprovalManager) Pending(sessionID string) []PendingApproval {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	out := make([]PendingApproval, 0)
	for id, entry := range m.pending {
		// 过期判定与 provider 一致:ExpiresAt 为唯一事实源;零值视为未过期
		// (中间件恒设 ExpiresAt=now+DefaultTimeout,hitl.go:137,零值仅在
		// 防御路径出现,其 Wait 已走 DefaultTimeout 兜底)。
		if !entry.pa.Request.ExpiresAt.IsZero() && !entry.pa.Request.ExpiresAt.After(now) {
			delete(m.pending, id)
			continue
		}
		if entry.resolved || entry.pa.SessionID != sessionID {
			continue
		}
		out = append(out, entry.pa)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID < out[j].RequestID })
	return out
}

// decideFunc 为后台 Run 提供跨连接审批回调：迁移到 waiting_input 并记录状态，
// 阻塞等待决策后在未取消时回迁 running。等待不持 run/runtime/hub 锁；取消由既有
// Run 取消路径收敛，审批事件由回调 tee 发布。
func (m *ApprovalManager) decideFunc(runCtx context.Context, sess *Session, run *AgentRun, sessionID string, hub *EventHub) func(*hitl.InterruptRequest) hitl.Decision {
	return func(req *hitl.InterruptRequest) hitl.Decision {
		from := run.snapshot().State
		run.transition(RunWaiting)
		emitRunEvent(hub, run.ID, session.EventRunStateChanged, map[string]any{"from": string(from), "to": string(RunWaiting)})
		journalRunState(sess, session.RunStateRecord{
			Kind:      "state",
			SessionID: run.SessionID,
			RunID:     run.ID,
			State:     string(RunWaiting),
			At:        time.Now(),
		})
		decision := m.Wait(runCtx, sessionID, run.ID, req)
		// 取消收敛守卫(P4 评审 issue4):Wait 因 runCtx 取消而 reject 时不再
		// 回迁 running——transition 前查 runCtx.Err()==nil,取消路径的
		// RunHandle.State() 轮询者不再瞬时看到与事实不符的 running(finalizeRun
		// 随后归一 cancelled;journal 无影响,本处回迁本就不落 journal)。
		// 超时/决策出口(runCtx 未取消)照常回迁,状态机语义不变。
		if runCtx.Err() == nil {
			from = run.snapshot().State
			run.transition(RunRunning)
			emitRunEvent(hub, run.ID, session.EventRunStateChanged, map[string]any{"from": string(from), "to": string(RunRunning)})
		}
		return decision
	}
}
