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

// 本文件是 HITL 审批的独立决策管理器(P4-1 骨架;总方案 §6 的
// ApprovalManager):Pending Approval 以独立 requestID 寻址、归属
// SessionID+RunID(非连接)——后台 Run 的决策跨连接化由此承担,P4-2 的
// decideFunc 把 cb.Decide 换成本文件的 Wait 即完成接线,ai-core/中间件零改动。
//
// 语义铁律(总方案 §6):
//   - 相同 requestID 只接受一次有效决策(第一个有效决策获胜,迟到/重复一律
//     ErrApprovalResolved);
//   - 超时默认拒绝,不允许隐式通过(ExpiresAt 是唯一超时事实源,形态对齐
//     hitl/channel_decision_provider.go:12-38);
//   - V1 审批不持久化:进程内存量,重启清空,无跨重启 requestID 碰撞
//     (interrupt id=req-N 进程级 atomic,hitl.go:68)。

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

// ApprovalManager 是审批的进程内注册表:requestID → entry。mu 只护 pending
// map 与 entry 状态;等待在 decide channel 上,绝不在锁内(并发论证见 plan
// P4-1:不触 manager.mu/rt.mu/gate/hub.mu,无锁环)。
type ApprovalManager struct {
	mu      sync.Mutex
	pending map[string]*approvalEntry
}

// approvalEntry 是一笔在途审批的状态:decide 容量 1(Resolve 恰好投递一次,
// 首个有效决策),resolved 标志令迟到决策显式失败。已 resolved 的 entry 保留在
// map 里(二次 Resolve 才能报 ErrApprovalResolved 而非 ErrApprovalUnknown),
// 由 Pending 的读时清扫按 ExpiresAt 逐出。
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

// Wait 阻塞至决策/超时/取消(总方案 §6 的 ApprovalManager.Request 适配形):
//   - runCtx 取消 → reject(等待期间的 CancelRun/宿主 Close 经 runCtx 传播,
//     取消绝不隐式通过);
//   - ExpiresAt 到点(含调用时已过期)→ reject——超时后默认拒绝,不允许隐式
//     通过;ExpiresAt 缺失时兜底 DefaultTimeout(对齐
//     hitl/channel_decision_provider.go:17-18);
//   - Resolve 的首个有效决策获胜;同 requestID 的重复 Wait(既有 entry 在表)
//     立即 reject,不参与决策。
//
// 超时形态对齐 hitl/channel_decision_provider.go:12-38:time.Timer +
// runCtx.Done select,ExpiresAt 为唯一超时事实源。ctx 取消与决策竞争时
// select 随机取胜者——与 provider 先例同构,不做额外优先级。
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

	// markResolved 在 Wait 的超时/取消出口收口 entry:此后迟到的 Resolve 显式
	// 撞 ErrApprovalResolved(迟到决策无效,对齐 provider 的「迟到决策本来就
	// 无效」)。entry 本身保留在表中,交由 Pending 的读时清扫逐出。
	//
	// 竞争边界:若 Resolve 已先行置 resolved 并投递(返回 nil),本处的置位是
	// 幂等空操作,决策已从 decide channel 被本 goroutine 之外的分支放弃的情形
	// 与 provider 先例一致(select 随机)。本函数内的 select 命中 decide 时
	// entry 必已 resolved,直接返回该决策即可。
	markResolved := func() {
		m.mu.Lock()
		entry.resolved = true
		m.mu.Unlock()
	}

	m.mu.Lock()
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

// decideFunc 返回 StartRun 审批回退用的 HITL 决策闭包(P4-2,总方案 §6 的
// Decide → approvalManager.Wait 适配形):run 的 cb.Decide 为 nil 时由
// supervisor 注入本闭包,后台 Run 的 HITL 决策由此跨连接化——决策不归属任何
// 连接,「原控制端断开、新控制端 Resolve」即总方案 §6 的断连语义。
//
// 闭包序列(调用点在 Session.Run→streamTokens 的 cb.Decide 面,即 execute
// goroutine;req 为 streamTokens 从 interrupt token 解码的副本,非中间件原件):
//  1. run.transition(RunWaiting):§6 状态机 Running→WFA——HITL 决策挂起期间
//     Run 停留 waiting_input,经 RunHandle.State() 可观测;transition 在 run.mu
//     内、幂等(run.go:72-90),与 ApprovalManager.mu 无嵌套;
//  2. journalRunState(sess, {state:"waiting_input", ...}):durable-first 观测面
//     ——等待事实先落盘,审批跨重启不恢复(V1 审批在内存),恢复扫描据此把
//     卡在 waiting 的 run 标 interrupted 而非误报 lost;journalRunState 在
//     rt.mu 外(runtime.go 的锁内不做文件 I/O 纪律),失败 warn 不阻塞;
//  3. m.approvals.Wait(runCtx, sessionID, run.ID, req):阻塞至决策/超时/取消
//     ——等待期间的取消(CancelRun/宿主 Close)经 runCtx.Done → Wait reject →
//     中间件 reject → 工具中止 → 既有取消路径收敛(execute 层归一覆盖
//     RunCancelled),本闭包不发明新的取消出口;
//  4. run.transition(RunRunning):决策落定即回到运行态(§6 状态机 WFA→Running;
//     终态仍由 finalizeRun 归一,此处不触终态);
//  5. 返回决策,交 streamTokens 投进 decisions channel → 中间件裁决工具放行
//     /拒绝。
//
// 事件面(P5-2 起):waiting 两处迁移增发 run.state_changed{from,to}(hub 非
// nil 时,emitRunEvent 收敛降级)——迁移与 transition 同点产出,回放面可从事件
// 流重建 RunWaiting↔RunRunning 的往返;事件失败仅 warn,不影响决策链。审批
// requested/resolved 事件对仍由 tee 承担:审批回退先于 teeRunCallbacks 注入
// (supervisor.go StartRun ⑦.5),tee 检测 cb.Decide 非 nil 自动包装。
//
// 并发论证(plan P4-2):闭包在 execute goroutine 上阻塞——与 P3-4 tee 论证同构
// (等待在 hub 锁外);transition 在 run.mu 内;ApprovalManager.mu 与 run.mu/
// rt.mu/hub.mu 无嵌套,无锁环。
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
		from = run.snapshot().State
		run.transition(RunRunning)
		emitRunEvent(hub, run.ID, session.EventRunStateChanged, map[string]any{"from": string(from), "to": string(RunRunning)})
		return decision
	}
}
