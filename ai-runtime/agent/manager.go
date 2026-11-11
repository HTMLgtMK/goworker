package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// SessionManager 按 SessionID 管理运行时、后台并发配额和停机收口。

// ErrManagerClosed 表示 manager 已关闭，不再接受新 runtime 或 Run。
var ErrManagerClosed = errors.New("session: manager closed")

// StartRun 共用的运行状态错误。
var (
	ErrSessionNotFound   = errors.New("session: session not found")
	ErrRunActive         = errors.New("session: run already active")
	ErrTooManyPending    = errors.New("session: too many pending runs")
	ErrSessionNotBound   = errors.New("session: session not bound")
	ErrRunNotFound       = errors.New("session: run not found")
	ErrRunNotCancellable = errors.New("session: run not cancellable")
)

// ErrRunJournalWrite rejects StartRun when the queued acceptance record cannot be
// persisted. The error wraps both this sentinel and the journal error; no handle or
// tracking is produced. Other state writes remain best-effort.
var ErrRunJournalWrite = errors.New("session: queued run journal write failed (run rejected, not started)")

// 默认全局并发与排队上限；非正配置回落到此值。
const (
	defaultMaxConcurrentRuns = 4
	defaultMaxPendingRuns    = 16
)

// SessionManager 是 SessionRuntime 的进程内注册表:一个 SessionID 至多一个
// runtime,互斥执行权由各 runtime 自持的 gate 保证;全局并发配额由
// runSlots/pending 两级 channel 承担,manager 不参与会话内排队。
type SessionManager struct {
	mu       sync.RWMutex
	runtimes map[string]*SessionRuntime
	// ctx/cancel 是宿主生命周期 context(「runCtx, cancel :=
	// context.WithCancel(manager.ctx)」的父):构造时内建、此后只读,Close 时
	// cancel。无包外消费者,包内经 contextOf() 访问。
	ctx    context.Context
	cancel context.CancelFunc
	// closed 标记 Close 已发生,只在 mu 内读写。
	closed bool
	// runSlots 是全局并发槽(两级调度的外层):容量 = 最大同时在跑的后台
	// Run 数(每会话 gate 之上再限全局总量)。pending 是排队计数令牌(非锁):
	// 容量 = 最大排队数;StartRun 同步段非阻塞预留(满即拒 ErrTooManyPending),
	// execute 进入运行前(③)或失败出口归还。通道获取次序固定 gate → runSlots、
	// 释放逆序(supervisor.go 的锁次序论证),两把「锁」全序无环。
	runSlots chan struct{}
	pending  chan struct{}
	// approvals 是 HITL 审批决策管理器:进程内存量、全局单例,不随
	// runtime 增删联动——Wait/Resolve/Pending 以 SessionID/RunID/requestID
	// 寻址。V1 审批不持久化:重启清空,无跨重启 requestID 碰撞(interrupt
	// id=req-N 进程级 atomic,middlewares/hitl.go:68)。
	approvals *ApprovalManager
	// capabilities 是客户端工具能力注册表:sessionID→connectionID→binding
	// 的绑定面,能力代理工具(capability.go 的 proxy)在每次 Execute 时经
	// Lookup 解析当前执行通道。进程内存量、全局单例,不随 runtime 增删联动;
	// TTL 兜底「连接死而无人 Unbind」,UnbindConnection 由连接退出收口
	// (handler.Close 钩子 / daemon 探测面接线)。
	capabilities *CapabilityRegistry
	// runCtxValues 是 WithRunContext 声明的 runCtx 注入项:StartRun 派生
	// runCtx 时逐项 context.WithValue(supervisor.go ⑦)。构造期经 ManagerOption
	// 一次写入,此后只读——无运行期写者,无锁(并发论证见 WithRunContext)。
	runCtxValues []RunContextValue
	// runDoneHook is injected into each runtime and invoked after active is cleared.
	// It is set once and read-only thereafter; nil disables notification. It must not
	// block or reacquire the runtime gate.
	runDoneHook func(sessionID string)
}

// ManagerOption 是 NewSessionManager 的配置选项。
type ManagerOption func(*SessionManager)

// WithRunLimits 设置全局并发槽与排队上限。超限的后台 Run 在队列满时以
// ErrTooManyPending 拒绝；非正参数使用默认值。
func WithRunLimits(maxConcurrent, maxPending int) ManagerOption {
	return func(m *SessionManager) {
		if maxConcurrent <= 0 {
			maxConcurrent = defaultMaxConcurrentRuns
		}
		if maxPending <= 0 {
			maxPending = defaultMaxPendingRuns
		}
		m.runSlots = make(chan struct{}, maxConcurrent)
		m.pending = make(chan struct{}, maxPending)
	}
}

// RunContextValue 是注入后台 Run context 的键值对。Key 必须可比较，遵循
// context.WithValue 的约束。
type RunContextValue struct {
	Key   any
	Value any
}

// WithRunContext adds values to every background run context, typically a gate reentry
// marker. Values are written only at construction and read-only afterward; cancellation
// still roots at the host context.
func WithRunContext(values ...RunContextValue) ManagerOption {
	return func(m *SessionManager) {
		m.runCtxValues = append(m.runCtxValues, values...)
	}
}

// WithRunDoneHook 为每个新 runtime 设置 Run 终态通知回调。finalizeRun 在清除
// active、关闭 done 后于锁外调用，供宿主在运行结束后执行延迟收口。回调须避免阻塞
// 或重新获取该 runtime 的 gate。
func WithRunDoneHook(hook func(sessionID string)) ManagerOption {
	return func(m *SessionManager) {
		m.runDoneHook = hook
	}
}

// NewSessionManager 创建注册表、宿主生命周期 context 和默认全局配额。
func NewSessionManager(opts ...ManagerOption) *SessionManager {
	m := &SessionManager{
		runtimes:     make(map[string]*SessionRuntime),
		runSlots:     make(chan struct{}, defaultMaxConcurrentRuns),
		pending:      make(chan struct{}, defaultMaxPendingRuns),
		approvals:    NewApprovalManager(),
		capabilities: NewCapabilityRegistry(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	return m
}

// Register 注册 runtime，拒绝重复 ID、已关闭的 manager 和已退役的 runtime。
func (m *SessionManager) Register(rt *SessionRuntime) error {
	if rt == nil {
		return errors.New("session: register nil runtime")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrManagerClosed
	}
	if rt.Retired() {
		return fmt.Errorf("session: register retired runtime %q: %w", rt.ID, ErrSessionRetired)
	}
	if _, exists := m.runtimes[rt.ID]; exists {
		return fmt.Errorf("session: runtime %q already registered", rt.ID)
	}
	// run 终态通知接线(WithRunDoneHook):注册即注入,先于该 runtime
	// 的任何 Run 提交——StartRun 必经 m.mu 读注册表,与本写点全序,hook 字段
	// 的发布对后续 finalizeRun 的读取可见。
	if m.runDoneHook != nil {
		rt.onRunDone = m.runDoneHook
	}
	m.runtimes[rt.ID] = rt
	return nil
}

// Get 返回 id 对应的 runtime;不存在返回 nil。
func (m *SessionManager) Get(id string) *SessionRuntime {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runtimes[id]
}

// Remove 移除 runtime;id 不存在时为幂等空操作。
func (m *SessionManager) Remove(id string) {
	m.mu.Lock()
	delete(m.runtimes, id)
	m.mu.Unlock()
}

// List 返回全部 runtime,按 ID 排序保证输出稳定。
func (m *SessionManager) List() []*SessionRuntime {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := make([]*SessionRuntime, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		list = append(list, rt)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return list
}

// Approvals 返回审批决策管理器:manager 构造即建,恒非 nil。decideFunc 消费
// Wait,协议面经 Pending/Resolve 直转包装读取与决策。
func (m *SessionManager) Approvals() *ApprovalManager {
	return m.approvals
}

// Capabilities 返回客户端工具能力注册表:manager 构造即建,恒非 nil。
// daemon 每轮 prompt 探测到设备工具后经 Bind 刷新连接绑定,能力代理工具的
// Execute 经 Lookup 解析当前执行通道;连接退出时 UnbindConnection 收口
// (handler.Close 钩子)。绑定关系归属
// (session, connection) 二元组,与 approvals 的 (session, run) 寻址正交。
func (m *SessionManager) Capabilities() *CapabilityRegistry {
	return m.capabilities
}

// PendingApprovals 返回会话未过期的审批快照。无审批时返回空切片。
func (m *SessionManager) PendingApprovals(sessionID string) []PendingApproval {
	return m.approvals.Pending(sessionID)
}

// ResolveApproval 投递审批决策；首个有效决策获胜，错误原样返回。
func (m *SessionManager) ResolveApproval(requestID string, decision hitl.Decision) error {
	return m.approvals.Resolve(requestID, decision)
}

// ApprovalOwner returns the server-recorded session for an approval request. It is the
// authority for respond ownership checks and remains valid through the retention window;
// unknown or swept IDs return ok=false.
func (m *SessionManager) ApprovalOwner(requestID string) (string, bool) {
	return m.approvals.Owner(requestID)
}

// contextOf 返回宿主生命周期 context，后台 Run 以此派生，避免绑定请求连接。
func (m *SessionManager) contextOf() context.Context {
	return m.ctx
}

// Close 拒绝新注册、取消后台 Run 并等待终态，再关闭事件枢纽并清空注册表。
// 等待和文件 I/O 均不持 manager/runtime 锁；重复调用无操作。
func (m *SessionManager) Close() {
	m.mu.Lock()
	if m.closed { // 幂等:重复 Close 直接返回
		m.mu.Unlock()
		return
	}
	m.closed = true
	runtimes := make([]*SessionRuntime, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		runtimes = append(runtimes, rt)
	}
	m.mu.Unlock()

	m.cancel()

	// 逐 runtime 等待其 Active() 的 run 终态(nil 跳过):Close 返回 ⇒ 快照时
	// 在途的 run 全部 done。等待在两把锁之外进行。
	for _, rt := range runtimes {
		if run := rt.Active(); run != nil {
			<-run.done
		}
	}

	// 事件面收口:逐 runtime EndRun+Close 其 EventHub(均幂等;events
	// 为 nil 的 runtime——同步路径/未启用 RunEvents——是空操作)。必须落在
	// 上面的等待之后:先等 run done 再关 hub,执行中的 Publish 不可能写已关闭
	// 的 journal(见函数头语义边界);此后 hub 拒绝 Publish/Subscribe。
	for _, rt := range runtimes {
		rt.closeEvents()
	}

	m.mu.Lock()
	m.runtimes = make(map[string]*SessionRuntime)
	m.mu.Unlock()
}

// CancelRun 取消指定后台 Run。排队任务释放资源并以 cancelled 结束；运行中任务在返回后结束。
func (m *SessionManager) CancelRun(sessionID, runID string) error {
	rt := m.Get(sessionID)
	if rt == nil {
		return ErrRunNotFound
	}

	// rt.mu 内快照 Active()(Active 自身持 rt.mu)后立即释放:后续判定与
	// cancel 均在锁外。
	run := rt.Active()
	if run == nil || run.ID != runID {
		return ErrRunNotFound
	}

	cancel := run.cancelFunc()
	if cancel == nil {
		return ErrRunNotCancellable // 同步 RecordRun 路径无 cancel
	}
	cancel()
	return nil
}
