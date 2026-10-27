package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// 本文件是多会话执行内核的注册表:SessionManager 按 SessionID 管理
// SessionRuntime,并持有宿主生命周期 ctx(P2-4)与全局并发配额(P2-5 的
// runSlots/pending)——StartRun(P2-5,supervisor.go)的 runCtx 以宿主 ctx 为父,
// Close = 取消宿主 ctx + 等待在途 Run 终态 + 逐 runtime 收口事件枢纽(P3-5)
// + 拒绝后续 Register。

// ErrManagerClosed 是 manager 停机后的注册拒绝 sentinel:Close 之后不再接受
// 新 runtime(同步 Register 与 P2-5 的 StartRun 共用同一拒绝语义)。
var ErrManagerClosed = errors.New("session: manager closed")

// StartRun(P2-5)与其余 Run 记账面的拒绝语义 sentinel:ErrRunNotFound/
// ErrRunNotCancellable 的产生方是 CancelRun(P2-6,本文件末尾)。
var (
	ErrSessionNotFound   = errors.New("session: session not found")
	ErrRunActive         = errors.New("session: run already active")
	ErrTooManyPending    = errors.New("session: too many pending runs")
	ErrSessionNotBound   = errors.New("session: session not bound")
	ErrRunNotFound       = errors.New("session: run not found")
	ErrRunNotCancellable = errors.New("session: run not cancellable")
)

// ErrRunJournalWrite 是 StartRun 的 queued 登记持久化失败 sentinel(I1 修复,
// GPT 评审):run/start 的契约是「Run 被接受**并持久化登记**后返回」(总方案
// 第七节)——queued 记录写不进 journal(磁盘满/journal I/O 故障)时,已接受的
// run 在重启后无处可寻,属「已接受但无法追踪」,必须拒绝启动而非降级继续。
// StartRun 以 %w 链同时包裹本 sentinel 与底层 journal 错误(errors.Is 双向可判);
// 失败的 run 不产生 RunHandle、不占任何记账(journal 无记录即无痕)。运行中的
// 非关键遥测与终态记录仍走 journalRunState 的 warn 降级——严格化只针对「接受
// 登记」这一个写点(其余记录的缺口由恢复扫描显式标记 interrupted)。
var ErrRunJournalWrite = errors.New("session: queued run journal write failed (run rejected, not started)")

// 全局并发默认配额(总方案第四节建议值):WithRunLimits 对 <=0 回落此处,
// P2-7 接 SessionConfig 时 0 值配置由此双保险。
const (
	defaultMaxConcurrentRuns = 4
	defaultMaxPendingRuns    = 16
)

// SessionManager 是 SessionRuntime 的进程内注册表:一个 SessionID 至多一个
// runtime,互斥执行权由各 runtime 自持的 gate 保证;全局并发配额(P2-5)由
// runSlots/pending 两级 channel 承担,manager 不参与会话内排队。
type SessionManager struct {
	mu       sync.RWMutex
	runtimes map[string]*SessionRuntime
	// ctx/cancel 是宿主生命周期 context(总方案第四节「runCtx, cancel :=
	// context.WithCancel(manager.ctx)」的父):构造时内建、此后只读,Close 时
	// cancel。P2-4 无包外消费者,包内经 contextOf() 访问。
	ctx    context.Context
	cancel context.CancelFunc
	// closed 标记 Close 已发生,只在 mu 内读写。
	closed bool
	// runSlots 是全局并发槽(P2-5 两级调度的外层):容量 = 最大同时在跑的后台
	// Run 数(每会话 gate 之上再限全局总量)。pending 是排队计数令牌(非锁):
	// 容量 = 最大排队数;StartRun 同步段非阻塞预留(满即拒 ErrTooManyPending),
	// execute 进入运行前(③)或失败出口归还。通道获取次序固定 gate → runSlots、
	// 释放逆序(supervisor.go 的锁次序论证),两把「锁」全序无环。
	runSlots chan struct{}
	pending  chan struct{}
	// approvals 是 HITL 审批决策管理器(P4-1):进程内存量、全局单例,不随
	// runtime 增删联动——Wait/Resolve/Pending 以 SessionID/RunID/requestID
	// 寻址。V1 审批不持久化:重启清空,无跨重启 requestID 碰撞(interrupt
	// id=req-N 进程级 atomic,middlewares/hitl.go:68)。
	approvals *ApprovalManager
	// capabilities 是客户端工具能力注册表(P4-3):sessionID→connectionID→binding
	// 的绑定面,能力代理工具(capability.go 的 proxy)在每次 Execute 时经
	// Lookup 解析当前执行通道。进程内存量、全局单例,不随 runtime 增删联动;
	// TTL 兜底「连接死而无人 Unbind」,UnbindConnection 由连接退出收口(P5-7
	// handler.Close 钩子 / P5-9 daemon 探测面接线)。
	capabilities *CapabilityRegistry
	// runCtxValues 是 WithRunContext(P5-1)声明的 runCtx 注入项:StartRun 派生
	// runCtx 时逐项 context.WithValue(supervisor.go ⑦)。构造期经 ManagerOption
	// 一次写入,此后只读——无运行期写者,无锁(并发论证见 WithRunContext)。
	runCtxValues []RunContextValue
}

// ManagerOption 是 NewSessionManager 的配置选项。
type ManagerOption func(*SessionManager)

// WithRunLimits 设置全局并发槽与排队上限:超限的后台 Run 在 pending 排队,
// 排队也满时 StartRun 拒绝(ErrTooManyPending)。任一参数 <=0 回落默认
// (defaultMaxConcurrentRuns/defaultMaxPendingRuns)——P2-7 接 config 时的
// 0 值双保险,不依赖 yaml 载入路径。
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

// RunContextValue 是 WithRunContext 的单项注入(P5-1):Key/Value 原样传给
// context.WithValue,由 StartRun 的 runCtx 携带(supervisor.go ⑦)。Key 必须
// 可比较(与 context.WithValue 同约束)——daemon 以包内非导出的 sessionGateKey{}
// 为 Key、*AgentPlugin 为 Value 注入 gate 重入标记。
type RunContextValue struct {
	Key   any
	Value any
}

// WithRunContext 声明注入每次 StartRun runCtx 的键值对(P5-1,兑现 P2-5 契约
// supervisor.go 的 StartRun 注释②):典型用途是 sessionGateKey 重入标记——后台
// Run 内 hub 工具嵌套 Eval 同会话命令(commands.go withSession)时,标记放行
// SessionGate,杜绝「持 gate 重复排队」死锁(总方案 §6 风险 2,P5-6/P5-7
// StartRun 接线的顺序硬前置)。
//
// 并发论证:写入只发生在 NewSessionManager 的选项应用(构造期),此后
// runCtxValues 只读(StartRun 派生 ctx 时只遍历),无运行期写者、无锁;派生
// context.WithValue 链不改变既有取消传播(WithCancel 仍以宿主 ctx 为根)。
func WithRunContext(values ...RunContextValue) ManagerOption {
	return func(m *SessionManager) {
		m.runCtxValues = append(m.runCtxValues, values...)
	}
}

// NewSessionManager 创建空注册表,并内建宿主生命周期 ctx 与默认全局配额。
// 变参化(P2-5):不传选项时行为与旧无参签名完全一致,现有调用点(daemon
// plugin 等)零改动;配额经 WithRunLimits 覆盖。
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

// Register 注册 runtime;同 ID 冲突报错(一个会话至多一个 runtime)。manager
// 已 Close 时拒绝注册(ErrManagerClosed)——宿主级统一停机后不再有新成员
// (P0 注释里的「宿主级统一停机属 P2」在此兑现)。
func (m *SessionManager) Register(rt *SessionRuntime) error {
	if rt == nil {
		return errors.New("session: register nil runtime")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrManagerClosed
	}
	if _, exists := m.runtimes[rt.ID]; exists {
		return fmt.Errorf("session: runtime %q already registered", rt.ID)
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

// Approvals 返回审批决策管理器(P4-1):manager 构造即建,恒非 nil。P4-2 起
// decideFunc 消费 Wait,P5 协议面经 Pending/Resolve 直转包装读取与决策。
func (m *SessionManager) Approvals() *ApprovalManager {
	return m.approvals
}

// Capabilities 返回客户端工具能力注册表(P4-3):manager 构造即建,恒非 nil。
// daemon 每轮 prompt 探测到设备工具后经 Bind 刷新连接绑定(P5-9 接线),能力
// 代理工具的 Execute 经 Lookup 解析当前执行通道(P4-4);连接退出时
// UnbindConnection 收口(P5-7 handler.Close 钩子)。绑定关系归属
// (session, connection) 二元组,与 approvals 的 (session, run) 寻址正交。
func (m *SessionManager) Capabilities() *CapabilityRegistry {
	return m.capabilities
}

// PendingApprovals 返回会话未过期的审批快照(P4-2,ApprovalManager.Pending 的
// 直转包装):P5-6/P5-7 的 x-goworker 协议面(PendingApprovals 消费)按
// requestID 逐笔读取;未知会话/无审批返回空切片,不区分「未知会话」与
// 「无审批」——审批注册表按 SessionID 过滤,快照即全部事实。
func (m *SessionManager) PendingApprovals(sessionID string) []PendingApproval {
	return m.approvals.Pending(sessionID)
}

// ResolveApproval 投递一笔审批决策(P4-2,ApprovalManager.Resolve 的直转包装):
// 首个有效决策获胜;未知 requestID → ErrApprovalUnknown,迟到/重复 →
// ErrApprovalResolved(sentinel 语义见 approval.go)。P5-6/P5-7 的
// x-goworker/approval/respond 经此收口,错误原样上抛由协议面映射响应。
func (m *SessionManager) ResolveApproval(requestID string, decision hitl.Decision) error {
	return m.approvals.Resolve(requestID, decision)
}

// contextOf 返回宿主生命周期 ctx(包内访问器):StartRun(P2-5)以它为父派生
// runCtx——Run 的生命周期归宿主而非单个请求,连接断开不影响后台 Run。不导出:
// P2 无包外消费者(导出面留给 P5 前端接入时再定)。
func (m *SessionManager) contextOf() context.Context {
	return m.ctx
}

// Close 是总方案 P2 第 4 条的 Shutdown:取消宿主生命周期 ctx → 等待全部在途
// Run 终态 → 逐 runtime 收口事件枢纽(P3-5 的 closeEvents)→ 拒绝后续 Register
// (ErrManagerClosed)→ 清空注册表。幂等。
//
// 语义边界:
//   - 同步路径(RecordRun)的 fn 用调用方 ctx,与 m.ctx 无父子关系,Close 的
//     cancel 不影响在途同步 prompt——它们的取消仍由请求侧 ctx(连接断开/
//     session cancel)语义管理,不经 manager;
//   - 等待只盯 run.done(由 transition 在 run.mu 内关闭):不持 m.mu 也不持
//     rt.mu 等待,Get/Remove/List 与记账推进在等待期间照常工作,无锁环;
//   - 事件面收口(P3-5)在「等全部在途 run 终态」之后、「清表」之前:次序即
//     论证——tee 的 Publish 只发生在 Session.Run 内(supervisor.go 的 cb 调用
//     面,先于 finalizeRun 关闭 run.done),等待完成后再关 hub ⇒ 执行中的
//     Publish 不可能写已关闭的 journal;Close 后 hub 一律拒绝新 Subscribe
//     (ErrHubClosed),事件流随 manager 生命周期终结;
//   - P2-7 起 daemon 在 Stop 时**不**调用它:同步路径(daemon 现状唯一调用
//     形态)没有 manager 持有的 run,等待在途 prompt 反而拖慢停机——Stop 的
//     收尾仍是各 store/repo 各自 Close,P5 StartRun 接入前端后再接线。
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

	// 事件面收口(P3-5):逐 runtime EndRun+Close 其 EventHub(均幂等;events
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

// CancelRun 取消一个后台 Run(P2-6,总方案 P2 第 4 条):运行中与排队中的
// StartRun Run 均可取消。cancel 使 runCtx 就绪.Done,取消的落点全部在 execute
// 既有分支(supervisor.go):
//   - 排队中(等待在 ① gate / ② runSlots 的 select)→ 命中 runCtx.Done →
//     释放已持有的 gate/pending → finalizeRun(RunCancelled)——总方案第四节
//     「排队取消必须直接释放占用权」由该失败分支的释放满足;
//   - 运行中 → ⑤ 返回后 runCtx.Err() 非 nil → execute 层归一覆盖 RunCancelled
//     (P2-1 归一保证状态正确,取消绝不记 failed)。
//
// 拒绝语义:未知会话 / 无在途 Run / RunID 不匹配 → ErrRunNotFound;cancelFunc()
// == nil → ErrRunNotCancellable——同步 RecordRun 路径的 Run 无 cancel,其取消
// 入口是调用方请求 ctx(连接断开 / session cancel),P5 前不经 CancelRun。
//
// 并发论证:两把锁(m.mu / rt.mu)内都只做快照,cancel() 在任何锁之外调用
// (锁内不调回调);快照后 run.ID/cancelFunc() 的读取只依赖 run 自身的 mu,
// 与 rt.mu 无关。取消与完成竞争幂等:谁先到 finalizeRun 都成立(transition
// 防重入 run.go:67-85、done 幂等关闭;cancel 落在终态之后只是关闭一个已无
// 等待者的 ctx,无副作用)。
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
