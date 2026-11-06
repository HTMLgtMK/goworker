// Package agentcontrol 是 daemon 的 Application Control Plane(总方案 §四):
// 高层业务操作(StartRun/CancelRun/RunStatus/ResolveApproval)的唯一入口,负责
// 「谁可以操作哪个 Session」——Control Lease(fencing)、Run 提交幂等(per-key
// single-flight)与审批授权在本层收口;执行平面(ai-runtime 的 SessionManager)
// 不感知 clientID,协议层(ACP)不直接触碰 Runtime,AgentPlugin 只做装配与委托。
//
// 依赖方向(总方案 §三):ACP / AgentPlugin → agentcontrol → ai-runtime → ai-core。
// 本包禁止 import daemon/internal/plugins/agent、daemon/internal/core/service/acp、
// daemon/internal/core/model 以及任何 daemon/internal 兄弟包(constraint_test.go
// 以 go/parser 钉死,比靠 Code Review 记住规则可靠);跨包需要的类型
// (hitl.Decision、runtimeagent.*、session.*)直接使用。Service 自身不持有
// 「当前会话」状态与会话级资源(它们随会话边界重建、属于调用入口的会话选择,
// 总方案 §六),经 SessionRouter/JournalSource 窄接口由宿主注入,接口定义按方案
// 要求放在本包(sessions.go)。Plan A2 起的唯一新增是生命周期绑定登记
// (BindingRegistry,bindings.go):它是跨连接的引用计数事实源,不是会话选择
// 状态,路由/解析不消费它(架构边界收窄论证见 sessions.go/bindings.go 文件头)。
package agentcontrol

import (
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
)

// ClientKind 是 Principal 的调用方类别(§4.1)。V1 仅 Client(客户端连接);
// Internal 为后台任务/内部 Plugin 预留——§4.1 明确「不建议使用一个默认可信的
// 空 Principal 来绕过 Lease」,内部调度即便不依赖 Connection Lease,也必须有
// 明确的执行身份,不做静默的可信缺省。
type ClientKind string

const (
	// ClientKindClient 是客户端连接(ACP/CLI)的调用方类别:xgw 面的约定映射是
	// Kind=Client、ClientID=connectionID(handler 工厂生成的连接句柄)。
	ClientKindClient ClientKind = "client"
	// ClientKindInternal 是进程内非连接调用方的预留类别(PR4 统一同步路径时
	// 启用;本 PR 的 Service 方法以 clientID 字符串入参,即 Client 的 ClientID)。
	ClientKindInternal ClientKind = "internal"
)

// Principal 是调用方身份(§4.1 草案类型,PR1 先落定义):表达「谁在提交控制
// 操作」,不在底层 AgentRuntime 中传播(§4.1:执行平面不知道 ConnectionID)。
// 本 PR 的 Service 方法以 clientID 字符串入参(与 RunControlSource 现状签名对齐
// 的最薄委托面,clientID 即 Principal.ClientID=connectionID、Kind=Client);
// PR2 已落地连接级 SessionBinding(sessions.go,连接的会话选择器,标准 ACP 经
// ctx 载体流入插件解析点);Principal 携带形与 §4.1 的 StartRunCommand/
// RunAccepted 命令对象随 PR3 收敛订阅契约时一并升级。
type Principal struct {
	ClientID string
	Kind     ClientKind
}

// ClientPrincipal 构造客户端连接身份(xgw 约定:ClientID=connectionID、
// Kind=Client)。空 clientID 不构造(LeaseRegistry 对空持有者一律拒绝,与
// 「空闲」不可辨的既有语义一致)。
func ClientPrincipal(clientID string) Principal {
	return Principal{ClientID: clientID, Kind: ClientKindClient}
}

// Service 是应用内唯一的 Application Control Plane 实例(§4.2):由 plugin 构造
// 并持有,ACP 与 plugin 共享同一实例——两套 Lease Registry / Request Registry 是
// 本设计明确禁止的形态。本 PR 先迁四个控制操作(StartRun/CancelRun/RunStatus/
// ResolveApproval)+ control lease 四操作;RunControlSource 的 ActiveRun/
// PendingApprovals/Attach 暂留 plugin(PR3 收敛事件订阅、PR4 统一同步路径时迁),
// 同步 /agent 路径本 PR 不经 Service(PR4)。
//
// 并发与锁论证(原 plugin runcontrol.go 文件头的迁移版):
//   - lease.mu(每会话一把,LeaseRegistry)是本会话全部控制操作「lease 校验 +
//     提交」与接管的线性化点;manager.StartRun 同步段的 journal fsync 只占住本
//     会话 lease,跨会话控制操作互不阻塞(DE2-I1 语义原样保留);
//   - registry.mu 只护幂等表/flight 结构读写(查表/记账,短临界区、无 I/O),
//     底层 manager.StartRun 在 registry.mu 之外执行(§4.3:不同 Key 不因本注册
//     表的锁互相阻塞);锁序 lease.mu→registry.mu→manager 内部锁单向(registry.mu
//     各临界区与 manager 各路径绝不取 lease.mu,无环);ReleaseConnection 先在
//     registry.mu 下快照再逐个锁 lease.mu,两锁不同时持有(反向无嵌套);
//   - BindingRegistry 的 registry.mu(bindings.go,Plan A2)同为叶锁:只护映射
//     读写、无 I/O 无回调,与 lease.mu 永不嵌套;绑定登记的调用方(handler 断言
//     面/插件)在拿到「受影响的会话 id」后于锁外执行生命周期评估(manager/repo
//     的锁不进入本包),锁序仍单向无环;
//   - 跨层嵌套唯一一处:ACP handler 的 h.mu 临界区内调用 Acquire/ReleaseControl
//     ——锁序 h.mu→session lease 单向(lease 是叶锁,绝不回调 handler、不取
//     h.mu),Close 侧同向,无环(guard 换名 lease,DE2-I1 论证随方法整体迁入);
//   - 其余方法(manager.StartRun/CancelRun、rt.Active/Hub、ApprovalManager)的
//     并发面由 SDK 侧既有锁序承担,本层不做新共享状态。
type Service struct {
	// sessions 是多会话执行内核的注册表(§4.1 草案的同名字段):Run 生命周期的
	// 真正所有者,Service 只编排(寻址 → fencing 校验 → 幂等 → 直转)。
	sessions *runtimeagent.SessionManager
	// leases 是 per-session control lease 注册表(§五):fencing 的唯一实现点。
	leases *LeaseRegistry
	// requests 是 (sessionID, requestID) 幂等注册表(§4.3):per-key single-flight。
	requests *RequestRegistry
	// bindings 是 per-connection 会话绑定登记(Plan A2 生命周期半边,bindings.go):
	// 生命周期引用计数的唯一事实源,只服务计数与 A3 的 active 判定,路由/解析
	// 不消费(架构边界收窄论证见 sessions.go SessionBinding 与 bindings.go 文件头)。
	bindings *BindingRegistry
	// router 是会话寻址窄接口(宿主注入):「当前会话优先 + 注册表权威」的统一
	// 解析点由 plugin 提供(RouteSession),Service 不持有会话选择状态。
	router SessionRouter
	// journals 是被路由会话的 journal 解析窄接口(宿主注入,可为 nil):RunStatus
	// 的 journal 权威分支用;nil = 宿主无 journal 面,RunStatus 走幂等表回退。
	journals JournalSource
}

// NewService 构造 Service。sessions 与 router 不可为 nil(router 承载「current
// 优先」的寻址语义,§三 的解析规则属于宿主的统一解析点);journals 可为 nil
// (纯内存宿主:RunStatus 的 journal 分支跳过,幂等表是唯一进程内观测面)。
func NewService(sessions *runtimeagent.SessionManager, router SessionRouter, journals JournalSource) *Service {
	return &Service{
		sessions: sessions,
		leases:   NewLeaseRegistry(),
		requests: NewRequestRegistry(),
		bindings: NewBindingRegistry(),
		router:   router,
		journals: journals,
	}
}

// AcquireControl 获取 (sessionID, clientID) 的 control lease:该会话空闲 → 占用
// 并返回 false;他人持有 → 显式接管(返回 true 并 slog.Info 审计);同持有者重复
// → false。RunControlSource.AcquireControl 的委托实现面消费。
func (s *Service) AcquireControl(sessionID, clientID string) bool {
	return s.leases.Acquire(sessionID, clientID)
}

// ReleaseControl 释放 (sessionID, clientID) 的 control lease:仅该会话的持有者
// 本人有效,他人调用为空操作。
func (s *Service) ReleaseControl(sessionID, clientID string) {
	s.leases.Release(sessionID, clientID)
}

// HoldControl 报告 clientID 当前是否持有 sessionID 的 control lease。与 fenced
// 控制操作的 WithControl 校验共用同一判定,保证守卫语义与守卫执行点永不漂移。
func (s *Service) HoldControl(sessionID, clientID string) bool {
	return s.leases.Hold(sessionID, clientID)
}

// ReleaseConnection 释放 clientID 持有的全部会话的 control lease(断连收口:
// handler.Close 一次调用替代逐会话归还,「审批不悬死在已死连接上」),并清除该
// 连接的会话绑定登记(Plan A2:与 lease 同构的断连清账——连接退出即失去对绑定
// 会话的引用)。返回被清除登记指向的会话 id(未登记返回空串),供调用方在锁外
// 评估该会话的引用归零收口;连接不持有任何租约/绑定时幂等空操作。
func (s *Service) ReleaseConnection(clientID string) string {
	s.leases.ReleaseConnection(clientID)
	return s.bindings.Unbind(clientID)
}

// BindSession 把 clientID 的会话绑定登记到 sessionID,返回改绑前的会话 id
// (bindings.go Bind 语义:空 sessionID = 解除绑定;空 clientID 无操作)。登记
// 只是记账——改绑/解绑导致的引用变化由调用方拿返回值评估收口(Plan A2 的
// 「切换」触发点),本方法不执行任何生命周期动作。
func (s *Service) BindSession(clientID, sessionID string) string {
	return s.bindings.Bind(clientID, sessionID)
}

// UnbindSession 解除 clientID 的绑定登记(bindings.go Unbind),返回被解除的
// 会话 id。/new 收口「摘发起方绑定」的登记侧调用点;生命周期评估由调用方以
// 返回值驱动,本方法不执行任何收口动作。
func (s *Service) UnbindSession(clientID string) string {
	return s.bindings.Unbind(clientID)
}

// SessionBindings 返回当前绑定在 sessionID 上的全部连接 id(bindings.go
// Bindings;升序拷贝)。生命周期归零判定「绑定集合空 && 无在飞 run」与 A3 的
// state=active 判定都从这里取数——它是跨连接绑定的事实源。
func (s *Service) SessionBindings(sessionID string) []string {
	return s.bindings.Bindings(sessionID)
}

// UnbindAllSession 解除一个会话上的全部连接绑定，返回被摘除的 clientID（升序）。
// 删除会话后由宿主调用；登记面不触碰 lease/runtime，调用方负责其它收口。
func (s *Service) UnbindAllSession(sessionID string) []string {
	return s.bindings.UnbindSession(sessionID)
}

// WithControl 是受 lease 保护的操作提交入口的宿主面(LeaseRegistry.WithControl
// 的 Service 透传;Plan A3 的归档 fencing 消费——run/cancel/审批的 fencing 编排
// 都封装在 Service 的对应方法内,归档的动作面(MarkArchived)持在宿主(plugin),
// 编排经本方法回到同一 lease 临界区):本会话 lease.mu 临界区内先校验 holder
// 再执行 fn——校验通过后到 fn 返回之间不可能被接管,旧持有者绝不能在失权后
// 落地归档。fn 必须是短提交操作(一次校验 + 一次文件突变,如 MarkArchived 的
// append+fsync——与 manager.StartRun 同步段的 journal fsync 同款先例),绝不是
// 整个 AgentRun;后台执行仍在锁外完成。仅宿主消费——协议层经
// BoundSessionSource.ArchiveSession 的方法面触达,静态类型拿不到本方法,
// 「不向协议层暴露任意长 callback 注入口」的边界(R3 纪律)不变。
func (s *Service) WithControl(sessionID, clientID string, fn func() error) error {
	return s.leases.WithControl(sessionID, clientID, fn)
}

// TrackedRequest 返回 (sessionID, requestID) 的已记账 RunHandle(幂等表按键直查
// 的观测面):RunStatus 的纯内存回退消费按 (sessionID, runID) 的反查
// (RequestRegistry.byRun),本方法供「按 requestId 观测」与测试断言
// 「拒绝路径不入表」消费。ok=false = 该键无记账(含空 requestID)。
func (s *Service) TrackedRequest(sessionID, requestID string) (runtimeagent.RunHandle, bool) {
	return s.requests.Tracked(sessionID, requestID)
}
