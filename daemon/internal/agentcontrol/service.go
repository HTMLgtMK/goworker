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
// 要求放在本包(sessions.go)。
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
// PR2 引入 SessionBinding、PR3 收敛订阅契约时升级为 Principal 携带形(§4.1 的
// StartRunCommand/RunAccepted 命令对象随 PR2 一并落地)。
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
// handler.Close 一次调用替代逐会话归还,「审批不悬死在已死连接上」)。连接不
// 持有任何租约时幂等空操作。
func (s *Service) ReleaseConnection(clientID string) {
	s.leases.ReleaseConnection(clientID)
}

// TrackedRequest 返回 (sessionID, requestID) 的已记账 RunHandle(幂等表按键直查
// 的观测面):RunStatus 的纯内存回退消费按 (sessionID, runID) 的反查
// (RequestRegistry.byRun),本方法供「按 requestId 观测」与测试断言
// 「拒绝路径不入表」消费。ok=false = 该键无记账(含空 requestID)。
func (s *Service) TrackedRequest(sessionID, requestID string) (runtimeagent.RunHandle, bool) {
	return s.requests.Tracked(sessionID, requestID)
}
