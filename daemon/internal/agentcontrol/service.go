// Package agentcontrol 编排会话寻址、Run 控制、租约、幂等和审批授权。
// 依赖边界与锁序见 ../../docs/README.md。
package agentcontrol

import (
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
)

// ClientKind 是 Principal 的调用方类别。V1 仅 Client(客户端连接);Internal
// 为后台任务/内部 Plugin 预留——「不建议使用一个默认可信的空 Principal 来绕过
// Lease」,内部调度即便不依赖 Connection Lease,也必须有明确的执行身份,不做
// 静默的可信缺省。
type ClientKind string

const (
	// ClientKindClient 是客户端连接(ACP/CLI)的调用方类别:xgw 面的约定映射是
	// Kind=Client、ClientID=connectionID(handler 工厂生成的连接句柄)。
	ClientKindClient ClientKind = "client"
	// ClientKindInternal 是进程内非连接调用方的预留类别(统一同步路径时
	// 启用;本 PR 的 Service 方法以 clientID 字符串入参,即 Client 的 ClientID)。
	ClientKindInternal ClientKind = "internal"
)

// Principal 表示提交控制操作的调用方；执行平面不依赖连接身份。
type Principal struct {
	ClientID string
	Kind     ClientKind
}

// ClientPrincipal 构造客户端连接身份；空 clientID 不具备 lease 权限。
func ClientPrincipal(clientID string) Principal {
	return Principal{ClientID: clientID, Kind: ClientKindClient}
}

// Service 是 daemon 共享的应用控制面，编排会话寻址、租约、幂等和审批。
// 每个会话的 lease 锁保护校验与提交，不能在持锁期间回调 handler。
// 幂等表和绑定表仅在短临界区内记账，不在各自的锁内执行 I/O 或回调。
// 锁序和跨层约束见 ../../docs/README.md。
type Service struct {
	// sessions 是多会话执行内核的注册表:Run 生命周期的
	// 真正所有者,Service 只编排(寻址 → fencing 校验 → 幂等 → 直转)。
	sessions *runtimeagent.SessionManager
	// leases 是 per-session control lease 注册表:fencing 的唯一实现点。
	leases *LeaseRegistry
	// requests 是 (sessionID, requestID) 幂等注册表:per-key single-flight。
	requests *RequestRegistry
	// bindings 是 per-connection 会话绑定登记(bindings.go):生命周期引用计数的
	// 唯一事实源,只服务计数与 active 判定,路由/解析不消费(架构边界收窄论证见
	// sessions.go SessionBinding 与 bindings.go 文件头)。
	bindings *BindingRegistry
	// router 是会话寻址窄接口(宿主注入):「当前会话优先 + 注册表权威」的统一
	// 解析点由 plugin 提供(RouteSession),Service 不持有会话选择状态。
	router SessionRouter
	// journals 是被路由会话的 journal 解析窄接口(宿主注入,可为 nil):RunStatus
	// 的 journal 权威分支用;nil = 宿主无 journal 面,RunStatus 走幂等表回退。
	journals JournalSource
}

// NewService 构造 Service。sessions 与 router 不可为 nil(router 承载「current
// 优先」的寻址语义,解析规则属于宿主的统一解析点);journals 可为 nil
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

// ReleaseConnection releases all leases held by clientID and removes its connection binding.
// It returns the previously bound session ID so the caller can recheck lifecycle release
// outside the control-plane locks; clients without leases or bindings are no-ops.
func (s *Service) ReleaseConnection(clientID string) string {
	s.leases.ReleaseConnection(clientID)
	return s.bindings.Unbind(clientID)
}

// BindSession 把 clientID 的会话绑定登记到 sessionID,返回改绑前的会话 id
// (bindings.go Bind 语义:空 sessionID = 解除绑定;空 clientID 无操作)。登记
// 只是记账——改绑/解绑导致的引用变化由调用方拿返回值评估收口(「切换」
// 触发点),本方法不执行任何生命周期动作。
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
// Bindings;升序拷贝)。生命周期归零判定「绑定集合空 && 无在飞 run」与
// state=active 判定都从这里取数——它是跨连接绑定的事实源。
func (s *Service) SessionBindings(sessionID string) []string {
	return s.bindings.Bindings(sessionID)
}

// WithControl delegates to the per-session fencing guard. The holder check and short
// commit run under the same lease lock, so takeover cannot interleave with submission.
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
