package agentcontrol

import (
	"sort"
	"sync"
)

// bindings.go 是 per-connection 会话绑定登记（Plan A2 生命周期半边）：BindingRegistry
// 记录 clientID → 其当前绑定的 sessionID，作为多 session 运行时生命周期引用计数的
// 唯一事实源——「任一连接绑定即活；最后解绑（断连/切换 /new）→ 关 store + 注销
// manager」（Plan A2），归零判定的「绑定集合」从这里取数（A3 的 state=active 判定
// 亦消费同一集合）。
//
// 架构边界（有意的变化，取代 PR2 的「不存在全局 Binding 事实源」表述——
// sessions.go SessionBinding 注释四条纪律的第 4 条同步收窄）：
//   - PR2 原语义保留：会话路由/解析（plugin.sessionRuntimeIn、RouteSession）**不
//     消费本注册表**——寻址仍只读连接私有的 SessionBinding（ctx 载体）与 manager
//     注册表，§二不变量 6 由「不存在全局事实源」收窄为「路由/解析不消费注册表」；
//   - 收窄的原因：生命周期计数必须有跨连接的全局事实源，而寻址不需要——两者
//     曾经被 PR2 一并否定，Plan A2 只放行前者。注册表是生命周期记账，不是会话
//     选择状态（Service 依旧不持有「当前会话」选择，service.go 文件头不变量不变）；
//   - 登记只增减计数、绝无副作用：Bind/Unbind 不删除会话、不取消 Run、不动
//     control lease、不影响其他连接（§6.2 四条纪律对登记同样成立）；生命周期
//     收口动作（评估引用、关 store）由宿主在登记变更之外显式执行——本文件不
//     回调任何宿主函数，注册表是纯记账叶节点。
//
// 并发与锁序：registry.mu 只护映射读写（短临界区、无 I/O、无回调），是锁序图
// 中的叶节点（lease.go SessionLease 同位）；与 lease.mu 永不嵌套，Service 方法
// 先在 registry.mu 内完成记账并取出受影响的 sessionID，锁外由调用方执行生命周期
// 评估（plugin 侧），manager/repo 的锁不进入本文件。与 SessionBinding（连接私有
// 选择器）的一致性：登记与 b.Set 由同一批调用点成对执行（handler 的 binding.Set
// 处、/new 收口的 b.Set("") 处），两侧是同一事实的两种粒度——连接内寻址用
// SessionBinding，跨连接计数用本注册表。

// BindingRegistry 是 clientID → 当前绑定 sessionID 的登记表。每条连接至多绑定
// 一个会话（与 SessionBinding 的「本连接当前选择」语义一一对应）：改绑即替换
// （旧会话失去该连接的引用），断连即整行清除。
type BindingRegistry struct {
	mu       sync.Mutex
	byClient map[string]string // clientID → 当前绑定的 sessionID
}

// NewBindingRegistry 构造空登记表。
func NewBindingRegistry() *BindingRegistry {
	return &BindingRegistry{byClient: make(map[string]string)}
}

// Bind 把 clientID 绑定到 sessionID，返回其改绑前的会话 id（无改绑历史返回空串）。
// 语义：
//   - 空 clientID 拒绝（与 LeaseRegistry 对空持有者一律拒绝同款——连接句柄恒为
//     非空随机 hex，空串与「未登记」不可辨）；
//   - 空 sessionID 视为解除绑定（/new 收口 b.Set("") 的登记侧同形），返回改绑前
//     的会话 id；
//   - 重复绑定同一会话幂等（prev == sessionID，调用方无需为它做生命周期评估）。
func (r *BindingRegistry) Bind(clientID, sessionID string) (prev string) {
	if clientID == "" {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byClient == nil {
		r.byClient = make(map[string]string)
	}
	prev = r.byClient[clientID]
	if sessionID == "" {
		delete(r.byClient, clientID)
		return prev
	}
	if prev == sessionID {
		return prev // 幂等：同一会话重复登记，引用计数不变
	}
	r.byClient[clientID] = sessionID
	return prev
}

// Unbind 解除 clientID 的绑定（整行清除，不区分此前绑定的会话），返回改绑前的
// 会话 id（未登记返回空串）。空 clientID 幂等空操作。断连收口（ReleaseConnection）
// 与 /new 摘发起方绑定共用本方法。
func (r *BindingRegistry) Unbind(clientID string) (prev string) {
	if clientID == "" {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	prev = r.byClient[clientID]
	delete(r.byClient, clientID)
	return prev
}

// UnbindSession 解除一个会话上的全部连接绑定，返回被摘除的 clientID（升序）。
// 删除会话专用：登记表本身只记账、不做生命周期副作用；调用方在锁外收口
// runtime/store。空 sessionID 幂等返回空切片。
func (r *BindingRegistry) UnbindSession(sessionID string) []string {
	if sessionID == "" {
		return []string{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	clients := make([]string, 0, 2)
	for client, sid := range r.byClient {
		if sid == sessionID {
			clients = append(clients, client)
			delete(r.byClient, client)
		}
	}
	sort.Strings(clients)
	return clients
}

// Bindings 返回当前绑定在 sessionID 上的全部连接 id（升序拷贝，调用方可安全
// 持有/遍历）。无绑定返回空切片（非 nil，len 判定即「引用归零」）。这是生命周期
// 归零判定与 A3 state=active 判定的取数点。
func (r *BindingRegistry) Bindings(sessionID string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, 2)
	for client, sid := range r.byClient {
		if sid == sessionID {
			out = append(out, client)
		}
	}
	sort.Strings(out)
	return out
}
