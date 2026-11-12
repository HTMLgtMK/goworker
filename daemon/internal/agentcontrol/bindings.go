package agentcontrol

import (
	"sort"
	"sync"
)

// BindingRegistry 仅用于生命周期记账，不参与会话路由。
// 锁内不做 I/O 或回调；绑定变化后的生命周期评估由调用方在锁外执行。
// 连接绑定与引用收口的关系见 ../../docs/README.md。

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

// Bind records the connection's session and returns its previous session ID.
// Empty client IDs are ignored; an empty session ID unbinds; repeating the same ID is a no-op.
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

// Bindings 返回当前绑定在 sessionID 上的全部连接 id（升序拷贝，调用方可安全
// 持有/遍历）。无绑定返回空切片（非 nil，len 判定即「引用归零」）。这是生命周期
// 归零判定与 state=active 判定的取数点。
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
