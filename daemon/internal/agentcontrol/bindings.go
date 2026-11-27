package agentcontrol

import (
	"sort"
	"sync"
)

// BindingRegistry 仅用于生命周期记账，不参与会话路由。
// 锁内不做 I/O 或回调；绑定变化后的生命周期评估由调用方在锁外执行。
// 连接绑定与引用收口的关系见 ../../docs/README.md。

type BindingRegistry struct {
	mu        sync.Mutex
	byClient  map[string]map[string]struct{} // clientID → referenced session IDs
	bySession map[string]map[string]struct{} // sessionID → referencing client IDs
}

// NewBindingRegistry 构造空登记表。
func NewBindingRegistry() *BindingRegistry {
	return &BindingRegistry{
		byClient:  make(map[string]map[string]struct{}),
		bySession: make(map[string]map[string]struct{}),
	}
}

// BindReference adds one lifecycle reference. Duplicate bindings are idempotent.
// Empty client or session IDs are ignored.
func (r *BindingRegistry) BindReference(clientID, sessionID string) bool {
	if clientID == "" || sessionID == "" {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byClient == nil {
		r.byClient = make(map[string]map[string]struct{})
	}
	if r.bySession == nil {
		r.bySession = make(map[string]map[string]struct{})
	}
	if _, ok := r.byClient[clientID][sessionID]; ok {
		return false
	}
	if r.byClient[clientID] == nil {
		r.byClient[clientID] = make(map[string]struct{})
	}
	if r.bySession[sessionID] == nil {
		r.bySession[sessionID] = make(map[string]struct{})
	}
	r.byClient[clientID][sessionID] = struct{}{}
	r.bySession[sessionID][clientID] = struct{}{}
	return true
}

// Bind preserves the legacy replace-current behavior for callers that model a single
// selected session. Lifecycle registration uses BindReference instead.
func (r *BindingRegistry) Bind(clientID, sessionID string) (prev string) {
	if clientID == "" {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byClient == nil {
		r.byClient = make(map[string]map[string]struct{})
	}
	if r.bySession == nil {
		r.bySession = make(map[string]map[string]struct{})
	}
	prev = firstSortedKey(r.byClient[clientID])
	if prev == sessionID && len(r.byClient[clientID]) == 1 {
		return prev
	}
	r.removeClientLocked(clientID)
	if sessionID != "" {
		r.addLocked(clientID, sessionID)
	}
	return prev
}

// ReplaceReferences preserves the single-selected-session contract while returning every
// previous reference so lifecycle owners can recheck sessions removed by replacement.
func (r *BindingRegistry) ReplaceReferences(clientID, sessionID string) []string {
	if clientID == "" {
		return []string{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byClient == nil {
		r.byClient = make(map[string]map[string]struct{})
	}
	if r.bySession == nil {
		r.bySession = make(map[string]map[string]struct{})
	}
	previous := sortedKeys(r.byClient[clientID])
	r.removeClientLocked(clientID)
	if sessionID != "" {
		r.addLocked(clientID, sessionID)
	}
	return previous
}

// UnbindReference removes only the requested client/session relationship.
func (r *BindingRegistry) UnbindReference(clientID, sessionID string) bool {
	if clientID == "" || sessionID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byClient[clientID][sessionID]; !ok {
		return false
	}
	r.removeLocked(clientID, sessionID)
	return true
}

// UnbindAll removes all lifecycle references held by a client and returns their session IDs
// in sorted order. Locks protect only the ledger update; callers perform lifecycle work later.
func (r *BindingRegistry) UnbindAll(clientID string) []string {
	if clientID == "" {
		return []string{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sessions := sortedKeys(r.byClient[clientID])
	r.removeClientLocked(clientID)
	return sessions
}

// Unbind retains the legacy all-references behavior and returns one prior session ID.
// Callers that need exact cleanup should use UnbindReference; disconnect uses UnbindAll.
func (r *BindingRegistry) Unbind(clientID string) (prev string) {
	sessions := r.UnbindAll(clientID)
	if len(sessions) > 0 {
		return sessions[0]
	}
	return ""
}

// Bindings returns all client IDs referencing sessionID as a sorted copy.
func (r *BindingRegistry) Bindings(sessionID string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return sortedKeys(r.bySession[sessionID])
}

func (r *BindingRegistry) addLocked(clientID, sessionID string) {
	if r.byClient[clientID] == nil {
		r.byClient[clientID] = make(map[string]struct{})
	}
	if r.bySession[sessionID] == nil {
		r.bySession[sessionID] = make(map[string]struct{})
	}
	r.byClient[clientID][sessionID] = struct{}{}
	r.bySession[sessionID][clientID] = struct{}{}
}

func (r *BindingRegistry) removeLocked(clientID, sessionID string) {
	delete(r.byClient[clientID], sessionID)
	if len(r.byClient[clientID]) == 0 {
		delete(r.byClient, clientID)
	}
	delete(r.bySession[sessionID], clientID)
	if len(r.bySession[sessionID]) == 0 {
		delete(r.bySession, sessionID)
	}
}

func (r *BindingRegistry) removeClientLocked(clientID string) {
	for sessionID := range r.byClient[clientID] {
		delete(r.bySession[sessionID], clientID)
		if len(r.bySession[sessionID]) == 0 {
			delete(r.bySession, sessionID)
		}
	}
	delete(r.byClient, clientID)
}

func firstSortedKey(values map[string]struct{}) string {
	keys := sortedKeys(values)
	if len(keys) > 0 {
		return keys[0]
	}
	return ""
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
