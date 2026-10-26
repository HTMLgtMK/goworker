package agent

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// 本文件是多会话执行内核的注册表:SessionManager 按 SessionID 管理
// SessionRuntime。注释明确:全局并发配额(runSlots)属 P2「有界全局并发调度」,
// 宿主生命周期 ctx/cancel 同属 P2(RunSupervisor);本结构预留 Register/Remove
// 即可,均不在 P0 实现。

// SessionManager 是 SessionRuntime 的进程内注册表:一个 SessionID 至多一个
// runtime,互斥执行权由各 runtime 自持的 gate 保证,manager 不参与排队。
type SessionManager struct {
	mu       sync.RWMutex
	runtimes map[string]*SessionRuntime
}

// NewSessionManager 创建空注册表。
func NewSessionManager() *SessionManager {
	return &SessionManager{runtimes: make(map[string]*SessionRuntime)}
}

// Register 注册 runtime;同 ID 冲突报错(一个会话至多一个 runtime)。
func (m *SessionManager) Register(rt *SessionRuntime) error {
	if rt == nil {
		return errors.New("session: register nil runtime")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
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

// Close 清空注册表(幂等)。持锁中的 runtime 由调用方各自收尾;
// 宿主级统一停机(取消宿主 ctx、拒绝后续 Register)属 P2 RunSupervisor。
func (m *SessionManager) Close() {
	m.mu.Lock()
	m.runtimes = make(map[string]*SessionRuntime)
	m.mu.Unlock()
}
