package agentcontrol

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// 控制权按会话隔离；租约校验、控制操作提交和接管使用同一把会话锁。
// 跨层锁序和权限语义见 ../../docs/README.md。

// ErrControlLeaseLost indicates the caller does not hold the session's control lease.
// Fenced operations use it when the lease is absent, released, or taken over.
var ErrControlLeaseLost = errors.New("control lease not held by the caller")

// controlLeaseLostErr 是 fencing 拒绝的定位化包装(sessionID + clientID 可观测;
// clientID 即原 connectionID,错误文案沿用「connection」措辞保持协议面兼容)。
func controlLeaseLostErr(sessionID, clientID string) error {
	return fmt.Errorf("agent: session %q: connection %q: %w", sessionID, clientID, ErrControlLeaseLost)
}

// SessionLease is one session's fencing guard. Its mutex linearizes lease validation,
// control submission, and takeover; holder is empty when the lease is free.
type SessionLease struct {
	mu     sync.Mutex
	holder string
}

// holds 报告 clientID 是否是当前持有者(调用方必须已持 mu)。空 clientID 不可能
// 成为 holder(Acquire 已拒),恒 false——「空串与空闲不可辨」的既有语义。
func (l *SessionLease) holds(clientID string) bool {
	return clientID != "" && l.holder == clientID
}

// LeaseRegistry 是 sessionID → SessionLease 的注册表(原 guardsMu + controlGuards
// 迁入):mu 只护表结构的 get-or-create 与快照(叶锁,临界区无 I/O 无回调),
// 与 lease.mu 永不嵌套。lease 条目只增不删(与会话注册表同生命周期量级,单
// 条目仅一把互斥锁 + 一个字符串;删除须与并发控制操作同步,徒增 fencing 复杂度)。
type LeaseRegistry struct {
	mu     sync.Mutex
	leases map[string]*SessionLease
}

// NewLeaseRegistry 构造空注册表(零值亦可惰性工作,见 lease)。
func NewLeaseRegistry() *LeaseRegistry {
	return &LeaseRegistry{leases: make(map[string]*SessionLease)}
}

// lease 取(或惰性建)sessionID 的 fencing lease(get-or-create)。表查找/创建
// 在 registry.mu 内完成并先于 lease.mu 加锁释放——两锁永不嵌套。
func (r *LeaseRegistry) lease(sessionID string) *SessionLease {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l, ok := r.leases[sessionID]; ok {
		return l
	}
	if r.leases == nil {
		r.leases = make(map[string]*SessionLease)
	}
	l := &SessionLease{}
	r.leases[sessionID] = l
	return l
}

// Acquire grants a free lease and returns false, or takes over an existing lease and returns
// true after recording an audit log. The same holder reacquiring returns false. Empty
// client IDs are rejected so they cannot be confused with an unheld lease.
func (r *LeaseRegistry) Acquire(sessionID, clientID string) bool {
	if clientID == "" {
		return false
	}
	l := r.lease(sessionID)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder == clientID {
		return false
	}
	tookOver := l.holder != ""
	if tookOver {
		slog.Info("x-goworker: control lease taken over", "session", sessionID, "previous", l.holder, "new", clientID)
	}
	l.holder = clientID
	return tookOver
}

// Release 释放 (sessionID, clientID) 的 control lease:仅该会话的持有者本人有效,
// 他人调用为空操作(不抢、不误清他会话)。空 clientID 不可能成为 holder。
func (r *LeaseRegistry) Release(sessionID, clientID string) {
	if clientID == "" {
		return
	}
	l := r.lease(sessionID)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder == clientID {
		l.holder = ""
	}
}

// Hold 报告 clientID 当前是否持有 sessionID 的 control lease。与 fenced 控制
// 操作的 WithControl 校验共用同一判定(同一 holds),守卫语义与守卫执行点
// 永不漂移。
func (r *LeaseRegistry) Hold(sessionID, clientID string) bool {
	l := r.lease(sessionID)
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holds(clientID)
}

// WithControl validates the holder and runs the short commit while holding that
// session's lease mutex, preventing takeover between authorization and submission.
// The commit may perform persistence I/O, but must not run the entire agent or call back into the handler.
func (r *LeaseRegistry) WithControl(sessionID, clientID string, fn func() error) error {
	l := r.lease(sessionID)
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.holds(clientID) {
		return controlLeaseLostErr(sessionID, clientID)
	}
	return fn()
}

// ReleaseConnection snapshots the lease pointers under registry.mu, then clears the
// client's holder under each lease lock. The locks are never nested.
func (r *LeaseRegistry) ReleaseConnection(clientID string) {
	if clientID == "" {
		return
	}
	r.mu.Lock()
	snapshot := make([]*SessionLease, 0, len(r.leases))
	for _, l := range r.leases {
		snapshot = append(snapshot, l)
	}
	r.mu.Unlock()
	for _, l := range snapshot {
		l.mu.Lock()
		if l.holder == clientID {
			l.holder = ""
		}
		l.mu.Unlock()
	}
}
