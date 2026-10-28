package agentcontrol

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// lease.go 是 per-session control lease 的 fencing 实现(总方案 §五):从 plugin
// runcontrol.go 的 sessionControlGuard / controlGuard / snapshotControlGuards
// 原样迁出,保留锁语义——每会话一把互斥锁,校验与提交同临界区,接管在同锁内
// 线性化。第四轮评审 DE2-I1 确立的「会话粒度」不变:某会话 manager.StartRun
// 同步段的 journal fsync 只占住本会话的 lease,跨会话的 Acquire/Release 与其他
// 控制操作互不阻塞(全局锁会把单会话慢盘放大为全 daemon 控制面卡顿)。

// ErrControlLeaseLost 是控制操作 fencing 的拒绝 sentinel(第三轮评审 I4 修复,
// 原 runcontrol.go 迁入):调用方声明的 clientID 在校验时刻不持有该会话的
// control lease(从未持有、已被接管或已释放)。控制类操作(StartRun/CancelRun/
// ResolveApproval)把 lease 校验合并进本会话 lease.mu 提交临界区(WithControl)
// ——与接管 Acquire 同锁,「校验通过 → 提交」之间不可能被接管,旧持有者绝不
// 能在失权后落地控制操作。
var ErrControlLeaseLost = errors.New("control lease not held by the caller")

// controlLeaseLostErr 是 fencing 拒绝的定位化包装(sessionID + clientID 可观测;
// clientID 即原 connectionID,错误文案沿用「connection」措辞保持协议面兼容)。
func controlLeaseLostErr(sessionID, clientID string) error {
	return fmt.Errorf("agent: session %q: connection %q: %w", sessionID, clientID, ErrControlLeaseLost)
}

// SessionLease 是单会话的 control lease fencing guard(原 sessionControlGuard
// 迁入):mu 是本会话全部控制操作(lease 校验 + 提交)与接管的线性化点,holder
// 是当前持有者 clientID(空 = 空闲)。fencing 语义与迁移前逐字一致:校验与提交
// 同临界区,接管在锁内线性化——会话维度上「校验通过后到提交完成」不可能被
// 接管。lease 不持任何其他锁、临界区内不回调 handler,是锁序图中的叶节点。
//
// 不要为了「提升并发性能」改成先读 holder、锁外执行提交:那会把原来的竞态换
// 一种形式(总方案 §五「控制权的线性化规则」原文;fencing token 化需要底层
// 接受 Run 的位置做可靠校验才等价,V1 不做)。当前每 Session 一把控制锁已经
// 足够好。
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
// sessionID 取调用方寻址原串:lease 与控制操作/接管以同一寻址串对齐(ACP 面的
// attach/control 与 run/start|cancel|approval 传同一 sessionId);纯内存降级的
// 空串是合法租约键(CurrentSessionID() 为空串的会话句柄形态)。
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

// Acquire 获取 (sessionID, clientID) 的 control lease:该会话空闲 → 占用并返回
// false;他人持有 → 显式接管(置新 holder,返回 true 并 slog.Info 审计——
// 「权限检查必须在 Daemon 侧完成」,总方案 §7.4);同持有者重复 acquire → false
// (非接管)。空 clientID 拒绝(调用方句柄恒为非空随机 hex,防御空串与「空闲」
// 不可辨)。
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

// WithControl 是受 lease 保护的操作提交入口(fencing 提交入口,总方案 §五):
// 本会话 lease.mu 临界区内先校验 holder 再执行 fn——校验通过后到 fn 返回之间
// 不可能被接管,旧持有者的提交一律明确拒绝。fn 必须是短提交操作(一次校验 +
// 一次提交/接受,如 manager.StartRun 的同步段),绝不是整个 AgentRun;后台执行
// 仍在锁外完成。本方法只供控制层内部(Service)调用,不向协议层暴露任意长时间
// callback 的注入口。
func (r *LeaseRegistry) WithControl(sessionID, clientID string, fn func() error) error {
	l := r.lease(sessionID)
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.holds(clientID) {
		return controlLeaseLostErr(sessionID, clientID)
	}
	return fn()
}

// ReleaseConnection 释放 clientID 持有的全部会话的 control lease(断连收口,
// handler.Close 一次调用替代逐会话归还)。连接不持有任何租约时幂等空操作。
//
// 锁纪律(DE2-I1 原样):先在 registry.mu 下快照全部 lease,再逐个锁 lease.mu
// 清本 client 持有者——registry.mu 与 lease.mu 永不嵌套,某会话 lease 被慢盘
// 控制操作占住时不阻塞其他会话的清理,也不与并发控制操作成环。快照后新建的
// lease 不可能持本 client 租约(holder 只经 Acquire 置入,而快照覆盖此刻已存在
// 的全部会话键;handler 侧 Close 与 attach 提交段同以 h.mu 串行,断连窗口内无
// 新租约)。
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
