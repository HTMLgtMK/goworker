package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// 本文件是多会话执行内核的注册表:SessionManager 按 SessionID 管理
// SessionRuntime,并持有宿主生命周期 ctx(P2-4)——StartRun(P2-5)的 runCtx
// 以它为父,Close = 取消宿主 ctx + 等待在途 Run 终态 + 拒绝后续 Register。
// 全局并发配额(runSlots)仍属 P2「有界全局并发调度」(P2-5),本结构预留。

// ErrManagerClosed 是 manager 停机后的注册拒绝 sentinel:Close 之后不再接受
// 新 runtime(同步 Register 与 P2-5 的 StartRun 共用同一拒绝语义)。
var ErrManagerClosed = errors.New("session: manager closed")

// SessionManager 是 SessionRuntime 的进程内注册表:一个 SessionID 至多一个
// runtime,互斥执行权由各 runtime 自持的 gate 保证,manager 不参与排队。
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
}

// NewSessionManager 创建空注册表,并内建宿主生命周期 ctx。签名保持无参:
// 调用点(daemon plugin 等)零改动,runSlots/配额选项在 P2-5 变参化时接入。
func NewSessionManager() *SessionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &SessionManager{
		runtimes: make(map[string]*SessionRuntime),
		ctx:      ctx,
		cancel:   cancel,
	}
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

// contextOf 返回宿主生命周期 ctx(包内访问器):StartRun(P2-5)以它为父派生
// runCtx——Run 的生命周期归宿主而非单个请求,连接断开不影响后台 Run。不导出:
// P2 无包外消费者(导出面留给 P5 前端接入时再定)。
func (m *SessionManager) contextOf() context.Context {
	return m.ctx
}

// Close 是总方案 P2 第 4 条的 Shutdown:取消宿主生命周期 ctx → 等待全部在途
// Run 终态 → 拒绝后续 Register(ErrManagerClosed)→ 清空注册表。幂等。
//
// 语义边界:
//   - 同步路径(RecordRun)的 fn 用调用方 ctx,与 m.ctx 无父子关系,Close 的
//     cancel 不影响在途同步 prompt——它们的取消仍由请求侧 ctx(连接断开/
//     session cancel)语义管理,不经 manager;
//   - 等待只盯 run.done(由 transition 在 run.mu 内关闭):不持 m.mu 也不持
//     rt.mu 等待,Get/Remove/List 与记账推进在等待期间照常工作,无锁环;
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

	m.mu.Lock()
	m.runtimes = make(map[string]*SessionRuntime)
	m.mu.Unlock()
}
