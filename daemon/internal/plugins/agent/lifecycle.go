package service

import (
	"log/slog"
)

// 会话绑定与收口流程见 ../../../docs/README.md。
// 生命周期操作由 lifecycleMu 串行化；Retire 与 Remove 不嵌套持锁。

// markSuspendedRelease 登记一个「引用已归零但 run 在飞」的挂起收口。幂等。
func (p *AgentPlugin) markSuspendedRelease(sessionID string) {
	p.suspendMu.Lock()
	if p.suspendedReaps == nil {
		p.suspendedReaps = make(map[string]struct{})
	}
	p.suspendedReaps[sessionID] = struct{}{}
	p.suspendMu.Unlock()
}

// clearSuspendedRelease 摘除挂起登记(引用重新非零/已收口/守卫跳过时调用)。
// 幂等;零值 map 容忍(测试直构插件)。
func (p *AgentPlugin) clearSuspendedRelease(sessionID string) {
	p.suspendMu.Lock()
	delete(p.suspendedReaps, sessionID)
	p.suspendMu.Unlock()
}

// takeSuspendedRelease 摘除并报告挂起登记(run 终态通知的消费闸门):只有确实
// 挂起过的会话才需要终态后的重判——否则每次 run 收尾都会对「零绑定的 current
// 会话」(CLI 形态)做归零判定,误伤仅靠 current 指针持引用的活跃会话。
func (p *AgentPlugin) takeSuspendedRelease(sessionID string) bool {
	p.suspendMu.Lock()
	defer p.suspendMu.Unlock()
	if _, ok := p.suspendedReaps[sessionID]; !ok {
		return false
	}
	delete(p.suspendedReaps, sessionID)
	return true
}

// isSuspendedRelease reports a reference-zero session waiting for its active run to finish.
// Listing treats it as active until terminal handling reaps it, avoiding an idle flicker
// between run completion and the suspended-release recheck.
func (p *AgentPlugin) isSuspendedRelease(sessionID string) bool {
	p.suspendMu.Lock()
	defer p.suspendMu.Unlock()
	_, ok := p.suspendedReaps[sessionID]
	return ok
}

// onSessionRunDone 是 manager.WithRunDoneHook 注入的 run 终态通知(完成
// 通知面):仅唤醒挂起中的收口——重判在 tryReleaseSession 的
// 收口执行点做(原子复查,见其注释),本回调只做闸门,不在 execute goroutine 上
// 携带判定状态。
func (p *AgentPlugin) onSessionRunDone(sessionID string) {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if !p.takeSuspendedRelease(sessionID) {
		return
	}
	p.tryReleaseSessionLocked(sessionID)
}

// BindConnectionSession records the connection's selected session and rechecks its previous
// session for release. It is serialized by lifecycleMu and is a no-op without control.
func (p *AgentPlugin) BindConnectionSession(clientID, sessionID string) {
	if p.control == nil {
		return
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	p.bindConnectionSessionLocked(clientID, sessionID)
}

// BindConnectionSessionGuarded linearizes a connection binding against disconnect.
// allowBind must be a non-blocking state check and must not acquire lifecycle locks.
func (p *AgentPlugin) BindConnectionSessionGuarded(clientID, sessionID string, allowBind func() bool) bool {
	if p.control == nil {
		return false
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if allowBind == nil || !allowBind() {
		p.tryReleaseSessionLocked(sessionID)
		return false
	}
	p.bindConnectionSessionLocked(clientID, sessionID)
	return true
}

func (p *AgentPlugin) bindConnectionSessionLocked(clientID, sessionID string) {
	prev := p.control.BindSession(clientID, sessionID)
	if prev != "" && prev != sessionID {
		p.tryReleaseSessionLocked(prev)
	}
}

// tryReleaseSession evaluates release after unbinding or run completion. Bindings and current
// references retain it; active runs suspend release until terminal notification, while a new
// binding cancels release. Retire, Remove, CloseEvents, and CloseSession share admission with
// run reservation so a retired runtime cannot receive a run as its store closes.
func (p *AgentPlugin) tryReleaseSession(sessionID string) {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	p.tryReleaseSessionLocked(sessionID)
}

func (p *AgentPlugin) tryReleaseSessionLocked(sessionID string) {
	if sessionID == "" {
		return
	}
	if p.control != nil && len(p.control.SessionBindings(sessionID)) > 0 {
		// 绑定集合非空:保留,并撤销可能存在的挂起(新引用到达,取消收口)。
		p.clearSuspendedRelease(sessionID)
		return
	}
	rt := p.manager.Get(sessionID)
	if rt == nil {
		p.clearSuspendedRelease(sessionID)
		return
	}
	if cur := p.currentSession(); cur == rt {
		// current 隐式引用守卫(见函数头):便利指针仍是引用。
		p.clearSuspendedRelease(sessionID)
		return
	}
	if err := rt.Retire(); err != nil {
		// Active Retire means ErrRunActive, so suspend until the terminal-run notification.
		// Retire and reserveRun share one admission point: after retirement, a caller that
		// obtained the runtime before reserving fails with ErrSessionRetired and cannot reach
		// the closing hub or store.
		p.markSuspendedRelease(sessionID)
		return
	}
	p.clearSuspendedRelease(sessionID)

	p.manager.Remove(sessionID)
	// Close events before the store so no late publisher reaches a closed journal.
	rt.CloseEvents()
	// store 收口:同 id 重建窗口(纯内存 /new 的 Register 冲突、会话边界的同 id
	// 重建——manager 里同 id 的旧条目是误路由隐患,上面已移除)下,该 id 可能仍
	// 是 current 会话的 store id——store 归 current 持有,不能随旧条目一起关。
	// 持久化 /new 的常规路径 current 已换新 id,判定不命中,正常收口。
	if p.repo != nil && p.CurrentSessionID() != sessionID {
		if err := p.repo.CloseSession(sessionID); err != nil {
			slog.Warn("agent: released session store close failed", "session", sessionID, "err", err)
		}
	}
}
