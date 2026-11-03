package service

import (
	"log/slog"
)

// lifecycle.go 是多 session 运行时的生命周期半边(Plan A2,定案需求 #2):
// 「runtime 生命周期 = 连接绑定引用」——任一连接绑定即活;最后解绑(断连/切换
// /new)→ 关 store + 注销 manager;有未完 run 时延到 run 终态再关(定案需求 #8
// 的「run 在飞仍算活」由 A3 消费,本文件只负责收口时点)。
//
// 引用的事实源:agentcontrol.BindingRegistry(Service 装配,bindings.go)记录
// clientID → 绑定 sessionID。登记点与 SessionBinding.Set 成对(ACP handler 的
// CreateSession/LoadSession 经 model.ConnectionBinder 断言面),摘除点有三处,
// 全部汇入 tryReleaseSession 的归零判定:
//   1. /new 收口(commands.go P5-5 块):摘发起方登记后评估旧会话;
//   2. 断连(runcontrol.go ReleaseConnection):清登记后评估被解除的会话;
//   3. 改绑(lifecycle.go BindConnectionSession):替换登记后评估被放弃的会话。
//
// 锁序:本文件不引入新锁面——suspendedReaps 台账自带小锁且是叶锁(只护 map,
// 不回调);tryReleaseSession 依次取 agentcontrol registry.mu(已释放)、manager.mu
// (Get/Remove,短临界区)、repo.mu/store.mu(CloseSession),全为单向短临界区,
// 与 withSession 的 gate、SessionLease.mu 无嵌套。run 终态回调
// (onSessionRunDone)跑在 manager execute goroutine 上(finalizeRun 触发点,
// ai-runtime/agent/runtime.go),此刻该 run 已终态、rt.active 已清、run.done 已
// 关——manager.Close 的等待面已放行,回调内做 store 关闭的文件 I/O 不阻塞任何
// 等待者。

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

// isSuspendedRelease 报告会话是否处于挂起收口台账(只读取数面,Plan A3):清单
// state=active 判定消费——挂起中(引用归零但 run 在飞)的会话 run 一旦终态即被
// 收口注销,若 state 只看 rt.Active() 会读到「终态已清、台账未摘」的瞬间窗口
// (onSessionRunDone 尚未执行),清单把一个即将消失的会话标成 idle 产生 UI 抖动
// (定案需求 #8 的反面);台账判定让该会话在收口发生前恒为 active。
func (p *AgentPlugin) isSuspendedRelease(sessionID string) bool {
	p.suspendMu.Lock()
	defer p.suspendMu.Unlock()
	_, ok := p.suspendedReaps[sessionID]
	return ok
}

// onSessionRunDone 是 manager.WithRunDoneHook 注入的 run 终态通知(Plan A2,
// A7 设计点 2 的完成通知面):仅唤醒挂起中的收口——重判在 tryReleaseSession 的
// 收口执行点做(原子复查,见其注释),本回调只做闸门,不在 execute goroutine 上
// 携带判定状态。
func (p *AgentPlugin) onSessionRunDone(sessionID string) {
	if !p.takeSuspendedRelease(sessionID) {
		return
	}
	p.tryReleaseSession(sessionID)
}

// BindConnectionSession 实现 model.ConnectionBinder(Plan A2 生命周期半边的
// 登记面;ACP handler 在 SessionBinding.Set 点经 type-assert 调用,工厂内断言,
// handler.go):登记 clientID → sessionID(改绑即替换),改绑时被放弃的会话失去
// 本连接的引用 → 立即评估收口。登记动作本身零副作用(§6.2 四条纪律);评估是
// 收口动作,只在引用真的变化时发生。control 未装配(测试直构插件)时跳过——
// 与 tryReleaseSession 的 nil 容忍同款。
func (p *AgentPlugin) BindConnectionSession(clientID, sessionID string) {
	if p.control == nil {
		return
	}
	prev := p.control.BindSession(clientID, sessionID)
	if prev != "" && prev != sessionID {
		p.tryReleaseSession(prev)
	}
}

// tryReleaseSession 评估 sessionID 的引用并尝试收口(Plan A2 生命周期半边的
// 唯一判定点,三处触发共用):
//
//	绑定集合空 && 无在飞 run → manager.Remove(id) + repo.CloseSession(id)
//	有 run 在飞             → 挂起,run 终态通知(onSessionRunDone)再判
//	绑定集合非空            → 保留(其他连接仍在用,旧会话可继续 prompt)
//
// 收口执行点的原子复查(挂起取消语义,Plan A2 实施注意点):绑定集合的读取
// 就在执行点完成——挂起期间若有新绑定到达(session/load 重开把连接绑回该会话,
// 经 BindConnectionSession 登记),run 终态通知触发的重判在这里读到非空集合,
// 挂起收口即被取消,绝不关闭仍有引用的会话。判定与执行(Remove/CloseSession)
// 之间不追求跨包的分布式原子——那段窗口的并发改绑落在「已判定收口」的会话上
// 时,该连接的下次解析走既有诚实失败语义(悬空 Binding → ErrSessionNotFound,
// PR2 契约),且 store 数据未被删除,重开路径(session/load → repo.Open)仍可
// 恢复;宁取诚实失败,不为它把 manager/repo 锁与 agentcontrol 注册表锁互取
// (锁序单向是硬边界,见文件头)。
//
// 收口次序:Remove 先于 CloseSession——Remove 后对旧 id 的 StartRun/路由立即
// 得 ErrSessionNotFound(诚实拒绝),不存在「store 已关但 runtime 仍可路由、
// run 往死 store 里写」的窗口;反序则两者之间到达的 run 会在 CloseSession 之后
// 才被 manager 拒绝,执行面撞已关句柄。Remove 后对捕获的 runtime 防御复查
// Active():StartRun 的同步段(manager.Get 命中 → rt.mu 占位)可能恰在「判定
// 无 run」与「Remove」之间插入——此刻 run 尚未获得 gate(/new 闭包持有旧
// runtime 的 gate;断连路径无人在跑),取消其 runCtx 让 execute 在 gate/执行
// 入口按取消收敛(finalizeRun 归一 cancelled),绝不留孤儿 run 对已收口会话
// 继续执行。cancel 为 nil(同步 RecordRun 形态,daemon 无生产调用方)只留痕。
//
// current 隐式引用守卫:p.current 指向的 runtime 恒有一个「无绑定调用方的
// 便利引用」(CLI/stdio/测试回落 current 语义,plugin.go current 注释)——它
// 不进 BindingRegistry(登记只服务 ACP 连接),但收口判定必须尊重,否则 CLI 的
// 活跃会话会被 run 终态通知或断连评估误收口。按 runtime 指针判等而非 id:纯
// 内存降级下新旧 runtime 同 id("current"),/new 收口要移除的正是同 id 的旧
// 条目(指针不同)——旧条目移除后其 store 是否可关由下文的 current store id
// 判定兜底(store 归 current 会话持有的同 id 重建窗口不关 store)。A3 收缩
// current 语义时本守卫随之重审。
//
// 幂等:id 不在 manager(Get 为 nil)即已收口或从未注册,直接返回——三处触发
// (含 /new 闭包里 UnbindSession 后的显式评估与挂起重判)重复到达安全。
// control/repo 为 nil(测试直构插件,零值容忍同 inFlightTracker)时:无登记
// 事实源按零绑定处理、无持久化跳过 store 收口。
func (p *AgentPlugin) tryReleaseSession(sessionID string) {
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
	if run := rt.Active(); run != nil {
		// 有未完 run:挂起,等 run 终态通知再判(定案需求 #8 的收口时点半边)。
		p.markSuspendedRelease(sessionID)
		return
	}
	p.clearSuspendedRelease(sessionID)

	p.manager.Remove(sessionID)
	// 事件枢纽收口（A7 设计点 3）：订阅随 runtime 注销诚实断开——hub.Close 关
	// 闭全部订阅通道，daemon 侧通知泵经「通道关闭 → 泵退出自清」收敛，订阅不
	// 泄漏、客户端经事件流停止 + session/list 状态变化 + attach 的
	// ErrSessionNotFound 可感知结束（不 panic、不静默悬挂）。重开路径
	// （session/load → mountRuntime）构造新 runtime 与新 hub，EnsureHub 经
	// journal.SeqRange 磁盘播种续接既有事件历史——Seq 不从零，重连按 afterSeq
	// 续放。次序：先关 hub 再关 store——hub 桥接（RunJournalStore）不实现
	// io.Closer，两步无交互，但 hub 若晚于 store 关闭，残余 Publish 会撞已关
	// 句柄（warn 噪音）；此刻不可能有在飞 Publish（挂起判定已排除在飞 run，
	// 本路径只在终态后到达）。
	rt.CloseEvents()
	if run := rt.Active(); run != nil {
		// 防御复查(Remove 前后的 queued-run 竞态窗口,论证见函数头)。
		if run.Cancel() {
			slog.Warn("agent: released session had a queued run, cancelled", "session", sessionID, "run", run.ID)
		} else {
			slog.Warn("agent: released session had an uncancellable queued run", "session", sessionID, "run", run.ID)
		}
	}
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
