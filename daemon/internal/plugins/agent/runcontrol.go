package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// runcontrol.go 是 RunControlSource(P5-6)的 AgentPlugin 实现面:x-goworker 扩展
// 协议(P5-7/P5-8 接线)消费的后台 Run 控制、审批直转、事件 attach 与 control
// lease。放在独立文件:plugin.go 只持有会话状态与生命周期,本文件是「后台 Run 的
// daemon 面」——两个入口(同步 /agent 与后台 run/start)靠 runtime gate 互斥
// (P2P3 §6,拍板⑦:同步面不切 StartRun)。
//
// 行为变化声明(P5-6):零——接口与实现新增,无协议暴露(StartRun 仍无前端调用
// 方,P5-7 才注册 x-goworker 方法);同步 RecordRun 路径完全不经此处。
//
// 并发与锁论证:
//   - reqMu 只护 requestId 幂等表(map + 环形逐出队列);临界区覆盖
//     manager.StartRun 同步段(μs~ms 级,含一次 journal fsync)——同会话第二
//     run 本就被 manager 的 ErrRunActive 串行,V1 单活动会话下并发 StartRun
//     成功本不可能,持锁串行的额外代价可忽略,换来同 requestID 并发重试的严格
//     幂等(第二个等待者持锁后命中表,不再撞 ErrRunActive);
//   - leaseMu 只护 controlLease 单字符串;不与 mu/reqMu/manager 锁嵌套,无锁环;
//   - 其余方法(manager.StartRun/CancelRun、rt.Active/Hub、Approvals、
//     Capabilities)的并发面由 SDK 侧既有锁序承担,本层不做新共享状态。

// RunControlSource 编译期断言(P5-6):AgentPlugin 满足可选能力接口,handler
// 以 type-assert 获取(SessionSource 先例,capability.go:61-78)。
var _ model.RunControlSource = (*AgentPlugin)(nil)

// ClientToolBinder 编译期断言(P5-9):AgentPlugin 同时满足设备工具的能力代理
// 面,ACP handler 优先 type-assert 它走 CapabilityRegistry + proxy(P5 显式
// 可见变化 #1 的生产者),断言失败回落原始 relay。
var _ model.ClientToolBinder = (*AgentPlugin)(nil)

// maxTrackedRequestIDs 是 requestId 幂等表的容量上限(V1 进程内存量,环形逐出
// 防无界;审批 requestID 与 run/start requestID 是两个命名空间——§6 风险 6,
// 审批表在 SDK 的 ApprovalManager,互不共享)。
const maxTrackedRequestIDs = 4096

// defaultCapabilityWaitTimeout 是 CapabilityWaitTimeout 的兜底值(拍板③ 45s;
// 与 SDK 的 defaultCapabilityWaitTimeout 同值——那边未导出,daemon 侧按配置
// 秒化后 <=0 回落此处,双保险不依赖 yaml 载入路径)。
const defaultCapabilityWaitTimeout = 45 * time.Second

// requireCurrentSession 校验 sessionID 是当前会话(V1 路由面,拍板⑤):run/start|
// status|cancel、attach 与 approval 全部限 current,归档历史走既有 session/load
// 只读重放。不匹配 → 明确错误(/new 收口后对旧 id 的调用在此诚实失败,§6 风险 7)。
// 纯内存降级(无 store)下 CurrentSessionID() 为空串,调用方传空串即匹配——降级
// 形态的会话句柄本就是空,与 CurrentSessionID 的既有语义一致。
func (p *AgentPlugin) requireCurrentSession(sessionID string) error {
	if current := p.CurrentSessionID(); sessionID != current {
		return fmt.Errorf("agent: session %q is not the current session %q (V1 routes the current session only)", sessionID, current)
	}
	return nil
}

// noActiveSessionErr 是无活动 runtime(Init 未完成/失败回滚)时的统一错误。
func noActiveSessionErr() error {
	return fmt.Errorf("agent: no active session")
}

// registerCurrentRuntime 修复「current runtime 已注册」不变量(P5-6;/new 后的
// 注册表形态处理,P5-5 报备):纯内存降级下 /new 的新旧 runtime 同 id,startSession
// 的 Register 撞旧条目保留旧值,handleNew 尾部 Remove 移除的正是旧条目——新
// runtime 此刻不在册(Get == nil);经同 id 重建的路径(如测试 fixture 的
// refreshSession)则可能有同 id 旧条目占位(Get != rt)。两者都违反不变量,
// Remove+Register 幂等重建。Register 的错误不在此放大:唯一可达失败是
// ErrManagerClosed,后续 manager.StartRun 会给出同一权威错误。
func (p *AgentPlugin) registerCurrentRuntime(rt *runtimeagent.SessionRuntime) {
	if got := p.manager.Get(rt.ID); got != rt {
		p.manager.Remove(rt.ID)
		_ = p.manager.Register(rt)
	}
}

// runJournalStore 返回当前会话的 Run journal 聚合实例:从 p.deps.RunEvents
// type-assert(P5-4 起 RunEvents 与 RunJournal 注入同一个 *session.RunJournalStore
// 实例,runjournal_wiring_test.go 编译期+运行期钉死)。这是「最简形态」取舍:
// p.deps 在每个会话边界(startSession)全量重建,恒与 current store 同源,无需
// 经 repo+currentStore 重建第二个实例(那会绕开句柄缓存的单例语义)。nil =
// 纯内存/持久化禁用(typed-nil 纪律,spec.go:96),调用方按 run-not-found 处理。
func (p *AgentPlugin) runJournalStore() *session.RunJournalStore {
	if js, ok := p.deps.RunEvents.(*session.RunJournalStore); ok {
		return js
	}
	return nil
}

// StartRun 实现 RunControlSource(P5-6):校验 current 会话 → 幂等表命中短路 →
// 注册表不变量修复 → 空回调后台提交。
//
//   - 幂等:requestID 非空时查表,命中返回原 runID 与其当前状态(RunHandle.State()
//     的实时快照,终态保留),不重复起跑;空 requestID 不记账(幂等键缺失,每次
//     新起跑)。命中与记录同持 reqMu,并发同 requestID 重试严格幂等(见文件头)。
//   - 回调全 nil(runtimeagent.RunCallbacks{}):tee 只包装非 nil 入口
//     (supervisor.go teeRunCallbacks 的 nil 检查)、streamTokens 对 nil 回调有
//     既有检查(session.go)——后台 Run 的输出只进事件流/journal,不进任何终端;
//     Decide 留 nil → P4-2 的审批回退接管(decideFunc → ApprovalManager.Wait),
//     决策经 ResolveApproval 跨连接投递。
//   - 返回的 state 是提交时快照(manager.StartRun 同步段落盘 queued 记录后
//     durable-first 返回,恒为 queued)。
func (p *AgentPlugin) StartRun(sessionID, input, requestID string) (string, string, error) {
	if err := p.requireCurrentSession(sessionID); err != nil {
		return "", "", err
	}
	rt := p.currentSession()
	if rt == nil {
		return "", "", noActiveSessionErr()
	}

	p.reqMu.Lock()
	defer p.reqMu.Unlock()

	if requestID != "" {
		if h, ok := p.runRequests[requestID]; ok {
			return h.RunID, string(h.State()), nil
		}
	}

	p.registerCurrentRuntime(rt)
	h, err := p.manager.StartRun(rt.ID, runtimeagent.RunRequest{Input: input}, runtimeagent.RunCallbacks{})
	if err != nil {
		return "", "", err
	}
	if requestID != "" {
		p.recordRequestID(requestID, h)
	}
	return h.RunID, string(h.State()), nil
}

// recordRequestID 记录一笔 requestId → RunHandle 并按容量环形逐出最老条目
// (调用方必须持 reqMu)。同 requestID 重复记录只刷新句柄、不重复入队(幂等表
// 的保守自愈;正常路径 put 前已查表,不可达)。
func (p *AgentPlugin) recordRequestID(requestID string, h runtimeagent.RunHandle) {
	if p.runRequests == nil {
		p.runRequests = make(map[string]runtimeagent.RunHandle)
	}
	if _, ok := p.runRequests[requestID]; !ok {
		p.requestOrder = append(p.requestOrder, requestID)
		for len(p.requestOrder) > maxTrackedRequestIDs {
			oldest := p.requestOrder[0]
			delete(p.runRequests, oldest)
			// 环形逐出:copy-shift O(容量),仅在表满后的每次插入发生——
			// 4096 条的量级下代价可忽略,换无别名滚动的简单性。
			copy(p.requestOrder, p.requestOrder[1:])
			p.requestOrder = p.requestOrder[:len(p.requestOrder)-1]
		}
	}
	p.runRequests[requestID] = h
}

// lookupTrackedRun 在幂等表中按 runID 反查(调用方必须持 reqMu):纯内存宿主
// (无 journal)下本表是进程内唯一 run 观测面,RunStatus 的兜底回退。持久化模式
// 不走此处(journal 权威,覆盖包括无 requestID 同步 run 在内的全部 run)。
func (p *AgentPlugin) lookupTrackedRun(runID string) (runtimeagent.RunHandle, bool) {
	for _, h := range p.runRequests {
		if h.RunID == runID {
			return h, true
		}
	}
	return runtimeagent.RunHandle{}, false
}

// CancelRun 实现 RunControlSource(P5-6):校验 current 会话 → 不变量修复 →
// 直转 manager。manager 的拒绝语义原样上抛:未知 run → ErrRunNotFound;同步
// RecordRun 路径的 run 无 cancel → ErrRunNotCancellable(其取消入口是调用方
// 请求 ctx,P2-6 契约)。不变量修复的必要性:同步 run 活跃在未注册的 current
// runtime 上时(纯内存 /new 后),不经修复会被误报 ErrRunNotFound。
func (p *AgentPlugin) CancelRun(sessionID, runID string) error {
	if err := p.requireCurrentSession(sessionID); err != nil {
		return err
	}
	rt := p.currentSession()
	if rt == nil {
		return noActiveSessionErr()
	}
	p.registerCurrentRuntime(rt)
	return p.manager.CancelRun(rt.ID, runID)
}

// RunStatus 实现 RunControlSource(P5-6):run 当前状态 + 会话事件流 latest seq。
// 观测面三分支,按权威序:
//  1. rt.Active() 命中 runID(在跑/排队)→ AgentRun.State() 实时快照;latestSeq
//     取 hub.LatestSeq()(会话维度游标,P3-0 契约);
//  2. RunJournalStore.LastRunState(已终态,active 已清):journal 权威,读
//     runs/<runID>.jsonl 末条 state 记录;latestSeq 取 SeqRange 的磁盘 latest
//     (hub 可能尚未经 StartRun 建立——只有同步 run 的会话即此形态);
//  3. 幂等表反查(仅纯内存):无 journal 时本插件经 StartRun 记账的 RunHandle
//     是唯一观测面;持久化模式不回退——journal 覆盖本会话全部 run,miss 即
//     未找到,不发明第二事实源。
//
// 未命中 → 包裹 runtimeagent.ErrRunNotFound 的错误(P5-7 协议面映射 -32000)。
func (p *AgentPlugin) RunStatus(sessionID, runID string) (string, uint64, error) {
	if err := p.requireCurrentSession(sessionID); err != nil {
		return "", 0, err
	}
	rt := p.currentSession()
	if rt == nil {
		return "", 0, noActiveSessionErr()
	}

	if run := rt.Active(); run != nil && run.ID == runID {
		latest := uint64(0)
		if hub := rt.Hub(); hub != nil {
			latest = hub.LatestSeq()
		}
		return string(run.State()), latest, nil
	}

	if js := p.runJournalStore(); js != nil {
		rec, ok := js.LastRunState(runID)
		if !ok {
			return "", 0, fmt.Errorf("agent: run %q: %w", runID, runtimeagent.ErrRunNotFound)
		}
		_, latest, _ := js.SeqRange()
		return rec.State, latest, nil
	}

	p.reqMu.Lock()
	h, ok := p.lookupTrackedRun(runID)
	p.reqMu.Unlock()
	if ok {
		latest := uint64(0)
		if hub := rt.Hub(); hub != nil {
			latest = hub.LatestSeq()
		}
		return string(h.State()), latest, nil
	}
	return "", 0, fmt.Errorf("agent: run %q: %w", runID, runtimeagent.ErrRunNotFound)
}

// PendingApprovals 实现 RunControlSource(P5-6):校验 current 会话 → manager
// 审批快照 → 扁平化为 model.PendingApprovalInfo。内部寻址用 rt.ID 而非调用方
// sessionID:审批的 SessionID 归属是 StartRun 传入的 runtime id(纯内存下为
// "current" 而 CurrentSessionID() 为空串),按 rt.ID 寻址在两种形态下都正确;
// 调用方 sessionID 已由 requireCurrentSession 校验。未知会话/无审批返回空切片
// (manager.PendingApprovals 语义)。
func (p *AgentPlugin) PendingApprovals(sessionID string) ([]model.PendingApprovalInfo, error) {
	if err := p.requireCurrentSession(sessionID); err != nil {
		return nil, err
	}
	rt := p.currentSession()
	if rt == nil {
		return nil, noActiveSessionErr()
	}
	return flattenPendingApprovals(p.manager.PendingApprovals(rt.ID)), nil
}

// flattenPendingApprovals 把 agent 包的 PendingApproval 快照压成 daemon 侧扁平
// 结构(wire 最小面,见 model.PendingApprovalInfo 注释;manager 已按 RequestID
// 排序,顺序保留)。
func flattenPendingApprovals(pending []runtimeagent.PendingApproval) []model.PendingApprovalInfo {
	out := make([]model.PendingApprovalInfo, 0, len(pending))
	for _, pa := range pending {
		out = append(out, model.PendingApprovalInfo{
			RequestID: pa.RequestID,
			SessionID: pa.SessionID,
			RunID:     pa.RunID,
			ToolName:  pa.Request.ToolName,
			Command:   pa.Request.Command,
			RiskLevel: pa.Request.RiskLevel,
			ExpiresAt: pa.Request.ExpiresAt,
		})
	}
	return out
}

// ResolveApproval 实现 RunControlSource(P5-6):直转 manager(ApprovalManager.
// Resolve 的直转包装)。错误原样上抛:未知 requestID → ErrApprovalUnknown;
// 迟到/重复决策 → ErrApprovalResolved(首个有效决策获胜,P4-1 语义)。
// requestID 全局寻址(审批归属 SessionID+RunID 非连接),无需 current 校验。
func (p *AgentPlugin) ResolveApproval(requestID string, decision hitl.Decision) error {
	return p.manager.ResolveApproval(requestID, decision)
}

// Attach 实现 RunControlSource(P5-6,P5-8 的 attach 面消费):校验 current
// 会话 → rt.EnsureHub()——遗留③「重启后 attach 时 hub 未建」的解法,attach
// 即建(含 SeqRange 磁盘播种,游标续接既有历史);RunEvents 未接线/纯内存 →
// 诚实错误(P5-8 起映射 -32000,客户端指引 session/load)。latest 是当前事件
// 流位置:仅描述,客户端不得跳游标(总方案 §7 :586-589,P5-10 写入 PROTOCOL.md)。
func (p *AgentPlugin) Attach(sessionID string) (*runtimeagent.EventHub, uint64, error) {
	if err := p.requireCurrentSession(sessionID); err != nil {
		return nil, 0, err
	}
	rt := p.currentSession()
	if rt == nil {
		return nil, 0, noActiveSessionErr()
	}
	hub := rt.EnsureHub()
	if hub == nil {
		return nil, 0, fmt.Errorf("agent: event stream not enabled for session %q (persistence disabled)", sessionID)
	}
	return hub, hub.LatestSeq(), nil
}

// ActiveRun 实现 RunControlSource(P5-8):current 会话的 rt.Active() 快照直转,
// 与 RunStatus 的 active 分支同源(同一观察点)。语义:非 current 会话/无
// runtime/无活动 run → ok=false——attach 响应的 currentRun=null 不是错误
// (Attach 才是会话路由的权威校验面,本访问器只做快照观察)。
func (p *AgentPlugin) ActiveRun(sessionID string) (string, string, bool) {
	if err := p.requireCurrentSession(sessionID); err != nil {
		return "", "", false
	}
	rt := p.currentSession()
	if rt == nil {
		return "", "", false
	}
	if run := rt.Active(); run != nil {
		return run.ID, string(run.State()), true
	}
	return "", "", false
}

// AcquireControl 实现 RunControlSource 的 control lease(P5-6,V1 宿主授权简化
// 形,拍板②):空闲 → 占用并返回 false;他人持有 → 显式接管(置新 holder,
// 返回 true 并 slog.Info 审计——「权限检查必须在 Daemon 侧完成」,总方案 §7.4
// :610);同持有者重复 acquire → false(非接管)。空 connectionID 拒绝(P5-7
// 工厂恒发非空 crypto/rand hex;防御空串与「空闲」不可辨)。
func (p *AgentPlugin) AcquireControl(connectionID string) bool {
	if connectionID == "" {
		return false
	}
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	if p.controlLease == connectionID {
		return false
	}
	tookOver := p.controlLease != ""
	if tookOver {
		slog.Info("x-goworker: control lease taken over", "previous", p.controlLease, "new", connectionID)
	}
	p.controlLease = connectionID
	return tookOver
}

// ReleaseControl 实现 RunControlSource(P5-6):仅持有者本人有效,他人调用为
// 空操作(不抢、不误清)。空 connectionID 不可能成为 holder(Acquire 已拒)。
func (p *AgentPlugin) ReleaseControl(connectionID string) {
	if connectionID == "" {
		return
	}
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	if p.controlLease == connectionID {
		p.controlLease = ""
	}
}

// HoldControl 实现 RunControlSource(P5-6):connectionID 当前是否持有 lease。
func (p *AgentPlugin) HoldControl(connectionID string) bool {
	if connectionID == "" {
		return false
	}
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	return p.controlLease == connectionID
}

// UnbindCapabilities 实现 RunControlSource(P5-6):直转 manager 能力注册表的
// 连接级解绑(P4-3 的 UnbindConnection;P5-9 起 handler.Close 断连收口消费)。
// 连接不存在时幂等空操作。
func (p *AgentPlugin) UnbindCapabilities(connectionID string) {
	p.manager.Capabilities().UnbindConnection(connectionID)
}

// CapabilityWaitTimeout 实现 RunControlSource(P5-6):cfg.Session.
// CapabilityWaitTimeout(秒,P4-4 字段)秒化;<=0 → 45s 兜底(拍板③,硬约束
// < ai-core 工具执行 60s——SDK defaultCapabilityWaitTimeout 的同值镜像)。
func (p *AgentPlugin) CapabilityWaitTimeout() time.Duration {
	secs := 0
	if p.cfg != nil {
		secs = p.cfg.Session.CapabilityWaitTimeout
	}
	if secs <= 0 {
		return defaultCapabilityWaitTimeout
	}
	return time.Duration(secs) * time.Second
}

// ─────────────────────────────────────────────────────────────────────────────
// ClientToolBinder(P5-9):设备工具的能力代理面(总方案 §8 的 daemon 接线,
// P5 显式可见变化 #1)。stdio worker 路径(acpworker 自持原始 relay)不经此处。
// ─────────────────────────────────────────────────────────────────────────────

// deviceToolExecutor 把 SDK 的 ClientToolExecutor 收窄面适配到 x-device/call:
// Execute 收到的 toolName 是注册名(sys_ 前缀,与原始中继的 core.Tool.Name 同
// 名),CallDeviceTool 统一剥前缀转发客户端原名(wire 形状单一事实源,
// sys_relay.go CallDeviceTool)。Invoke 失败由代理 Execute 即时 Invalidate 该
// 绑定并进入等待重绑(P4-4 语义),绝不静默路由到其他连接。
type deviceToolExecutor struct {
	caller model.DeviceCaller
}

func (e deviceToolExecutor) Invoke(ctx context.Context, toolName string, argsJSON []byte) (string, error) {
	return acpworker.CallDeviceTool(ctx, e.caller, toolName, argsJSON)
}

// ClientToolSource 实现 model.ClientToolBinder(P5-9):返回本连接的设备工具惰性
// 源。调用时机 = 每轮 prompt 工具收集(勘察结论 D),closure 内:
//
//   - 探测(acpworker.SpecDeviceTools,ProbeTimeout 2s 自计时)失败/空 → nil:
//     现状收缩语义「无能力即无工具」,不登记任何绑定(方向必须是收缩不是放行);
//   - 成功 → manager.Capabilities().Bind(会话 id, connectionID, 注册名,
//     ttl=2*CapabilityWaitTimeout, deviceToolExecutor{caller}) →
//     runtimeagent.NewClientToolProxy(reg, sessionID, specs, waitTimeout)。
//
// 键与预算(plan P5-9 表):
//   - 会话 id 用 rt.ID:持久化模式它与 currentStore().SessionID() 同源
//     (startSession/refreshSession 的会话边界以 store id 注册 runtime),纯内存
//     降级为 "current" 单例——Bind/Lookup 同键,不引入第二会话标识;
//   - ttl = 2*CapabilityWaitTimeout:等待预算内绑定不得被 TTL 判过期(过期条目
//     视同无绑定,capability.go lazy expiry),2 倍留余量;
//   - 每次 prompt 探测成功都重新 Bind:幂等刷新 TTL + 工具集(P4-3 的「每次
//     prompt 探测都会 Bind」契约)。
//
// 断连语义(P5 显式可见变化 #1):handler.Close → UnbindCapabilities(connID)
// 连接退出即解绑;在途的设备工具调用由 P4-4 的「无绑定/Invoke 失败 → 等待重绑
// (默认 45s)→ 明确失败文本或重绑成功」接管;未声明 x-device 的客户端(VS Code
// 等)不进此路径,零变化;沙箱/HITL 门控面零变化(spec.Metadata 透传,重绑后
// 照常 per-call 裁决)。无 runtime(Init 未完成/失败回滚)→ nil。
func (p *AgentPlugin) ClientToolSource(connectionID string, caller model.DeviceCaller) func(context.Context) []core.Tool {
	return func(_ context.Context) []core.Tool {
		specs, err := acpworker.SpecDeviceTools(caller)
		if err != nil || len(specs) == 0 {
			return nil
		}
		rt := p.currentSession()
		if rt == nil {
			return nil
		}
		sessionID := rt.ID
		wait := p.CapabilityWaitTimeout()
		names := make([]string, 0, len(specs))
		for _, spec := range specs {
			names = append(names, spec.Name)
		}
		reg := p.manager.Capabilities()
		reg.Bind(sessionID, connectionID, names, 2*wait, deviceToolExecutor{caller: caller})
		return runtimeagent.NewClientToolProxy(reg, sessionID, specs, wait)
	}
}
