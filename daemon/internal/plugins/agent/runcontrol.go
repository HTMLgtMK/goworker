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
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// runcontrol.go 是 AgentPlugin 的控制面适配文件（PR5 重构后职责收口）：x-goworker
// 扩展协议（P5-7/P5-8 接线）消费的 model.RunControlSource/ClientToolBinder 能力
// 面在这里实现为对 agentcontrol.Service（plugin.go 装配，PR1 建立的控制平面）的
// 薄委托 + wire 扁平化。执行编排（Lease fencing、(sessionID, requestID) 幂等
// per-key single-flight、审批归属校验、事件订阅编排）全部在 agentcontrol 包；
// 本文件不再维护任何 Lease 表、幂等表或事件订阅状态（原 reqMu/runRequests/
// requestOrder/guardsMu/controlGuards 字段与 sessionControlGuard 实现随 PR1
// 迁入 agentcontrol；Attach 具体句柄形态随 PR3 收敛为 Service.SubscribeEvents
// 的订阅契约）。
//
// 为什么这些面留在插件而不进 agentcontrol（总方案 §三/§八 依赖方向）：
//   - RouteSession/RunJournal：SessionRouter/JournalSource 窄接口的宿主实现——
//     Service 经此取 current 权威路由与被路由会话的 journal，会话寻址语义属于
//     调用入口，Service 不持有会话选择状态；
//   - PendingApprovals/ActiveRun：wire 扁平化面——把 runtime 观测结构压成
//     model.PendingApprovalInfo / agentcontrol.RunSnapshot，agent 包结构不进
//     model，agentcontrol 也不得 import core/model（constraint_test.go 钉死）；
//   - UnbindCapabilities/CapabilityWaitTimeout/ClientToolSource：CapabilityRegistry
//     是 Runtime 执行过程的外部能力桥接（§八「能力桥接不进控制层」），只由
//     控制层协调授权、不归其实现。
//
// 锁序：本文件不持有任何锁（委托面无状态；跨层锁序 lease.mu→registry.mu→
// manager 单向、h.mu→session lease 单向，论证随方法迁入 agentcontrol）。
//
// 行为契约（PR1 起委托面以下逐字保留）：错误 sentinel
// (ErrControlLeaseLost/ErrRequestConflict/ErrApprovalOwnership)与错误文案原样
// 在 agentcontrol 产生，ACP 协议面的 -32000 映射零感知；幂等注册表的 per-key
// single-flight（§4.3）是唯一计划内升级。RecordRun（ai-runtime 的同步记账形态）
// 在 daemon 无生产调用方（PR4 起 prompt 走 StartRunSync），仅存于 SDK 与测试。

// RunControlSource 编译期断言(P5-6):AgentPlugin 满足可选能力接口,handler
// 以 type-assert 获取(SessionSource 先例,capability.go:61-78)。
var _ model.RunControlSource = (*AgentPlugin)(nil)

// InternalRunner 编译期断言(第四轮评审 R3):AgentPlugin 同时实现同步宿主入口的
// 受限方法集——StartRunSync/CancelRunSync 只作为 API surface isolation，减少把无
// fencing 的同步入口误带进公共 RunControlSource 的机会。ACP handler 当前只消费
// model.RunControlSource，也没有调用 Sync 方法；但 Go 的动态 type assertion 或
// 具体类型引用仍可触达这些方法，所以这不是运行时授权边界。协议控制面的真正
// 授权仍由 agentcontrol.Service 的 Control Lease 与 fencing 保证。此处与
// agentcontrol.runs.go 的正向断言只钉住方法集和依赖方向，不能替代运行时授权。
var _ agentcontrol.InternalRunner = (*AgentPlugin)(nil)

// ClientToolBinder 编译期断言(P5-9):AgentPlugin 同时满足设备工具的能力代理
// 面,ACP handler 优先 type-assert 它走 CapabilityRegistry + proxy(P5 显式
// 可见变化 #1 的生产者),断言失败回落原始 relay。
var _ model.ClientToolBinder = (*AgentPlugin)(nil)

// defaultCapabilityWaitTimeout 是 CapabilityWaitTimeout 的兜底值(拍板③ 45s;
// 与 SDK 的 defaultCapabilityWaitTimeout 同值——那边未导出,daemon 侧按配置
// 秒化后 <=0 回落此处,双保险不依赖 yaml 载入路径)。
const defaultCapabilityWaitTimeout = 45 * time.Second

// maxCapabilityWaitTimeout 是 CapabilityWaitTimeout 的安全钳制上限(P4 评审
// issue2,与 SDK 侧 resolveCapabilityWait 的钳制同型:硬约束 < ai-core 工具
// 执行 60s——55s 给 Invoke 留余量;超配的等待必被 toolCtx 掐断,「等待重绑→
// 明确失败文本」语义无声劣化,故显式降级 + warn)。
const maxCapabilityWaitTimeout = 55 * time.Second

// RouteSession 实现 agentcontrol.SessionRouter:统一解析点(C3 修复后的多会话
// 路由)原样暴露给 Service——current 优先 + manager.Get 注册表权威。Service 不
// 持有会话选择状态(会话选择属于调用入口,总方案 §六),寻址语义由本插件的
// 统一解析点承担。
//
// 解析序:
//  1. current 优先:sessionID 命中当前会话的身份(runtime id,含纯内存降级的
//     空串形态)→ 取 current runtime——同 id 重建窗口(纯内存 /new、测试
//     fixture 的 refreshSession)下注册表可能仍挂旧条目,current 才是权威路由
//     目标,ensureRegistered 随后把注册表拉齐。持久化模式下空串不匹配
//     任何 store id → 落到第 2 步 → nil(诚实失败,不把「未声明」静默当成
//     current)。
//  2. manager.Get(sessionID):注册表权威,覆盖非 current 的 runtime(/new 后
//     保留在册的旧 runtime、多会话注册)。
func (p *AgentPlugin) RouteSession(sessionID string) *runtimeagent.SessionRuntime {
	if cur := p.currentSession(); cur != nil {
		if sessionID == cur.ID || (sessionID == "" && p.CurrentSessionID() == "") {
			return cur
		}
	}
	return p.manager.Get(sessionID)
}

// RunJournal 实现 agentcontrol.JournalSource:返回 rt 会话的 Run journal 只读
// 查询面(RunStatus 的 journal 权威分支必须用被路由会话自己的 journal——p.deps
// 是当前会话边界重建的 deps,对跨会话路由的 runtime 是错误事实源)。解析:
//   - rt 即 current runtime → p.runJournalStore()(deps.RunEvents type-assert,
//     与该会话同源的单例,写面句柄缓存单例语义不受影响);
//   - 跨会话路由的 runtime → repo.Open(rt.ID) 命中实例唯一缓存(会话边界打开
//     的 store 在 Stop 前常驻缓存,plugin.go),构造只读查询实例——LastRunState/
//     SeqRange 是纯磁盘只读扫描(journal_store.go 读路径不触 handles 锁),第二
//     实例不参与写面、不双开句柄。
//
// 解析失败(纯内存/持久化未启用/仓库已收尾)→ 真 nil(不是 typed-nil,
// agent spec.go:96 纪律),Service.RunStatus 据此走幂等表回退。
func (p *AgentPlugin) RunJournal(rt *runtimeagent.SessionRuntime) agentcontrol.RunStatusJournal {
	js := p.sessionJournalStore(rt)
	if js == nil {
		return nil
	}
	return js
}

// sessionNotFoundErr 是 C3 多会话路由下「sessionID 未定位到 runtime」的统一
// 错误:包裹 manager 的 ErrSessionNotFound sentinel(errors.Is 可判,P5-7 协议
// 面映射 -32000)。覆盖三种形态:任意未注册 id、/new 已收口的旧 id(诚实失败)、
// 无活动 runtime(Init 未完成/失败回滚——原「no active session」并入同一语义,
// 路由面 nil 即 not found)。供本文件的 PendingApprovals 与 plugin.go 的
// sessionRuntimeIn 消费;agentcontrol 内部有同语义 helper(错误文案与 sentinel
// 一致)。
func sessionNotFoundErr(sessionID string) error {
	return fmt.Errorf("agent: session %q: %w", sessionID, runtimeagent.ErrSessionNotFound)
}

// sessionJournalStore 返回 rt 会话的 Run journal 只读查询面(C3:RunStatus 的
// journal 权威分支必须用被路由会话自己的 journal——p.deps 是当前会话边界重建
// 的 deps,对跨会话路由的 runtime 是错误事实源)。解析:
//   - rt 即 current runtime → p.runJournalStore()(deps.RunEvents type-assert,
//     与该会话同源的单例,写面句柄缓存单例语义不受影响);
//   - 跨会话路由的 runtime → repo.Open(rt.ID) 命中实例唯一缓存(会话边界打开
//     的 store 在 Stop 前常驻缓存,plugin.go),构造只读查询实例——LastRunState/
//     SeqRange 是纯磁盘只读扫描(journal_store.go 读路径不触 handles 锁),第二
//     实例不参与写面、不双开句柄。
//
// 解析失败(纯内存/持久化未启用/仓库已收尾)→ nil,调用方按各自降级分支处理
// (RunStatus 回落幂等表反查或 run-not-found)。
func (p *AgentPlugin) sessionJournalStore(rt *runtimeagent.SessionRuntime) *session.RunJournalStore {
	if p.currentSession() == rt {
		return p.runJournalStore()
	}
	if p.repo == nil {
		return nil
	}
	st, err := p.repo.Open(rt.ID)
	if err != nil {
		return nil
	}
	return session.NewRunJournalStore(p.repo, st)
}

// runJournalStore 返回当前会话的 Run journal 聚合实例:从 p.deps.RunEvents
// type-assert(P5-4 起 RunEvents 与 RunJournal 注入同一个 *session.RunJournalStore
// 实例,runjournal_wiring_test.go 编译期+运行期钉死)。这是「最简形态」取舍:
// p.deps 在每个会话边界(startSession)全量重建,恒与 current store 同源,无需
// 经 repo+currentStore 重建第二个实例(那会绕开句柄缓存的单例语义)。跨会话
// 路由(C3)的只读查询面见 sessionJournalStore——非 current 会话才按 repo 只读
// 实例解析,本函数仍是 current 会话的单例取数点。nil = 纯内存/持久化禁用
// (typed-nil 纪律,spec.go:96),调用方按 run-not-found 处理。
func (p *AgentPlugin) runJournalStore() *session.RunJournalStore {
	if js, ok := p.deps.RunEvents.(*session.RunJournalStore); ok {
		return js
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// RunControlSource 的薄委托面(PR1):控制操作的全部编排(fencing、幂等、审批
// 归属)在 agentcontrol.Service 内完成;本层只做签名透传。零值回调即迁移前的
// 语义:事件采集由 supervisor 的 tee 自行供给(C2),Decide 留 nil → P4-2 审批
// 回退接管,决策经 ResolveApproval 跨连接投递。
// ─────────────────────────────────────────────────────────────────────────────

// StartRun 实现 RunControlSource(P5-6;PR1 起薄委托):多会话路由 → 本会话
// lease 临界区内原子完成「lease 校验 + 幂等 single-flight + manager.StartRun」
// (fencing:校验通过后到提交完成之间不可能被接管,未持有 → agentcontrol 的
// ErrControlLeaseLost;幂等键 (sessionID, requestID) 组合寻址,同键同 Input
// 返回原 runID 与其当前状态,不同 Input 显式冲突)。requestID 的记账完全在
// Service 层;协议面强制 requestId 非空的前提不变(xgwRunStart 守卫)。
func (p *AgentPlugin) StartRun(sessionID, connectionID, input, requestID string) (string, string, error) {
	return p.control.StartRun(sessionID, connectionID, input, requestID, runtimeagent.RunCallbacks{})
}

// StartRunSync 是同步宿主入口(/agent、session/prompt、CLI prompt)的 Run 提交
// 委托(PR4,总方案 §九):handleAgent 经此进入与后台路径同一的统一门禁
// (manager.StartRun:每会话 active 占位、ErrRunActive 拒绝第二 Run、runSlots/
// pending 配额、queued→running→终态 journal、hub tee 事件面)。无 Connection
// Lease/幂等记账——那是协议面 StartRun 的契约,同步入口是 §4.1 的内部执行身份
// (ClientKindInternal 预留的启用点)。会话寻址由调用方先经 binding 形态解析点
// (sessionRuntimeIn)解析出 rt 再传 rt.ID,与互斥/记账同源。
func (p *AgentPlugin) StartRunSync(sessionID string, req runtimeagent.RunRequest, cb runtimeagent.RunCallbacks) (runtimeagent.RunHandle, error) {
	return p.control.StartRunSync(sessionID, req, cb)
}

// CancelRunSync 是同步入口的 Run 取消委托(PR4,总方案 §九 Legacy 取消语义):
// StartRun 的 runCtx 来自宿主,请求 ctx 取消不传播——handleAgent 的等待面显式
// 转本方法落地「调用方 ctx 取消 → 终止 Agent」。
func (p *AgentPlugin) CancelRunSync(sessionID, runID string) error {
	return p.control.CancelRunSync(sessionID, runID)
}

// CancelRun 实现 RunControlSource(P5-6;PR1 起薄委托):多会话路由 → fencing
// 校验与取消提交原子 → 直转 manager。manager 的拒绝语义原样上抛(未知 run →
// ErrRunNotFound;PR4 起 daemon 全部入口的 Run 均经 manager.StartRun 提交、均可
// 取消,ErrRunNotCancellable 仅剩 RecordRun 记账形态)。
func (p *AgentPlugin) CancelRun(sessionID, connectionID, runID string) error {
	return p.control.CancelRun(sessionID, connectionID, runID)
}

// RunStatus 实现 RunControlSource(P5-6;PR1 起薄委托):run 当前状态 + 该会话
// 事件流 latest seq + 回放完整性三态 replayIntegrity。观测面三分支(active →
// journal 权威 → 幂等表回退)按权威序在 Service 内完成;journal 解析经
// RunJournal 窄接口回到本插件(被路由会话自己的事实源)。
func (p *AgentPlugin) RunStatus(sessionID, runID string) (string, uint64, string, error) {
	return p.control.RunStatus(sessionID, runID)
}

// ResolveApproval 实现 RunControlSource(P5-6;C1 修复的归属三段校验 + I4
// fencing,PR1 起薄委托):lease 校验、归属校验与 Resolve 提交三步在同一会话
// lease 临界区内原子完成——「服务端记录为准,不信客户端声明」;未持有该会话
// lease 的连接(含被接管的旧持有者)在提交前即被拒绝;匹配才投递,首个有效
// 决策获胜,迟到/重复决策返回错误。
func (p *AgentPlugin) ResolveApproval(sessionID, connectionID, requestID string, decision hitl.Decision) error {
	return p.control.ResolveApproval(sessionID, connectionID, requestID, decision)
}

// AcquireControl 实现 RunControlSource 的 per-session control lease(P5-6,
// PR1 起薄委托):该会话空闲 → 占用并返回 false;他人持有 → 显式接管(置新
// holder,返回 true 并 slog.Info 审计);同持有者重复 acquire → false。空
// connectionID 拒绝。sessionID 允许空串(纯内存降级的合法租约键)。
func (p *AgentPlugin) AcquireControl(sessionID, connectionID string) bool {
	return p.control.AcquireControl(sessionID, connectionID)
}

// ReleaseControl 实现 RunControlSource(I3 后按会话释放,PR1 起薄委托):仅该
// 会话的持有者本人有效,他人调用为空操作。
func (p *AgentPlugin) ReleaseControl(sessionID, connectionID string) {
	p.control.ReleaseControl(sessionID, connectionID)
}

// HoldControl 实现 RunControlSource(I3 后按会话判定,PR1 起薄委托):
// connectionID 当前是否持有 sessionID 的 control lease。与 fenced 控制操作的
// WithControl 校验共用同一判定,保证守卫语义与守卫执行点永不漂移。
func (p *AgentPlugin) HoldControl(sessionID, connectionID string) bool {
	return p.control.HoldControl(sessionID, connectionID)
}

// ReleaseConnection 实现 RunControlSource(I3 新增断连收口,PR1 起薄委托):
// 释放 connectionID 持有的全部会话的 control lease——handler.Close 一次调用
// 替代逐会话归还。连接不持有任何租约时幂等空操作。
func (p *AgentPlugin) ReleaseConnection(connectionID string) {
	p.control.ReleaseConnection(connectionID)
}

// PendingApprovals 实现 RunControlSource(P5-6;C3 修正):多会话路由 → manager
// 审批快照 → 扁平化为 model.PendingApprovalInfo(wire 扁平化留插件,见文件头
// 依赖方向论证)。内部寻址用 rt.ID 而非调用方 sessionID:审批的
// SessionID 归属是 StartRun 传入的 runtime id(纯内存下为 "current" 而调用方
// 寻址空串),按 rt.ID 寻址在两种形态下都正确。未知会话在路由层已报
// sessionNotFoundErr;已注册会话无审批返回空切片(manager.PendingApprovals 语义)。
func (p *AgentPlugin) PendingApprovals(sessionID string) ([]model.PendingApprovalInfo, error) {
	rt := p.RouteSession(sessionID)
	if rt == nil {
		return nil, sessionNotFoundErr(sessionID)
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

// SubscribeEvents 实现 RunControlSource(总方案 §七,PR3 收敛事件订阅接口):
// 事件订阅的完整编排(多会话路由 →
// rt.EnsureHub(I3:journal 播种失败上抛)→ hub.Subscribe → 订阅后读
// replayIntegrity(第四轮评审:不缓存旧状态)→ rt.Active() 快照)整体在
// agentcontrol.Service 内完成,本层只做签名透传——返回订阅契约
// agentcontrol.AttachResult,不向协议面暴露具体 EventHub。§七 边界限制:本方法
// **不** AcquireControl(数据面与授权面拆分);attach(mode=control) 的两动作
// 编排(先订阅成功、再取 lease、失败回滚不动 lease,第四轮 I4 语义)由 ACP
// handler 显式保证。
func (p *AgentPlugin) SubscribeEvents(sessionID string, afterSeq uint64) (agentcontrol.AttachResult, error) {
	return p.control.SubscribeEvents(sessionID, afterSeq)
}

// ActiveRun 实现 RunControlSource(P5-8;第二轮评审 I1 修正):RouteSession 多
// 会话路由 → rt.Active() 快照直转(快照观察面留插件,见文件头),
// 与 RunStatus 的 active 分支同源(同一观察点)。语义:无 runtime(未注册//
// new 已收口)或无活动 run → ok=false——attach 响应的 currentRun=null 不是错误。
// 本访问器只做快照观察,Attach 才是会话路由的权威校验面。
func (p *AgentPlugin) ActiveRun(sessionID string) (string, string, bool) {
	rt := p.RouteSession(sessionID)
	if rt == nil {
		return "", "", false
	}
	if run := rt.Active(); run != nil {
		return run.ID, string(run.State()), true
	}
	return "", "", false
}

// UnbindCapabilities 实现 RunControlSource(P5-6;CapabilityRegistry 能力桥接
// 留插件,见文件头):直转 manager 能力注册表的连接级解绑(P4-3 的 UnbindConnection;P5-9 起 handler.Close 断连收口
// 消费)。连接不存在时幂等空操作。
func (p *AgentPlugin) UnbindCapabilities(connectionID string) {
	p.manager.Capabilities().UnbindConnection(connectionID)
}

// CapabilityWaitTimeout 实现 RunControlSource(P5-6):cfg.Session.
// CapabilityWaitTimeout(秒,P4-4 字段)秒化;<=0 → 45s 兜底(拍板③,硬约束
// < ai-core 工具执行 60s——SDK defaultCapabilityWaitTimeout 的同值镜像);
// >55s 钳到 maxCapabilityWaitTimeout 并 slog.Warn(P4 评审 issue2:硬约束
// 从注释落为代码,SDK 侧 resolveCapabilityWait 的出口镜像)。
func (p *AgentPlugin) CapabilityWaitTimeout() time.Duration {
	secs := 0
	if p.cfg != nil {
		secs = p.cfg.Session.CapabilityWaitTimeout
	}
	if secs <= 0 {
		return defaultCapabilityWaitTimeout
	}
	wait := time.Duration(secs) * time.Second
	if wait > maxCapabilityWaitTimeout {
		slog.Warn("agent: capability wait timeout exceeds ai-core toolCtx budget, clamped",
			"configured", wait, "clamped_to", maxCapabilityWaitTimeout,
			"reason", "wait runs inside ai-core toolCtx (60s), budget must leave Invoke headroom")
		wait = maxCapabilityWaitTimeout
	}
	return wait
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
	return func(ctx context.Context) []core.Tool {
		specs, err := acpworker.SpecDeviceTools(caller)
		if err != nil || len(specs) == 0 {
			return nil
		}
		// PR2:能力绑定跟随本回合实际执行的会话——prompt 链携带连接级 Binding 时,
		// 命令回合解析出的 runtime(withSession 注入,经 Session.Run 的 toolCtx 流
		// 入本闭包的 ctx)才是绑定目标;无绑定路径(CLI/stdio/测试)回落 current,
		// 原语义不变。不跟随的话 prompt 路由到绑定会话、能力却绑在 current 的 id
		// 上,代理会路由到错误会话的 runtime。
		rt, rtErr := p.sessionRuntimeIn(ctx)
		if rtErr != nil || rt == nil {
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
