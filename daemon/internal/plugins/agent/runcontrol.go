package service

import (
	"context"
	"errors"
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

// AgentPlugin 将控制操作委托给 agentcontrol，并适配宿主的会话、journal 和设备能力。
// 控制面职责与依赖边界见 ../../../docs/README.md。

// RunControlSource 编译期断言:AgentPlugin 满足可选能力接口,handler
// 以 type-assert 获取(SessionSource 先例,capability.go:61-78)。
var _ model.RunControlSource = (*AgentPlugin)(nil)
var _ model.NegotiatedRunControlSource = (*AgentPlugin)(nil)

// InternalRunner 编译期断言：AgentPlugin 实现受限的同步宿主入口。该接口只隔离静态方法集，
// 不构成运行时授权边界；协议控制仍由 Service 的 lease fencing 保护。
var _ agentcontrol.InternalRunner = (*AgentPlugin)(nil)

// ClientToolBinder 编译期断言:AgentPlugin 同时满足设备工具的能力代理面,
// ACP handler 优先 type-assert 它走 CapabilityRegistry + proxy,断言失败回落
// 原始 relay。
var _ model.ClientToolBinder = (*AgentPlugin)(nil)

// ConnectionBinder lets ACP register each connection's selected session in the
// BindingRegistry, the source of truth for lifecycle reference counts.
var _ model.ConnectionBinder = (*AgentPlugin)(nil)

// defaultCapabilityWaitTimeout mirrors the SDK's unexported 45s fallback when
// configuration is absent or non-positive.
const defaultCapabilityWaitTimeout = 45 * time.Second

// maxCapabilityWaitTimeout stays below the ai-core 60s tool context, leaving 5s for
// invocation after capability waiting. Values above the cap are clamped and logged.
const maxCapabilityWaitTimeout = 55 * time.Second

// RouteSession implements agentcontrol.SessionRouter. It prefers the current
// runtime, then resolves registered sessions; an unknown ID never falls back silently.
func (p *AgentPlugin) RouteSession(sessionID string) *runtimeagent.SessionRuntime {
	if cur := p.currentSession(); cur != nil {
		if sessionID == cur.ID || (sessionID == "" && p.CurrentSessionID() == "") {
			return cur
		}
	}
	return p.manager.Get(sessionID)
}

// RunJournal returns the routed runtime's read-only journal. The current session's
// dependencies may not describe another runtime, so journal lookup follows routing.
func (p *AgentPlugin) RunJournal(rt *runtimeagent.SessionRuntime) agentcontrol.RunStatusJournal {
	js := p.sessionJournalStore(rt)
	if js == nil {
		return nil
	}
	return js
}

// sessionNotFoundErr wraps the runtime sentinel so callers can distinguish an
// unresolved session while preserving the protocol's stable error mapping.
func sessionNotFoundErr(sessionID string) error {
	return fmt.Errorf("agent: session %q: %w", sessionID, runtimeagent.ErrSessionNotFound)
}

// sessionJournalStore resolves the routed runtime's journal. The current runtime
// reuses its existing instance; other persistent sessions get a read-only store view.
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

// runJournalStore returns the current session's journal instance when persistence
// is enabled. Non-current runtimes are resolved by sessionJournalStore.
func (p *AgentPlugin) runJournalStore() *session.RunJournalStore {
	if js, ok := p.deps.RunEvents.(*session.RunJournalStore); ok {
		return js
	}
	return nil
}

// RunControlSource methods. Control orchestration, lease fencing, idempotency, and
// approval ownership live in agentcontrol.Service; this plugin supplies runtime access.

// StartRun submits a background run after routing the session. The Service performs
// lease fencing and per-(session, request) idempotency before committing to the manager.
func (p *AgentPlugin) StartRun(sessionID, connectionID, input, requestID string) (string, string, error) {
	return p.StartRunWithTools(sessionID, connectionID, input, requestID, nil)
}

// StartRunWithTools freezes the negotiated device-tool source into the initial run
// submission. The idempotency registry ignores sources on replays by construction.
func (p *AgentPlugin) StartRunWithTools(sessionID, connectionID, input, requestID string, toolSource func(context.Context) []core.Tool) (string, string, error) {
	runID, state, err := p.control.StartRunWithTools(sessionID, connectionID, input, requestID, runtimeagent.RunCallbacks{}, toolSource)
	if errors.Is(err, runtimeagent.ErrSessionArchived) {
		return "", "", fmt.Errorf("agent: session %q is archived and read-only", sessionID)
	}
	return runID, state, err
}

// StartRunSync submits an internal synchronous run without connection fencing or
// idempotency. Run admission, quotas, journal writes, and finalization remain in the manager.
func (p *AgentPlugin) StartRunSync(sessionID string, req runtimeagent.RunRequest, cb runtimeagent.RunCallbacks) (runtimeagent.RunHandle, error) {
	h, err := p.control.StartRunSync(sessionID, req, cb)
	if errors.Is(err, runtimeagent.ErrSessionArchived) {
		return runtimeagent.RunHandle{}, fmt.Errorf("agent: session %q is archived and read-only", sessionID)
	}
	return h, err
}

// CancelRunSync 是同步入口的 Run 取消委托(Legacy 取消语义):StartRun 的
// runCtx 来自宿主,请求 ctx 取消不传播——handleAgent 的等待面显式转本方法
// 落地「调用方 ctx 取消 → 终止 Agent」。
func (p *AgentPlugin) CancelRunSync(sessionID, runID string) error {
	return p.control.CancelRunSync(sessionID, runID)
}

// CancelRun 实现 RunControlSource(薄委托):多会话路由 → fencing 校验与取消
// 提交原子 → 直转 manager。manager 的拒绝语义原样上抛(未知 run →
// ErrRunNotFound;daemon 全部入口的 Run 均经 manager.StartRun 提交、均可取消,
// ErrRunNotCancellable 仅剩 RecordRun 记账形态)。
func (p *AgentPlugin) CancelRun(sessionID, connectionID, runID string) error {
	return p.control.CancelRun(sessionID, connectionID, runID)
}

// RunStatus 实现 RunControlSource(薄委托):run 当前状态 + 该会话
// 事件流 latest seq + 回放完整性三态 replayIntegrity。观测面三分支(active →
// journal 权威 → 幂等表回退)按权威序在 Service 内完成;journal 解析经
// RunJournal 窄接口回到本插件(被路由会话自己的事实源)。
func (p *AgentPlugin) RunStatus(sessionID, runID string) (string, uint64, string, error) {
	return p.control.RunStatus(sessionID, runID)
}

// ResolveApproval 实现 RunControlSource(归属三段校验 + fencing,薄委托):
// lease 校验、归属校验与 Resolve 提交三步在同一会话
// lease 临界区内原子完成——「服务端记录为准,不信客户端声明」;未持有该会话
// lease 的连接(含被接管的旧持有者)在提交前即被拒绝;匹配才投递,首个有效
// 决策获胜,迟到/重复决策返回错误。
func (p *AgentPlugin) ResolveApproval(sessionID, connectionID, requestID string, decision hitl.Decision) error {
	return p.control.ResolveApproval(sessionID, connectionID, requestID, decision)
}

// AcquireControl 实现 RunControlSource 的 per-session control lease(薄委托):
// 该会话空闲 → 占用并返回 false;他人持有 → 显式接管(置新
// holder,返回 true 并 slog.Info 审计);同持有者重复 acquire → false。空
// connectionID 拒绝。sessionID 允许空串(纯内存降级的合法租约键)。
func (p *AgentPlugin) AcquireControl(sessionID, connectionID string) bool {
	return p.control.AcquireControl(sessionID, connectionID)
}

// ReleaseControl 实现 RunControlSource(按会话释放,薄委托):仅该
// 会话的持有者本人有效,他人调用为空操作。
func (p *AgentPlugin) ReleaseControl(sessionID, connectionID string) {
	p.control.ReleaseControl(sessionID, connectionID)
}

// HoldControl 实现 RunControlSource(按会话判定,薄委托):
// connectionID 当前是否持有 sessionID 的 control lease。与 fenced 控制操作的
// WithControl 校验共用同一判定,保证守卫语义与守卫执行点永不漂移。
func (p *AgentPlugin) HoldControl(sessionID, connectionID string) bool {
	return p.control.HoldControl(sessionID, connectionID)
}

// ReleaseConnection releases every lease and binding owned by the connection, then checks
// the returned previous session for lifecycle release. Active runs keep that session alive
// until the terminal-run callback rechecks it.
func (p *AgentPlugin) ReleaseConnection(connectionID string) {
	prev := p.control.ReleaseConnection(connectionID)
	if prev != "" {
		p.tryReleaseSession(prev)
	}
}

// PendingApprovals returns the routed session's pending approvals as wire-safe snapshots.
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

// SubscribeEvents subscribes to the routed session's event hub after afterSeq. It does
// not acquire a lease; attach orchestration handles subscription and control separately.
func (p *AgentPlugin) SubscribeEvents(sessionID string, afterSeq uint64) (agentcontrol.AttachResult, error) {
	return p.control.SubscribeEvents(sessionID, afterSeq)
}

// ActiveRun returns the routed runtime's active run snapshot. A missing runtime or
// inactive session reports ok=false; this observation does not validate an attach request.
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

// UnbindCapabilities removes the connection's capability registrations. Repeated calls
// are harmless, including calls for connections with no registered tools.
func (p *AgentPlugin) UnbindCapabilities(connectionID string) {
	p.manager.Capabilities().UnbindConnection(connectionID)
}

// CapabilityWaitTimeout converts the configured seconds to a duration. Non-positive values
// use the 45s default; values above 55s are clamped so device-tool invocation still fits
// inside the ai-core 60s execution context.
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

// ClientToolBinder is the device-tool capability surface wired by the daemon. The stdio
// worker path keeps its own relay and does not use this proxy.

// BindClientToolSpecs registers the immutable initialize-time descriptor contract for a
// specific runtime session and connection. Registry matching rejects schema or policy drift.
func (p *AgentPlugin) BindClientToolSpecs(sessionID, connectionID string, specs []runtimeagent.ClientToolSpec, caller model.DeviceCaller) bool {
	if len(specs) == 0 || caller == nil {
		return false
	}
	return p.manager.Capabilities().BindConnectionSpecs(sessionID, connectionID, specs, deviceToolExecutor{caller: caller})
}

// ClientToolSourceForSpecs builds proxies from the cached declaration snapshot without probing.
func (p *AgentPlugin) ClientToolSourceForSpecs(sessionID string, specs []runtimeagent.ClientToolSpec) func(context.Context) []core.Tool {
	if len(specs) == 0 {
		return nil
	}
	frozen := cloneRuntimeClientToolSpecs(specs)
	wait := p.CapabilityWaitTimeout()
	reg := p.manager.Capabilities()
	return func(context.Context) []core.Tool {
		return runtimeagent.NewSpecAwareClientToolProxy(reg, sessionID, frozen, wait)
	}
}

func cloneRuntimeClientToolSpecs(specs []runtimeagent.ClientToolSpec) []runtimeagent.ClientToolSpec {
	cloned := make([]runtimeagent.ClientToolSpec, len(specs))
	for i, spec := range specs {
		cloned[i] = spec
		if spec.Metadata != nil {
			cloned[i].Metadata = make(map[string]string, len(spec.Metadata))
			for key, value := range spec.Metadata {
				cloned[i].Metadata[key] = value
			}
		}
	}
	return cloned
}

// deviceToolExecutor adapts the SDK executor to x-device/call.
type deviceToolExecutor struct {
	caller model.DeviceCaller
}

func (e deviceToolExecutor) Invoke(ctx context.Context, toolName string, argsJSON []byte) (string, error) {
	return acpworker.CallDeviceTool(ctx, e.caller, toolName, argsJSON)
}

// ClientToolSource returns a lazy device-tool collector for the connection. A failed or
// empty probe yields no tools; success binds proxied tools to the round's resolved session
// for twice the capability wait, then returns a client proxy. Every prompt refreshes it.
func (p *AgentPlugin) ClientToolSource(connectionID string, caller model.DeviceCaller) func(context.Context) []core.Tool {
	return func(ctx context.Context) []core.Tool {
		specs, err := acpworker.SpecDeviceTools(caller)
		if err != nil || len(specs) == 0 {
			return nil
		}
		// 能力绑定跟随本回合实际执行的会话——prompt 链携带连接级 Binding 时,
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
