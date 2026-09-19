package middlewares

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	sandbox "github.com/tinguo/goworker/ai-sandbox"
)

// HITLMiddleware 通过 DecisionProvider 对接沙箱检查，拦截风险工具调用。
// 覆盖两类工具：
//   - bash：走 sandbox 正则规则（deny 直接拒绝 / risky 需要确认）
//   - mcp_*：外部进程，sandbox 约束不到 —— strict/readonly 直接拒绝，normal 走 HITL 确认
type HITLMiddleware struct {
	sandboxCfg       sandbox.Config
	decisionProvider hitl.DecisionProvider
	audit            *sandbox.AuditLogger // nil = 不审计
}

type HITLOption func(*HITLMiddleware)

var reqID atomic.Int64

// WithAudit 注入审计记录器；nil 时跳过审计（默认）。
func WithAudit(a *sandbox.AuditLogger) HITLOption {
	return func(mw *HITLMiddleware) { mw.audit = a }
}

func NewHITLMiddleware(cfg sandbox.Config, dp hitl.DecisionProvider, opts ...HITLOption) *HITLMiddleware {
	mw := &HITLMiddleware{sandboxCfg: cfg, decisionProvider: dp}
	for _, o := range opts {
		o(mw)
	}
	return mw
}

func (mw *HITLMiddleware) Name() string { return "HITLMiddleware" }

func (mw *HITLMiddleware) OnBeforeModel(ev *core.BeforeModelEvent) *core.MiddlewareResponse {
	return &core.MiddlewareResponse{}
}

func (mw *HITLMiddleware) OnBeforeTool(ev *core.BeforeToolEvent) *core.MiddlewareResponse {
	if ev.Tool == nil {
		return &core.MiddlewareResponse{}
	}
	switch name := ev.Tool.Function.Name; {
	case name == "bash":
		return mw.checkBash(ev)
	case strings.HasPrefix(name, "mcp_"):
		return mw.checkMCP(ev)
	case strings.HasPrefix(name, "sys_"):
		return mw.checkSys(ev)
	}
	return &core.MiddlewareResponse{}
}

// ---- sys_*：client-ward 设备能力，按声明的 risk_level × 沙箱模式裁决 ----

const (
	RiskLevelNever  = "never"  // 声明方担保无害：任何模式直接放行
	RiskLevelMode   = "mode"   // 跟随沙箱模式（缺省）：normal 询问 / strict、readonly 拒绝 / off 放行
	RiskLevelAlways = "always" // 高危兜底：连 off 都要询问
)

// checkSys 裁决客户端声明工具（x-device 中继）的风险档位。
// risk_level 声明在工具 Metadata（client 描述符透传），裁决矩阵与 mcp 同构：
// strict/readonly 下除 never 外一律拒绝（无人值守不做设备副作用）。
func (mw *HITLMiddleware) checkSys(ev *core.BeforeToolEvent) *core.MiddlewareResponse {
	level := RiskLevelMode
	if ev.ToolDef != nil {
		if l := ev.ToolDef.Metadata["risk_level"]; l == RiskLevelNever || l == RiskLevelMode || l == RiskLevelAlways {
			level = l
		}
	}

	decision := func() string {
		switch mw.sandboxCfg.Mode {
		case sandbox.ModeStrict, sandbox.ModeReadOnly:
			if level == RiskLevelNever {
				return "allow"
			}
			return "deny"
		case sandbox.ModeOff:
			if level == RiskLevelAlways {
				return "hitl"
			}
			return "allow"
		default: // normal
			if level == RiskLevelNever {
				return "allow"
			}
			return "hitl"
		}
	}()

	argsJSON, _ := json.Marshal(ev.Args)
	now := time.Now()
	req := &hitl.InterruptRequest{
		ID:         fmt.Sprintf("req-%d", reqID.Add(1)),
		ToolName:   ev.Tool.Function.Name,
		Command:    string(argsJSON),
		RiskReason: fmt.Sprintf("declared risk_level=%s (device capability)", level),
		RiskLevel:  level,
		CreatedAt:  now,
		ExpiresAt:  now.Add(hitl.DefaultTimeout),
	}
	switch decision {
	case "allow":
		return &core.MiddlewareResponse{}
	case "deny":
		reason := fmt.Sprintf("⛔ tool %s denied in %s mode (risk_level=%s)", ev.Tool.Function.Name, mw.sandboxCfg.Mode, level)
		return mw.block(ev, reason)
	default: // hitl
		return mw.confirm(ev, req, nil)
	}
}

// ---- bash：sandbox 正则规则 ----

func (mw *HITLMiddleware) checkBash(ev *core.BeforeToolEvent) *core.MiddlewareResponse {
	cmdStr, ok := ev.Args["command"].(string)
	if !ok || cmdStr == "" {
		return &core.MiddlewareResponse{}
	}

	// 走结构化决策链路：Assess（分级）→ Policy（allow/hitl/deny），而不是直接对旧三态 error 分类。
	out := sandbox.Evaluate(sandbox.CommandRequest{Command: cmdStr}, &mw.sandboxCfg)
	switch out.Decision {
	case sandbox.DecisionAllow, sandbox.DecisionSandbox:
		mw.recordAudit(out, "", "executed")
		return &core.MiddlewareResponse{}
	case sandbox.DecisionDeny:
		mw.recordAudit(out, "", "blocked")
		reason := out.Error().Error()
		return mw.block(ev, "⛔ "+reason)
	default: // DecisionHitl
		now := time.Now()
		req := &hitl.InterruptRequest{
			ID:         fmt.Sprintf("req-%d", reqID.Add(1)),
			ToolName:   "bash",
			Command:    cmdStr,
			RiskReason: riskReason(out),
			RiskLevel:  out.Level.String(),
			Effects:    out.Effects.Names(),
			CreatedAt:  now,
			ExpiresAt:  now.Add(hitl.DefaultTimeout),
		}
		return mw.confirm(ev, req, func(d hitl.Decision) {
			outcome := "executed"
			if d.Type == hitl.DecisionReject {
				outcome = "blocked"
			}
			if d.Type == hitl.DecisionRespond {
				outcome = "aborted"
			}
			mw.recordAudit(out, string(d.Type), outcome)
		})
	}
}

// recordAudit 把一次命令决策写入审计（nil logger 时跳过）。
// UserDecision 只在走 HITL 后有值——那是未来训练数据的标签（宪法 V）。
func (mw *HITLMiddleware) recordAudit(out sandbox.Outcome, userDec, outcome string) {
	if mw.audit == nil {
		return
	}
	mw.audit.Record(sandbox.AuditEntry{
		Timestamp:      time.Now(),
		Command:        out.Command,
		RiskLevel:      out.Level.String(),
		Effects:        out.Effects.Names(),
		Reasons:        out.Reasons,
		Source:         out.Source.String(),
		EngineDecision: out.Decision.String(),
		UserDecision:   userDec,
		Outcome:        outcome,
	})
}

// riskReason 组装 HITL 展示用的风险原因：中文描述 + 风险等级 + 副作用。
func riskReason(out sandbox.Outcome) string {
	reason := ""
	if len(out.Reasons) > 0 {
		reason = out.Reasons[0].Detail
	}
	effs := out.Effects.Names()
	switch {
	case reason != "" && len(effs) > 0:
		return fmt.Sprintf("%s (risk %s, %s)", reason, out.Level, strings.Join(effs, ","))
	case reason != "":
		return fmt.Sprintf("%s (risk %s)", reason, out.Level)
	default:
		return fmt.Sprintf("risk %s", out.Level)
	}
}

// ---- MCP 工具：外部进程，sandbox 约束不到 ----

func (mw *HITLMiddleware) checkMCP(ev *core.BeforeToolEvent) *core.MiddlewareResponse {
	switch mw.sandboxCfg.Mode {
	case sandbox.ModeOff:
		return &core.MiddlewareResponse{}
	case sandbox.ModeStrict, sandbox.ModeReadOnly:
		reason := fmt.Sprintf("MCP tool %s executes outside the sandbox, blocked in %s mode", ev.Tool.Function.Name, mw.sandboxCfg.Mode)
		return mw.block(ev, reason)
	default: // ModeNormal：外部进程需用户确认
		args, _ := json.Marshal(ev.Args)
		now := time.Now()
		req := &hitl.InterruptRequest{
			ID:          fmt.Sprintf("req-%d", reqID.Add(1)),
			ToolName:    ev.Tool.Function.Name,
			Command:     string(args),
			RiskReason:  "MCP tool executes in an external process outside the sandbox",
			Description: fmt.Sprintf("MCP tool %s with args %s", ev.Tool.Function.Name, string(args)),
			CreatedAt:   now,
			ExpiresAt:   now.Add(hitl.DefaultTimeout),
		}
		return mw.confirm(ev, req, nil)
	}
}

// ---- 公共 HITL 流程 ----

// confirm 走完整 HITL 决策流程：发 interrupt token → 等用户决策 → 处理。
// onDecide 在拿到用户决策后立即回调（审计用），nil 时跳过。
func (mw *HITLMiddleware) confirm(ev *core.BeforeToolEvent, req *hitl.InterruptRequest, onDecide func(d hitl.Decision)) *core.MiddlewareResponse {
	payload, err := json.Marshal(req)
	if err != nil {
		return mw.reject(ev, "⛔ failed to encode interrupt request")
	}
	if ev.Emit != nil {
		ev.Emit.Emit(ev.Ctx, core.Token{
			Type: core.TokenTypeEvent,
			Event: &core.RuntimeEvent{
				Type: hitl.EventInterrupt,
				ID:   req.ID,
				Data: payload,
			},
		})
	}

	d := mw.decisionProvider.GetDecision(ev.Ctx, req)
	if onDecide != nil {
		onDecide(d)
	}

	switch d.Type {
	case hitl.DecisionApprove:
		// execute as-is
	case hitl.DecisionEdit:
		// 仅 bash 支持命令编辑；MCP 工具没有 command 字段，编辑视为批准
		if req.ToolName == "bash" {
			if edited := strings.TrimSpace(d.Command); edited != "" {
				ev.Args["command"] = edited
				re := sandbox.Evaluate(sandbox.CommandRequest{Command: edited}, &mw.sandboxCfg)
				if re.Decision == sandbox.DecisionDeny {
					mw.recordAudit(re, string(d.Type), "blocked")
					return mw.block(ev, "⛔ "+re.Error().Error())
				}
			}
		}
	case hitl.DecisionReject:
		return mw.reject(ev, "⛔ rejected by user")
	case hitl.DecisionRespond:
		msg := strings.TrimSpace(d.Message)
		if msg == "" {
			msg = "user declined to answer"
		}
		ev.Abort = &core.ToolAbort{Messages: []core.Message{
			{Role: "tool", Content: fmt.Sprintf("[tool not executed] user replied: %s", msg), ToolCallID: ev.Tool.ID},
		}}
	}

	return &core.MiddlewareResponse{}
}

// block 直接拒绝工具执行并回填会话历史。
func (mw *HITLMiddleware) block(ev *core.BeforeToolEvent, toolMsg string) *core.MiddlewareResponse {
	ev.Abort = &core.ToolAbort{Messages: []core.Message{
		{Role: "tool", Content: toolMsg, ToolCallID: ev.Tool.ID},
	}}
	return &core.MiddlewareResponse{}
}

// reject 拒绝执行并回填 tool 结果消息。
func (mw *HITLMiddleware) reject(ev *core.BeforeToolEvent, msg string) *core.MiddlewareResponse {
	ev.Abort = &core.ToolAbort{Messages: []core.Message{
		{Role: "tool", Content: msg, ToolCallID: ev.Tool.ID},
	}}
	return &core.MiddlewareResponse{}
}
