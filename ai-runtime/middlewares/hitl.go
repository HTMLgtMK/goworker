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
// bash 使用命令决策引擎；带 requires_permission=true 的外部工具使用统一的
// metadata 风险策略。未标记的内部工具保持原有直通行为。
type HITLMiddleware struct {
	sandboxCfg       sandbox.Config
	decisionProvider hitl.DecisionProvider
	audit            *sandbox.AuditLogger // nil = 不审计
	allowAlways      AllowAlwaysHandler
	toolAllowAlways  ToolAllowAlwaysHandler
}

type HITLOption func(*HITLMiddleware)

type AllowAlwaysHandler func(sandbox.AllowRule) error

type ToolAllowAlwaysHandler func(sandbox.ToolAllowRule) error

func allowRuleForRequest(req *hitl.InterruptRequest) (sandbox.AllowRule, error) {
	if req == nil || req.ToolName != "bash" {
		return sandbox.AllowRule{}, fmt.Errorf("allow_always only supports bash")
	}
	maxRisk, err := sandbox.ParseRiskLevel(req.RiskLevel)
	if err != nil {
		return sandbox.AllowRule{}, fmt.Errorf("parse risk level: %w", err)
	}
	rule, ok := sandbox.AllowRuleForCommand(req.Command, maxRisk, req.Effects)
	if !ok {
		return sandbox.AllowRule{}, fmt.Errorf("empty or invalid command")
	}
	return rule, nil
}

func toolAllowRuleForRequest(req *hitl.InterruptRequest) (sandbox.ToolAllowRule, error) {
	if req == nil || strings.TrimSpace(req.AllowAlwaysKey) == "" {
		return sandbox.ToolAllowRule{}, fmt.Errorf("allow_always is unavailable for this tool")
	}
	maxRisk, err := sandbox.ParseRiskLevel(req.RiskLevel)
	if err != nil {
		return sandbox.ToolAllowRule{}, fmt.Errorf("parse risk level: %w", err)
	}
	return sandbox.ToolAllowRule{Key: req.AllowAlwaysKey, MaxRisk: maxRisk}, nil
}

// WithAllowAlways injects the persistence boundary for command allow rules.
func WithAllowAlways(handler AllowAlwaysHandler) HITLOption {
	return func(mw *HITLMiddleware) { mw.allowAlways = handler }
}

// WithToolAllowAlways injects the persistence boundary for external tool rules.
func WithToolAllowAlways(handler ToolAllowAlwaysHandler) HITLOption {
	return func(mw *HITLMiddleware) { mw.toolAllowAlways = handler }
}

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
	name := ev.Tool.Function.Name
	if name == "bash" {
		return mw.checkBash(ev)
	}
	if policy, ok := resolveToolPolicy(ev); ok {
		return mw.checkPolicyTool(ev, policy)
	}
	// Compatibility fallback for callers that have not populated ToolDef yet.
	// Registered MCP/sys bridges always carry metadata and use the generic path above.
	switch {
	case strings.HasPrefix(name, "mcp_"):
		return mw.checkMCP(ev)
	case strings.HasPrefix(name, "sys_"):
		return mw.checkSys(ev)
	default:
		return &core.MiddlewareResponse{}
	}
}

// resolveToolPolicy returns only explicit permission-gated tool declarations.
func resolveToolPolicy(ev *core.BeforeToolEvent) (sandbox.ToolPolicyMetadata, bool) {
	if ev == nil || ev.ToolDef == nil {
		return sandbox.ToolPolicyMetadata{}, false
	}
	metadata := ev.ToolDef.Metadata
	if !strings.EqualFold(metadata[sandbox.ToolMetadataRequiresPermission], "true") {
		return sandbox.ToolPolicyMetadata{}, false
	}
	return sandbox.NormalizeToolMetadata(metadata), true
}

func (mw *HITLMiddleware) checkPolicyTool(ev *core.BeforeToolEvent, policy sandbox.ToolPolicyMetadata) *core.MiddlewareResponse {
	decision := mw.toolDecision(policy)
	argsJSON, _ := json.Marshal(ev.Args)
	now := time.Now()
	req := &hitl.InterruptRequest{
		ID:             fmt.Sprintf("req-%d", reqID.Add(1)),
		ToolName:       ev.Tool.Function.Name,
		Command:        string(argsJSON),
		RiskReason:     fmt.Sprintf("declared risk_level=%s (tool metadata)", policy.RiskLevel),
		Description:    fmt.Sprintf("tool %s with args %s", ev.Tool.Function.Name, string(argsJSON)),
		RiskLevel:      policy.RiskLevel.String(),
		AllowAlwaysKey: policy.AllowAlwaysKey,
		CreatedAt:      now,
		ExpiresAt:      now.Add(hitl.DefaultTimeout),
	}
	switch decision {
	case "allow":
		return &core.MiddlewareResponse{}
	case "deny":
		return mw.block(ev, fmt.Sprintf("⛔ tool %s denied in %s mode (risk_level=%s)", ev.Tool.Function.Name, mw.sandboxCfg.Mode, policy.RiskLevel))
	default:
		return mw.confirm(ev, req, nil)
	}
}

func (mw *HITLMiddleware) toolDecision(policy sandbox.ToolPolicyMetadata) string {
	level := policy.RiskLevel
	switch mw.sandboxCfg.Mode {
	case sandbox.ModeStrict, sandbox.ModeReadOnly:
		if level <= sandbox.RiskLow {
			return "allow"
		}
		return "deny"
	case sandbox.ModeAuto:
		if !policy.ForceHITL && level <= mw.sandboxCfg.AutoMaxRisk {
			return "allow"
		}
		return "hitl"
	case sandbox.ModeOff:
		if !policy.ForceHITL {
			return "allow"
		}
		return "hitl"
	default:
		if !policy.ForceHITL && level <= sandbox.RiskLow {
			return "allow"
		}
		if !policy.ForceHITL && toolAllowRuleMatches(mw.sandboxCfg.ToolAllowRules, policy) {
			return "allow"
		}
		return "hitl"
	}
}

func toolAllowRuleMatches(rules []sandbox.ToolAllowRule, policy sandbox.ToolPolicyMetadata) bool {
	if policy.AllowAlwaysKey == "" {
		return false
	}
	for _, rule := range rules {
		if rule.Key == policy.AllowAlwaysKey && policy.RiskLevel <= rule.MaxRisk {
			return true
		}
	}
	return false
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
			ID:          fmt.Sprintf("req-%d", reqID.Add(1)),
			ToolName:    "bash",
			Command:     cmdStr,
			AllowAlways: true,
			RiskReason:  riskReason(out),
			RiskLevel:   out.Level.String(),
			Effects:     out.Effects.Names(),
			CreatedAt:   now,
			ExpiresAt:   now.Add(hitl.DefaultTimeout),
		}
		return mw.confirm(ev, req, func(d hitl.Decision, outcome string) {
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
	case sandbox.ModeOff, sandbox.ModeAuto:
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

func appendToolAllowRule(rules []sandbox.ToolAllowRule, next sandbox.ToolAllowRule) []sandbox.ToolAllowRule {
	for i, rule := range rules {
		if rule.Key != next.Key {
			continue
		}
		updated := append([]sandbox.ToolAllowRule(nil), rules...)
		updated[i] = next
		return updated
	}
	return append(append([]sandbox.ToolAllowRule(nil), rules...), next)
}

// ---- 公共 HITL 流程 ----

// confirm 走完整 HITL 决策流程：发 interrupt token → 等用户决策 → 处理。
// onDecide 在工具决策完成后回调（审计用），nil 时跳过。
func (mw *HITLMiddleware) confirm(ev *core.BeforeToolEvent, req *hitl.InterruptRequest, onDecide func(d hitl.Decision, outcome string)) *core.MiddlewareResponse {
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

	if mw.decisionProvider == nil {
		return mw.reject(ev, "⛔ no decision provider available")
	}
	d := mw.decisionProvider.GetDecision(ev.Ctx, req)
	audit := func(outcome string) {
		if onDecide != nil {
			onDecide(d, outcome)
		}
	}

	switch d.Type {
	case hitl.DecisionApprove:
		audit("executed")
	case hitl.DecisionApproveAlways:
		if req.AllowAlwaysKey == "" {
			if mw.allowAlways == nil {
				audit("blocked")
				return mw.reject(ev, "⛔ allow_always is unavailable")
			}
			rule, err := allowRuleForRequest(req)
			if err != nil {
				audit("blocked")
				return mw.reject(ev, "⛔ "+err.Error())
			}
			if err := mw.allowAlways(rule); err != nil {
				audit("blocked")
				return mw.reject(ev, "⛔ failed to persist allow_always")
			}
			mw.sandboxCfg.AllowRules = append(mw.sandboxCfg.AllowRules, rule)
			audit("executed")
			break
		}
		if mw.toolAllowAlways == nil {
			audit("blocked")
			return mw.reject(ev, "⛔ allow_always is unavailable")
		}
		rule, err := toolAllowRuleForRequest(req)
		if err != nil {
			audit("blocked")
			return mw.reject(ev, "⛔ "+err.Error())
		}
		if err := mw.toolAllowAlways(rule); err != nil {
			audit("blocked")
			return mw.reject(ev, "⛔ failed to persist allow_always")
		}
		mw.sandboxCfg.ToolAllowRules = appendToolAllowRule(mw.sandboxCfg.ToolAllowRules, rule)
		audit("executed")
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
		audit("executed")
	case hitl.DecisionReject:
		audit("blocked")
		return mw.reject(ev, "⛔ rejected by user")
	case hitl.DecisionRespond:
		audit("aborted")
		msg := strings.TrimSpace(d.Message)
		if msg == "" {
			msg = "user declined to answer"
		}
		ev.Abort = &core.ToolAbort{Messages: []core.Message{
			{Role: "tool", Content: fmt.Sprintf("[tool not executed] user replied: %s", msg), ToolCallID: ev.Tool.ID},
		}}
	default:
		audit("blocked")
		return mw.reject(ev, "⛔ unknown HITL decision")
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
