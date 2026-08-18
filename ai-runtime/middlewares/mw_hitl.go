package middlewares

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-core/spec"
	"github.com/tinguo/goworker/ai-sandbox"
)

var reqID atomic.Int64

// WithAudit 注入审计记录器；nil 时跳过审计（默认）。
func WithAudit(a *sandbox.AuditLogger) HITLOption {
	return func(mw *HITLMiddleware) { mw.audit = a }
}

func NewHITLMiddleware(cfg sandbox.Config, dp core.DecisionProvider, opts ...HITLOption) *HITLMiddleware {
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
	}
	return &core.MiddlewareResponse{}
}

// ---- bash：sandbox 正则规则 ----

func (mw *HITLMiddleware) checkBash(ev *core.BeforeToolEvent) *core.MiddlewareResponse {
	cmdStr, ok := ev.Args["command"].(string)
	if !ok || cmdStr == "" {
		return &core.MiddlewareResponse{}
	}

	// 走结构化决策链路：Assess（分级）→ Policy（allow/hitl/deny），
	// 而不是直接对旧三态 error 分类。
	out := sandbox.Evaluate(sandbox.CommandRequest{Command: cmdStr}, &mw.sandboxCfg)
	switch out.Decision {
	case sandbox.DecisionAllow, sandbox.DecisionSandbox:
		mw.recordAudit(out, "", "executed")
		return &core.MiddlewareResponse{}
	case sandbox.DecisionDeny:
		mw.recordAudit(out, "", "blocked")
		reason := out.Error().Error()
		return mw.block(ev, "⛔ "+reason, "⛔ "+reason)
	default: // DecisionHitl
		req := &spec.InterruptRequest{
			ID:         fmt.Sprintf("req-%d", reqID.Add(1)),
			ToolName:   "bash",
			Command:    cmdStr,
			RiskReason: riskReason(out),
			RiskLevel:  out.Level.String(),
			Effects:    out.Effects.Names(),
			CreatedAt:  time.Now(),
			ExpiresAt:  time.Now().Add(30 * time.Second),
		}
		return mw.confirm(ev, req, func(d spec.HITLDecision) {
			outcome := "executed"
			if d.Type == spec.DecisionReject {
				outcome = "blocked"
			}
			if d.Type == spec.DecisionRespond {
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
		return mw.block(ev, "⛔ "+reason, reason)
	default: // ModeNormal：外部进程需用户确认
		args, _ := json.Marshal(ev.Args)
		req := &spec.InterruptRequest{
			ID:          fmt.Sprintf("req-%d", reqID.Add(1)),
			ToolName:    ev.Tool.Function.Name,
			Command:     string(args),
			RiskReason:  "MCP tool executes in an external process outside the sandbox",
			Description: fmt.Sprintf("MCP tool %s with args %s", ev.Tool.Function.Name, string(args)),
			CreatedAt:   time.Now(),
			ExpiresAt:   time.Now().Add(30 * time.Second),
		}
		return mw.confirm(ev, req, nil)
	}
}

// ---- 公共 HITL 流程 ----

// confirm 走完整 HITL 决策流程：发 interrupt token → 等用户决策 → 处理。
// onDecide 在拿到用户决策后立即回调（审计用），nil 时跳过。
func (mw *HITLMiddleware) confirm(ev *core.BeforeToolEvent, req *spec.InterruptRequest, onDecide func(d spec.HITLDecision)) *core.MiddlewareResponse {
	// 发送 interrupt token，通知前端展示确认选项
	select {
	case ev.TokenCh <- core.Token{Type: core.TokenTypeInterrupt, Interrupt: req}:
	case <-ev.Ctx.Done():
		return &core.MiddlewareResponse{}
	}

	// 通过 DecisionProvider 获取用户决策
	d := mw.decisionProvider.GetDecision(ev.Ctx, req)
	if onDecide != nil {
		onDecide(d)
	}

	switch d.Type {
	case spec.DecisionApprove:
		// execute as-is
	case spec.DecisionEdit:
		// 仅 bash 支持命令编辑；MCP 工具没有 command 字段，编辑视为批准
		if req.ToolName == "bash" {
			if edited := strings.TrimSpace(d.Command); edited != "" {
				ev.Args["command"] = edited
				// 编辑后的命令重新过 sandbox：用户编辑不是免检令牌，
				// deny 级风险（strict/readonly 硬拒、deny_patterns）依然拦截
				re := sandbox.Evaluate(sandbox.CommandRequest{Command: edited}, &mw.sandboxCfg)
				if re.Decision == sandbox.DecisionDeny {
					mw.recordAudit(re, string(d.Type), "blocked")
					return mw.block(ev, "⛔ "+re.Error().Error(), "⛔ "+re.Error().Error())
				}
			}
		}
	case spec.DecisionReject:
		return mw.reject(ev, "⛔ rejected by user")
	case spec.DecisionRespond:
		msg := strings.TrimSpace(d.Message)
		if msg == "" {
			msg = "user declined to answer"
		}
		select {
		case ev.TokenCh <- core.Token{Type: core.TokenTypeToolResult, Content: "💬 " + msg}:
		case <-ev.Ctx.Done():
		}
		ev.Aborted = true
		// 必须用 tool 消息回填并带 ToolCallID：assistant 的每个 tool_call 都要有对应
		// tool 响应，否则下一轮请求被 OpenAI 兼容后端以 400/空 choices 拒绝 —— 这正是
		// 多轮 tool call"莫名停止"的诱因之一。模型借此得知工具没执行、用户说了什么。
		ev.ResponseMessages = []core.Message{
			{Role: "tool", Content: fmt.Sprintf("[tool not executed] user replied: %s", msg), ToolCallID: ev.Tool.ID},
		}
	}

	return &core.MiddlewareResponse{}
}

// block 直接拒绝工具执行并回填会话历史。
func (mw *HITLMiddleware) block(ev *core.BeforeToolEvent, tokenMsg, toolMsg string) *core.MiddlewareResponse {
	select {
	case ev.TokenCh <- core.Token{Type: core.TokenTypeToolCall, Content: tokenMsg}:
	case <-ev.Ctx.Done():
	}
	ev.Aborted = true
	ev.ResponseMessages = []core.Message{
		{Role: "tool", Content: toolMsg, ToolCallID: ev.Tool.ID},
	}
	return &core.MiddlewareResponse{}
}

// reject 拒绝执行并回填 tool 结果消息。
func (mw *HITLMiddleware) reject(ev *core.BeforeToolEvent, msg string) *core.MiddlewareResponse {
	select {
	case ev.TokenCh <- core.Token{Type: core.TokenTypeToolResult, Content: msg}:
	case <-ev.Ctx.Done():
	}
	ev.Aborted = true
	ev.ResponseMessages = []core.Message{
		{Role: "tool", Content: msg, ToolCallID: ev.Tool.ID},
	}
	return &core.MiddlewareResponse{}
}
