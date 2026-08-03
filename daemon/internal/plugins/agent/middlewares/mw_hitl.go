package middlewares

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/sandbox"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

var reqID atomic.Int64

// HITLMiddleware 通过 DecisionProvider 对接沙箱检查，拦截风险工具调用。
// 覆盖两类工具：
//   - bash：走 sandbox 正则规则（deny 直接拒绝 / risky 需要确认）
//   - mcp_*：外部进程，sandbox 约束不到 —— strict/readonly 直接拒绝，normal 走 HITL 确认
type HITLMiddleware struct {
	sandboxCfg       sandbox.Config
	decisionProvider core.DecisionProvider
}

func NewHITLMiddleware(cfg sandbox.Config, dp core.DecisionProvider) *HITLMiddleware {
	return &HITLMiddleware{sandboxCfg: cfg, decisionProvider: dp}
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

	err := sandbox.Check(cmdStr, &mw.sandboxCfg)
	if err == nil {
		return &core.MiddlewareResponse{}
	}

	var needsConf *sandbox.NeedsConfirmationError
	if !errors.As(err, &needsConf) {
		// Denied/strict/readonly — block outright
		reason := err.Error()
		return mw.block(ev, "⛔ "+reason, "⛔ "+reason)
	}

	// Normal mode: HITL via DecisionProvider
	req := &spec.InterruptRequest{
		ID:         fmt.Sprintf("req-%d", reqID.Add(1)),
		ToolName:   "bash",
		Command:    cmdStr,
		RiskReason: needsConf.Reason,
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(30 * time.Second),
	}
	return mw.confirm(ev, req)
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
		return mw.confirm(ev, req)
	}
}

// ---- 公共 HITL 流程 ----

// confirm 走完整 HITL 决策流程：发 interrupt token → 等用户决策 → 处理。
func (mw *HITLMiddleware) confirm(ev *core.BeforeToolEvent, req *spec.InterruptRequest) *core.MiddlewareResponse {
	// 发送 interrupt token，通知前端展示确认选项
	select {
	case ev.TokenCh <- core.Token{Type: core.TokenTypeInterrupt, Interrupt: req}:
	case <-ev.Ctx.Done():
		return &core.MiddlewareResponse{}
	}

	// 通过 DecisionProvider 获取用户决策
	d := mw.decisionProvider.GetDecision(ev.Ctx, req)

	switch d.Type {
	case spec.DecisionApprove:
		// execute as-is
	case spec.DecisionEdit:
		// 仅 bash 支持命令编辑；MCP 工具没有 command 字段，编辑视为批准
		if req.ToolName == "bash" {
			if edited := strings.TrimSpace(d.Command); edited != "" {
				ev.Args["command"] = edited
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
