package middlewares

import (
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

// HITLMiddleware 通过 DecisionProvider 对接沙箱检查，拦截风险命令。
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
	if ev.Tool == nil || ev.Tool.Function.Name != "bash" {
		return &core.MiddlewareResponse{}
	}

	cmdStr, ok := ev.Args["command"].(string)
	if !ok || cmdStr == "" {
		return &core.MiddlewareResponse{}
	}

	err := sandbox.Check(cmdStr, &mw.sandboxCfg)
	if err == nil {
		return &core.MiddlewareResponse{}
	}

	// Denied/strict/readonly — block outright
	var needsConf *sandbox.NeedsConfirmationError
	if !errors.As(err, &needsConf) {
		select {
		case ev.TokenCh <- core.Token{Type: core.TokenTypeToolCall, Content: fmt.Sprintf("\n⛔ %v", err)}:
		case <-ev.Ctx.Done():
		}
		ev.Aborted = true
		ev.ResponseMessages = []core.Message{
			{Role: "tool", Content: "⛔ " + err.Error(), ToolCallID: ev.Tool.ID},
		}
		return &core.MiddlewareResponse{}
	}

	// ---- Normal mode: HITL via DecisionProvider ----

	req := &spec.InterruptRequest{
		ID:         fmt.Sprintf("req-%d", reqID.Add(1)),
		ToolName:   "bash",
		Command:    cmdStr,
		RiskReason: needsConf.Reason,
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(30 * time.Second),
	}
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
		if edited := strings.TrimSpace(d.Command); edited != "" {
			ev.Args["command"] = edited
		}
	case spec.DecisionReject:
		select {
		case ev.TokenCh <- core.Token{Type: core.TokenTypeToolResult, Content: "⛔ rejected by user"}:
		case <-ev.Ctx.Done():
		}
		ev.Aborted = true
		ev.ResponseMessages = []core.Message{
			{Role: "tool", Content: "⛔ rejected by user", ToolCallID: ev.Tool.ID},
		}
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
		ev.ResponseMessages = []core.Message{
			{Role: "user", Content: msg},
		}
	}

	return &core.MiddlewareResponse{}
}
