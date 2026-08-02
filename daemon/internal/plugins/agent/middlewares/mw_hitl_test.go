package middlewares

import (
	"context"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/sandbox"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// drainTokens 读完 buffered token channel（不含阻塞）。
func drainTokens(ch chan core.Token) []core.Token {
	var out []core.Token
	for {
		select {
		case tok := <-ch:
			out = append(out, tok)
		default:
			return out
		}
	}
}

func TestHITL_BashRiskyConfirmsAndApproves(t *testing.T) {
	cfg := sandbox.NewFromConfig(&config.SandboxConfig{Mode: "normal"})
	decisions := make(chan spec.HITLDecision, 1)
	mw := NewHITLMiddleware(*cfg, NewChannelDecisionProvider(decisions))

	tokenCh := make(chan core.Token, 10)
	ev := &core.BeforeToolEvent{
		Ctx:     context.Background(),
		Tool:    &core.ToolCall{ID: "t1", Type: "function", Function: core.ToolCallFunction{Name: "bash"}},
		TokenCh: tokenCh,
		Args:    map[string]any{"command": "rm -rf /tmp/goworker-test"},
	}
	go func() {
		decisions <- spec.HITLDecision{InterruptID: "req-1", Type: spec.DecisionApprove}
	}()
	mw.OnBeforeTool(ev)

	if ev.Aborted {
		t.Fatal("approved bash should not abort")
	}
	toks := drainTokens(tokenCh)
	var interrupted bool
	for _, tok := range toks {
		if tok.Type == core.TokenTypeInterrupt && tok.Interrupt != nil && tok.Interrupt.ToolName == "bash" {
			interrupted = true
		}
	}
	if !interrupted {
		t.Fatal("risky bash should trigger interrupt for confirmation")
	}
}

func TestHITL_BashReadonlyPassesWithoutConfirm(t *testing.T) {
	cfg := sandbox.NewFromConfig(&config.SandboxConfig{Mode: "normal"})
	// 只读命令不应触发 HITL，channel 留空也安全
	mw := NewHITLMiddleware(*cfg, NewChannelDecisionProvider(make(chan spec.HITLDecision)))

	tokenCh := make(chan core.Token, 10)
	ev := &core.BeforeToolEvent{
		Ctx:     context.Background(),
		Tool:    &core.ToolCall{ID: "t1", Type: "function", Function: core.ToolCallFunction{Name: "bash"}},
		TokenCh: tokenCh,
		Args:    map[string]any{"command": `curl -s "https://wttr.in/Changsha?format=3&lang=zh" || echo "failed"`},
	}
	mw.OnBeforeTool(ev)

	if ev.Aborted || len(ev.ResponseMessages) != 0 {
		t.Fatalf("readonly bash should pass through, got aborted=%v msgs=%v", ev.Aborted, ev.ResponseMessages)
	}
	toks := drainTokens(tokenCh)
	for _, tok := range toks {
		if tok.Type == core.TokenTypeInterrupt {
			t.Fatalf("readonly bash should not trigger interrupt, got %+v", tok)
		}
	}
}

func TestHITL_MCPBlockedInStrictMode(t *testing.T) {
	cfg := sandbox.NewFromConfig(&config.SandboxConfig{Mode: "strict"})
	// strict 模式直接拒绝，不会调用 DecisionProvider，channel 留空也安全
	mw := NewHITLMiddleware(*cfg, NewChannelDecisionProvider(make(chan spec.HITLDecision)))

	tokenCh := make(chan core.Token, 10)
	ev := &core.BeforeToolEvent{
		Ctx:     context.Background(),
		Tool:    &core.ToolCall{ID: "t1", Type: "function", Function: core.ToolCallFunction{Name: "mcp_fs_read"}},
		TokenCh: tokenCh,
		Args:    map[string]any{"path": "/etc/passwd"},
	}
	mw.OnBeforeTool(ev)

	if !ev.Aborted {
		t.Fatal("MCP tool should be blocked in strict mode")
	}
	if len(ev.ResponseMessages) != 1 || !strings.Contains(ev.ResponseMessages[0].Content, "blocked") {
		t.Errorf("response = %+v", ev.ResponseMessages)
	}
}

func TestHITL_MCPConfirmsInNormalMode(t *testing.T) {
	cfg := sandbox.NewFromConfig(&config.SandboxConfig{Mode: "normal"})
	decisions := make(chan spec.HITLDecision, 1)
	mw := NewHITLMiddleware(*cfg, NewChannelDecisionProvider(decisions))

	tokenCh := make(chan core.Token, 10)
	ev := &core.BeforeToolEvent{
		Ctx:     context.Background(),
		Tool:    &core.ToolCall{ID: "t1", Type: "function", Function: core.ToolCallFunction{Name: "mcp_fs_write"}},
		TokenCh: tokenCh,
		Args:    map[string]any{"path": "/tmp/x", "content": "hi"},
	}
	go func() {
		decisions <- spec.HITLDecision{InterruptID: "req-1", Type: spec.DecisionApprove}
	}()
	mw.OnBeforeTool(ev)

	if ev.Aborted {
		t.Fatal("approved MCP tool should not abort")
	}
	toks := drainTokens(tokenCh)
	var interrupted bool
	for _, tok := range toks {
		if tok.Type == core.TokenTypeInterrupt && tok.Interrupt != nil && tok.Interrupt.ToolName == "mcp_fs_write" {
			interrupted = true
		}
	}
	if !interrupted {
		t.Fatal("MCP tool should trigger interrupt in normal mode")
	}
}

func TestHITL_MCPAllowedInOffMode(t *testing.T) {
	cfg := sandbox.NewFromConfig(&config.SandboxConfig{Mode: "off"})
	mw := NewHITLMiddleware(*cfg, NewChannelDecisionProvider(make(chan spec.HITLDecision)))

	tokenCh := make(chan core.Token, 10)
	ev := &core.BeforeToolEvent{
		Ctx:     context.Background(),
		Tool:    &core.ToolCall{ID: "t1", Type: "function", Function: core.ToolCallFunction{Name: "mcp_fs_read"}},
		TokenCh: tokenCh,
		Args:    map[string]any{"path": "/etc/passwd"},
	}
	mw.OnBeforeTool(ev)

	if ev.Aborted || len(ev.ResponseMessages) != 0 {
		t.Fatal("MCP tool should pass through in off mode")
	}
}

func TestHITL_NonBashNonMCPNotTouched(t *testing.T) {
	cfg := sandbox.NewFromConfig(&config.SandboxConfig{Mode: "normal"})
	mw := NewHITLMiddleware(*cfg, NewChannelDecisionProvider(make(chan spec.HITLDecision)))

	tokenCh := make(chan core.Token, 10)
	ev := &core.BeforeToolEvent{
		Ctx:     context.Background(),
		Tool:    &core.ToolCall{ID: "t1", Type: "function", Function: core.ToolCallFunction{Name: "read_file"}},
		TokenCh: tokenCh,
		Args:    map[string]any{"path": "/tmp/x"},
	}
	mw.OnBeforeTool(ev)

	if ev.Aborted || len(ev.ResponseMessages) != 0 {
		t.Fatal("unrelated tool should pass through untouched")
	}
}
