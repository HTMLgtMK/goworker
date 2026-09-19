package middlewares

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-sandbox"
)

type captureEmitter struct {
	tokens []core.Token
}

func (e *captureEmitter) Emit(_ context.Context, tok core.Token) {
	e.tokens = append(e.tokens, tok)
}

func newToolEvent(name string, args map[string]any) (*core.BeforeToolEvent, *captureEmitter) {
	em := &captureEmitter{}
	return &core.BeforeToolEvent{
		Ctx:  context.Background(),
		Tool: &core.ToolCall{ID: "t1", Type: "function", Function: core.ToolCallFunction{Name: name}},
		Emit: em,
		Args: args,
	}, em
}

func interruptFor(t *testing.T, toks []core.Token, toolName string) (*hitl.InterruptRequest, bool) {
	t.Helper()
	for _, tok := range toks {
		if tok.Type != core.TokenTypeEvent || tok.Event == nil || tok.Event.Type != hitl.EventInterrupt {
			continue
		}
		var req hitl.InterruptRequest
		if err := json.Unmarshal(tok.Event.Data, &req); err != nil {
			t.Fatalf("decode interrupt: %v", err)
		}
		if req.ToolName == toolName {
			return &req, true
		}
	}
	return nil, false
}

func TestHITL_BashRiskyConfirmsAndApproves(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
	ev, em := newToolEvent("bash", map[string]any{"command": "rm -rf /tmp/goworker-test"})

	go func() { decisions <- hitl.Decision{InterruptID: "req-1", Type: hitl.DecisionApprove} }()
	mw.OnBeforeTool(ev)

	if ev.Abort != nil {
		t.Fatal("approved bash should not abort")
	}
	if _, ok := interruptFor(t, em.tokens, "bash"); !ok {
		t.Fatal("risky bash should trigger interrupt for confirmation")
	}
}

func TestHITL_BashReadonlyPassesWithoutConfirm(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(make(chan hitl.Decision)))
	ev, em := newToolEvent("bash", map[string]any{"command": `curl -s "https://wttr.in/Changsha?format=3&lang=zh" || echo "failed"`})

	mw.OnBeforeTool(ev)

	if ev.Abort != nil {
		t.Fatalf("readonly bash should pass through, got abort=%v", ev.Abort)
	}
	if _, ok := interruptFor(t, em.tokens, "bash"); ok {
		t.Fatal("readonly bash should not trigger interrupt")
	}
}

func TestHITL_MCPBlockedInStrictMode(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "strict"})
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(make(chan hitl.Decision)))
	ev, _ := newToolEvent("mcp_fs_read", map[string]any{"path": "/etc/passwd"})

	mw.OnBeforeTool(ev)

	if ev.Abort == nil {
		t.Fatal("MCP tool should be blocked in strict mode")
	}
	if len(ev.Abort.Messages) != 1 || !strings.Contains(ev.Abort.Messages[0].Content, "blocked") {
		t.Errorf("response = %+v", ev.Abort.Messages)
	}
}

func TestHITL_MCPConfirmsInNormalMode(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
	ev, em := newToolEvent("mcp_fs_write", map[string]any{"path": "/tmp/x", "content": "hi"})

	go func() { decisions <- hitl.Decision{InterruptID: "req-1", Type: hitl.DecisionApprove} }()
	mw.OnBeforeTool(ev)

	if ev.Abort != nil {
		t.Fatal("approved MCP tool should not abort")
	}
	if _, ok := interruptFor(t, em.tokens, "mcp_fs_write"); !ok {
		t.Fatal("MCP tool should trigger interrupt in normal mode")
	}
}

func TestHITL_MCPAllowedInOffMode(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "off"})
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(make(chan hitl.Decision)))
	ev, _ := newToolEvent("mcp_fs_read", map[string]any{"path": "/etc/passwd"})

	mw.OnBeforeTool(ev)

	if ev.Abort != nil {
		t.Fatal("MCP tool should pass through in off mode")
	}
}

func TestHITL_RespondBackfillsToolMessageWithCallID(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
	ev, _ := newToolEvent("bash", map[string]any{"command": "rm -rf /tmp/goworker-test"})

	go func() {
		decisions <- hitl.Decision{InterruptID: "req-1", Type: hitl.DecisionRespond, Message: "别删"}
	}()
	mw.OnBeforeTool(ev)

	if ev.Abort == nil {
		t.Fatal("respond should abort the tool")
	}
	if len(ev.Abort.Messages) != 1 {
		t.Fatalf("response msgs = %d, want 1", len(ev.Abort.Messages))
	}
	rm := ev.Abort.Messages[0]
	if rm.Role != "tool" {
		t.Errorf("role = %q, want tool", rm.Role)
	}
	if rm.ToolCallID != "t1" {
		t.Errorf("tool_call_id = %q, want t1", rm.ToolCallID)
	}
	if !strings.Contains(rm.Content, "别删") {
		t.Errorf("tool content should carry the user's reply, got %q", rm.Content)
	}
}

func TestHITL_BashInterruptCarriesRiskInfo(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
	ev, em := newToolEvent("bash", map[string]any{"command": "rm -rf /tmp/goworker-test"})

	go func() { decisions <- hitl.Decision{InterruptID: "req-1", Type: hitl.DecisionApprove} }()
	mw.OnBeforeTool(ev)

	if ev.Abort != nil {
		t.Fatal("approved bash should not abort")
	}
	req, ok := interruptFor(t, em.tokens, "bash")
	if !ok {
		t.Fatal("risky bash should trigger interrupt")
	}
	if req.RiskLevel != "R3" {
		t.Errorf("RiskLevel = %q, want R3", req.RiskLevel)
	}
	if len(req.Effects) == 0 || req.Effects[0] != "destructive" {
		t.Errorf("Effects = %v, want destructive first", req.Effects)
	}
}

func TestHITL_AuditRecordsUserDecision(t *testing.T) {
	dir := t.TempDir()
	audit, err := sandbox.OpenAudit(dir)
	if err != nil {
		t.Fatalf("OpenAudit: %v", err)
	}
	defer audit.Close()

	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions), WithAudit(audit))
	ev, _ := newToolEvent("bash", map[string]any{"command": "rm -rf /tmp/goworker-test"})

	go func() { decisions <- hitl.Decision{InterruptID: "req-1", Type: hitl.DecisionApprove} }()
	mw.OnBeforeTool(ev)

	b, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	line := string(b)
	for _, want := range []string{`"command":"rm -rf /tmp/goworker-test"`, `"risk_level":"R3"`, `"engine_decision":"hitl"`, `"user_decision":"approve"`, `"outcome":"executed"`} {
		if !strings.Contains(line, want) {
			t.Errorf("audit missing %s: %s", want, line)
		}
	}
}

func TestHITL_NonBashNonMCPNotTouched(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(make(chan hitl.Decision)))
	ev, _ := newToolEvent("read_file", map[string]any{"path": "/tmp/x"})

	mw.OnBeforeTool(ev)

	if ev.Abort != nil {
		t.Fatal("unrelated tool should pass through untouched")
	}
}

// ---- sys_*：risk_level × 沙箱模式 裁决矩阵 ----

func newSysToolEvent(name string, args map[string]any, riskLevel string) (*core.BeforeToolEvent, *captureEmitter) {
	ev, em := newToolEvent(name, args)
	ev.ToolDef = &core.Tool{Name: name, Metadata: map[string]string{"risk_level": riskLevel}}
	return ev, em
}

func TestHITL_SysRiskLevelMatrix(t *testing.T) {
	cases := []struct {
		mode  string
		level string
		want  string // allow | hitl | deny
	}{
		{"normal", "never", "allow"},
		{"normal", "mode", "hitl"},
		{"normal", "always", "hitl"},
		{"normal", "", "hitl"},      // 缺省 = mode
		{"normal", "bogus", "hitl"}, // 非法值 = mode
		{"strict", "never", "allow"},
		{"strict", "mode", "deny"},
		{"strict", "always", "deny"},
		{"readonly", "mode", "deny"},
		{"off", "never", "allow"},
		{"off", "mode", "allow"},
		{"off", "always", "hitl"},
	}
	for _, tc := range cases {
		t.Run(tc.mode+"/"+tc.level, func(t *testing.T) {
			cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: tc.mode})
			decisions := make(chan hitl.Decision, 1)
			mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
			var ev *core.BeforeToolEvent
			var em *captureEmitter
			if tc.level == "" || tc.level == "bogus" {
				ev, em = newToolEvent("sys_send_notification", map[string]any{"title": "t"}) // ToolDef 无 risk_level → 按 mode
			} else {
				ev, em = newSysToolEvent("sys_send_notification", map[string]any{"title": "t"}, tc.level)
			}

			switch tc.want {
			case "allow":
				resp := mw.OnBeforeTool(ev)
				if ev.Abort != nil {
					t.Fatalf("want allow, got aborted: %+v", ev.Abort)
				}
				if resp == nil {
					t.Fatal("nil response")
				}
			case "deny":
				mw.OnBeforeTool(ev)
				if ev.Abort == nil {
					t.Fatal("want deny (Abort), got pass-through")
				}
			case "hitl":
				go func() { decisions <- hitl.Decision{InterruptID: "req-1", Type: hitl.DecisionApprove} }()
				mw.OnBeforeTool(ev)
				if ev.Abort != nil {
					t.Fatalf("want confirm then approve, got aborted: %+v", ev.Abort)
				}
				if _, ok := interruptFor(t, em.tokens, "sys_send_notification"); !ok {
					t.Fatal("want interrupt token, got none")
				}
			}
		})
	}
}

// TestHITL_SysHitlCarriesDeclaredLevel：HITL 请求应携带声明档位（展示给用户）。
func TestHITL_SysHitlCarriesDeclaredLevel(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
	ev, em := newSysToolEvent("sys_send_sms", map[string]any{"to": "x"}, "always")

	go func() { decisions <- hitl.Decision{InterruptID: "req-1", Type: hitl.DecisionReject} }()
	mw.OnBeforeTool(ev)

	req, ok := interruptFor(t, em.tokens, "sys_send_sms")
	if !ok {
		t.Fatal("no interrupt emitted for sys tool")
	}
	if req.RiskLevel != "always" {
		t.Errorf("RiskLevel = %q, want always", req.RiskLevel)
	}
}
