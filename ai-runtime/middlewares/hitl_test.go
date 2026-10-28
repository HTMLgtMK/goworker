package middlewares

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-sandbox"
)

type captureEmitter struct {
	mu     sync.Mutex
	tokens []core.Token
	ids    chan string // 每观测到一个 interrupt 事件,送入其 InterruptRequest.ID
}

func (e *captureEmitter) Emit(_ context.Context, tok core.Token) {
	e.mu.Lock()
	e.tokens = append(e.tokens, tok)
	e.mu.Unlock()
	if tok.Type == core.TokenTypeEvent && tok.Event != nil && tok.Event.Type == hitl.EventInterrupt {
		var req hitl.InterruptRequest
		if err := json.Unmarshal(tok.Event.Data, &req); err == nil {
			// 决策回执契约:Decision.InterruptID 必须等于 InterruptRequest.ID
			// (GetDecision 仅消费 InterruptID 匹配的决策,P4 评审 issue1 的
			// 排空语义)。测试按真实 ID 回执,不猜测 req-N(进程级原子计数,
			// 序号随测试执行序漂移)。
			select {
			case e.ids <- req.ID:
			default:
			}
		}
	}
}

// pushDecisionFor 按真实中断 ID 回执决策:后台等待 em 发出的第一个 interrupt,
// 用其 ID 填充 d 后投进 decisions(主 goroutine 此刻阻塞在 GetDecision 的
// select 上)。5s 等不到即放弃——测试主体会按超时拒绝收敛,断言自会失败。
func pushDecisionFor(decisions chan hitl.Decision, em *captureEmitter, d hitl.Decision) {
	go func() {
		select {
		case id := <-em.ids:
			d.InterruptID = id
			decisions <- d
		case <-time.After(5 * time.Second):
		}
	}()
}

func newToolEvent(name string, args map[string]any) (*core.BeforeToolEvent, *captureEmitter) {
	em := &captureEmitter{ids: make(chan string, 8)}
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

	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApprove})
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

	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApprove})
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

func TestHITL_MCPAllowedInAutoMode(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "auto"})
	decisions := make(chan hitl.Decision, 1)
	decisions <- hitl.Decision{Type: hitl.DecisionReject}
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
	ev, em := newToolEvent("mcp_fs_write", map[string]any{"path": "/tmp/x"})

	mw.OnBeforeTool(ev)

	if ev.Abort != nil {
		t.Fatal("MCP tool should pass through in auto mode")
	}
	if _, ok := interruptFor(t, em.tokens, "mcp_fs_write"); ok {
		t.Fatal("auto mode should not trigger interrupt")
	}
}

func TestHITL_AllowAlwaysPersistsRuleBeforeExecution(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	var saved sandbox.AllowRule
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions), WithAllowAlways(func(rule sandbox.AllowRule) error {
		saved = rule
		return nil
	}))
	ev, em := newToolEvent("bash", map[string]any{"command": "git push origin main"})
	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApproveAlways})

	mw.OnBeforeTool(ev)

	if ev.Abort != nil {
		t.Fatalf("allow_always should execute after persistence, abort=%v", ev.Abort)
	}
	if len(saved.MatchTokens) == 0 || saved.MaxRisk == 0 {
		t.Fatalf("saved rule = %+v, want normalized command and risk", saved)
	}
}

func TestHITL_AllowAlwaysPersistenceFailureRejects(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions), WithAllowAlways(func(sandbox.AllowRule) error {
		return errors.New("save failed")
	}))
	ev, em := newToolEvent("bash", map[string]any{"command": "git push origin main"})
	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApproveAlways})

	mw.OnBeforeTool(ev)

	if ev.Abort == nil {
		t.Fatal("allow_always persistence failure must reject execution")
	}
}

func TestHITL_EditDoesNotPersistAlwaysRule(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	persisted := false
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions), WithAllowAlways(func(sandbox.AllowRule) error {
		persisted = true
		return nil
	}))
	ev, em := newToolEvent("bash", map[string]any{"command": "rm -rf /tmp/goworker-test"})
	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionEdit, Command: "echo safe"})

	mw.OnBeforeTool(ev)

	if ev.Abort != nil || ev.Args["command"] != "echo safe" {
		t.Fatalf("edited command result: abort=%v args=%v", ev.Abort, ev.Args)
	}
	if persisted {
		t.Fatal("edit must not persist an always rule")
	}
}

func TestHITL_RespondBackfillsToolMessageWithCallID(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
	ev, em := newToolEvent("bash", map[string]any{"command": "rm -rf /tmp/goworker-test"})

	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionRespond, Message: "别删"})
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

	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApprove})
	mw.OnBeforeTool(ev)

	if ev.Abort != nil {
		t.Fatal("approved bash should not abort")
	}
	req, ok := interruptFor(t, em.tokens, "bash")
	if !ok {
		t.Fatal("risky bash should trigger interrupt")
	}
	if req.RiskLevel != "elevated" {
		t.Errorf("RiskLevel = %q, want elevated", req.RiskLevel)
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
	ev, em := newToolEvent("bash", map[string]any{"command": "rm -rf /tmp/goworker-test"})

	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApprove})
	mw.OnBeforeTool(ev)

	b, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	line := string(b)
	for _, want := range []string{`"command":"rm -rf /tmp/goworker-test"`, `"risk_level":"elevated"`, `"engine_decision":"hitl"`, `"user_decision":"approve"`, `"outcome":"executed"`} {
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
	ev.ToolDef = &core.Tool{Name: name, Metadata: map[string]string{
		sandbox.ToolMetadataRiskLevel:          riskLevel,
		sandbox.ToolMetadataRequiresPermission: "true",
	}}
	return ev, em
}

func TestHITL_SysRiskLevelMatrix(t *testing.T) {
	cases := []struct {
		mode  string
		level string
		want  string // allow | hitl | deny
	}{
		{"normal", "low", "allow"},
		{"normal", "unclassified", "hitl"},
		{"normal", "critical", "hitl"},
		{"strict", "low", "allow"},
		{"strict", "unclassified", "deny"},
		{"strict", "critical", "deny"},
		{"readonly", "unclassified", "deny"},
		{"off", "low", "allow"},
		{"off", "critical", "allow"},
	}
	for _, tc := range cases {
		t.Run(tc.mode+"/"+tc.level, func(t *testing.T) {
			cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: tc.mode})
			decisions := make(chan hitl.Decision, 1)
			mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
			ev, em := newSysToolEvent("sys_send_notification", map[string]any{"title": "t"}, tc.level)

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
				pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApprove})
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
	ev, em := newSysToolEvent("sys_send_sms", map[string]any{"to": "x"}, "critical")

	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionReject})
	mw.OnBeforeTool(ev)

	req, ok := interruptFor(t, em.tokens, "sys_send_sms")
	if !ok {
		t.Fatal("no interrupt emitted for sys tool")
	}
	if req.RiskLevel != "critical" {
		t.Errorf("RiskLevel = %q, want critical", req.RiskLevel)
	}
}

func TestHITL_MarkedToolRiskMatrix(t *testing.T) {
	cases := []struct {
		mode  string
		level string
		want  string
	}{
		{mode: "normal", level: "benign", want: "allow"},
		{mode: "normal", level: "low", want: "allow"},
		{mode: "normal", level: "medium", want: "hitl"},
		{mode: "normal", level: "critical", want: "hitl"},
		{mode: "normal", level: "unclassified", want: "hitl"},
		{mode: "auto", level: "low", want: "allow"},
		{mode: "auto", level: "medium", want: "hitl"},
		{mode: "strict", level: "low", want: "allow"},
		{mode: "strict", level: "medium", want: "deny"},
		{mode: "readonly", level: "reserved", want: "deny"},
		{mode: "off", level: "reserved", want: "allow"},
	}
	for _, tc := range cases {
		t.Run(tc.mode+"/"+tc.level, func(t *testing.T) {
			cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: tc.mode})
			decisions := make(chan hitl.Decision, 1)
			mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
			ev, em := newSysToolEvent("custom_operation", map[string]any{"value": "x"}, tc.level)
			ev.Tool.Function.Name = "custom_operation"

			switch tc.want {
			case "allow":
				mw.OnBeforeTool(ev)
				if ev.Abort != nil {
					t.Fatalf("want allow, got abort: %+v", ev.Abort)
				}
				if _, ok := interruptFor(t, em.tokens, ev.Tool.Function.Name); ok {
					t.Fatal("allow must not emit interrupt")
				}
			case "deny":
				mw.OnBeforeTool(ev)
				if ev.Abort == nil {
					t.Fatal("want deny, got pass-through")
				}
			case "hitl":
				pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApprove})
				mw.OnBeforeTool(ev)
				if ev.Abort != nil {
					t.Fatalf("approved tool aborted: %+v", ev.Abort)
				}
				if _, ok := interruptFor(t, em.tokens, ev.Tool.Function.Name); !ok {
					t.Fatal("want interrupt, got none")
				}
			}
		})
	}
}

func TestHITL_MissingOrInvalidMarkedRiskFailsClosedThroughHITL(t *testing.T) {
	for _, risk := range []string{"", "not-a-risk"} {
		t.Run(risk, func(t *testing.T) {
			cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
			decisions := make(chan hitl.Decision, 1)
			mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
			ev, em := newToolEvent("custom_operation", nil)
			ev.ToolDef = &core.Tool{Name: "custom_operation", Metadata: map[string]string{
				sandbox.ToolMetadataRiskLevel:          risk,
				sandbox.ToolMetadataRequiresPermission: "true",
			}}
			pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionReject})
			mw.OnBeforeTool(ev)
			if ev.Abort == nil {
				t.Fatal("unclassified marked tool must not pass through")
			}
			if _, ok := interruptFor(t, em.tokens, "custom_operation"); !ok {
				t.Fatal("unclassified marked tool must emit interrupt")
			}
		})
	}
}

func TestHITL_UnmarkedInternalToolPassesThrough(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "strict"})
	mw := NewHITLMiddleware(*cfg, nil)
	ev, _ := newToolEvent("internal_operation", map[string]any{"value": "x"})
	ev.ToolDef = &core.Tool{Name: "internal_operation"}

	mw.OnBeforeTool(ev)
	if ev.Abort != nil {
		t.Fatalf("unmarked internal tool must pass through: %+v", ev.Abort)
	}
}

func TestHITL_NilDecisionProviderRejectsMarkedTool(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	mw := NewHITLMiddleware(*cfg, nil)
	ev, _ := newSysToolEvent("custom_operation", nil, "critical")

	mw.OnBeforeTool(ev)
	if ev.Abort == nil {
		t.Fatal("nil decision provider must fail closed")
	}
}

func TestHITL_ExternalAllowAlwaysPersistsStableRuleAndBypassesNextCall(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	var saved sandbox.ToolAllowRule
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions), WithToolAllowAlways(func(rule sandbox.ToolAllowRule) error {
		saved = rule
		return nil
	}))

	ev, em := newSysToolEvent("custom_operation", map[string]any{"secret": "must-not-persist"}, "elevated")
	ev.ToolDef.Metadata[sandbox.ToolMetadataAllowAlwaysKey] = "custom-operation"
	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApproveAlways})
	mw.OnBeforeTool(ev)
	if ev.Abort != nil {
		t.Fatalf("approved external tool aborted: %+v", ev.Abort)
	}
	if saved.Key != "custom-operation" || saved.MaxRisk != sandbox.RiskElevated {
		t.Fatalf("saved rule = %+v", saved)
	}
	if strings.Contains(saved.Desc, "secret") {
		t.Fatal("tool arguments must not be persisted in rule")
	}
	if _, ok := interruptFor(t, em.tokens, "custom_operation"); !ok {
		t.Fatal("first call should emit an interrupt")
	}
	req, ok := interruptFor(t, em.tokens, "custom_operation")
	if !ok || req.AllowAlwaysKey != "custom-operation" {
		t.Fatalf("interrupt allow_always_key = %q, want custom-operation", req.AllowAlwaysKey)
	}

	next, nextEmitter := newSysToolEvent("custom_operation", map[string]any{"secret": "different"}, "elevated")
	next.ToolDef.Metadata[sandbox.ToolMetadataAllowAlwaysKey] = "custom-operation"
	mw.OnBeforeTool(next)
	if next.Abort != nil {
		t.Fatalf("matching external rule should bypass HITL: %+v", next.Abort)
	}
	if _, ok := interruptFor(t, nextEmitter.tokens, "custom_operation"); ok {
		t.Fatal("matching external rule must not emit interrupt")
	}
}

func TestHITL_ExternalAllowAlwaysPersistenceFailureRejects(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{Mode: "normal"})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions), WithToolAllowAlways(func(sandbox.ToolAllowRule) error {
		return errors.New("save failed")
	}))
	ev, em := newSysToolEvent("custom_operation", nil, "elevated")
	ev.ToolDef.Metadata[sandbox.ToolMetadataAllowAlwaysKey] = "custom-operation"
	pushDecisionFor(decisions, em, hitl.Decision{Type: hitl.DecisionApproveAlways})

	mw.OnBeforeTool(ev)
	if ev.Abort == nil {
		t.Fatal("external allow_always persistence failure must reject")
	}
}

func TestHITL_ExternalAllowAlwaysCannotBypassStrictMode(t *testing.T) {
	cfg := sandbox.NewFromConfig(&sandbox.SandboxConfig{
		Mode:           "strict",
		ToolAllowRules: []sandbox.ToolAllowRuleConfig{{Key: "custom-operation", MaxRisk: "reserved"}},
	})
	decisions := make(chan hitl.Decision, 1)
	mw := NewHITLMiddleware(*cfg, hitl.NewChannelDecisionProvider(decisions))
	ev, _ := newSysToolEvent("custom_operation", nil, "elevated")
	ev.ToolDef.Metadata[sandbox.ToolMetadataAllowAlwaysKey] = "custom-operation"

	mw.OnBeforeTool(ev)
	if ev.Abort == nil {
		t.Fatal("strict mode must reject despite matching external rule")
	}
}

func TestHITL_ToolAllowRuleMatchesStableKeyAndRisk(t *testing.T) {
	cases := []struct {
		name   string
		policy sandbox.ToolPolicyMetadata
		rules  []sandbox.ToolAllowRule
		want   bool
	}{
		{
			name:   "same key within risk",
			policy: sandbox.ToolPolicyMetadata{RiskLevel: sandbox.RiskElevated, AllowAlwaysKey: "tool"},
			rules:  []sandbox.ToolAllowRule{{Key: "tool", MaxRisk: sandbox.RiskElevated}},
			want:   true,
		},
		{
			name:   "different key",
			policy: sandbox.ToolPolicyMetadata{RiskLevel: sandbox.RiskMedium, AllowAlwaysKey: "other"},
			rules:  []sandbox.ToolAllowRule{{Key: "tool", MaxRisk: sandbox.RiskReserved}},
			want:   false,
		},
		{
			name:   "risk upgrade",
			policy: sandbox.ToolPolicyMetadata{RiskLevel: sandbox.RiskHigh, AllowAlwaysKey: "tool"},
			rules:  []sandbox.ToolAllowRule{{Key: "tool", MaxRisk: sandbox.RiskElevated}},
			want:   false,
		},
		{
			name:   "empty key",
			policy: sandbox.ToolPolicyMetadata{RiskLevel: sandbox.RiskMedium},
			rules:  []sandbox.ToolAllowRule{{Key: "tool", MaxRisk: sandbox.RiskReserved}},
			want:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := toolAllowRuleMatches(tc.rules, tc.policy); got != tc.want {
				t.Errorf("toolAllowRuleMatches() = %t, want %t", got, tc.want)
			}
		})
	}
}
