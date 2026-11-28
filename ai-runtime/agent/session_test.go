package agent

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/middlewares"
	"github.com/tinguo/goworker/ai-sandbox"
)

// testSession 构造一个带 fake provider 的 Session，绕开真实 LLM 端点。
// NewProvider 注入点让 Session.Run 可以被单元测试直接驱动 —— 这是把会话状态
// 收敛进 Session 带来的附带收益：原来 handleAgent 直接 NewOpenAIProvider 无法测试。
func testSession(pv core.Provider) (*Session, *runtimeconfig.Config) {
	cfg := runtimeconfig.Default()
	s := NewSession(SessionDeps{
		Config:       cfg,
		AuditDir:     "",
		Memory:       nil,
		CollectTools: func(*sandbox.Config, ToolCollectionOptions) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
	})
	return s, cfg
}

func TestSessionCompactKeepsConversationWhenSummaryUnusable(t *testing.T) {
	usage := &core.UsageInfo{PromptTokens: 19, CompletionTokens: 3, TotalTokens: 22}
	provider := &emptyChoiceProvider{usage: usage}
	s, cfg := testSession(provider)
	cfg.LLM.CompactKeep = 1
	history := []core.Message{{Role: "user", Content: "one"}, {Role: "user", Content: "two"}, {Role: "user", Content: "three"}}
	s.mu.Lock()
	s.conversation = history
	s.mu.Unlock()

	if err := s.Compact(context.Background(), RunCallbacks{}); err != nil {
		t.Fatalf("Compact error = %v", err)
	}
	if got := s.Conversation(); !reflect.DeepEqual(got, history) {
		t.Errorf("conversation after failed compact = %+v, want original %+v", got, history)
	}
	calls, compactions, total := s.UsageSnapshot()
	if len(calls) != 1 || calls[0].TotalTokens != usage.TotalTokens {
		t.Errorf("usage calls = %+v, want exactly one call with %d tokens", calls, usage.TotalTokens)
	}
	if total.TotalTokens != usage.TotalTokens {
		t.Errorf("total tokens = %d, want %d", total.TotalTokens, usage.TotalTokens)
	}
	if len(compactions) != 0 {
		t.Errorf("compactions = %+v, want none", compactions)
	}
}

func TestSessionCompressorRecordsUsageWithoutCompaction(t *testing.T) {
	usage := &core.UsageInfo{PromptTokens: 17, CompletionTokens: 5, TotalTokens: 22}
	provider := &emptyChoiceProvider{usage: usage}
	s, cfg := testSession(provider)
	cfg.LLM.CompactKeep = 1
	window := contextWindow(cfg)
	compressor := s.newCompressor(provider, cfg, false)
	history := []core.Message{{Role: "user", Content: "one"}, {Role: "user", Content: "two"}, {Role: "user", Content: "three"}}

	if _, err := compressor.Compress(context.Background(), history); err == nil {
		t.Fatal("Compress error = nil, want empty response error")
	}
	calls, compactions, total := s.UsageSnapshot()
	if len(calls) != 1 {
		t.Fatalf("usage calls = %d, want 1", len(calls))
	}
	if got := calls[0]; got.PromptTokens != 17 || got.CompletionTokens != 5 || got.TotalTokens != 22 || got.ContextWindow != window {
		t.Errorf("usage call = %+v, want provider usage with context window %d", got, window)
	}
	if total.TotalTokens != 22 {
		t.Errorf("total tokens = %d, want 22", total.TotalTokens)
	}
	if len(compactions) != 0 {
		t.Errorf("compactions = %+v, want none for failed compression", compactions)
	}
}

type emptyChoiceProvider struct {
	usage *core.UsageInfo
}

func (p *emptyChoiceProvider) Name() string  { return "empty" }
func (p *emptyChoiceProvider) Model() string { return "model" }
func (p *emptyChoiceProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{Usage: p.usage}, nil
}
func (p *emptyChoiceProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return make(chan core.Token), nil
}

func TestSessionCompressorEstimatesUsageForUnusableSummary(t *testing.T) {
	tests := []struct {
		name        string
		usage       *core.UsageInfo
		choices     []core.ResponseChoice
		nilResponse bool
	}{
		{name: "empty response without usage"},
		{
			name:    "blank summary with usage",
			usage:   &core.UsageInfo{PromptTokens: 13, CompletionTokens: 2, TotalTokens: 15},
			choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "  \n  "}}},
		},
		{name: "nil response without usage", nilResponse: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &unusableSummaryProvider{usage: tt.usage, choices: tt.choices, nilResponse: tt.nilResponse}
			s, cfg := testSession(provider)
			cfg.LLM.CompactKeep = 1
			window := contextWindow(cfg)
			compressor := s.newCompressor(provider, cfg, false)
			history := []core.Message{{Role: "user", Content: "one"}, {Role: "user", Content: "two"}, {Role: "user", Content: "three"}}

			if _, err := compressor.Compress(context.Background(), history); err == nil {
				t.Fatal("Compress error = nil, want empty summary error")
			}
			calls, compactions, total := s.UsageSnapshot()
			if len(calls) != 1 {
				t.Fatalf("usage calls = %d, want 1", len(calls))
			}
			got := calls[0]
			if got.ContextWindow != window || got.EstimateTokens == 0 {
				t.Errorf("usage call = %+v, want estimated tokens and context window %d", got, window)
			}
			if tt.usage != nil && (got.PromptTokens != 13 || got.CompletionTokens != 2 || got.TotalTokens != 15) {
				t.Errorf("usage call = %+v, want provider usage %+v", got, tt.usage)
			}
			if len(compactions) != 0 {
				t.Errorf("compactions = %+v, want none for failed compression", compactions)
			}
			if total.TotalTokens != got.TotalTokens {
				t.Errorf("total usage = %+v, does not include exactly one failed compression call", total)
			}
		})
	}
}

type unusableSummaryProvider struct {
	usage       *core.UsageInfo
	choices     []core.ResponseChoice
	nilResponse bool
}

func (p *unusableSummaryProvider) Name() string  { return "unusable" }
func (p *unusableSummaryProvider) Model() string { return "model" }
func (p *unusableSummaryProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	if p.nilResponse {
		return nil, nil
	}
	return &core.ChatResponse{Choices: p.choices, Usage: p.usage}, nil
}
func (p *unusableSummaryProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return make(chan core.Token), nil
}

func TestSessionCompressorConcurrentUnusableSummariesAreCountedOnce(t *testing.T) {
	provider := &unusableSummaryProvider{usage: &core.UsageInfo{PromptTokens: 10, CompletionTokens: 2, TotalTokens: 12}}
	s, cfg := testSession(provider)
	cfg.LLM.CompactKeep = 1
	compressor := s.newCompressor(provider, cfg, false)
	history := []core.Message{{Role: "user", Content: "one"}, {Role: "user", Content: "two"}, {Role: "user", Content: "three"}}

	const calls = 8
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := compressor.Compress(context.Background(), history); err == nil {
				t.Error("Compress error = nil, want empty response error")
			}
		}()
	}
	wg.Wait()

	usageCalls, compactions, total := s.UsageSnapshot()
	if len(usageCalls) != calls {
		t.Errorf("usage calls = %d, want %d", len(usageCalls), calls)
	}
	if total.TotalTokens != calls*12 {
		t.Errorf("total tokens = %d, want %d", total.TotalTokens, calls*12)
	}
	if len(compactions) != 0 {
		t.Errorf("compactions = %d, want 0", len(compactions))
	}
}

// interruptIDEmitter 捕获 interrupt 事件的请求 ID:决策回执契约要求
// Decision.InterruptID == InterruptRequest.ID(GetDecision 仅消费 InterruptID
// 匹配的决策,P4 评审 issue1 的排空语义),测试按真实 ID 回执而非猜测 req-N
// (进程级原子计数,序号随测试执行序漂移)。
type interruptIDEmitter struct {
	ids chan string
}

func (e *interruptIDEmitter) Emit(_ context.Context, tok core.Token) {
	if tok.Type == core.TokenTypeEvent && tok.Event != nil && tok.Event.Type == hitl.EventInterrupt {
		var req hitl.InterruptRequest
		if err := json.Unmarshal(tok.Event.Data, &req); err == nil {
			select {
			case e.ids <- req.ID:
			default:
			}
		}
	}
}

func TestSessionToolAllowRulesSnapshotAndRefresh(t *testing.T) {
	cfg := runtimeconfig.Default()
	persisted := 0
	s := NewSession(SessionDeps{
		Config: cfg,
		PersistToolAllowRule: func(sandbox.ToolAllowRule) error {
			persisted++
			return nil
		},
	})
	if got := s.toolAllowRulesSnapshot(); len(got) != 0 {
		t.Fatalf("initial tool rule snapshot = %+v, want empty", got)
	}
	cfg.Sandbox.ToolAllowRules = append(cfg.Sandbox.ToolAllowRules, sandbox.ToolAllowRuleConfig{Key: "config-only", MaxRisk: "reserved"})
	if got := s.toolAllowRulesSnapshot(); len(got) != 0 {
		t.Fatalf("session snapshot changed through config mutation: %+v", got)
	}

	provider := &captureProvider{}
	sandboxCfg := sandbox.NewFromConfig(&cfg.Sandbox)
	decisions := make(chan hitl.Decision, 1)
	mws := s.buildMiddlewareChain(RunCallbacks{}, provider, sandboxCfg, cfg, decisions, &runErrorCapture{})
	mw := mws[0].(*middlewares.HITLMiddleware)
	em := &interruptIDEmitter{ids: make(chan string, 1)}
	ev := &core.BeforeToolEvent{
		Ctx:  context.Background(),
		Tool: &core.ToolCall{ID: "call-1", Function: core.ToolCallFunction{Name: "custom_write"}},
		Emit: em,
		ToolDef: &core.Tool{
			Name: "custom_write",
			Metadata: map[string]string{
				sandbox.ToolMetadataRiskLevel:          "elevated",
				sandbox.ToolMetadataRequiresPermission: "true",
				sandbox.ToolMetadataAllowAlwaysKey:     "custom-write",
			},
		},
	}
	// 决策按真实中断 ID 回执:主 goroutine 阻塞在 GetDecision,后台等
	// interrupt 事件拿到 ID 后投递;5s 等不到即放弃,断言自会失败。
	go func() {
		select {
		case id := <-em.ids:
			decisions <- hitl.Decision{InterruptID: id, Type: hitl.DecisionApproveAlways}
		case <-time.After(5 * time.Second):
		}
	}()
	mw.OnBeforeTool(ev)
	if ev.Abort != nil {
		t.Fatalf("allow_always aborted: %+v", ev.Abort)
	}
	if persisted != 1 {
		t.Fatalf("persist calls = %d, want 1", persisted)
	}
	got := s.toolAllowRulesSnapshot()
	if len(got) != 1 || got[0].Key != "custom-write" || got[0].MaxRisk != sandbox.RiskElevated {
		t.Fatalf("refreshed session snapshot = %+v", got)
	}
}

func testRunCallbacks(buf *strings.Builder) RunCallbacks {
	return RunCallbacks{
		Write: func(s string) { buf.WriteString(s) },
		WriteToken: func(_ RenderKind, s string, _ bool) {
			buf.WriteString(s)
		},
	}
}

func TestRenderKind_Thinking(t *testing.T) {
	kind, content := renderKind(core.Token{Type: core.TokenTypeThinking, Content: "reasoning"})
	if kind != KindThinking || content != "reasoning" {
		t.Errorf("renderKind = (%q, %q), want (%q, reasoning)", kind, content, KindThinking)
	}
}

func TestSessionRun_HidesThinkingWithoutDroppingHistory(t *testing.T) {
	provider := &captureProvider{response: core.Message{
		Role:     "assistant",
		Content:  "answer",
		Thinking: core.Thinking{Text: "reasoning"},
	}}
	s, cfg := testSession(provider)
	cfg.LLM.Thinking.Show = false

	var kinds []RenderKind
	var output strings.Builder
	callbacks := RunCallbacks{
		Write: func(value string) { output.WriteString(value) },
		WriteToken: func(kind RenderKind, content string, _ bool) {
			kinds = append(kinds, kind)
			output.WriteString(content)
		},
	}
	if _, err := s.Run(context.Background(), RunRequest{Input: "hello"}, callbacks); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, kind := range kinds {
		if kind == KindThinking {
			t.Fatalf("thinking should be hidden, kinds = %v", kinds)
		}
	}
	if strings.Contains(output.String(), "reasoning") {
		t.Errorf("output = %q, should hide reasoning", output.String())
	}
	last := s.conversation[len(s.conversation)-1]
	if last.Thinking.Text != "reasoning" {
		t.Errorf("history thinking = %q, want reasoning", last.Thinking.Text)
	}
}

func TestSessionRun_ShowsThinkingByDefault(t *testing.T) {
	provider := &captureProvider{response: core.Message{
		Role:     "assistant",
		Content:  "answer",
		Thinking: core.Thinking{Text: "reasoning"},
	}}
	s, _ := testSession(provider)

	var kinds []RenderKind
	callbacks := RunCallbacks{
		WriteToken: func(kind RenderKind, _ string, _ bool) { kinds = append(kinds, kind) },
	}
	if _, err := s.Run(context.Background(), RunRequest{Input: "hello"}, callbacks); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(kinds) < 2 || kinds[0] != KindThinking || kinds[1] != KindText {
		t.Errorf("render kinds = %v, want thinking then text", kinds)
	}
}

func TestSessionRun_MergesToolSourceTools(t *testing.T) {
	provider := &captureProvider{}
	s, _ := testSession(provider)

	var probes int
	_, err := s.Run(context.Background(), RunRequest{
		Input: "hello",
		ToolSource: func(context.Context) []core.Tool {
			probes++
			return []core.Tool{{
				Name:        "sys_device_ping",
				Description: "connection-level device tool",
				Parameters:  map[string]any{"type": "object"},
				Execute: func(context.Context, map[string]any) (string, error) {
					return "pong", nil
				},
			}}
		},
	}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if probes != 1 {
		t.Errorf("ToolSource probe count = %d, want 1 (lazy, once per Run)", probes)
	}

	req := provider.lastReq
	if req == nil {
		t.Fatal("provider received no chat request")
	}
	found := false
	for _, tool := range req.Tools {
		if tool.Name == "sys_device_ping" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("tools = %+v, want sys_device_ping among them", req.Tools)
	}
}

func TestSessionRun_WritesBackConversation(t *testing.T) {
	s, _ := testSession(&captureProvider{})

	var buf strings.Builder
	if _, err := s.Run(context.Background(), RunRequest{Input: "hello"}, testRunCallbacks(&buf)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(s.conversation) == 0 {
		t.Fatal("conversation not written back after Run")
	}
	first := s.conversation[0]
	if first.Role != "user" || first.Content != "hello" {
		t.Errorf("conversation[0] = %+v, want user hello", first)
	}
	last := s.conversation[len(s.conversation)-1]
	if last.Role != "assistant" || last.Content != "final" {
		t.Errorf("last message = %+v, want assistant final", last)
	}
}

func TestSessionRun_AccumulatesUsageAcrossRuns(t *testing.T) {
	s, _ := testSession(&captureProvider{})
	// 预置一次历史调用；会话累计语义下 Run 不清零，只追加
	s.usage.Record(0, 10, &core.UsageInfo{TotalTokens: 100})
	if n := len(s.usage.Calls()); n != 1 {
		t.Fatalf("precondition: calls = %d, want 1", n)
	}

	var buf strings.Builder
	if _, err := s.Run(context.Background(), RunRequest{Input: "hello"}, testRunCallbacks(&buf)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 本轮 captureProvider 记 1 次 → 累计 2 次
	if n := len(s.usage.Calls()); n != 2 {
		t.Errorf("calls = %d, want 2 (accumulated across runs)", n)
	}
}

func TestSessionStreamTokens_ForwardsEmptyToolResult(t *testing.T) {
	s, _ := testSession(&captureProvider{})
	tokens := make(chan core.Token, 2)
	tokens <- core.Token{Type: core.TokenTypeToolCall, ToolCall: core.ToolCall{ID: "call_1"}}
	tokens <- core.Token{Type: core.TokenTypeToolResult, ToolCallID: "call_1"}
	close(tokens)

	var emitted []core.Token
	s.streamTokens(s.deps.Config, RunCallbacks{EmitToken: func(token core.Token) {
		emitted = append(emitted, token)
	}}, context.Background(), tokens, make(chan hitl.Decision, 1))

	if len(emitted) != 2 {
		t.Fatalf("emitted tokens = %+v, want tool call and result", emitted)
	}
	if emitted[1].Type != core.TokenTypeToolResult || emitted[1].ToolCallID != "call_1" {
		t.Errorf("tool result = %+v, want empty result for call_1", emitted[1])
	}
}

func TestSessionRun_EmptyInputShowsUsage(t *testing.T) {
	s, _ := testSession(&captureProvider{})

	var buf strings.Builder
	if _, err := s.Run(context.Background(), RunRequest{}, testRunCallbacks(&buf)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(buf.String(), "用法: /agent") {
		t.Errorf("output = %q, want usage hint", buf.String())
	}
	if len(s.conversation) != 0 {
		t.Errorf("conversation should stay empty, got %d msgs", len(s.conversation))
	}
}
