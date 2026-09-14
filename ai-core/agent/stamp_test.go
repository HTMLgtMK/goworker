package agent

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

func TestNewMsgID_Unique(t *testing.T) {
	// 并发生成不应重复。
	const n = 10000
	ids := make(map[string]struct{}, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(4)
	for range 4 {
		go func() {
			defer wg.Done()
			local := make([]string, n/4)
			for j := range local {
				local[j] = core.NewMsgID()
			}
			mu.Lock()
			for _, id := range local {
				ids[id] = struct{}{}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(ids) != n {
		t.Errorf("generated %d ids but only %d unique", n, len(ids))
	}
	// 格式校验：m_ + 8 hex
	for id := range ids {
		if len(id) != 2+8 {
			t.Errorf("id %q length = %d, want 10", id, len(id))
		}
		if id[0] != 'm' || id[1] != '_' {
			t.Errorf("id %q should start with m_", id)
		}
	}
}

func TestNewMsgID_NoPanic(t *testing.T) {
	// 循环大量生成确保回退路径不会 panic。
	for range 10000 {
		id := core.NewMsgID()
		if id == "" {
			t.Fatal("NewMsgID returned empty string")
		}
	}
}

func TestStampMessageIDs_FillEmpty(t *testing.T) {
	msgs := []core.Message{
		{Role: "system", Content: "sys"},
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "resp"},
	}
	out := stampMessageIDs(msgs)
	if len(out) != len(msgs) {
		t.Fatalf("len = %d, want %d", len(out), len(msgs))
	}
	ids := make(map[string]struct{})
	for i, m := range out {
		if m.MsgID == "" {
			t.Errorf("msg[%d] MsgID is empty after stamping", i)
		}
		if m.CreatedAt.IsZero() {
			t.Errorf("msg[%d] CreatedAt is zero after stamping", i)
		}
		if _, dup := ids[m.MsgID]; dup {
			t.Errorf("msg[%d] has duplicate MsgID %q", i, m.MsgID)
		}
		ids[m.MsgID] = struct{}{}
	}
	// 原 slice 不应被修改。
	for i, m := range msgs {
		if m.MsgID != "" {
			t.Errorf("original msg[%d] was mutated (MsgID = %q)", i, m.MsgID)
		}
	}
}

func TestStampMessageIDs_PreserveExisting(t *testing.T) {
	msgs := []core.Message{
		{Role: "user", Content: "a", MsgID: "m_existing"},
		{Role: "user", Content: "b"},
		{Role: "user", Content: "c", MsgID: "m_keepme"},
	}
	out := stampMessageIDs(msgs)
	if out[0].MsgID != "m_existing" {
		t.Errorf("msg[0] MsgID = %q, want m_existing", out[0].MsgID)
	}
	if out[1].MsgID == "" {
		t.Error("msg[1] MsgID should be filled")
	}
	if out[2].MsgID != "m_keepme" {
		t.Errorf("msg[2] MsgID = %q, want m_keepme", out[2].MsgID)
	}
}

func TestStampMessageIDs_Idempotent(t *testing.T) {
	msgs := []core.Message{
		{Role: "user", Content: "x"},
		{Role: "assistant", Content: "y", MsgID: "m_already"},
	}
	first := stampMessageIDs(msgs)
	second := stampMessageIDs(first)
	if len(first) != len(second) {
		t.Fatal("lengths differ between first and second pass")
	}
	for i := range first {
		if first[i].MsgID != second[i].MsgID {
			t.Errorf("msg[%d] MsgID changed: %q -> %q", i, first[i].MsgID, second[i].MsgID)
		}
		if !first[i].CreatedAt.Equal(second[i].CreatedAt) {
			t.Errorf("msg[%d] CreatedAt changed on second pass", i)
		}
	}
}

// toolThenFinalProvider 首次调用返回 tool call，第二次返回 final 答复。
// 用于验证工具消息+最终消息都被盖章。
type toolThenFinalProvider struct {
	calls int
}

func (p *toolThenFinalProvider) Name() string  { return "toolfinal" }
func (p *toolThenFinalProvider) Model() string { return "m" }
func (p *toolThenFinalProvider) Chat(_ context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.calls++
	if p.calls == 1 {
		return &core.ChatResponse{Choices: []core.ResponseChoice{{
			Message: core.Message{
				Role: "assistant",
				ToolCalls: []core.ToolCall{{
					ID: "t1", Type: "function",
					Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"echo ok"}`},
				}},
			},
		}}}, nil
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{
		Message: core.Message{Role: "assistant", Content: "final"},
	}}}, nil
}
func (p *toolThenFinalProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, nil
}

type unknownToolThenFinalProvider struct {
	calls int
}

func (p *unknownToolThenFinalProvider) Name() string  { return "unknown-tool" }
func (p *unknownToolThenFinalProvider) Model() string { return "m" }
func (p *unknownToolThenFinalProvider) Chat(_ context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.calls++
	if p.calls == 1 {
		return &core.ChatResponse{Choices: []core.ResponseChoice{{
			Message: core.Message{Role: "assistant", ToolCalls: []core.ToolCall{{
				ID: "missing_1", Type: "function",
				Function: core.ToolCallFunction{Name: "missing", Arguments: `{}`},
			}}},
		}}}, nil
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{
		Message: core.Message{Role: "assistant", Content: "final"},
	}}}, nil
}
func (p *unknownToolThenFinalProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, nil
}

func TestAgentRun_ToolTokensKeepCallCorrelation(t *testing.T) {
	a := NewAgent(&toolThenFinalProvider{}, "", testTools(), nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}

	var call, result core.Token
	for tok := range tokenCh {
		switch tok.Type {
		case core.TokenTypeToolCall:
			call = tok
		case core.TokenTypeToolResult:
			result = tok
		}
	}
	<-msgCh

	if call.ToolCall.ID != "t1" {
		t.Errorf("tool call ID = %q, want t1", call.ToolCall.ID)
	}
	if call.ToolCall.Function.Name != "bash" {
		t.Errorf("tool call name = %q, want bash", call.ToolCall.Function.Name)
	}
	if result.ToolCallID != call.ToolCall.ID {
		t.Errorf("tool result ID = %q, want %q", result.ToolCallID, call.ToolCall.ID)
	}
}

func TestAgentRun_UnknownToolTokensKeepCallCorrelation(t *testing.T) {
	a := NewAgent(&unknownToolThenFinalProvider{}, "", nil, nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}

	var call, result core.Token
	for tok := range tokenCh {
		switch tok.Type {
		case core.TokenTypeToolCall:
			call = tok
		case core.TokenTypeToolResult:
			result = tok
		}
	}
	<-msgCh

	if call.ToolCall.ID != "missing_1" {
		t.Errorf("tool call ID = %q, want missing_1", call.ToolCall.ID)
	}
	if result.ToolCallID != call.ToolCall.ID {
		t.Errorf("tool result ID = %q, want %q", result.ToolCallID, call.ToolCall.ID)
	}
	if result.Content != "unknown tool: missing" {
		t.Errorf("tool result = %q, want unknown tool error", result.Content)
	}
}

func TestAgentRun_FailedToolTokensKeepCallCorrelation(t *testing.T) {
	tests := []struct {
		name   string
		call   core.ToolCall
		tools  []core.Tool
		mws    []core.Middleware
		result string
	}{
		{
			name:   "unsupported type",
			call:   core.ToolCall{ID: "unsupported_1", Type: "computer", Function: core.ToolCallFunction{Name: "click", Arguments: `{}`}},
			result: "unsupported tool call type: computer",
		},
		{
			name:   "invalid arguments",
			call:   core.ToolCall{ID: "invalid_1", Type: "function", Function: core.ToolCallFunction{Name: "bash", Arguments: `{`}},
			tools:  testTools(),
			result: "invalid args: unexpected end of JSON input",
		},
		{
			name:   "middleware abort",
			call:   core.ToolCall{ID: "aborted_1", Type: "function", Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"echo ok"}`}},
			tools:  testTools(),
			mws:    []core.Middleware{abortToolMiddleware{}},
			result: "blocked by policy",
		},
		{
			name: "tool execution error",
			call: core.ToolCall{ID: "failed_1", Type: "function", Function: core.ToolCallFunction{Name: "fail", Arguments: `{}`}},
			tools: []core.Tool{{Name: "fail", Execute: func(context.Context, map[string]any) (string, error) {
				return "", errors.New("execution failed")
			}}},
			result: "error: execution failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAgent(&singleToolThenFinalProvider{call: tt.call}, "", tt.tools, tt.mws)
			tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
			if err != nil {
				t.Fatalf("Run err = %v", err)
			}

			var calls, results []core.Token
			for tok := range tokenCh {
				switch tok.Type {
				case core.TokenTypeToolCall:
					calls = append(calls, tok)
				case core.TokenTypeToolResult:
					results = append(results, tok)
				}
			}
			<-msgCh

			if len(calls) != 1 || calls[0].ToolCall.ID != tt.call.ID {
				t.Fatalf("tool calls = %+v, want one call for %q", calls, tt.call.ID)
			}
			if len(results) != 1 || results[0].ToolCallID != tt.call.ID {
				t.Fatalf("tool results = %+v, want one result for %q", results, tt.call.ID)
			}
			if results[0].Content != tt.result {
				t.Errorf("tool result = %q, want %q", results[0].Content, tt.result)
			}
		})
	}
}

func TestAgentRun_MultipleToolCallsKeepIndependentCorrelation(t *testing.T) {
	calls := []core.ToolCall{
		{ID: "ok_1", Type: "function", Function: core.ToolCallFunction{Name: "ok", Arguments: `{}`}},
		{ID: "unsupported_1", Type: "computer", Function: core.ToolCallFunction{Name: "click", Arguments: `{}`}},
		{ID: "unknown_1", Type: "function", Function: core.ToolCallFunction{Name: "unknown", Arguments: `{}`}},
		{ID: "invalid_1", Type: "function", Function: core.ToolCallFunction{Name: "ok", Arguments: `{`}},
		{ID: "abort_1", Type: "function", Function: core.ToolCallFunction{Name: "abort", Arguments: `{}`}},
		{ID: "error_1", Type: "function", Function: core.ToolCallFunction{Name: "error", Arguments: `{}`}},
	}
	tools := []core.Tool{
		{Name: "ok", Execute: func(context.Context, map[string]any) (string, error) { return "ok", nil }},
		{Name: "abort", Execute: func(context.Context, map[string]any) (string, error) { return "unexpected", nil }},
		{Name: "error", Execute: func(context.Context, map[string]any) (string, error) { return "", errors.New("execution failed") }},
	}
	a := NewAgent(&multiToolThenFinalProvider{calls: calls}, "", tools, []core.Middleware{abortNamedToolMiddleware{name: "abort"}})
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}

	seenCalls := make(map[string]int, len(calls))
	results := make(map[string]core.Token, len(calls))
	for tok := range tokenCh {
		switch tok.Type {
		case core.TokenTypeToolCall:
			seenCalls[tok.ToolCall.ID]++
		case core.TokenTypeToolResult:
			if _, duplicate := results[tok.ToolCallID]; duplicate {
				t.Errorf("duplicate tool result for %q", tok.ToolCallID)
			}
			results[tok.ToolCallID] = tok
		}
	}
	<-msgCh

	want := map[string]string{
		"ok_1":          "ok",
		"unsupported_1": "unsupported tool call type: computer",
		"unknown_1":     "unknown tool: unknown",
		"invalid_1":     "invalid args: unexpected end of JSON input",
		"abort_1":       "blocked by policy",
		"error_1":       "error: execution failed",
	}
	if len(seenCalls) != len(want) || len(results) != len(want) {
		t.Fatalf("calls = %v, results = %v, want one of each for %d IDs", seenCalls, results, len(want))
	}
	for id, content := range want {
		if seenCalls[id] != 1 {
			t.Errorf("tool call count for %q = %d, want 1", id, seenCalls[id])
		}
		if got, ok := results[id]; !ok {
			t.Errorf("missing tool result for %q", id)
		} else if got.Content != content {
			t.Errorf("tool result for %q = %q, want %q", id, got.Content, content)
		}
	}
}

type multiToolThenFinalProvider struct {
	calls []core.ToolCall
	runs  int
}

func (p *multiToolThenFinalProvider) Name() string  { return "multi-tool" }
func (p *multiToolThenFinalProvider) Model() string { return "m" }
func (p *multiToolThenFinalProvider) Chat(_ context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.runs++
	if p.runs == 1 {
		return &core.ChatResponse{Choices: []core.ResponseChoice{{
			Message: core.Message{Role: "assistant", ToolCalls: p.calls},
		}}}, nil
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{
		Message: core.Message{Role: "assistant", Content: "final"},
	}}}, nil
}
func (p *multiToolThenFinalProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, nil
}

type abortNamedToolMiddleware struct{ name string }

func (mw abortNamedToolMiddleware) Name() string { return "abort-named-tool" }
func (mw abortNamedToolMiddleware) OnBeforeTool(ev *core.BeforeToolEvent) *core.MiddlewareResponse {
	if ev.Tool.Function.Name != mw.name {
		return &core.MiddlewareResponse{}
	}
	ev.Abort = &core.ToolAbort{Messages: []core.Message{{
		Role: "tool", Content: "blocked by policy", ToolCallID: ev.Tool.ID,
	}}}
	return &core.MiddlewareResponse{}
}

type singleToolThenFinalProvider struct {
	call  core.ToolCall
	calls int
}

func (p *singleToolThenFinalProvider) Name() string  { return "single-tool" }
func (p *singleToolThenFinalProvider) Model() string { return "m" }
func (p *singleToolThenFinalProvider) Chat(_ context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.calls++
	if p.calls == 1 {
		return &core.ChatResponse{Choices: []core.ResponseChoice{{
			Message: core.Message{Role: "assistant", ToolCalls: []core.ToolCall{p.call}},
		}}}, nil
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{
		Message: core.Message{Role: "assistant", Content: "final"},
	}}}, nil
}
func (p *singleToolThenFinalProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, nil
}

type abortToolMiddleware struct{}

func (abortToolMiddleware) Name() string { return "abort-tool" }
func (abortToolMiddleware) OnBeforeTool(ev *core.BeforeToolEvent) *core.MiddlewareResponse {
	ev.Abort = &core.ToolAbort{Messages: []core.Message{{
		Role: "tool", Content: "blocked by policy", ToolCallID: ev.Tool.ID,
	}}}
	return &core.MiddlewareResponse{}
}

func TestAgentRun_CancelledToolKeepsResultCorrelation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tool := core.Tool{
		Name: "wait",
		Execute: func(ctx context.Context, _ map[string]any) (string, error) {
			cancel()
			<-ctx.Done()
			return "", ctx.Err()
		},
	}
	call := core.ToolCall{
		ID: "cancelled_1", Type: "function",
		Function: core.ToolCallFunction{Name: "wait", Arguments: `{}`},
	}
	a := NewAgent(&singleToolThenFinalProvider{call: call}, "", []core.Tool{tool}, nil)
	tokenCh, msgCh, err := a.Run(ctx, nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}

	var result core.Token
	for tok := range tokenCh {
		if tok.Type == core.TokenTypeToolResult {
			result = tok
		}
	}
	<-msgCh

	if result.ToolCallID != call.ID {
		t.Errorf("tool result ID = %q, want %q", result.ToolCallID, call.ID)
	}
	if result.Content != "error: context canceled" {
		t.Errorf("tool result = %q, want cancellation error", result.Content)
	}
}

func TestAgentRun_MessagesAllStamped(t *testing.T) {
	a := NewAgent(&toolThenFinalProvider{}, "", testTools(), nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	for tok := range tokenCh {
		if tok.Done {
			break
		}
	}
	msgs := <-msgCh
	if len(msgs) == 0 {
		t.Fatal("no messages returned")
	}
	ids := make(map[string]struct{}, len(msgs))
	for i, m := range msgs {
		if m.MsgID == "" {
			t.Errorf("msg[%d] (role=%q) has empty MsgID after Run", i, m.Role)
		}
		if m.CreatedAt.IsZero() {
			t.Errorf("msg[%d] (role=%q) has zero CreatedAt after Run", i, m.Role)
		}
		if _, dup := ids[m.MsgID]; dup {
			t.Errorf("msg[%d] (role=%q) duplicate MsgID %q", i, m.Role, m.MsgID)
		}
		ids[m.MsgID] = struct{}{}
	}
	// tool 消息也应该被盖章。
	var hasTool bool
	for _, m := range msgs {
		if m.Role == "tool" {
			hasTool = true
			break
		}
	}
	if !hasTool {
		t.Error("expected at least one tool message in history")
	}
}

func TestAgentRun_ProviderErrorMessagesStamped(t *testing.T) {
	a := NewAgent(errProvider{}, "", nil, nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "hi")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	for tok := range tokenCh {
		if tok.Done {
			break
		}
	}
	msgs := <-msgCh
	// 错误场景下所有消息也被盖章（包括 emitAgentNote 注入的 assistant 错误消息）。
	ids := make(map[string]struct{}, len(msgs))
	for i, m := range msgs {
		if m.MsgID == "" {
			t.Errorf("msg[%d] (role=%q) has empty MsgID after error Run", i, m.Role)
		}
		if _, dup := ids[m.MsgID]; dup {
			t.Errorf("msg[%d] (role=%q) duplicate MsgID %q", i, m.Role, m.MsgID)
		}
		ids[m.MsgID] = struct{}{}
	}
}
