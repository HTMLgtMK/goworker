package anthropic

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

func TestAnthropicProvider_DoesNotExposeUpstreamErrorBody(t *testing.T) {
	const upstreamBody = "internal gateway diagnostics: user prompt"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	t.Cleanup(srv.Close)

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	_, err := NewProvider("", srv.URL, "token", "claude-test", client, AnthropicAuthAPIKey).Chat(context.Background(), &core.ChatRequest{})
	if err == nil {
		t.Fatal("Chat should fail")
	}
	if strings.Contains(err.Error(), upstreamBody) {
		t.Errorf("upstream error body leaked to caller: %v", err)
	}
	if !strings.Contains(err.Error(), "502") {
		t.Errorf("error should retain status code: %v", err)
	}
}

func TestAnthropicProvider_EncodesMessagesToolsAndBearerAuth(t *testing.T) {
	var request map[string]json.RawMessage
	var authorization string
	srv := newAnthropicServer(t, func(r *http.Request, body []byte) string {
		authorization = r.Header.Get("Authorization")
		if got := r.Header.Get("anthropic-version"); got != anthropicVersion {
			t.Errorf("anthropic-version = %q, want %q", got, anthropicVersion)
		}
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		return `{"id":"msg_1","type":"message","role":"assistant","model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":2}}`
	})

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	provider := NewProvider("", srv.URL, "token", "claude-test", client, AnthropicAuthBearer)
	assistantReplay := `{"version":1,"assistant_content":[` +
		`{"type":"thinking","thinking":"inspect","signature":"sig_1"},` +
		`{"type":"tool_use","id":"tool_1","name":"bash","input":{"command":"pwd"}}]}`
	_, err := provider.Chat(context.Background(), &core.ChatRequest{
		Messages: []core.Message{
			{Role: "system", Content: "be concise"},
			{Role: "user", Content: "inspect"},
			{
				Role: "assistant",
				Custom: map[string]json.RawMessage{
					"anthropic": json.RawMessage(assistantReplay),
				},
			},
			{Role: "tool", ToolCallID: "tool_1", Content: "workspace"},
			{Role: "user", Content: "continue"},
		},
		Tools:    []core.Tool{{Name: "bash", Description: "run command", Parameters: map[string]any{"type": "object"}}},
		JSONMode: true,
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if authorization != "Bearer token" {
		t.Errorf("Authorization = %q, want bearer token", authorization)
	}

	var system []map[string]any
	if err := json.Unmarshal(request["system"], &system); err != nil {
		t.Fatalf("decode system: %v", err)
	}
	if got := system[0]["text"]; got != "be concise\n\nRespond with valid JSON only." {
		t.Errorf("system text = %#v", got)
	}

	var messages []struct {
		Role    string            `json:"role"`
		Content []json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(request["messages"], &messages); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	if len(messages) != 3 || messages[1].Role != "assistant" || messages[2].Role != "user" {
		t.Fatalf("messages = %#v", messages)
	}
	if got := string(messages[1].Content[0]); got != `{"type":"thinking","thinking":"inspect","signature":"sig_1"}` {
		t.Errorf("assistant replay block = %s", got)
	}
	var toolUse map[string]any
	if err := json.Unmarshal(messages[1].Content[1], &toolUse); err != nil {
		t.Fatalf("decode assistant tool use: %v", err)
	}
	if toolUse["type"] != "tool_use" || toolUse["id"] != "tool_1" || toolUse["name"] != "bash" {
		t.Errorf("assistant tool use = %#v", toolUse)
	}
	var toolResult map[string]any
	if err := json.Unmarshal(messages[2].Content[0], &toolResult); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	if toolResult["type"] != "tool_result" || toolResult["tool_use_id"] != "tool_1" || toolResult["content"] != "workspace" {
		t.Errorf("tool result = %#v", toolResult)
	}
	var followUp map[string]any
	if err := json.Unmarshal(messages[2].Content[1], &followUp); err != nil {
		t.Fatalf("decode follow-up text: %v", err)
	}
	if followUp["type"] != "text" || followUp["text"] != "continue" {
		t.Errorf("follow-up block = %#v", followUp)
	}
}

func TestAnthropicProvider_APIKeyAuthAndRequestOptions(t *testing.T) {
	var request map[string]json.RawMessage
	var apiKey, authorization string
	srv := newAnthropicServer(t, func(r *http.Request, body []byte) string {
		apiKey = r.Header.Get("x-api-key")
		authorization = r.Header.Get("Authorization")
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		return `{"id":"msg_1","type":"message","role":"assistant","model":"claude-test","content":[],"stop_reason":"end_turn","usage":{}}`
	})

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	provider := NewProvider("", srv.URL, "key", "claude-test", client, AnthropicAuthAPIKey, WithAnthropicMaxTokens(4096))
	_, err := provider.Chat(context.Background(), &core.ChatRequest{
		Tools:      []core.Tool{{Name: "bash", Description: "run command", Parameters: map[string]any{"type": "object"}}},
		ToolChoice: core.ToolChoiceRequired,
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if apiKey != "key" || authorization != "" {
		t.Errorf("auth headers were not exclusive")
	}
	if got := string(request["max_tokens"]); got != "4096" {
		t.Errorf("max_tokens = %s, want 4096", got)
	}
	if got := string(request["tool_choice"]); got != `{"type":"any"}` {
		t.Errorf("tool_choice = %s, want any", got)
	}
	var tools []anthropicToolDefinition
	if err := json.Unmarshal(request["tools"], &tools); err != nil {
		t.Fatalf("decode tools: %v", err)
	}
	if !reflect.DeepEqual(tools, []anthropicToolDefinition{{Name: "bash", Description: "run command", InputSchema: map[string]any{"type": "object"}}}) {
		t.Errorf("tools = %#v", tools)
	}
}

func TestAnthropicProvider_MergesToolResultsAndDoesNotReplayForeignPayload(t *testing.T) {
	var messages []anthropicWireMessage
	srv := newAnthropicServer(t, func(_ *http.Request, body []byte) string {
		var request anthropicMessageRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		messages = request.Messages
		return `{"id":"msg_1","type":"message","role":"assistant","model":"claude-test","content":[],"stop_reason":"end_turn","usage":{}}`
	})

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	_, err := NewProvider("", srv.URL, "key", "claude-test", client, AnthropicAuthBearer).Chat(context.Background(), &core.ChatRequest{Messages: []core.Message{
		{Role: "assistant", ToolCalls: []core.ToolCall{{ID: "tool_1", Type: "function", Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"pwd"}`}}}},
		{Role: "tool", ToolCallID: "tool_1", Content: "one"},
		{Role: "tool", ToolCallID: "tool_2", Content: "two"},
	}})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if len(messages) != 2 || len(messages[0].Content) != 1 || len(messages[1].Content) != 2 {
		t.Fatalf("messages = %#v", messages)
	}
	var toolUse struct {
		Type  string          `json:"type"`
		ID    string          `json:"id"`
		Name  string          `json:"name"`
		Input json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(messages[0].Content[0], &toolUse); err != nil {
		t.Fatalf("decode assistant tool use: %v", err)
	}
	if toolUse.Type != "tool_use" || toolUse.ID != "tool_1" || toolUse.Name != "bash" || string(toolUse.Input) != `{"command":"pwd"}` {
		t.Errorf("assistant tool use = %#v", toolUse)
	}
	for i, id := range []string{"tool_1", "tool_2"} {
		var block struct {
			Type      string `json:"type"`
			ToolUseID string `json:"tool_use_id"`
		}
		if err := json.Unmarshal(messages[1].Content[i], &block); err != nil {
			t.Fatalf("decode tool result: %v", err)
		}
		if block.Type != "tool_result" || block.ToolUseID != id {
			t.Errorf("tool result %d = %#v", i, block)
		}
	}
}

func TestAnthropicProvider_ErrorBodyIsTruncated(t *testing.T) {
	big := strings.Repeat("x", 10000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(big))
	}))
	t.Cleanup(srv.Close)

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	_, err := NewProvider("", srv.URL, "key", "claude-test", client, AnthropicAuthAPIKey).Chat(context.Background(), &core.ChatRequest{})
	if err == nil {
		t.Fatal("want error on 502")
	}
	if len(err.Error()) >= len(big) || !strings.Contains(err.Error(), "502") {
		t.Errorf("error = %q", err)
	}
}

func TestAnthropicProvider_RejectsMalformedContent(t *testing.T) {
	srv := newAnthropicServer(t, func(*http.Request, []byte) string {
		return `{"id":"msg_1","type":"message","role":"assistant","model":"claude-test","content":{},"stop_reason":"end_turn","usage":{}}`
	})

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	_, err := NewProvider("", srv.URL, "key", "claude-test", client, AnthropicAuthAPIKey).Chat(context.Background(), &core.ChatRequest{})
	if err == nil || !strings.Contains(err.Error(), "content") {
		t.Fatalf("error = %v, want malformed content error", err)
	}
}

func TestAnthropicProvider_DoesNotMutateChatRequest(t *testing.T) {
	var model string
	srv := newAnthropicServer(t, func(_ *http.Request, body []byte) string {
		var request anthropicMessageRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		model = request.Model
		return `{"id":"msg_1","type":"message","role":"assistant","model":"claude-test","content":[],"stop_reason":"end_turn","usage":{}}`
	})

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	request := &core.ChatRequest{}
	_, err := NewProvider("", srv.URL, "key", "claude-test", client, AnthropicAuthAPIKey).Chat(context.Background(), request)
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if request.Model != "" || model != "claude-test" {
		t.Errorf("request model = %q, wire model = %q", request.Model, model)
	}
}

func TestAnthropicProvider_RejectsRedirectsAndOversizedResponses(t *testing.T) {
	t.Run("redirect", func(t *testing.T) {
		redirectTargetCalled := false
		target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			redirectTargetCalled = true
		}))
		t.Cleanup(target.Close)
		redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Redirect(w, &http.Request{}, target.URL, http.StatusTemporaryRedirect)
		}))
		t.Cleanup(redirector.Close)

		client := &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		_, err := NewProvider("", redirector.URL, "key", "claude-test", client, AnthropicAuthAPIKey).Chat(context.Background(), &core.ChatRequest{})
		if err == nil || !strings.Contains(err.Error(), "307") || redirectTargetCalled {
			t.Errorf("redirect error = %v, target called = %v", err, redirectTargetCalled)
		}
	})
	t.Run("oversized response", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(strings.Repeat("x", maxAnthropicResponseSize+1)))
		}))
		t.Cleanup(srv.Close)

		client := &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		_, err := NewProvider("", srv.URL, "key", "claude-test", client, AnthropicAuthAPIKey).Chat(context.Background(), &core.ChatRequest{})
		if err == nil || !strings.Contains(err.Error(), "byte limit") {
			t.Errorf("oversized response error = %v", err)
		}
	})
}

func TestAnthropicProvider_Defaults(t *testing.T) {
	provider := NewProvider("", "", "key", "", &http.Client{}, AnthropicAuthAPIKey)
	if provider.Name() != "anthropic" || provider.Endpoint() != "https://api.anthropic.com" || provider.Model() != "claude-sonnet-4-6" || provider.maxTokens != 8192 {
		t.Errorf("defaults = %#v", provider)
	}
}

func TestAnthropicProtocolMappings(t *testing.T) {
	if got := anthropicToolChoice(core.ToolChoiceAuto); got != nil {
		t.Errorf("auto tool choice = %#v, want omitted", got)
	}
	choice, err := json.Marshal(anthropicToolChoice(core.ToolChoiceNone))
	if err != nil {
		t.Fatalf("marshal tool choice: %v", err)
	}
	if got := string(choice); got != `{"type":"none"}` {
		t.Errorf("none tool choice = %s", got)
	}
	if got := anthropicFinishReason("max_tokens"); got != "length" {
		t.Errorf("max_tokens finish reason = %q", got)
	}
	if got := anthropicFinishReason("unknown"); got != "unknown" {
		t.Errorf("unknown finish reason = %q", got)
	}
}

func newAnthropicServer(t *testing.T, respond func(*http.Request, []byte) string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respond(r, body)))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestAnthropicProvider_ChatStreamStreamsThinkingAndReassembles 覆盖 SSE 流式：
// thinking/text 增量实时投递、tool_use 分片重组、收尾响应与 usage 归并。
func TestAnthropicProvider_ChatStreamStreamsThinkingAndReassembles(t *testing.T) {
	sseLines := []string{
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-test","content":[],"stop_reason":null,"usage":{"input_tokens":11,"cache_read_input_tokens":3,"cache_creation_input_tokens":2,"output_tokens":1}}}`,
		"",
		"event: content_block_start",
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"先算"}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"再答"}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-1"}}`,
		"",
		"event: content_block_stop",
		`data: {"type":"content_block_stop","index":0}`,
		"",
		"event: content_block_start",
		`data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"答案"}}`,
		"",
		"event: content_block_start",
		`data: {"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_1","name":"bash","input":{}}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"command\":"}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"\"ls\"}"}}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":9}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"",
	}

	var gotStreamFlag bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var wire anthropicMessageRequest
		_ = json.Unmarshal(raw, &wire)
		gotStreamFlag = wire.Stream
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join(sseLines, "\n")))
	}))
	t.Cleanup(srv.Close)

	p := NewProvider("", srv.URL, "token", "claude-test", srv.Client(), AnthropicAuthAPIKey)
	ch, err := p.ChatStream(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}

	var (
		thinkingDeltas []string
		textDeltas     []string
		final          *core.ChatResponse
	)
	for tok := range ch {
		if tok.Response != nil {
			final = tok.Response
			continue
		}
		switch tok.Type {
		case core.TokenTypeThinking:
			thinkingDeltas = append(thinkingDeltas, tok.Content)
		case core.TokenTypeText:
			textDeltas = append(textDeltas, tok.Content)
		}
	}

	if !gotStreamFlag {
		t.Error("request should carry stream:true")
	}
	if want := []string{"先算", "再答"}; !reflect.DeepEqual(thinkingDeltas, want) {
		t.Errorf("thinking deltas = %v, want %v", thinkingDeltas, want)
	}
	if want := []string{"答案"}; !reflect.DeepEqual(textDeltas, want) {
		t.Errorf("text deltas = %v, want %v", textDeltas, want)
	}
	if final == nil {
		t.Fatal("stream should finish with a reassembled response")
	}
	msg := final.Choices[0].Message
	if msg.Content != "答案" {
		t.Errorf("content = %q, want 答案", msg.Content)
	}
	// 同一 thinking 块内的增量直接拼接（多块之间才以 \n 连接）。
	if msg.Thinking.Text != "先算再答" {
		t.Errorf("thinking = %q, want 先算再答", msg.Thinking.Text)
	}
	if len(msg.ToolCalls) != 1 || msg.ToolCalls[0].ID != "toolu_1" || msg.ToolCalls[0].Function.Name != "bash" || msg.ToolCalls[0].Function.Arguments != `{"command":"ls"}` {
		t.Errorf("tool calls = %+v", msg.ToolCalls)
	}
	if final.Choices[0].FinishReason != "tool_calls" {
		t.Errorf("finish reason = %q, want tool_calls", final.Choices[0].FinishReason)
	}
	if final.Usage == nil || final.Usage.PromptTokens != 11 || final.Usage.CompletionTokens != 9 ||
		final.Usage.PromptCacheHitTokens != 3 || final.Usage.PromptCacheMissTokens != 2 || final.Usage.TotalTokens != 20 {
		t.Errorf("usage = %+v", final.Usage)
	}
	// thinking signature 必须随 Custom 回放，后续带思考的轮次才不会被拒。
	payload, ok := msg.Custom[p.Name()]
	if !ok || !strings.Contains(string(payload), "sig-1") {
		t.Errorf("custom payload missing thinking signature: %s", payload)
	}
}

// TestAnthropicProvider_ChatStreamEmptyStreamHasNoFinalResponse 空流不重组不发收尾。
func TestAnthropicProvider_ChatStreamEmptyStreamHasNoFinalResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			"event: message_start",
			`data: {"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"x","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":0}}}`,
			"",
			"event: message_stop",
			`data: {"type":"message_stop"}`,
			"",
		}, "\n")))
	}))
	t.Cleanup(srv.Close)

	p := NewProvider("", srv.URL, "token", "claude-test", srv.Client(), AnthropicAuthAPIKey)
	ch, err := p.ChatStream(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	for tok := range ch {
		if tok.Response != nil {
			t.Fatal("empty stream should not deliver a reassembled response")
		}
	}
}

// TestAnthropicProvider_ThinkingBlockAlwaysCarriesThinkingField 回归：服务端可能
// 返回「思考文本为空、只有 signature」的块；重组回放时 `thinking` 字段必须存在，
// 否则上游反序列化报 missing field（实测 CC-Switch 422）。
func TestAnthropicProvider_ThinkingBlockAlwaysCarriesThinkingField(t *testing.T) {
	sse := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"m1","type":"message","role":"assistant","model":"x","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":0}}}`,
		"",
		"event: content_block_start",
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-empty"}}`,
		"",
		"event: content_block_start",
		`data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"ok"}}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"",
	}, "\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(sse))
	}))
	t.Cleanup(srv.Close)

	p := NewProvider("", srv.URL, "token", "claude-test", srv.Client(), AnthropicAuthAPIKey)
	ch, err := p.ChatStream(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	var final *core.ChatResponse
	for tok := range ch {
		if tok.Response != nil {
			final = tok.Response
		}
	}
	if final == nil {
		t.Fatal("no reassembled response")
	}
	payload := final.Choices[0].Message.Custom[p.Name()]
	if !strings.Contains(string(payload), `"thinking":""`) {
		t.Errorf("thinking block must carry thinking field: %s", payload)
	}
}

// TestAnthropicProvider_ReplaySanitizesLegacyBlocks 回归：历史 Custom 里存着旧版
// 坏块（thinking 缺字段 / 空 text / 非对象 input）时，回放前必须清洗。
func TestAnthropicProvider_ReplaySanitizesLegacyBlocks(t *testing.T) {
	legacy := `{"version":1,"assistant_content":[{"type":"thinking","signature":"sig"},{"type":"text","text":""},{"type":"tool_use","id":"t1","name":"bash","input":"oops"}]}`
	msg := core.Message{
		Role:    "assistant",
		Content: "",
		Custom:  map[string]json.RawMessage{"anthropic": json.RawMessage(legacy)},
	}
	blocks, err := anthropicBlocks(msg, "anthropic")
	if err != nil {
		t.Fatalf("anthropicBlocks: %v", err)
	}
	joined := ""
	for _, b := range blocks {
		joined += string(b) + "\n"
	}
	if !strings.Contains(joined, `"thinking":""`) {
		t.Errorf("legacy thinking block missing thinking field: %s", joined)
	}
	if strings.Contains(joined, `"text":""`) {
		t.Errorf("empty text block should be dropped: %s", joined)
	}
	if !strings.Contains(joined, `"input":{}`) {
		t.Errorf("non-object tool input should degrade to {}: %s", joined)
	}
}

// TestAnthropicProvider_SurfacesStructuredErrorMessage 上游 JSON 错误体的
// error.message 要进 error（前端可见，便于排查）；非 JSON body 不泄漏。
func TestAnthropicProvider_SurfacesStructuredErrorMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"messages[139]: missing field thinking"}}`))
	}))
	t.Cleanup(srv.Close)

	p := NewProvider("", srv.URL, "key", "claude-test", srv.Client(), AnthropicAuthAPIKey)
	_, err := p.Chat(context.Background(), &core.ChatRequest{})
	if err == nil {
		t.Fatal("Chat should fail")
	}
	if !strings.Contains(err.Error(), "422") || !strings.Contains(err.Error(), "missing field thinking") {
		t.Errorf("error should carry status and upstream message: %v", err)
	}
}

// TestAnthropicProvider_ChatStreamNonSSEFallback 覆盖兼容代理无视 stream:true 直接
// 回非流式 JSON 的分支：只投递一条带 Response 的收尾 token，而不是把事件流读成空流。
func TestAnthropicProvider_ChatStreamNonSSEFallback(t *testing.T) {
	body := `{"id":"msg_1","type":"message","role":"assistant","model":"claude-test","content":[{"type":"text","text":"直接 JSON 回包"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":3}}`
	t.Run("json content-type", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(srv.Close)
		assertNonSSEFallback(t, srv.URL, "直接 JSON 回包")
	})
	t.Run("missing content-type", func(t *testing.T) {
		// 代理连 Content-Type 都不带：首字节探测应识别出 JSON 对象，走非流式回退。
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(srv.Close)
		assertNonSSEFallback(t, srv.URL, "直接 JSON 回包")
	})
}

func assertNonSSEFallback(t *testing.T, baseURL, wantContent string) {
	t.Helper()
	p := NewProvider("", baseURL, "token", "claude-test", &http.Client{}, AnthropicAuthAPIKey)
	ch, err := p.ChatStream(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	var final *core.ChatResponse
	for tok := range ch {
		if tok.Response != nil {
			final = tok.Response
		} else {
			t.Errorf("non-SSE fallback should only deliver the terminal token, got streaming delta %+v", tok)
		}
	}
	if final == nil {
		t.Fatal("non-SSE fallback should deliver a terminal response")
	}
	if got := final.Choices[0].Message.Content; got != wantContent {
		t.Errorf("content = %q, want %q", got, wantContent)
	}
}

// TestAnthropicProvider_ChatStreamDeltaOnlyReassembles 回归：部分代理省略
// content_block_start 只发 content_block_delta；此前只靠 start 置 sawContent，
// 整段重组响应会被 !sawContent 丢弃，调用方拿到增量却回填不了历史。
func TestAnthropicProvider_ChatStreamDeltaOnlyReassembles(t *testing.T) {
	sse := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"m1","type":"message","role":"assistant","model":"claude-test","content":[],"stop_reason":null,"usage":{"input_tokens":2,"output_tokens":0}}}`,
		"",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"裸"}}`,
		"",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"增量"}}`,
		"",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`,
		"",
		`data: {"type":"message_stop"}`,
		"",
	}, "\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(sse))
	}))
	t.Cleanup(srv.Close)

	p := NewProvider("", srv.URL, "token", "claude-test", srv.Client(), AnthropicAuthAPIKey)
	ch, err := p.ChatStream(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}

	var textDeltas []string
	var final *core.ChatResponse
	for tok := range ch {
		if tok.Response != nil {
			final = tok.Response
			continue
		}
		if tok.Type == core.TokenTypeText {
			textDeltas = append(textDeltas, tok.Content)
		}
	}
	if want := []string{"裸", "增量"}; !reflect.DeepEqual(textDeltas, want) {
		t.Errorf("text deltas = %v, want %v", textDeltas, want)
	}
	if final == nil {
		t.Fatal("delta-only stream should still deliver a reassembled response")
	}
	if msg := final.Choices[0].Message; msg.Content != "裸增量" {
		t.Errorf("content = %q, want 裸增量", msg.Content)
	}
	if final.Choices[0].FinishReason != "stop" {
		t.Errorf("finish reason = %q, want stop", final.Choices[0].FinishReason)
	}
}
