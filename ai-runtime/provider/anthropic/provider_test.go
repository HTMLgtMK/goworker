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

func TestAnthropicProvider_DefaultsAndUnsupportedStream(t *testing.T) {
	provider := NewProvider("", "", "key", "", &http.Client{}, AnthropicAuthAPIKey)
	if provider.Name() != "anthropic" || provider.Endpoint() != "https://api.anthropic.com" || provider.Model() != "claude-sonnet-4-6" || provider.maxTokens != 8192 {
		t.Errorf("defaults = %#v", provider)
	}
	if _, err := provider.ChatStream(context.Background(), &core.ChatRequest{}); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("ChatStream error = %v", err)
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
