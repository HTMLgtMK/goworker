package openai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	coreagent "github.com/tinguo/goworker/ai-core/agent"
	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	runtimeprovider "github.com/tinguo/goworker/ai-runtime/provider"
)

func newChatServer(t *testing.T, respond func([]byte) string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		resp := respond(body)
		// agent 循环走 ChatStream：respond 返回非 SSE 的 JSON 响应时，
		// 转成单 delta 块的 SSE 流下发（message → delta），保持用例写法不变
		if probeStream(body) && !strings.HasPrefix(resp, "data: ") {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte(toSSE(t, resp)))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(resp))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func probeStream(body []byte) bool {
	var probe struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &probe) == nil && probe.Stream
}

// toSSE 把 OpenAIChatResponse 形状的 JSON 转成单 delta 块的 SSE 文本。
// message 对象整体搬到 delta（content/reasoning_content/tool_calls 通吃）。
func toSSE(t *testing.T, resp string) string {
	t.Helper()
	var wire struct {
		Choices []struct {
			Message map[string]any `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(resp), &wire); err != nil {
		t.Fatalf("toSSE decode response: %v", err)
	}
	delta := map[string]any{}
	finish := "stop"
	if len(wire.Choices) > 0 {
		delta = wire.Choices[0].Message
	}
	chunk, err := json.Marshal(map[string]any{
		"choices": []map[string]any{{"delta": delta, "finish_reason": finish}},
	})
	if err != nil {
		t.Fatalf("toSSE encode chunk: %v", err)
	}
	return "data: " + string(chunk) + "\n\ndata: [DONE]\n"
}

func newTestProvider(endpoint string, options ...ProviderOptions) *Provider {
	return NewProvider("openai", endpoint, "key", "m", &http.Client{}, options...)
}

func TestProvider_Defaults(t *testing.T) {
	p := NewProvider("", "", "key", "", &http.Client{})
	if got := p.Endpoint(); got != "http://localhost:8000/v1" {
		t.Errorf("endpoint = %q, want default v1 endpoint", got)
	}
	if got := p.Model(); got != "gpt-4o" {
		t.Errorf("model = %q, want gpt-4o", got)
	}
}

func TestProvider_DoesNotMutateRequest(t *testing.T) {
	srv := newChatServer(t, func([]byte) string {
		return `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`
	})
	req := &core.ChatRequest{Messages: []core.Message{{Role: "user", Content: "hello"}}, Stream: true}
	if _, err := newTestProvider(srv.URL).Chat(context.Background(), req); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if req.Model != "" || !req.Stream {
		t.Errorf("Chat mutated request: %#v", req)
	}

	streamSrv := newChatServer(t, func([]byte) string { return "data: [DONE]\n" })
	streamReq := &core.ChatRequest{Messages: []core.Message{{Role: "user", Content: "hello"}}}
	tokens, err := newTestProvider(streamSrv.URL).ChatStream(context.Background(), streamReq)
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	for range tokens {
	}
	if streamReq.Model != "" || streamReq.Stream {
		t.Errorf("ChatStream mutated request: %#v", streamReq)
	}
}

func TestProvider_DoesNotExposeUpstreamErrorBody(t *testing.T) {
	const upstreamBody = "internal gateway diagnostics: user prompt"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	t.Cleanup(srv.Close)

	_, err := newTestProvider(srv.URL).Chat(context.Background(), &core.ChatRequest{Model: "m"})
	if err == nil {
		t.Fatal("want error on 502")
	}
	if strings.Contains(err.Error(), upstreamBody) {
		t.Errorf("upstream error body leaked to caller: %v", err)
	}
	if !strings.Contains(err.Error(), "502") {
		t.Errorf("error should carry status code: %v", err)
	}

	_, err = newTestProvider(srv.URL).ChatStream(context.Background(), &core.ChatRequest{Model: "m"})
	if err == nil {
		t.Fatal("ChatStream want error on 502")
	}
	if strings.Contains(err.Error(), upstreamBody) {
		t.Errorf("upstream error body leaked to caller: %v", err)
	}
}

func TestProvider_RequestThinkingModes(t *testing.T) {
	tests := []struct {
		name       string
		options    runtimeprovider.ThinkingOptions
		wantField  string
		wantValue  any
		absentKeys []string
	}{
		{
			name:       "auto",
			options:    runtimeprovider.ThinkingOptions{RequestMode: runtimeconfig.ThinkingRequestAuto},
			absentKeys: []string{"enable_thinking", "reasoning_effort"},
		},
		{
			name:       "enable thinking",
			options:    runtimeprovider.ThinkingOptions{RequestMode: runtimeconfig.ThinkingRequestEnable},
			wantField:  "enable_thinking",
			wantValue:  true,
			absentKeys: []string{"reasoning_effort"},
		},
		{
			name:       "reasoning effort",
			options:    runtimeprovider.ThinkingOptions{RequestMode: runtimeconfig.ThinkingRequestEffort, Effort: runtimeconfig.ThinkingEffortHigh},
			wantField:  "reasoning_effort",
			wantValue:  "high",
			absentKeys: []string{"enable_thinking"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var request map[string]any
			srv := newChatServer(t, func(body []byte) string {
				if err := json.Unmarshal(body, &request); err != nil {
					t.Fatalf("decode request: %v", err)
				}
				return `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`
			})
			if _, err := newTestProvider(srv.URL, WithThinkingOptions(tt.options)).Chat(context.Background(), &core.ChatRequest{}); err != nil {
				t.Fatalf("Chat: %v", err)
			}
			if tt.wantField != "" && request[tt.wantField] != tt.wantValue {
				t.Errorf("request[%q] = %#v, want %#v", tt.wantField, request[tt.wantField], tt.wantValue)
			}
			for _, key := range tt.absentKeys {
				if _, ok := request[key]; ok {
					t.Errorf("request should omit %q: %#v", key, request)
				}
			}
		})
	}
}

func customPayload(t *testing.T, fields string) map[string]json.RawMessage {
	t.Helper()
	return map[string]json.RawMessage{"openai": json.RawMessage(`{"version":1,"fields":{` + fields + `}}`)}
}

func TestProvider_NormalizesThinkingFields(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{name: "reasoning_content wins", message: `{"reasoning_content":"primary","reasoning":"secondary","reasoning_details":[{"text":"fallback"}]}`, want: "primary"},
		{name: "reasoning wins over details", message: `{"reasoning":"secondary","reasoning_details":[{"text":"fallback"}]}`, want: "secondary"},
		{name: "details are fallback", message: `{"reasoning_content":"","reasoning":"","reasoning_details":[{"text":"first"},{"content":"second"}]}`, want: "first\nsecond"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newChatServer(t, func([]byte) string {
				return `{"choices":[{"message":` + tt.message + `}]}`
			})
			resp, err := newTestProvider(srv.URL).Chat(context.Background(), &core.ChatRequest{})
			if err != nil {
				t.Fatalf("Chat: %v", err)
			}
			if got := resp.Choices[0].Message.Thinking.Text; got != tt.want {
				t.Errorf("Thinking.Text = %q, want %q", got, tt.want)
			}
			raw, ok := resp.Choices[0].Message.Custom["openai"]
			if !ok || len(raw) == 0 {
				t.Fatal("Custom should retain the recognized provider payload")
			}
		})
	}
}

func TestProvider_PreservesAllRecognizedThinkingFields(t *testing.T) {
	srv := newChatServer(t, func([]byte) string {
		return `{"choices":[{"message":{"reasoning_content":"primary","reasoning":"secondary","reasoning_details":[{"text":"fallback"}]}}]}`
	})
	resp, err := newTestProvider(srv.URL).Chat(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}

	message := resp.Choices[0].Message
	if message.Thinking.Text != "primary" {
		t.Fatalf("Thinking.Text = %q, want %q", message.Thinking.Text, "primary")
	}
	var payload struct {
		Version int                        `json:"version"`
		Fields  map[string]json.RawMessage `json:"fields"`
	}
	raw, ok := message.Custom["openai"]
	if !ok {
		t.Fatal("Custom should retain recognized provider payload")
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode Custom payload: %v", err)
	}
	if payload.Version != 1 {
		t.Errorf("payload version = %d, want 1", payload.Version)
	}
	for _, key := range []string{"reasoning_content", "reasoning", "reasoning_details"} {
		if len(payload.Fields[key]) == 0 {
			t.Errorf("Custom should retain %q: %s", key, raw)
		}
	}
}

func TestProvider_PreservesOpaqueReasoningDetailsWithoutDisplayingThem(t *testing.T) {
	for _, message := range []string{
		`{"reasoning_details":[]}`,
		`{"reasoning_details":[{"type":"opaque","data":{"x":1}}]}`,
		`{"reasoning_details":{"text":"wrong shape"}}`,
	} {
		srv := newChatServer(t, func([]byte) string {
			return `{"choices":[{"message":` + message + `}]}`
		})
		resp, err := newTestProvider(srv.URL).Chat(context.Background(), &core.ChatRequest{})
		if err != nil {
			t.Fatalf("Chat: %v", err)
		}
		got := resp.Choices[0].Message
		if got.Thinking.Text != "" {
			t.Errorf("message %s produced display text %q, want empty", message, got.Thinking.Text)
		}
		if raw, ok := got.Custom["openai"]; !ok || len(raw) == 0 {
			t.Errorf("message %s dropped Custom metadata", message)
		}
	}
}

func TestProvider_ReplayIsScopedToConfiguredName(t *testing.T) {
	var request map[string]any
	srv := newChatServer(t, func(body []byte) string {
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		return `{"choices":[{"message":{"role":"assistant","content":"ok","reasoning_content":"private"}}]}`
	})
	provider := NewProvider("secondary", srv.URL, "key", "model", &http.Client{})
	response, err := provider.Chat(context.Background(), &core.ChatRequest{Messages: []core.Message{{
		Role:   "assistant",
		Custom: customPayload(t, `"reasoning_content":"must not leak"`),
	}}})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if _, ok := response.Choices[0].Message.Custom["secondary"]; !ok {
		t.Errorf("response Custom should be keyed by provider name: %#v", response.Choices[0].Message.Custom)
	}
	message := request["messages"].([]any)[0].(map[string]any)
	if _, ok := message["reasoning_content"]; ok {
		t.Errorf("foreign replay leaked into OpenAI request: %#v", message)
	}
}

func TestProvider_DoesNotReplayMalformedPayload(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "unknown version", payload: `{"version":0,"fields":{"reasoning_content":"must not leak"}}`},
		{name: "broken json", payload: `{"version":1,"fields":`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var request struct {
				Messages []map[string]json.RawMessage `json:"messages"`
			}
			srv := newChatServer(t, func(body []byte) string {
				if err := json.Unmarshal(body, &request); err != nil {
					t.Fatalf("decode request: %v", err)
				}
				return `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`
			})
			_, err := newTestProvider(srv.URL).Chat(context.Background(), &core.ChatRequest{Messages: []core.Message{{
				Role:   "assistant",
				Custom: map[string]json.RawMessage{"openai": json.RawMessage(tt.payload)},
			}}})
			if err != nil {
				t.Fatalf("Chat: %v", err)
			}
			if _, ok := request.Messages[0]["reasoning_content"]; ok {
				t.Errorf("malformed replay leaked into OpenAI request: %s", request.Messages[0])
			}
		})
	}
}

func TestProvider_ReplaysThinkingWithToolCall(t *testing.T) {
	var requests []map[string]any
	srv := newChatServer(t, func(body []byte) string {
		var request map[string]any
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		requests = append(requests, request)
		return `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`
	})

	raw := customPayload(t, `"reasoning_content":"must replay"`)
	_, err := newTestProvider(srv.URL).Chat(context.Background(), &core.ChatRequest{Messages: []core.Message{{
		Role:      "assistant",
		Custom:    raw,
		ToolCalls: []core.ToolCall{{ID: "call-1", Type: "function", Function: core.ToolCallFunction{Name: "bash", Arguments: `{}`}}},
	}}})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	messages := requests[0]["messages"].([]any)
	assistant := messages[0].(map[string]any)
	if got := assistant["reasoning_content"]; got != "must replay" {
		t.Errorf("replayed reasoning_content = %#v, want %q", got, "must replay")
	}
}

func TestProvider_ReplaysThinkingWithoutNumericPrecisionLoss(t *testing.T) {
	var replayed json.RawMessage
	srv := newChatServer(t, func(body []byte) string {
		var request struct {
			Messages []struct {
				ReasoningDetails json.RawMessage `json:"reasoning_details"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		replayed = request.Messages[0].ReasoningDetails
		return `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`
	})

	fields := `"reasoning_details":[{"id":9007199254740993,"text":"keep precision"}]`
	_, err := newTestProvider(srv.URL).Chat(context.Background(), &core.ChatRequest{Messages: []core.Message{{
		Role:   "assistant",
		Custom: customPayload(t, fields),
	}}})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if got, want := string(replayed), `[{"id":9007199254740993,"text":"keep precision"}]`; got != want {
		t.Errorf("replayed reasoning_details = %s, want %s", got, want)
	}
}

func TestProvider_OrdinaryResponseHasNoThinking(t *testing.T) {
	srv := newChatServer(t, func([]byte) string {
		return `{"choices":[{"message":{"role":"assistant","content":"plain"}}]}`
	})
	resp, err := newTestProvider(srv.URL).Chat(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if got := resp.Choices[0].Message; got.Thinking.Text != "" || got.Custom != nil {
		t.Errorf("ordinary response = %#v, want no thinking or Custom", got)
	}
}

func TestProvider_ReplaysThinkingAcrossAgentToolRound(t *testing.T) {
	var mu sync.Mutex
	requestCount := 0
	srv := newChatServer(t, func(body []byte) string {
		mu.Lock()
		defer mu.Unlock()
		requestCount++
		if requestCount == 1 {
			return `{"choices":[{"message":{"role":"assistant","content":"","reasoning_content":"inspect tool","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}]}`
		}

		var request struct {
			Messages []map[string]any `json:"messages"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatalf("decode second request: %v", err)
		}
		var assistant map[string]any
		for _, message := range request.Messages {
			if message["role"] == "assistant" {
				assistant = message
			}
		}
		if assistant == nil || assistant["reasoning_content"] != "inspect tool" {
			t.Fatalf("second request did not replay reasoning: %#v", request.Messages)
		}
		return `{"choices":[{"message":{"role":"assistant","content":"done","reasoning_content":"compose answer"}}]}`
	})

	provider := newTestProvider(srv.URL)
	agent := coreagent.NewAgent(provider, "system", []core.Tool{{
		Name:       "lookup",
		Parameters: map[string]any{"type": "object"},
		Execute: func(context.Context, map[string]any) (string, error) {
			return "tool result", nil
		},
	}}, nil)
	tokens, history, err := agent.Run(context.Background(), nil, "run tool")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var kinds []string
	for token := range tokens {
		if token.Content != "" {
			kinds = append(kinds, token.Type)
		}
	}
	messages := <-history
	if requestCount != 2 {
		t.Fatalf("request count = %d, want 2", requestCount)
	}
	wantKinds := []string{core.TokenTypeThinking, core.TokenTypeToolCall, core.TokenTypeToolResult, core.TokenTypeThinking, core.TokenTypeText}
	if strings.Join(kinds, ",") != strings.Join(wantKinds, ",") {
		t.Errorf("token kinds = %v, want %v", kinds, wantKinds)
	}
	if got := messages[len(messages)-1]; got.Content != "done" || got.Thinking.Text != "compose answer" {
		t.Errorf("final message = %#v", got)
	}
}

func TestProvider_ChatStream(t *testing.T) {
	srv := newChatServer(t, func([]byte) string {
		return "data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"llo\"},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: [DONE]\n"
	})
	tokens, err := newTestProvider(srv.URL).ChatStream(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	var text strings.Builder
	done := false
	for token := range tokens {
		text.WriteString(token.Content)
		if token.Done {
			done = true
		}
	}
	if text.String() != "hello" {
		t.Errorf("streamed text = %q, want %q", text.String(), "hello")
	}
	if !done {
		t.Error("stream never signalled done")
	}
}

func TestProvider_ChatStreamEmptyDeltaChunksAreIgnored(t *testing.T) {
	srv := newChatServer(t, func([]byte) string {
		return "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
			"data: {\"choices\":[],\"usage\":{}}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n"
	})
	tokens, err := newTestProvider(srv.URL).ChatStream(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	var text strings.Builder
	for token := range tokens {
		text.WriteString(token.Content)
	}
	if text.String() != "ok" {
		t.Errorf("streamed text = %q, want %q", text.String(), "ok")
	}
}

func TestProvider_ChatStreamAssemblesToolCallDeltas(t *testing.T) {
	srv := newChatServer(t, func([]byte) string {
		return "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\"}}]}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"q\\\":\"}}]}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1}\"}}]}}]}\n\n" +
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":5,\"total_tokens\":8}}\n\n" +
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
			"data: [DONE]\n"
	})
	tokens, err := newTestProvider(srv.URL).ChatStream(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	var thinking, text strings.Builder
	var final *core.ChatResponse
	for token := range tokens {
		switch token.Type {
		case core.TokenTypeThinking:
			thinking.WriteString(token.Content)
		case core.TokenTypeText:
			text.WriteString(token.Content)
		}
		if token.Response != nil {
			final = token.Response
		}
	}
	if thinking.String() != "think" {
		t.Errorf("thinking deltas = %q, want %q", thinking.String(), "think")
	}
	if text.String() != "" {
		t.Errorf("text deltas = %q, want empty", text.String())
	}
	if final == nil {
		t.Fatal("stream never delivered assembled response")
	}
	msg := final.Choices[0].Message
	if len(msg.ToolCalls) != 1 || msg.ToolCalls[0].Function.Name != "lookup" ||
		msg.ToolCalls[0].Function.Arguments != `{"q":1}` {
		t.Errorf("assembled tool calls = %#v", msg.ToolCalls)
	}
	if msg.Thinking.Text != "think" {
		t.Errorf("assembled thinking = %q", msg.Thinking.Text)
	}
	if final.Usage == nil || final.Usage.TotalTokens != 8 {
		t.Errorf("usage = %#v, want total 8", final.Usage)
	}
	// Custom 回放契约：reasoning_content 键控到 provider name
	raw, ok := msg.Custom["openai"]
	if !ok {
		t.Fatalf("assembled message missing Custom replay: %#v", msg.Custom)
	}
	var payload struct {
		Fields map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode custom payload: %v", err)
	}
	var rc string
	if err := json.Unmarshal(payload.Fields["reasoning_content"], &rc); err != nil || rc != "think" {
		t.Errorf("custom reasoning_content = %q, err %v", rc, err)
	}
}

func TestProvider_ChatStreamNoTokensOnEmptyStream(t *testing.T) {
	srv := newChatServer(t, func([]byte) string {
		return "data: [DONE]\n"
	})
	tokens, err := newTestProvider(srv.URL).ChatStream(context.Background(), &core.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	n := 0
	for range tokens {
		n++
	}
	if n != 0 {
		t.Errorf("empty stream emitted %d tokens, want 0", n)
	}
}
