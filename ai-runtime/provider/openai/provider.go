package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/provider"
)

// Provider 兼容 OpenAI API（OpenAI, Ollama, vLLM, etc.）。
type Provider struct {
	provider.BaseProvider
	thinking provider.ThinkingOptions
}

type ProviderOptions func(*Provider)

func NewProvider(name, endpoint, apiKey, model string, client *http.Client, options ...ProviderOptions) *Provider {
	if endpoint == "" {
		endpoint = "http://localhost:8000/v1"
	}
	if model == "" {
		model = "gpt-4o"
	}
	if client == nil {
		panic("Provider requires a non-nil http.Client")
	}

	provider := &Provider{
		BaseProvider: *provider.NewBaseProvider(name, strings.TrimRight(endpoint, "/"), apiKey, model, client),
	}

	if len(options) > 0 {
		for _, opt := range options {
			opt(provider)
		}
	}

	return provider
}

func WithThinkingOptions(thinking provider.ThinkingOptions) ProviderOptions {
	return func(p *Provider) {
		p.thinking = thinking
	}
}

func (p *Provider) Chat(ctx context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	request := *req
	if request.Model == "" {
		request.Model = p.Model()
	}
	request.Stream = false

	wireReq := p.buildChatRequest(&request)
	body, err := json.Marshal(wireReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	// 调试观测：log.level=debug 时打印请求/响应全量 body。别打 Authorization 头（含密钥）。
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("llm.chat request", "url", p.Endpoint()+"/chat/completions",
			"model", request.Model, "messages", len(req.Messages), "body", string(body))
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.Endpoint()+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.ApiKey())

	resp, err := p.Client().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if slog.Default().Enabled(ctx, slog.LevelDebug) {
			slog.Debug("llm.chat response", "status", resp.StatusCode, "model", request.Model)
		}
		return nil, fmt.Errorf("API %d", resp.StatusCode)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("llm.chat response", "status", resp.StatusCode,
			"model", request.Model, "body", string(raw))
	}

	var wireResp OpenAIChatResponse
	if err := json.Unmarshal(raw, &wireResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return normalizeChatResponse(wireResp, p.Name()), nil
}

func (p *Provider) buildChatRequest(req *core.ChatRequest) OpenAIChatRequest {
	messages := make([]map[string]any, 0, len(req.Messages))
	for _, message := range req.Messages {
		wireMessage := map[string]any{
			"role":    message.Role,
			"content": message.Content,
		}
		if message.ToolCallID != "" {
			wireMessage["tool_call_id"] = message.ToolCallID
		}
		if len(message.ToolCalls) > 0 {
			wireMessage["tool_calls"] = message.ToolCalls
		}
		if raw := message.Custom[p.Name()]; len(raw) > 0 {
			replayCustom(wireMessage, raw)
		}
		messages = append(messages, wireMessage)
	}

	wireReq := OpenAIChatRequest{
		Model:          req.Model,
		Messages:       messages,
		Stream:         false,
		Tools:          toolSpec(req.Tools),
		ToolChoice:     openAIToolChoice(req.ToolChoice),
		ResponseFormat: openAIResponseFormat(req.JSONMode),
	}
	switch p.thinking.RequestMode {
	case runtimeconfig.ThinkingRequestEnable:
		wireReq.EnableThinking = true
	case runtimeconfig.ThinkingRequestEffort:
		wireReq.ReasoningEffort = string(p.thinking.Effort)
	}
	return wireReq
}

func toolSpec(definitions []core.Tool) []map[string]any {
	tools := make([]map[string]any, 0, len(definitions))
	for _, definition := range definitions {
		tools = append(tools, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        definition.Name,
				"description": definition.Description,
				"parameters":  definition.Parameters,
			},
		})
	}
	return tools
}

func openAIToolChoice(choice core.ToolChoice) any {
	switch choice {
	case core.ToolChoiceRequired:
		return "required"
	case core.ToolChoiceNone:
		return "none"
	default:
		return nil
	}
}

func openAIResponseFormat(jsonMode bool) any {
	if !jsonMode {
		return nil
	}
	return map[string]any{"type": "json_object"}
}

func replayCustom(message map[string]any, raw json.RawMessage) {
	var payload struct {
		Version int                        `json:"version"`
		Fields  map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil || payload.Version != 1 {
		return
	}
	for _, key := range []string{"reasoning_content", "reasoning", "reasoning_details"} {
		value, ok := payload.Fields[key]
		if ok && json.Valid(value) {
			message[key] = value
		}
	}
}

func normalizeChatResponse(wire OpenAIChatResponse, providerName string) *core.ChatResponse {
	choices := make([]core.ResponseChoice, 0, len(wire.Choices))
	for _, choice := range wire.Choices {
		message := choice.Message
		thinking := normalizeThinking(message)
		choices = append(choices, core.ResponseChoice{
			Index: choice.Index,
			Message: core.Message{
				Role:       message.Role,
				Content:    message.Content,
				ToolCallID: message.ToolCallID,
				ToolCalls:  message.ToolCalls,
				Thinking:   thinking,
				Custom:     openAICustom(message, providerName),
			},
			FinishReason: choice.FinishReason,
		})
	}
	return &core.ChatResponse{
		ID:      wire.ID,
		Object:  wire.Object,
		Created: wire.Created,
		Model:   wire.Model,
		Choices: choices,
		Usage:   wire.Usage,
	}
}

func openAICustom(message OpenAIResponseMessage, providerName string) map[string]json.RawMessage {
	fields := make(map[string]json.RawMessage, 3)
	for _, field := range []struct {
		name  string
		value json.RawMessage
	}{
		{name: "reasoning_content", value: message.ReasoningContent},
		{name: "reasoning", value: message.Reasoning},
		{name: "reasoning_details", value: message.ReasoningDetails},
	} {
		if len(field.value) > 0 {
			fields[field.name] = field.value
		}
	}
	if len(fields) == 0 {
		return nil
	}
	payload, err := json.Marshal(struct {
		Version int                        `json:"version"`
		Fields  map[string]json.RawMessage `json:"fields"`
	}{Version: 1, Fields: fields})
	if err != nil {
		return nil
	}
	return map[string]json.RawMessage{providerName: payload}
}

func normalizeThinking(message OpenAIResponseMessage) core.Thinking {
	fields := []struct {
		name  string
		value json.RawMessage
	}{
		{name: "reasoning_content", value: message.ReasoningContent},
		{name: "reasoning", value: message.Reasoning},
		{name: "reasoning_details", value: message.ReasoningDetails},
	}

	var text string
	for _, field := range fields {
		if text == "" {
			text = thinkingText(field.name, field.value)
		}
	}
	return core.Thinking{Text: text}
}

func thinkingText(name string, raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	if name != "reasoning_details" {
		var text string
		if json.Unmarshal(raw, &text) == nil {
			return strings.TrimSpace(text)
		}
		return ""
	}

	var details []map[string]json.RawMessage
	if json.Unmarshal(raw, &details) != nil {
		return ""
	}
	parts := make([]string, 0, len(details))
	for _, detail := range details {
		for _, key := range []string{"text", "content"} {
			var text string
			if json.Unmarshal(detail[key], &text) == nil && strings.TrimSpace(text) != "" {
				parts = append(parts, strings.TrimSpace(text))
				break
			}
		}
	}
	return strings.Join(parts, "\n")
}

func (p *Provider) ChatStream(ctx context.Context, req *core.ChatRequest) (<-chan core.Token, error) {
	request := *req
	if request.Model == "" {
		request.Model = p.Model()
	}
	request.Stream = true

	wireReq := p.buildChatRequest(&request)
	wireReq.Stream = true
	body, err := json.Marshal(wireReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.Endpoint()+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.ApiKey())

	resp, err := p.Client().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("API %d", resp.StatusCode)
	}

	tokenCh := make(chan core.Token)
	go p.readSSE(ctx, resp.Body, tokenCh)
	return tokenCh, nil
}

func (p *Provider) readSSE(ctx context.Context, body io.ReadCloser, tokenCh chan<- core.Token) {
	defer body.Close()
	defer close(tokenCh)

	reader := bufio.NewReader(body)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")

		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			return
		}

		var chunk core.StreamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) == 0 {
			continue
		}

		content := chunk.Choices[0].Delta.Content
		finishReason := chunk.Choices[0].FinishReason

		select {
		case tokenCh <- core.Token{Type: core.TokenTypeText, Content: content, Done: finishReason != ""}:
		case <-ctx.Done():
			return
		}
		if finishReason != "" {
			return
		}
	}
}
