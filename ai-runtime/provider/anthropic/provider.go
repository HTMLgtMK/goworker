package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/provider"
)

const (
	anthropicVersion         = "2023-06-01"
	anthropicJSONInstruction = "Respond with valid JSON only."
	maxAnthropicResponseSize = 10 << 20
)

// Provider 适配 Anthropic Messages API 的非流式请求。
type Provider struct {
	provider.BaseProvider
	authType  AnthropicAuthType
	maxTokens int
}

type ProviderOption func(*Provider)

func WithAnthropicMaxTokens(maxTokens int) ProviderOption {
	return func(p *Provider) {
		if maxTokens > 0 {
			p.maxTokens = maxTokens
		}
	}
}

func NewProvider(name, endpoint, apiKey, model string, client *http.Client, authType AnthropicAuthType, options ...ProviderOption) *Provider {
	if name == "" {
		name = "anthropic"
	}
	if endpoint == "" {
		endpoint = "https://api.anthropic.com"
	}
	if model == "" {
		model = "claude-sonnet-4-6"
	}
	if authType == "" {
		authType = AnthropicAuthAPIKey
	}
	provider := &Provider{
		BaseProvider: *provider.NewBaseProvider(name, strings.TrimRight(endpoint, "/"), apiKey, model, client),
		authType:     authType,
		maxTokens:    8192,
	}
	for _, option := range options {
		option(provider)
	}
	return provider
}

func (p *Provider) Chat(ctx context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	wireReq, err := p.buildMessageRequest(req)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(wireReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint()+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("anthropic-version", anthropicVersion)
	switch p.authType {
	case AnthropicAuthBearer:
		httpReq.Header.Set("Authorization", "Bearer "+p.ApiKey())
	case AnthropicAuthAPIKey:
		httpReq.Header.Set("x-api-key", p.ApiKey())
	default:
		return nil, fmt.Errorf("unsupported Anthropic auth type %q", p.authType)
	}

	resp, err := p.Client().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAnthropicResponseSize+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if len(data) > maxAnthropicResponseSize {
		return nil, fmt.Errorf("response exceeds %d byte limit", maxAnthropicResponseSize)
	}
	var wireResp anthropicMessageResponse
	if err := json.Unmarshal(data, &wireResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	response, err := normalizeAnthropicResponse(wireResp, p.Name())
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (p *Provider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, fmt.Errorf("Anthropic Messages streaming is not supported")
}

func (p *Provider) buildMessageRequest(req *core.ChatRequest) (anthropicMessageRequest, error) {
	model := req.Model
	if model == "" {
		model = p.Model()
	}
	system, messages, err := anthropicMessages(req.Messages, req.JSONMode, p.Name())
	if err != nil {
		return anthropicMessageRequest{}, err
	}
	return anthropicMessageRequest{
		Model:      model,
		MaxTokens:  p.maxTokens,
		System:     system,
		Messages:   messages,
		Tools:      anthropicTools(req.Tools),
		ToolChoice: anthropicToolChoice(req.ToolChoice),
	}, nil
}

func anthropicMessages(messages []core.Message, jsonMode bool, providerName string) ([]map[string]string, []anthropicWireMessage, error) {
	system := make([]map[string]string, 0, 1)
	wire := make([]anthropicWireMessage, 0, len(messages))
	for _, message := range messages {
		if message.Role == "system" {
			system = append(system, map[string]string{"type": "text", "text": message.Content})
			continue
		}
		blocks, err := anthropicBlocks(message, providerName)
		if err != nil {
			return nil, nil, err
		}
		role := message.Role
		if role == "tool" {
			role = "user"
		}
		if len(wire) > 0 && wire[len(wire)-1].Role == role {
			wire[len(wire)-1].Content = append(wire[len(wire)-1].Content, blocks...)
			continue
		}
		wire = append(wire, anthropicWireMessage{Role: role, Content: blocks})
	}
	if jsonMode {
		if len(system) == 0 {
			system = append(system, map[string]string{"type": "text", "text": anthropicJSONInstruction})
		} else {
			system[len(system)-1]["text"] += "\n\n" + anthropicJSONInstruction
		}
	}
	return system, wire, nil
}

func anthropicBlocks(message core.Message, providerName string) ([]json.RawMessage, error) {
	if message.Role == "tool" {
		return marshalAnthropicBlocks([]any{map[string]any{
			"type":        "tool_result",
			"tool_use_id": message.ToolCallID,
			"content":     message.Content,
		}})
	}
	if message.Role == "assistant" {
		if raw := message.Custom[providerName]; len(raw) > 0 {
			var payload struct {
				Version          int             `json:"version"`
				AssistantContent json.RawMessage `json:"assistant_content"`
			}
			if err := json.Unmarshal(raw, &payload); err == nil && payload.Version == 1 {
				var blocks []json.RawMessage
				if err := json.Unmarshal(payload.AssistantContent, &blocks); err == nil {
					return blocks, nil
				}
			}
		}
	}
	blocks := make([]any, 0, len(message.ToolCalls)+1)
	if message.Content != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": message.Content})
	}
	for _, call := range message.ToolCalls {
		var input json.RawMessage
		if err := json.Unmarshal([]byte(call.Function.Arguments), &input); err != nil {
			return nil, fmt.Errorf("decode tool call %q input: %w", call.ID, err)
		}
		blocks = append(blocks, map[string]any{
			"type":  "tool_use",
			"id":    call.ID,
			"name":  call.Function.Name,
			"input": input,
		})
	}
	return marshalAnthropicBlocks(blocks)
}

func marshalAnthropicBlocks(blocks []any) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(blocks))
	for _, block := range blocks {
		data, err := json.Marshal(block)
		if err != nil {
			return nil, err
		}
		out = append(out, data)
	}
	return out, nil
}

func anthropicTools(source []core.Tool) []anthropicToolDefinition {
	definitions := make([]anthropicToolDefinition, 0, len(source))
	for _, tool := range source {
		definitions = append(definitions, anthropicToolDefinition{Name: tool.Name, Description: tool.Description, InputSchema: tool.Parameters})
	}
	return definitions
}

func anthropicToolChoice(choice core.ToolChoice) any {
	switch choice {
	case core.ToolChoiceRequired:
		return map[string]string{"type": "any"}
	case core.ToolChoiceNone:
		return map[string]string{"type": "none"}
	default:
		return nil
	}
}

func normalizeAnthropicResponse(response anthropicMessageResponse, providerName string) (*core.ChatResponse, error) {
	message := core.Message{Role: response.Role}
	var blocks []json.RawMessage
	if err := json.Unmarshal(response.Content, &blocks); err != nil {
		return nil, fmt.Errorf("decode response content: %w", err)
	}
	message.Custom = anthropicCustom(response.Content, providerName)
	thinking := make([]string, 0)
	for _, block := range blocks {
		var header struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(block, &header) != nil {
			continue
		}
		switch header.Type {
		case "text":
			var text struct {
				Text string `json:"text"`
			}
			if json.Unmarshal(block, &text) == nil {
				message.Content += text.Text
			}
		case "thinking":
			var thought struct {
				Thinking string `json:"thinking"`
			}
			if json.Unmarshal(block, &thought) == nil && thought.Thinking != "" {
				thinking = append(thinking, thought.Thinking)
			}
		case "tool_use":
			var tool struct {
				ID    string          `json:"id"`
				Name  string          `json:"name"`
				Input json.RawMessage `json:"input"`
			}
			if json.Unmarshal(block, &tool) == nil {
				message.ToolCalls = append(message.ToolCalls, core.ToolCall{ID: tool.ID, Type: "function", Function: core.ToolCallFunction{Name: tool.Name, Arguments: string(tool.Input)}})
			}
		}
	}
	message.Thinking.Text = strings.Join(thinking, "\n")
	return &core.ChatResponse{
		ID:      response.ID,
		Object:  response.Type,
		Model:   response.Model,
		Choices: []core.ResponseChoice{{Message: message, FinishReason: anthropicFinishReason(response.StopReason)}},
		Usage: &core.UsageInfo{
			PromptTokens:          response.Usage.InputTokens,
			CompletionTokens:      response.Usage.OutputTokens,
			PromptCacheHitTokens:  response.Usage.CacheReadInputTokens,
			PromptCacheMissTokens: response.Usage.CacheCreationInputTokens,
			TotalTokens:           response.Usage.InputTokens + response.Usage.OutputTokens,
		},
	}, nil
}

func anthropicCustom(content json.RawMessage, providerName string) map[string]json.RawMessage {
	payload, err := json.Marshal(struct {
		Version          int             `json:"version"`
		AssistantContent json.RawMessage `json:"assistant_content"`
	}{Version: 1, AssistantContent: content})
	if err != nil {
		return nil
	}
	return map[string]json.RawMessage{providerName: payload}
}

func anthropicFinishReason(stopReason string) string {
	switch stopReason {
	case "tool_use":
		return "tool_calls"
	case "max_tokens":
		return "length"
	case "end_turn":
		return "stop"
	default:
		return stopReason
	}
}
