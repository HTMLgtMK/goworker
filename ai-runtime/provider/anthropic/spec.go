package anthropic

import (
	"encoding/json"
)

type AnthropicAuthType string

const (
	AnthropicAuthBearer AnthropicAuthType = "bearer"
	AnthropicAuthAPIKey AnthropicAuthType = "x-api-key"
)

type anthropicMessageRequest struct {
	Model      string                    `json:"model"`
	MaxTokens  int                       `json:"max_tokens"`
	System     []map[string]string       `json:"system,omitempty"`
	Messages   []anthropicWireMessage    `json:"messages"`
	Tools      []anthropicToolDefinition `json:"tools,omitempty"`
	ToolChoice any                       `json:"tool_choice,omitempty"`
}

type anthropicWireMessage struct {
	Role    string            `json:"role"`
	Content []json.RawMessage `json:"content"`
}

type anthropicToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"input_schema"`
}

type anthropicMessageResponse struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Role       string          `json:"role"`
	Model      string          `json:"model"`
	Content    json.RawMessage `json:"content"`
	StopReason string          `json:"stop_reason"`
	Usage      anthropicUsage  `json:"usage"`
}

type anthropicUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
}
