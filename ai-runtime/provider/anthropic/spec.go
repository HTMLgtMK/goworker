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
	// Stream 为 true 时服务端以 SSE 返回（ChatStream 路径）；非流式请求不带该字段。
	Stream bool `json:"stream,omitempty"`
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

// anthropicStreamEvent 是 SSE data 行的统一视图：事件类型 + 各事件用到的字段组。
// event: 行本身不参与判别（部分兼容代理只发 data: 行），一律按 data.type 分派。
type anthropicStreamEvent struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
	// message_start 携带完整消息头（id/model/role + 初始 usage）。
	Message      *anthropicMessageResponse `json:"message"`
	ContentBlock *anthropicStreamBlock     `json:"content_block"`
	Delta        *anthropicStreamDelta     `json:"delta"`
	Usage        *anthropicUsage           `json:"usage"`
}

// anthropicStreamBlock 描述 content_block_start 的块头（thinking/text/tool_use）。
type anthropicStreamBlock struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	Thinking string `json:"thinking"`
	ID       string `json:"id"`
	Name     string `json:"name"`
}

// anthropicStreamDelta 汇总各增量类型：text_delta/thinking_delta/input_json_delta/
// signature_delta，以及 message_delta 里的 stop_reason。
type anthropicStreamDelta struct {
	Type        string `json:"type"`
	Text        string `json:"text"`
	Thinking    string `json:"thinking"`
	PartialJSON string `json:"partial_json"`
	Signature   string `json:"signature"`
	StopReason  string `json:"stop_reason"`
}
