package openai

import (
	"encoding/json"

	"github.com/tinguo/goworker/ai-core/core"
)

type OpenAIChatRequest struct {
	Model           string           `json:"model"`
	Messages        []map[string]any `json:"messages"`
	Stream          bool             `json:"stream"`
	Tools           []map[string]any `json:"tools,omitempty"`
	ToolChoice      any              `json:"tool_choice,omitempty"`
	ResponseFormat  any              `json:"response_format,omitempty"`
	EnableThinking  bool             `json:"enable_thinking,omitempty"`
	ReasoningEffort string           `json:"reasoning_effort,omitempty"`
}

type OpenAIChatResponse struct {
	ID      string                 `json:"id"`
	Object  string                 `json:"object"`
	Created int64                  `json:"created"`
	Model   string                 `json:"model"`
	Choices []OpenAIResponseChoice `json:"choices"`
	Usage   *core.UsageInfo        `json:"usage,omitempty"`
}

type OpenAIResponseChoice struct {
	Index        int                   `json:"index"`
	Message      OpenAIResponseMessage `json:"message"`
	FinishReason string                `json:"finish_reason"`
}

type OpenAIResponseMessage struct {
	Role             string          `json:"role"`
	Content          string          `json:"content"`
	ToolCallID       string          `json:"tool_call_id,omitempty"`
	ToolCalls        []core.ToolCall `json:"tool_calls,omitempty"`
	ReasoningContent json.RawMessage `json:"reasoning_content,omitempty"`
	Reasoning        json.RawMessage `json:"reasoning,omitempty"`
	ReasoningDetails json.RawMessage `json:"reasoning_details,omitempty"`
}
