// Package core 定义 agent 插件的 spec（接口与数据类型），不包含实现。
package core

import (
	"context"
	"time"

	"github.com/tinguo/goworker/daemon/internal/spec"
)

// Tool 是 Agent 可调用的工具。
type Tool struct {
	Name        string
	Description string
	Parameters  map[string]any // JSON Schema
	Execute     func(ctx context.Context, args map[string]any) (string, error)
}

// ToolSpec 返回 OpenAI 兼容的工具定义。
func (t Tool) ToolSpec() map[string]any {
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        t.Name,
			"description": t.Description,
			"parameters":  t.Parameters,
		},
	}
}

// Message 是聊天会话中的单条消息。
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	MsgID      string     `json:"-"`
	CreatedAt  time.Time  `json:"-"`
}

// ToolCall 是 LLM 请求的函数调用。
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function ToolCallFunction `json:"function"`
}

// ToolCallFunction 是 ToolCall 的具体调用信息。
type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ChatRequest /v1/chat/completions 请求体。
type ChatRequest struct {
	Model          string           `json:"model"`
	Messages       []Message        `json:"messages"`
	Stream         bool             `json:"stream"`
	Tools          []map[string]any `json:"tools,omitempty"`
	ToolChoice     any              `json:"tool_choice,omitempty"`
	ResponseFormat any              `json:"response_format,omitempty"`
}

// ChatResponse 非流式响应。
type ChatResponse struct {
	ID      string           `json:"id"`
	Object  string           `json:"object"`
	Created int64            `json:"created"`
	Model   string           `json:"model"`
	Choices []ResponseChoice `json:"choices"`
	Usage   *UsageInfo       `json:"usage,omitempty"`
}

// ResponseChoice 表示 ChatResponse 中的一个候选回答。
type ResponseChoice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

// UsageInfo 记录每次请求的 token 用量。
type UsageInfo struct {
	PromptTokens          int `json:"prompt_tokens"`
	PromptCacheHitTokens  int `json:"prompt_cache_hit_tokens"`
	PromptCacheMissTokens int `json:"prompt_cache_miss_tokens"`
	CompletionTokens      int `json:"completion_tokens"`
	TotalTokens           int `json:"total_tokens"`
}

// StreamChunk SSE 流式响应的数据块。
type StreamChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []DeltaChoice `json:"choices"`
}

// DeltaChoice 表示流式响应中的一个增量选择。
type DeltaChoice struct {
	Index        int    `json:"index"`
	Delta        Delta  `json:"delta"`
	FinishReason string `json:"finish_reason,omitempty"`
}

// Delta 是流式响应中的增量更新。
type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// Token 流式输出中的一个 token。
type Token struct {
	Type      string // "text" / "tool_call" / "tool_result" / "interrupt"
	Content   string
	Done      bool
	Interrupt *spec.InterruptRequest // Type == "interrupt" 时填充
}

// Token 类型常量。
const (
	TokenTypeText       = "text"
	TokenTypeToolCall   = "tool_call"
	TokenTypeToolResult = "tool_result"
	TokenTypeInterrupt  = "interrupt"
)
