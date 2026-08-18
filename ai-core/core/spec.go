// Package core 定义 agent 引擎的 spec（接口与数据类型），不包含实现。
//
// 类型定义集中在本文件；方法实现分布在对应实现文件。
package core

import (
	"context"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-core/spec"
)

// ---- 工具 ----

// Tool 是 Agent 可调用的工具。
type Tool struct {
	Name        string
	Description string
	Parameters  map[string]any // JSON Schema
	Execute     func(ctx context.Context, args map[string]any) (string, error)
}

// ---- 消息 ----

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

// ---- LLM 请求/响应 ----

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

// ---- Token ----

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

// ---- Provider ----

// Provider 是 LLM 后端的接口。
type Provider interface {
	Name() string
	Model() string
	Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error)
	ChatStream(ctx context.Context, req *ChatRequest) (<-chan Token, error)
}

// ---- Middleware ----

// Middleware 标记接口，表示一个组件可以作为 Agent 的 middleware 注册。
type Middleware interface {
	Name() string
}

// MiddlewareResponse 包含 middleware 处理后的结果。
type MiddlewareResponse struct {
	Err error
}

// DecisionProvider 抽象 HITL 决策来源。
// 单机场景下由 ChannelDecisionProvider 包装 channel 实现，
// 分布式场景下可实现为轮询 API 端点。
type DecisionProvider interface {
	// GetDecision 获取用户对中断请求的决策。阻塞直到有结果或 ctx 取消。
	GetDecision(ctx context.Context, req *spec.InterruptRequest) spec.HITLDecision
}

// ---- Event structs ----

// BeforeAgentEvent BeforeAgent 点位的事件。
type BeforeAgentEvent struct {
	Ctx   context.Context
	Input string
}

// AfterAgentEvent AfterAgent 点位的事件。
type AfterAgentEvent struct {
	Ctx     context.Context
	History []Message
	Err     error
}

// BeforeModelEvent BeforeModel 点位的事件。
type BeforeModelEvent struct {
	Ctx     context.Context
	History []Message
	Input   string
	// History 可被 middleware 整体替换（如压缩历史）：Agent 循环 fire 事件后回读它作为实际发送的历史。
	// 约定：只能整体赋值，不要改元素 —— 切片共享底层数组，改元素会污染调用方持有的数据。
}

// AfterModelEvent AfterModel 点位的事件。
type AfterModelEvent struct {
	Ctx     context.Context
	History []Message
	Err     error
	Usage   *UsageInfo // 本次 Chat 调用的 token 用量，模型不返回时为 nil
}

// BeforeToolEvent BeforeTool 点位的事件。
// TokenCh 供 HITL middleware 发送 interrupt/reject token。
// middleware 可设置 Aborted=true 跳过本次 tool call，
// 或修改 Args 变更执行参数。ResponseMessages 会在 Aborted 后追加到会话历史。
type BeforeToolEvent struct {
	Ctx              context.Context
	History          []Message
	Tool             *ToolCall
	TokenCh          chan<- Token
	Args             map[string]any
	Aborted          bool
	ResponseMessages []Message
}

// AfterToolEvent AfterTool 点位的事件。
type AfterToolEvent struct {
	Ctx     context.Context
	History []Message
	Tool    *ToolCall
	Err     error
}

// ---- Hook 接口 ----

type BeforeAgent interface {
	Middleware
	OnBeforeAgent(*BeforeAgentEvent) *MiddlewareResponse
}

type AfterAgent interface {
	Middleware
	OnAfterAgent(*AfterAgentEvent) *MiddlewareResponse
}

type BeforeModel interface {
	Middleware
	OnBeforeModel(*BeforeModelEvent) *MiddlewareResponse
}

type AfterModel interface {
	Middleware
	OnAfterModel(*AfterModelEvent) *MiddlewareResponse
}

type BeforeTool interface {
	Middleware
	OnBeforeTool(*BeforeToolEvent) *MiddlewareResponse
}

type AfterTool interface {
	Middleware
	OnAfterTool(*AfterToolEvent) *MiddlewareResponse
}

// ---- Usage ----

// Usage 单次 Chat 调用的 token 账本。
type Usage struct {
	Iteration             int // ReAct 第几轮（从 0 开始）
	PromptTokens          int // 模型返回的输入 token
	CompletionTokens      int // 模型返回的输出 token
	TotalTokens           int // 模型返回的总计
	EstimateTokens        int // 发送前粗估（JSON 字节数 / 4），对照模型返回值
	PromptCacheHitTokens  int // 用户 prompt 中，命中上下文缓存的 token 数
	PromptCacheMissTokens int // 用户 prompt 中，未命中上下文缓存的 token 数

	LastPromptTokens int // 仅快照有效：最近一次 Chat 的输入 token（上下文占用计算用）
}

// Compaction 记录一次历史压缩。
// 压缩是 ReAct 循环外的模型调用（summarize），也必须进账，否则 /usage 是漏水的桶。
type Compaction struct {
	BeforeMsgs int // 压缩前消息条数
	AfterMsgs  int // 压缩后消息条数
	Tokens     int // summarize 调用消耗（模型返回 total，缺省用估算）
}

// UsageTracker 累加一次 /agent 会话内所有 Chat 调用的用量。
//
// 写入方是 agent 的 ReAct 循环（goroutine），读取方是 status bar addon
// 与 /usage 命令（可能在不同 goroutine），故内部用 mutex 保护。
// 只记当前会话，跨会话累计交给 slog 日志 + 未来的持久化。
type UsageTracker struct {
	mu          sync.Mutex
	calls       []Usage      // 明细，/usage 命令展示用
	total       Usage        // 累计快照，status bar 用
	compactions []Compaction // 历史压缩记录，/usage 命令展示用
}

// ---- Compressor ----

// CompressReport 压缩一次的结果，交给调用方记账/展示。
type CompressReport struct {
	BeforeMsgs int // 压缩前消息条数
	AfterMsgs  int // 压缩后消息条数
	Tokens     int // summarize 调用消耗（模型返回 total，缺省用估算）
}

// Compressor 通过 LLM 把过长的对话历史压成"滚动摘要"：
// 保留最近 keepLast 条消息原文，只把更早的旧段发给模型要一段摘要，
// 输出 [摘要] + 最近 N 条。
//
// 为什么不全量压成一条：ReAct 会话里工具结果、文件路径、最近结论是
// 继续干活的关键上下文，压掉就断线了。滚动只牺牲最古老的记忆，
// 而且压缩调用只读旧段，不会在压缩这一步把窗口撑爆。
type Compressor struct {
	provider Provider
	keepLast int
	// protectSystem 为 true 时，首位 system 消息（agent 系统提示）原样保留、不参与压缩。
	// 自动压缩路径要开：ReAct 循环里 history[0] 是 buildMessages 注入的提示词，压掉模型就忘了怎么用工具。
	// /compact 路径要关：p.conversation 从不含系统提示（plugin 存的是 messages[1:]），
	// 首位可能是上次的摘要，必须允许被再次滚动，否则摘要会一条条累积。
	protectSystem bool
	// onCompress 压缩成功后回调（可选），调用方用它把压缩记进 token 账本。
	onCompress func(CompressReport)
}
