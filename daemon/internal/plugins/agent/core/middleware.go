package core

import (
	"context"

	"github.com/tinguo/goworker/daemon/internal/spec"
)

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
