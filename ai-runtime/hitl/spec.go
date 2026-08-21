package hitl

import (
	"context"
	"time"
)

// DecisionType 表示用户对挂起工具执行的决策类型。
type DecisionType string

const (
	DecisionApprove DecisionType = "approve"
	DecisionReject  DecisionType = "reject"
	DecisionEdit    DecisionType = "edit"
	DecisionRespond DecisionType = "respond"
)

const DefaultTimeout = 2 * time.Minute

const EventInterrupt = "hitl:interrupt"

// InterruptRequest 描述工具执行因等待用户确认而暂停的原因。
type InterruptRequest struct {
	ID          string    `json:"id"`
	ToolName    string    `json:"tool_name"`
	Command     string    `json:"command"`
	RiskReason  string    `json:"risk_reason"`
	Description string    `json:"description"`
	RiskLevel   string    `json:"risk_level,omitempty"`
	Effects     []string  `json:"effects,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// Decision 是用户对 InterruptRequest 的响应。
type Decision struct {
	InterruptID string       `json:"interrupt_id"`
	Type        DecisionType `json:"type"`
	Command     string       `json:"command,omitempty"`
	Message     string       `json:"message,omitempty"`
}

type DecisionProvider interface {
	GetDecision(ctx context.Context, req *InterruptRequest) Decision
}

type ChannelDecisionProvider struct {
	decisions <-chan Decision
}
