// Package spec 定义 goworker 引擎的核心类型。
package spec

import "time"

// DecisionType 表示用户对挂起工具执行的决策类型。
type DecisionType string

const (
	DecisionApprove DecisionType = "approve" // 批准执行
	DecisionReject  DecisionType = "reject"  // 拒绝执行
	DecisionEdit    DecisionType = "edit"    // 编辑命令后执行
	DecisionRespond DecisionType = "respond" // 回复指令，不执行工具
)

// InterruptRequest 描述工具执行因等待用户确认而暂停的原因。
// 由 agent goroutine 通过 token channel 发送给前端。
type InterruptRequest struct {
	ID          string    `json:"id"`                   // 唯一请求 ID
	ToolName    string    `json:"tool_name"`            // 工具名称，如 "bash"
	Command     string    `json:"command"`              // 触发风险检查的命令文本
	RiskReason  string    `json:"risk_reason"`          // 匹配的风险模式描述
	Description string    `json:"description"`          // 前端展示用详细描述
	RiskLevel   string    `json:"risk_level,omitempty"` // 新增：R0-R7 风险等级，前端渲染 [R4] 标签
	Effects     []string  `json:"effects,omitempty"`    // 新增：副作用 Names()，如 ["destructive"]
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"` // 超时时间
}

// HITLDecision 是用户对 InterruptRequest 的响应。
// 前端构造此类型并通过 decisions channel 发送回 agent。
type HITLDecision struct {
	InterruptID string       `json:"interrupt_id"`
	Type        DecisionType `json:"type"`
	Command     string       `json:"command,omitempty"` // DecisionEdit 时填充
	Message     string       `json:"message,omitempty"` // DecisionRespond 时填充
}
