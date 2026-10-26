package model

import (
	"encoding/json"
	"time"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/hitl"
)

const SessionSourceCapability = "session.source"

// SessionSummary is the daemon's protocol-neutral view of one persisted
// session. Active is intentionally a domain flag; ACP SessionInfo must remain
// a pure wire DTO and does not carry it.
type SessionSummary struct {
	SessionID string
	Cwd       string
	Title     string
	UpdatedAt string
	Active    bool
}

// SessionMode is a protocol-neutral model/provider choice.
type SessionMode struct {
	ID          string
	Name        string
	Description string
}

// SessionModeState is the daemon domain representation of available modes.
type SessionModeState struct {
	CurrentModeID  string
	AvailableModes []SessionMode
}

// SessionContent is the protocol-neutral text content used by replay events.
type SessionContent struct {
	Type string
	Text string
}

// SessionUpdate is a typed port between the daemon session domain and a wire
// adapter. Raw carries extension fields without exposing map[string]any as a
// capability API.
type SessionUpdate struct {
	Kind       string
	Content    *SessionContent
	ToolCallID string
	Title      string
	Status     string
	Raw        json.RawMessage
}

const (
	SessionUpdateUserMessageChunk  = "user_message_chunk"
	SessionUpdateAgentMessageChunk = "agent_message_chunk"
	SessionUpdateAgentThoughtChunk = "agent_thought_chunk"
	SessionUpdateToolCall          = "tool_call"
	SessionUpdateToolCallUpdate    = "tool_call_update"
	SessionUpdateAvailableCommands = "available_commands_update"
)

// SessionSource is the consumer-facing capability exposed by the agent plugin.
// It deliberately uses daemon domain types; the VS Code/ACP adapter maps them
// to ai-acp/protocol values at the transport boundary.
type SessionSource interface {
	ListSessions() []SessionSummary
	CurrentSessionID() string
	// ActiveSessionID 返回 transport 层的活动会话 id：最近一次经 session/new 或
	// session/load（当前会话）有效声明的 transport UUID。它与 CurrentSessionID
	// 返回的 store 稳定 id 不同源——前者是连接方自造的会话句柄，后者是持久化
	// store 的逻辑身份。空 = 尚无 ACP 连接声明过会话（守卫不参与）。
	// daemon 是单活动会话模型：prompt 一个已被其他连接声明顶掉的 transport 会话
	// 会在 FrontendHandler.Prompt 被 superseded 守卫拒绝，而非静默串线执行。
	ActiveSessionID() string
	SetSession(sessionID, cwd string)
	ReloadCurrentSession() error
	CurrentSessionHistory() []SessionUpdate
	ArchivedSessionHistory(sessionID string) ([]SessionUpdate, error)
	SessionModes() *SessionModeState
}

// PendingApprovalInfo 是一笔等待决策的审批快照的 daemon 侧扁平结构（P5-6）：
// 字段取 hitl.InterruptRequest 按 wire 需要的最小面（requestId 寻址 + 展示所需
// 的命令/风险/截止），不透传 agent 包的 PendingApproval 结构——model 是协议
// 中立层，避免 model→agent 类型泄漏（SessionSource 的 domain-type 先例同理，
// 方向相反：那处是 agent 类型不进 model，这处是 agent 包结构不进 model）。
// 完整原始中断（Effects/AllowAlways/Description 等）经 P5-8 attach 回放的
// permission.requested 事件 payload 携带，不在本清单面上重复。
type PendingApprovalInfo struct {
	RequestID string
	SessionID string
	RunID     string
	ToolName  string
	Command   string
	RiskLevel string
	ExpiresAt time.Time
}

// RunControlSource 是 x-goworker 扩展协议的消费面（P5-6，可选能力接口，handler
// 以 type-assert 获取——SessionSource 先例；无此能力的插件/测试桩零感知）。
// V1 路由面（拍板⑤）：run/start|status|cancel、attach 与 approval 全部限当前
// 会话（CurrentSessionID），归档历史走既有 session/load 只读重放。Control
// lease 为 V1 宿主授权简化形（拍板②）：本机信任、每会话至多一个持有者、断开
// 即释放（handler.Close）、后到连接显式接管（tookOver=true + slog 审计）。
type RunControlSource interface {
	// StartRun 提交一次后台 Run 并立即返回 (runID, 提交时状态)。requestID 是
	// 与 JSON-RPC id 不同的幂等键：重试同 requestID 返回原 runID 与其当前状态，
	// 不重复起跑。
	StartRun(sessionID, input, requestID string) (runID, state string, err error)
	CancelRun(sessionID, runID string) error
	// RunStatus 返回 run 当前状态与会话事件流的 latest seq（游标描述，非游标
	// 授权——客户端只有真正应用了对应事件才能推进本地游标）。
	RunStatus(sessionID, runID string) (state string, latestSeq uint64, err error)
	// PendingApprovals 返回会话未过期审批快照（daemon 侧扁平结构，避免
	// model→agent 类型泄漏）。未知会话/无审批返回空切片。
	PendingApprovals(sessionID string) ([]PendingApprovalInfo, error)
	// ResolveApproval 投递一笔审批决策：首个有效决策获胜；未知 requestID 或
	// 迟到/重复决策返回错误。
	ResolveApproval(requestID string, decision hitl.Decision) error
	// Attach 返回会话事件枢纽与当前 latest seq（P5-8 的 attach 面消费；本步
	// 落签名与实现——EnsureHub 使「重启后 attach 时 hub 未建」的缺口关闭）。
	Attach(sessionID string) (hub *runtimeagent.EventHub, latest uint64, err error)
	// ActiveRun 返回会话当前活动 Run 的 (runID, state) 快照（P5-8 attach 响应
	// 的 currentRun 字段消费；RunControlSource 在 P5-6 未备该访问器，P5-8 按
	// 响应契约最小补齐——RunStatus 需 runID 寻址，无法充当）。无活动 Run、无
	// runtime 或非 current 会话 → ok=false（currentRun=null，非错误）。
	ActiveRun(sessionID string) (runID, state string, ok bool)
	// AcquireControl 获取 control lease：空闲→占用并返回 false；他人持有→
	// 显式接管（返回 true，slog.Info 审计）；同持有者重复→false。
	AcquireControl(connectionID string) (tookOver bool)
	// ReleaseControl 释放 control lease：仅持有者本人有效，他人调用为空操作。
	ReleaseControl(connectionID string)
	// HoldControl 报告 connectionID 当前是否持有 control lease。
	HoldControl(connectionID string) bool
	// UnbindCapabilities 解绑某连接登记的全部客户端工具能力（P5-9 的
	// handler.Close 断连收口消费）。
	UnbindCapabilities(connectionID string)
	// CapabilityWaitTimeout 返回设备工具断连等待重绑的预算（拍板③默认 45s，
	// 硬约束 < ai-core 工具执行 60s）。
	CapabilityWaitTimeout() time.Duration
}
