package model

import "encoding/json"

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
