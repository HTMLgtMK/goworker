package protocol

import "encoding/json"

// ACP 协议版本（REV_1）。
const Version = 1

// ACP 方法名。Client→Agent 与 Agent→Client 两方向共用同一连接。
const (
	// Client → Agent（requests）
	MethodInitialize     = "initialize"
	MethodAuthenticate   = "authenticate"
	MethodSessionList    = "session/list"
	MethodSessionNew     = "session/new"
	MethodSessionLoad    = "session/load"
	MethodSessionPrompt  = "session/prompt"
	MethodSessionSetMode = "session/set_mode"

	// Client → Agent（notifications）
	MethodSessionCancel = "session/cancel"

	// Agent → Client（requests）
	MethodSessionRequestPermission = "session/request_permission"

	// Agent → Client（notifications）
	MethodSessionUpdate = "session/update"
)

// ---- initialize ----

type InitializeRequest struct {
	ProtocolVersion    int                `json:"protocolVersion"`
	ClientCapabilities ClientCapabilities `json:"clientCapabilities,omitempty"`
}

type InitializeResponse struct {
	ProtocolVersion   int               `json:"protocolVersion"`
	AgentCapabilities AgentCapabilities `json:"agentCapabilities,omitempty"`
	AuthMethods       []AuthMethod      `json:"authMethods,omitempty"`
}

type ClientCapabilities struct {
	Fs *FsCapabilities `json:"fs,omitempty"`
}

type FsCapabilities struct {
	ReadTextFile  bool `json:"readTextFile"`
	WriteTextFile bool `json:"writeTextFile"`
}

type AgentCapabilities struct {
	LoadSession bool `json:"loadSession,omitempty"`
}

type AuthMethod struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
}

// ---- session/load（可选，能力协商 AgentCapabilities.LoadSession）----

type LoadSessionRequest struct {
	SessionID  string           `json:"sessionId"`
	Cwd        string           `json:"cwd"`
	McpServers []map[string]any `json:"mcpServers"`
}

// LoadSessionResponse 是 load 成功响应。按 ACP 只含可选的 modes；handler 未提供
// modes（SessionModesProvider 未实现或返回 nil）时序列化为 {}。
type LoadSessionResponse struct {
	Modes *SessionModeState `json:"modes,omitempty"`
}

// ---- session/modes（只显示不切换）----

// SessionMode 是一个可选模式条目（对齐 @agentclientprotocol/sdk 的 SessionMode）。
// daemon 数据源是 cfg.LLM：id = provider 名，name = 人类可读（provider (model)）。
type SessionMode struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// SessionModeState 是 session/new 与 session/load 响应携带的 modes 状态。
// daemon 只显示不切换：session/set_mode 未实现，客户端切换请求以
// method-not-found 诚实失败。
type SessionModeState struct {
	CurrentModeID  string        `json:"currentModeId"`
	AvailableModes []SessionMode `json:"availableModes"`
}

// ---- session/list ----

// ListSessionsRequest 的 cwd/cursor 当前被清单方忽略：清单一次给全（有上限），
// 不返回 nextCursor，客户端无需翻页。字段保留以对齐 @agentclientprotocol/sdk。
type ListSessionsRequest struct {
	Cwd    string `json:"cwd,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// SessionInfo 是清单里的一条会话摘要（字段名对齐 @agentclientprotocol/sdk）。
// IsCurrent 标记当前活动会话（单活动会话模型下清单里至多一条为 true），前端据此
// 区分可续接条目与只读归档条目。
type SessionInfo struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	Title     string `json:"title,omitempty"`
	UpdatedAt string `json:"updatedAt,omitempty"` // RFC3339
	IsCurrent bool   `json:"isCurrent,omitempty"`
}

type ListSessionsResponse struct {
	Sessions   []SessionInfo `json:"sessions"`
	NextCursor string        `json:"nextCursor,omitempty"`
}

// ---- session/new ----

type NewSessionRequest struct {
	Cwd        string           `json:"cwd"`
	McpServers []map[string]any `json:"mcpServers"`
}

type NewSessionResponse struct {
	SessionID string            `json:"sessionId"`
	Modes     *SessionModeState `json:"modes,omitempty"`
}

// ---- session/prompt ----

type PromptRequest struct {
	SessionID string         `json:"sessionId"`
	Prompt    []ContentBlock `json:"prompt"`
}

// ContentBlock 是任务输入块；dispatcher 只产出 text。
type ContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

func TextBlock(text string) ContentBlock {
	return ContentBlock{Type: "text", Text: text}
}

// stop reason 取值（ACP REV_1）。
const (
	StopEndTurn        = "end_turn"
	StopMaxTokens      = "max_tokens"
	StopMaxTurnRequest = "max_turn_requests"
	StopRefusal        = "refusal"
	StopCancelled      = "cancelled"
)

type PromptResponse struct {
	StopReason string `json:"stopReason"`
}

// ---- session/update（Agent → Client notification）----

type SessionUpdate struct {
	SessionID string            `json:"sessionId"`
	Update    SessionUpdateBody `json:"update"`
}

// SessionUpdateBody 按 update.sessionUpdate 判别具体类型；
// 未识别字段走 Raw 透传，不丢信息。
type SessionUpdateBody struct {
	SessionUpdate string          `json:"sessionUpdate"`
	Content       *ContentBlock   `json:"content,omitempty"`
	ToolCallID    string          `json:"toolCallId,omitempty"`
	Title         string          `json:"title,omitempty"`
	Kind          string          `json:"kind,omitempty"`
	Status        string          `json:"status,omitempty"`
	Raw           json.RawMessage `json:"-"`
}

// UnmarshalJSON 解码已知字段并保留完整原始报文（供逐字转发）。
func (b *SessionUpdateBody) UnmarshalJSON(data []byte) error {
	type alias SessionUpdateBody
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*b = SessionUpdateBody(a)
	b.Raw = append(b.Raw[:0], data...)
	return nil
}

// update 判别值（ACP REV_1 子集）。
const (
	UpdateUserMessageChunk  = "user_message_chunk"
	UpdateAgentMessageChunk = "agent_message_chunk"
	UpdateAgentThoughtChunk = "agent_thought_chunk"
	UpdateToolCall          = "tool_call"
	UpdateToolCallUpdate    = "tool_call_update"
	UpdatePlan              = "plan"
	UpdateAvailableCommands = "available_commands_update"
)

// ---- session/request_permission（Agent → Client request）----

type PermissionRequest struct {
	SessionID string             `json:"sessionId"`
	ToolCall  ToolCallInfo       `json:"toolCall"`
	Options   []PermissionOption `json:"options"`
}

type ToolCallInfo struct {
	ToolCallID string `json:"toolCallId"`
	Title      string `json:"title,omitempty"`
}

type PermissionOption struct {
	OptionID string `json:"optionId"`
	Name     string `json:"name,omitempty"`
	Kind     string `json:"kind,omitempty"` // allow_once / allow_always / reject_once / reject_always
}

type PermissionResponse struct {
	OptionID string `json:"optionId"`
}

// ---- session/cancel（Client → Agent notification）----

type CancelNotification struct {
	SessionID string `json:"sessionId"`
}
