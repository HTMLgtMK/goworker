package protocol

import "encoding/json"

// ACP 协议版本（REV_1）。
const Version = 1

// ACP 方法名。Client→Agent 与 Agent→Client 两方向共用同一连接。
const (
	// Client → Agent（requests）
	MethodInitialize     = "initialize"
	MethodAuthenticate   = "authenticate"
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

// ---- session/new ----

type NewSessionRequest struct {
	Cwd        string           `json:"cwd"`
	McpServers []map[string]any `json:"mcpServers,omitempty"`
}

type NewSessionResponse struct {
	SessionID string `json:"sessionId"`
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
	UpdateAgentMessageChunk = "agent_message_chunk"
	UpdateAgentThoughtChunk = "agent_thought_chunk"
	UpdateToolCall          = "tool_call"
	UpdateToolCallUpdate    = "tool_call_update"
	UpdatePlan              = "plan"
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
