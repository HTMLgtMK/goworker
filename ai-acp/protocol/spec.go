package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

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

type ClientCapabilities = Capabilities

type AgentCapabilities = Capabilities

// Capability 是 ACP capability advertisement 的最小契约。
// capability 的 wire value 由具体实现编码，集合负责 key 唯一性和对象聚合。
type Capability interface {
	Key() string
	json.Marshaler
}

// Capabilities 是不暴露动态 map 的 capability 集合。
// capability 是否被支持由它是否存在于集合中表达。
type Capabilities struct {
	items []Capability
}

type FileSystemCapability struct {
	ReadTextFile  bool `json:"readTextFile"`
	WriteTextFile bool `json:"writeTextFile"`
}

type LoadSessionCapability struct{}

type AuthMethod struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
}

// ---- session/load（可选，能力由 AgentCapabilities 中的 LoadSessionCapability 声明）----

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

// SessionModeState 是 session/new 与 session/load 响应携带的 ACP mode 状态。
// 是否支持 session/set_mode 由 server handler 决定。
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

type SessionInfo struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	Title     string `json:"title,omitempty"`
	UpdatedAt string `json:"updatedAt,omitempty"` // RFC3339
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

// ContentBlock 是当前 ACP text content 子集；其他 content kind 可在协议扩展时增加具体类型。
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

	rawKnown map[string]json.RawMessage
}

var sessionUpdateKnownFields = [...]string{
	"sessionUpdate",
	"content",
	"toolCallId",
	"title",
	"kind",
	"status",
}

// MarshalJSON 保留 ACP 扩展字段，同时让调用方对已知字段的修改生效。
func (b SessionUpdateBody) MarshalJSON() ([]byte, error) {
	type alias SessionUpdateBody
	if len(b.Raw) == 0 {
		return json.Marshal(alias(b))
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b.Raw, &raw); err != nil || raw == nil {
		if err == nil {
			err = errors.New("session update body must be a JSON object")
		}
		return nil, fmt.Errorf("marshal session update body: %w", err)
	}
	encoded, err := json.Marshal(alias(b))
	if err != nil {
		return nil, err
	}
	var current map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &current); err != nil {
		return nil, err
	}
	if b.rawKnown == nil {
		return append([]byte(nil), b.Raw...), nil
	}
	for _, field := range sessionUpdateKnownFields {
		original, wasKnown := b.rawKnown[field]
		value, isPresent := current[field]
		if wasKnown && bytes.Equal(original, value) {
			continue
		}
		if isPresent {
			raw[field] = value
		} else {
			delete(raw, field)
		}
	}
	return json.Marshal(raw)
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
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	b.rawKnown = make(map[string]json.RawMessage)
	for _, field := range sessionUpdateKnownFields {
		if value, ok := fields[field]; ok {
			b.rawKnown[field] = append(json.RawMessage(nil), value...)
		}
	}
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

// PermissionResponse 是 session/request_permission 的应答。
// ACP 规范里 outcome 是**嵌套**的判别联合（jsonrpc.ts / schema 的
// RequestPermissionOutcome），线上形状是：
//
//	{"outcome":{"outcome":"selected","optionId":"allow_once"}}
//	{"outcome":{"outcome":"cancelled"}}
//
// 不是扁平的 {"optionId":...} —— 后者对端 schema 校验不过，会当成取消/拒绝，
// 表现成"用户明明点了允许，agent 那边收到的还是 reject"。
type PermissionResponse struct {
	Outcome PermissionOutcome `json:"outcome"`
}

// PermissionOutcome 是用户的裁决：selected（选了某个 option）或 cancelled。
type PermissionOutcome struct {
	Outcome  string `json:"outcome"`            // selected / cancelled
	OptionID string `json:"optionId,omitempty"` // 仅 selected 时有意义
}

// SelectedPermissionOutcome 构造"用户选了某个 option"的应答。
func SelectedPermissionOutcome(optionID string) PermissionResponse {
	return PermissionResponse{Outcome: PermissionOutcome{Outcome: "selected", OptionID: optionID}}
}

// CancelledPermissionOutcome 构造"用户未裁决"的应答（关闭对话框/取消）。
func CancelledPermissionOutcome() PermissionResponse {
	return PermissionResponse{Outcome: PermissionOutcome{Outcome: "cancelled"}}
}

// ---- session/cancel（Client → Agent notification）----

type CancelNotification struct {
	SessionID string `json:"sessionId"`
}
