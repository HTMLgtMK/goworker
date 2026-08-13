package session

import (
	"encoding/json"
	"time"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
)

// 本文件定义 jsonl 记录的类型化模型。每种记录独立 struct（按 kind 区分），
// Record 按 kind 持有对应字段组，序列化时展平为磁盘单行（与旧聚合格式完全兼容）。

// 记录类型。
const (
	kindMsg        = "msg"
	kindCompact    = "compact"
	kindHead       = "head"
	kindCheckpoint = "checkpoint"
	kindCursor     = "cursor"
)

// msgFields 真实消息（含 compact 时的保留段副本）。
type msgFields struct {
	ID         string           `json:"id,omitempty"`
	Parent     string           `json:"parent,omitempty"`
	Role       string           `json:"role,omitempty"`
	Content    string           `json:"content,omitempty"`
	ToolCalls  []core.ToolCall  `json:"tool_calls,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	CloneOf    string           `json:"clone_of,omitempty"` // 副本标记：从哪个原消息复制而来
	CreatedAt  time.Time        `json:"created_at"`
}

// compactFields 摘要节点。
type compactFields struct {
	ID          string    `json:"id,omitempty"`
	Parent      string    `json:"parent,omitempty"`
	Summary     string    `json:"summary,omitempty"`
	CoveredFrom string    `json:"covered_from,omitempty"`
	CoveredTo   string    `json:"covered_to,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// headFields 当前尾节点（无 id/parent，tail 指向生效节点）。
type headFields struct {
	Tail      string    `json:"tail,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// checkpointFields rewind 锚点。
type checkpointFields struct {
	ID        string    `json:"id,omitempty"`
	At        string    `json:"at,omitempty"`
	Preview   string    `json:"preview,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// cursorFields 固化游标（无 id/parent）。
type cursorFields struct {
	MsgID     string    `json:"msg_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Record 是 jsonl 记录的统一视图：kind 判别 + 对应类型的字段组。
// 磁盘格式保持扁平单行，序列化/反序列化见 MarshalJSON/UnmarshalJSON。
type Record struct {
	Kind string `json:"kind"`

	Msg  *msgFields       // kind == kindMsg（含副本）
	Comp *compactFields   // kind == kindCompact
	Head *headFields      // kind == kindHead
	CK   *checkpointFields // kind == kindCheckpoint
	Cur  *cursorFields    // kind == kindCursor
}

// NodeID 返回节点 id（msg/compact/checkpoint 有，head/cursor 无则空串）。
func (r Record) NodeID() string {
	switch r.Kind {
	case kindMsg:
		if r.Msg != nil {
			return r.Msg.ID
		}
	case kindCompact:
		if r.Comp != nil {
			return r.Comp.ID
		}
	case kindCheckpoint:
		if r.CK != nil {
			return r.CK.ID
		}
	}
	return ""
}

// ParentID 返回 parent 引用（仅 msg/compact 有语义）。
func (r Record) ParentID() string {
	switch r.Kind {
	case kindMsg:
		if r.Msg != nil {
			return r.Msg.Parent
		}
	case kindCompact:
		if r.Comp != nil {
			return r.Comp.Parent
		}
	}
	return ""
}

// Created 返回记录创建时间。
func (r Record) Created() time.Time {
	switch r.Kind {
	case kindMsg:
		if r.Msg != nil {
			return r.Msg.CreatedAt
		}
	case kindCompact:
		if r.Comp != nil {
			return r.Comp.CreatedAt
		}
	case kindHead:
		if r.Head != nil {
			return r.Head.CreatedAt
		}
	case kindCheckpoint:
		if r.CK != nil {
			return r.CK.CreatedAt
		}
	case kindCursor:
		if r.Cur != nil {
			return r.Cur.CreatedAt
		}
	}
	return time.Time{}
}

// MarshalJSON 把 Record 展平为磁盘单行。用临时扁平结构保证字段名与 key 顺序
// 跟旧聚合格式一致（追加文件兼容读旧数据，也便于人工比对）。
//
// 注意：新增字段需同步改 3 处——类型化 struct（msgFields 等）、下方 flat struct、
// MarshalJSON 的 switch 填充。漏改任一处 = 字段静默丢失或输出漂移，
// record_format_test.go 的字节级断言会兜底抓漏。
func (r Record) MarshalJSON() ([]byte, error) {
	type flat struct {
		Kind        string           `json:"kind"`
		ID          string           `json:"id,omitempty"`
		Parent      string           `json:"parent,omitempty"`
		Summary     string           `json:"summary,omitempty"`
		Role        string           `json:"role,omitempty"`
		Content     string           `json:"content,omitempty"`
		ToolCalls   []core.ToolCall  `json:"tool_calls,omitempty"`
		ToolCallID  string           `json:"tool_call_id,omitempty"`
		CoveredFrom string           `json:"covered_from,omitempty"`
		CoveredTo   string           `json:"covered_to,omitempty"`
		CloneOf     string           `json:"clone_of,omitempty"`
		Tail        string           `json:"tail,omitempty"`
		At          string           `json:"at,omitempty"`
		Preview     string           `json:"preview,omitempty"`
		MsgID       string           `json:"msg_id,omitempty"`
		CreatedAt   time.Time        `json:"created_at"`
	}

	f := flat{Kind: r.Kind, CreatedAt: r.Created()}
	switch r.Kind {
	case kindMsg:
		if r.Msg != nil {
			f.ID, f.Parent = r.Msg.ID, r.Msg.Parent
			f.Role, f.Content = r.Msg.Role, r.Msg.Content
			f.ToolCalls, f.ToolCallID, f.CloneOf = r.Msg.ToolCalls, r.Msg.ToolCallID, r.Msg.CloneOf
		}
	case kindCompact:
		if r.Comp != nil {
			f.ID, f.Parent = r.Comp.ID, r.Comp.Parent
			f.Summary, f.CoveredFrom, f.CoveredTo = r.Comp.Summary, r.Comp.CoveredFrom, r.Comp.CoveredTo
		}
	case kindHead:
		if r.Head != nil {
			f.Tail = r.Head.Tail
		}
	case kindCheckpoint:
		if r.CK != nil {
			f.ID, f.At, f.Preview = r.CK.ID, r.CK.At, r.CK.Preview
		}
	case kindCursor:
		if r.Cur != nil {
			f.MsgID = r.Cur.MsgID
		}
	}
	return json.Marshal(f)
}

// UnmarshalJSON 先读 kind 再分发到对应字段组。json.Unmarshal 忽略多余 key，
// 因此旧文件里带"其他类型字段"的行也能正确解析到本类型。
func (r *Record) UnmarshalJSON(data []byte) error {
	var probe struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}
	r.Kind = probe.Kind

	switch r.Kind {
	case kindMsg:
		var f msgFields
		if err := json.Unmarshal(data, &f); err != nil {
			return err
		}
		r.Msg = &f
	case kindCompact:
		var f compactFields
		if err := json.Unmarshal(data, &f); err != nil {
			return err
		}
		r.Comp = &f
	case kindHead:
		var f headFields
		if err := json.Unmarshal(data, &f); err != nil {
			return err
		}
		r.Head = &f
	case kindCheckpoint:
		var f checkpointFields
		if err := json.Unmarshal(data, &f); err != nil {
			return err
		}
		r.CK = &f
	case kindCursor:
		var f cursorFields
		if err := json.Unmarshal(data, &f); err != nil {
			return err
		}
		r.Cur = &f
	}
	return nil
}

// ── 类型化构造函数 ──────────────────────────────────────────────────────

// newMsgRecord 构造消息记录（含 compact 副本，cloneOf 为空表示原始消息）。
func newMsgRecord(id, parent, role, content string, tc []core.ToolCall, toolCallID, cloneOf string, created time.Time) Record {
	return Record{
		Kind: kindMsg,
		Msg: &msgFields{
			ID: id, Parent: parent, Role: role, Content: content,
			ToolCalls: tc, ToolCallID: toolCallID, CloneOf: cloneOf, CreatedAt: created,
		},
	}
}

// newCompactRecord 构造摘要节点。
func newCompactRecord(id, parent, summary, from, to string, created time.Time) Record {
	return Record{
		Kind: kindCompact,
		Comp: &compactFields{ID: id, Parent: parent, Summary: summary, CoveredFrom: from, CoveredTo: to, CreatedAt: created},
	}
}

// newHeadRecord 构造 head 记录。
func newHeadRecord(tail string, created time.Time) Record {
	return Record{Kind: kindHead, Head: &headFields{Tail: tail, CreatedAt: created}}
}

// newCheckpointRecord 构造检查点记录。
func newCheckpointRecord(id, at, preview string, created time.Time) Record {
	return Record{Kind: kindCheckpoint, CK: &checkpointFields{ID: id, At: at, Preview: preview, CreatedAt: created}}
}

// newCursorRecord 构造游标记录。
func newCursorRecord(msgID string, created time.Time) Record {
	return Record{Kind: kindCursor, Cur: &cursorFields{MsgID: msgID, CreatedAt: created}}
}

// ── 记录与消息互转 ──────────────────────────────────────────────────────

func recordToMessage(r Record) core.Message {
	if r.Kind == kindCompact && r.Comp != nil {
		return core.Message{
			Role:      "system",
			Content:   r.Comp.Summary,
			MsgID:     r.Comp.ID,
			CreatedAt: r.Comp.CreatedAt,
		}
	}
	// msg 或 clone
	if r.Msg != nil {
		return core.Message{
			Role:       r.Msg.Role,
			Content:    r.Msg.Content,
			ToolCalls:  r.Msg.ToolCalls,
			ToolCallID: r.Msg.ToolCallID,
			MsgID:      r.Msg.ID,
			CreatedAt:  r.Msg.CreatedAt,
		}
	}
	return core.Message{}
}

func messageToRecord(m core.Message) Record {
	return newMsgRecord(m.MsgID, "", m.Role, m.Content, m.ToolCalls, m.ToolCallID, "", m.CreatedAt)
}

// isRealMessage 判断 Message 是否为真实对话消息（非 compact 摘要）。
// 摘要消息 Role=system 但非真实消息，靠 MsgID 前缀区分（compact 节点 id 以 s_ 开头，克隆以 m_ 开头）。
func isRealMessage(m core.Message) bool {
	if m.Role == "system" && len(m.MsgID) > 2 && m.MsgID[:2] == "s_" {
		return false
	}
	return true
}
