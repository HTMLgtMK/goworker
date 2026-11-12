package session

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
)

// 本文件是磁盘格式回归测试：守护 jsonl 记录序列化为扁平单行、字段名不变的契约。
// 任何改动如果让 Record 序列化偏离旧聚合格式（key 名/嵌套结构/字段丢失），这里立刻红。

func TestRecord_MarshalFlatFormat(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	recs := []Record{
		newMsgRecord("m_01", "s_01", "assistant", "hello",
			[]core.ToolCall{{ID: "t1", Type: "function"}}, "tc1", "", now),
		newCompactRecord("s_01", "", "summary text", "m_01", "m_03", now),
		newHeadRecord("m_08", now),
		newCheckpointRecord("ck_1", "m_08", "preview", now),
		newCursorRecord("m_08", now),
		// Plan A1 新增 kind：workspace（path + created_at）、archived（仅 created_at）
		newWorkspaceRecord("/tmp/wk/demo", now),
		newArchivedRecord(now),
	}
	want := []string{
		`{"kind":"msg","id":"m_01","parent":"s_01","role":"assistant","content":"hello","tool_calls":[{"id":"t1","type":"function","function":{"name":"","arguments":""}}],"tool_call_id":"tc1","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"compact","id":"s_01","summary":"summary text","covered_from":"m_01","covered_to":"m_03","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"head","tail":"m_08","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"checkpoint","id":"ck_1","at":"m_08","preview":"preview","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"cursor","msg_id":"m_08","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"workspace","path":"/tmp/wk/demo","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"archived","created_at":"2024-01-01T00:00:00Z"}`,
	}

	for i, r := range recs {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal rec[%d]: %v", i, err)
		}
		if string(b) != want[i] {
			t.Errorf("rec[%d] serialized mismatch:\n got  %s\n want %s", i, string(b), want[i])
		}
	}
}

func TestRecord_UnmarshalOldFormat(t *testing.T) {
	// 旧聚合格式的行（含本类型之外的 key）必须能解析，多余 key 忽略。
	var r Record
	if err := json.Unmarshal([]byte(`{"kind":"msg","id":"m_01","parent":"s_01","role":"user","content":"hi","tail":"ignored","created_at":"2024-01-01T00:00:00Z"}`), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Kind != kindMsg || r.Msg == nil {
		t.Fatalf("kind=%q Msg=%v, want msg with Msg set", r.Kind, r.Msg)
	}
	if r.NodeID() != "m_01" || r.ParentID() != "s_01" || r.Msg.Role != "user" || r.Msg.Content != "hi" {
		t.Errorf("msg fields wrong: id=%q parent=%q role=%q content=%q",
			r.NodeID(), r.ParentID(), r.Msg.Role, r.Msg.Content)
	}

	var c Record
	if err := json.Unmarshal([]byte(`{"kind":"compact","id":"s_01","summary":"s","covered_from":"a","covered_to":"b","created_at":"2024-01-01T00:00:00Z"}`), &c); err != nil {
		t.Fatalf("unmarshal compact: %v", err)
	}
	if c.Comp == nil || c.Comp.Summary != "s" || c.Comp.CoveredFrom != "a" || c.Comp.CoveredTo != "b" {
		t.Errorf("compact fields wrong: %+v", c.Comp)
	}
}

// TestRecord_WorkspaceArchivedRoundtrip verifies workspace and archived records preserve
// their fields and creation timestamps through JSON round trips.
func TestRecord_WorkspaceArchivedRoundtrip(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	var w Record
	wLine := `{"kind":"workspace","path":"/tmp/wk/x","created_at":"2024-06-01T12:00:00Z"}`
	if err := json.Unmarshal([]byte(wLine), &w); err != nil {
		t.Fatalf("unmarshal workspace: %v", err)
	}
	if w.Kind != kindWorkspace || w.Ws == nil || w.Ws.Path != "/tmp/wk/x" {
		t.Fatalf("workspace record = %+v, want Ws.Path=/tmp/wk/x", w)
	}
	if !w.Created().Equal(now) {
		t.Errorf("workspace Created() = %v, want %v", w.Created(), now)
	}
	// 往返一致（字节级断言已由 TestRecord_MarshalFlatFormat 覆盖，这里验结构等价）
	b, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("marshal workspace: %v", err)
	}
	if string(b) != wLine {
		t.Errorf("workspace roundtrip mismatch:\n got  %s\n want %s", string(b), wLine)
	}

	var a Record
	aLine := `{"kind":"archived","created_at":"2024-06-01T12:00:00Z"}`
	if err := json.Unmarshal([]byte(aLine), &a); err != nil {
		t.Fatalf("unmarshal archived: %v", err)
	}
	if a.Kind != kindArchived || a.Arc == nil {
		t.Fatalf("archived record = %+v, want Arc set", a)
	}
	if !a.Created().Equal(now) {
		t.Errorf("archived Created() = %v, want %v", a.Created(), now)
	}
	b, err = json.Marshal(a)
	if err != nil {
		t.Fatalf("marshal archived: %v", err)
	}
	if string(b) != aLine {
		t.Errorf("archived roundtrip mismatch:\n got  %s\n want %s", string(b), aLine)
	}
}

// TestRecord_UnknownKindTolerated verifies unknown kinds deserialize without errors
// or populating a known field group.
func TestRecord_UnknownKindTolerated(t *testing.T) {
	var r Record
	if err := json.Unmarshal([]byte(`{"kind":"future_kind","x":1,"created_at":"2024-06-01T12:00:00Z"}`), &r); err != nil {
		t.Fatalf("unknown kind must not error: %v", err)
	}
	if r.Kind != "future_kind" {
		t.Errorf("kind = %q, want preserved as-is", r.Kind)
	}
	if r.Msg != nil || r.Comp != nil || r.Head != nil || r.CK != nil || r.Cur != nil || r.Meta != nil || r.Ws != nil || r.Arc != nil {
		t.Errorf("unknown kind must not populate any field group: %+v", r)
	}
}
