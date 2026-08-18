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
	}
	want := []string{
		`{"kind":"msg","id":"m_01","parent":"s_01","role":"assistant","content":"hello","tool_calls":[{"id":"t1","type":"function","function":{"name":"","arguments":""}}],"tool_call_id":"tc1","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"compact","id":"s_01","summary":"summary text","covered_from":"m_01","covered_to":"m_03","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"head","tail":"m_08","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"checkpoint","id":"ck_1","at":"m_08","preview":"preview","created_at":"2024-01-01T00:00:00Z"}`,
		`{"kind":"cursor","msg_id":"m_08","created_at":"2024-01-01T00:00:00Z"}`,
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
