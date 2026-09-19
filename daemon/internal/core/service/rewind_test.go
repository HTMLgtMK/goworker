package service

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// openTestStore 打开临时目录下的 store 并注册 cleanup。
func openTestStore(t *testing.T) *session.Store {
	t.Helper()
	d := t.TempDir()
	s, err := session.Open(d)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// commMsgs 快捷构造一批带 id 的消息。
// id 必须唯一：Commit 对重复 id 幂等跳过，固定 id 会让多轮提交被静默丢弃。
// 用自增批次号 + 序号生成，保证每次调用产生全新 id。
var commMsgsBatch int

func commMsgs(roles []string) []core.Message {
	commMsgsBatch++
	var msgs []core.Message
	for i, role := range roles {
		msgs = append(msgs, core.Message{
			Role:    role,
			Content: fmt.Sprintf("msg_%02d", i),
			MsgID:   fmt.Sprintf("m_b%02d_%02d", commMsgsBatch, i),
		})
	}
	return msgs
}

// TestIsCompactMsg 锁死 compact 摘要节点的判定：Role=system + MsgID 以 "s_" 开头。
func TestIsCompactMsg(t *testing.T) {
	tests := []struct {
		name string
		msg  core.Message
		want bool
	}{
		{"compact node", core.Message{Role: "system", MsgID: "s_abc123", Content: "summary..."}, true},
		{"normal system", core.Message{Role: "system", MsgID: "m_00", Content: "system prompt"}, false},
		{"user message", core.Message{Role: "user", MsgID: "m_01", Content: "hello"}, false},
		{"short id", core.Message{Role: "system", MsgID: "s", Content: "x"}, false},
		{"no id", core.Message{Role: "system", MsgID: "", Content: "summary"}, false},
		{"assistant with s_prefix", core.Message{Role: "assistant", MsgID: "s_xyz", Content: "reply"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCompactMsg(tt.msg)
			if got != tt.want {
				t.Errorf("isCompactMsg(%+v) = %v, want %v", tt.msg, got, tt.want)
			}
		})
	}
}

// TestHandleRewind_Numbering 验证 /rewind 编号语义：#1 = 最新 checkpoint（倒序）。
// 这是 handler 层回归测试，锁死编号映射不回归（历史上 #1 曾误指向最旧）。
func TestHandleRewind_Numbering(t *testing.T) {
	st := openTestStore(t)
	// 提交三轮，每轮打 checkpoint
	var lastMsgID string
	for round := 0; round < 3; round++ {
		msgs := commMsgs([]string{"user", "assistant"})
		committed, err := st.Commit(msgs)
		if err != nil {
			t.Fatalf("Commit round %d: %v", round, err)
		}
		lastMsgID = committed[len(committed)-1].MsgID
		if err := st.Checkpoint(fmt.Sprintf("round %d", round)); err != nil {
			t.Fatalf("Checkpoint round %d: %v", round, err)
		}
	}

	s := runtimeagent.NewSession(runtimeagent.SessionDeps{Store: st})
	p := &AgentPlugin{store: st, session: s}

	// 列表：#1 应是最新（round 2），#3 是最旧（round 0）
	ctx, buf := newContext()
	if err := p.handleRewind(ctx); err != nil {
		t.Fatalf("handleRewind list: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "#1") || !strings.Contains(out, "#2") || !strings.Contains(out, "#3") {
		t.Fatalf("list should show #1 #2 #3:\n%s", out)
	}
	// #1 的 preview 是最新一轮 "round 2"（倒序取最新）
	idx1 := strings.Index(out, "#1")
	line1 := out[idx1 : idx1+40]
	if !strings.Contains(line1, "round 2") {
		t.Errorf("#1 should be latest checkpoint (round 2), got line: %q", line1)
	}

	// /rewind 1 → 回溯到最新 checkpoint（at = 第三轮最后消息 id）
	// conversation 当前为空（store 未挂到 session conversation），但 SetHead 应生效
	if err := p.doRewind(ctx, "1"); err != nil {
		t.Fatalf("doRewind 1: %v", err)
	}
	if st.ActiveView()[len(st.ActiveView())-1].MsgID != lastMsgID {
		t.Errorf("/rewind 1 should rewind to latest (msg %s), got %s",
			lastMsgID, st.ActiveView()[len(st.ActiveView())-1].MsgID)
	}

	// /rewind 3 → 回溯到最旧 checkpoint（第一轮）
	if err := p.doRewind(ctx, "3"); err != nil {
		t.Fatalf("doRewind 3: %v", err)
	}
	if view := st.ActiveView(); len(view) != 2 {
		t.Errorf("/rewind 3 should restore oldest checkpoint view (2 msgs), got %d", len(view))
	}
}
