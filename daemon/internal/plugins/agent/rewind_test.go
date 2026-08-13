package agent

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/session"
)

// 本文件测试 /rewind 依赖的 store 层回溯行为。
// 因 p.store 字段由并行 agent (D) 负责落地，handler 级集成测试在 D 落字段后再补。
// 当前聚焦：SetHead → ActiveView 正确性、多轮 rewind、compact 恢复原文、分叉后提交。

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

func TestRewind_SetHeadActiveView(t *testing.T) {
	s := openTestStore(t)

	// 提交 3 条消息
	m1 := commMsgs([]string{"user", "assistant"})
	committed, err := s.Commit(m1)
	if err != nil {
		t.Fatalf("Commit 1: %v", err)
	}
	if len(committed) != 2 {
		t.Fatalf("expected 2 committed, got %d", len(committed))
	}

	// 确认 ActiveView 有 2 条
	view := s.ActiveView()
	if len(view) != 2 {
		t.Fatalf("expected 2 in view, got %d", len(view))
	}

	// 打一个 checkpoint
	if err := s.Checkpoint("preview: first turn"); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// 再提交 2 条
	m2 := commMsgs([]string{"user", "assistant"})
	if _, err := s.Commit(m2); err != nil {
		t.Fatalf("Commit 2: %v", err)
	}

	view = s.ActiveView()
	if len(view) != 4 {
		t.Fatalf("expected 4 in view, got %d", len(view))
	}

	// 回溯到 checkpoint（at 是第一轮的最后一跳 = m_01）
	cks := s.Checkpoints(10)
	if len(cks) != 1 {
		t.Fatalf("expected 1 checkpoint, got %d", len(cks))
	}

	if err := s.SetHead(cks[0].At); err != nil {
		t.Fatalf("SetHead: %v", err)
	}

	view = s.ActiveView()
	if len(view) != 2 {
		t.Fatalf("after rewind, expected 2 in view, got %d", len(view))
	}
}

func TestRewind_MultipleCheckpoints(t *testing.T) {
	s := openTestStore(t)

	for round := 0; round < 5; round++ {
		m := commMsgs([]string{"user", "assistant"})
		if _, err := s.Commit(m); err != nil {
			t.Fatalf("Commit round %d: %v", round, err)
		}
		if err := s.Checkpoint(fmt.Sprintf("round %d preview", round)); err != nil {
			t.Fatalf("Checkpoint round %d: %v", round, err)
		}
	}

	cks := s.Checkpoints(10)
	if len(cks) != 5 {
		t.Fatalf("expected 5 checkpoints, got %d", len(cks))
	}

	// 回溯到第二个 checkpoint（中间位置）
	if err := s.SetHead(cks[2].At); err != nil {
		t.Fatalf("SetHead: %v", err)
	}

	view := s.ActiveView()
	// 第 3 个 checkpoint（索引 2），它之前提交了 3 轮（每轮 2 条 = 6 条消息）
	if len(view) != 6 {
		t.Fatalf("after rewind to round 3, expected 6 in view, got %d", len(view))
	}
}

func TestRewind_CompactRestoreFullText(t *testing.T) {
	s := openTestStore(t)

	// 提交 6 条消息
	m1 := commMsgs([]string{"user", "assistant", "user", "assistant", "user", "assistant"})
	committed, err := s.Commit(m1)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// 确认 view 有 6 条
	view := s.ActiveView()
	if len(view) != 6 {
		t.Fatalf("expected 6 in view, got %d", len(view))
	}

	// 找到 covered 段的头尾——压缩前 3 条消息（index 0, 1, 2）
	// committed 按顺序返回，取第一和第三条的 MsgID
	from := committed[0].MsgID
	to := committed[2].MsgID

	// 保存压缩前的 checkpoint
	if err := s.Checkpoint("before compact"); err != nil {
		t.Fatalf("Checkpoint before compact: %v", err)
	}

	// 执行 compact：覆盖前 3 条
	if err := s.Compact(from, to, "summary: first 3 messages compressed"); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// 压缩后 view 应有 compact 摘要 + 后 3 条 = 4
	view = s.ActiveView()
	if len(view) != 4 {
		t.Fatalf("after compact, expected 4 in view, got %d", len(view))
	}

	// 回溯到 compact 前的 checkpoint → 应恢复原文 6 条
	cks := s.Checkpoints(10)
	if len(cks) == 0 {
		t.Fatal("expected at least 1 checkpoint")
	}

	// checkpoint at 指向 compact 前 head（最后一条未压缩消息）
	if err := s.SetHead(cks[0].At); err != nil {
		t.Fatalf("SetHead to pre-compact checkpoint: %v", err)
	}

	view = s.ActiveView()
	if len(view) != 6 {
		t.Fatalf("after rewind from compact, expected 6 original messages, got %d", len(view))
	}

	// 验证恢复的是原文（非 compact 摘要）
	hasCompact := false
	for _, m := range view {
		if m.Role == "system" && len(m.MsgID) >= 2 && m.MsgID[:2] == "s_" {
			hasCompact = true
		}
	}
	if hasCompact {
		t.Fatal("expected no compact node in restored view")
	}
}

func TestRewind_ForkAfterRewind(t *testing.T) {
	s := openTestStore(t)

	// 提交第一轮
	m1 := commMsgs([]string{"user", "assistant"})
	if _, err := s.Commit(m1); err != nil {
		t.Fatalf("Commit 1: %v", err)
	}
	if err := s.Checkpoint("turn 1"); err != nil {
		t.Fatalf("Checkpoint 1: %v", err)
	}

	// 提交第二轮
	m2 := commMsgs([]string{"user", "assistant"})
	if _, err := s.Commit(m2); err != nil {
		t.Fatalf("Commit 2: %v", err)
	}

	// 回溯到第一轮 checkpoint
	cks := s.Checkpoints(10)
	if err := s.SetHead(cks[0].At); err != nil {
		t.Fatalf("SetHead: %v", err)
	}

	view := s.ActiveView()
	if len(view) != 2 {
		t.Fatalf("after rewind, expected 2 in view, got %d", len(view))
	}

	// 继续提交新消息 → 分叉新分支
	m3 := commMsgs([]string{"user", "assistant"})
	committed3, err := s.Commit(m3)
	if err != nil {
		t.Fatalf("Commit after rewind: %v", err)
	}
	if len(committed3) != 2 {
		t.Fatalf("expected 2 committed after rewind, got %d", len(committed3))
	}

	// ActiveView 应只有 2 + 2 = 4 条（旧的第二轮不在活跃路径上）
	view = s.ActiveView()
	if len(view) != 4 {
		t.Fatalf("after fork, expected 4 in view, got %d", len(view))
	}
}

func TestRewind_EmptyCheckpoints(t *testing.T) {
	s := openTestStore(t)

	cks := s.Checkpoints(10)
	if len(cks) != 0 {
		t.Fatalf("expected 0 checkpoints, got %d", len(cks))
	}
}

func TestRewind_AlreadyAtTarget(t *testing.T) {
	s := openTestStore(t)

	m := commMsgs([]string{"user", "assistant"})
	committed, err := s.Commit(m)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := s.Checkpoint("turn 1"); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	cks := s.Checkpoints(10)
	// 当前 head 就是 checkpoint 的 at（最后一跳 committed[1].MsgID）
	if cks[0].At != committed[1].MsgID {
		t.Fatalf("expected checkpoint.at == last committed, got %s vs %s", cks[0].At, committed[1].MsgID)
	}

	// SetHead 到当前位置应该仍然是 2 条
	if err := s.SetHead(cks[0].At); err != nil {
		t.Fatalf("SetHead to same position: %v", err)
	}

	view := s.ActiveView()
	if len(view) != 2 {
		t.Fatalf("expected 2 after rewind to same position, got %d", len(view))
	}
}

func TestRewind_OutOfBounds(t *testing.T) {
	s := openTestStore(t)
	cks := s.Checkpoints(10)
	// 越界由 handler 层校验，这里只确认 Checkpoints 返回空列表时不 panic
	if cks == nil {
		t.Fatal("Checkpoints should return empty slice, not nil")
	}
}

// ── 纯函数测试 ──────────────────────────────────────────────────────────

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

// TestRewind_StoreCloseReopen 验证 store 关闭重开后状态正确保持（jsonl 持久化闭环）。
func TestRewind_StoreCloseReopen(t *testing.T) {
	d := t.TempDir()

	// 第一段：创建 store、提交消息、打 checkpoint
	s1, err := session.Open(d)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}

	m1 := commMsgs([]string{"user", "assistant"})
	committed, err := s1.Commit(m1)
	if err != nil {
		t.Fatalf("Commit 1: %v", err)
	}
	if err := s1.Checkpoint("turn 1 preview"); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// 第二段：重开 store
	s2, err := session.Open(d)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer s2.Close()

	// 验证 checkpoint 恢复
	cks := s2.Checkpoints(10)
	if len(cks) != 1 {
		t.Fatalf("expected 1 checkpoint after reopen, got %d", len(cks))
	}
	if cks[0].Preview != "turn 1 preview" {
		t.Errorf("checkpoint preview mismatch: %q", cks[0].Preview)
	}

	// 验证活跃视图恢复
	view := s2.ActiveView()
	if len(view) != 2 {
		t.Fatalf("expected 2 in view after reopen, got %d", len(view))
	}

	// 再提交一轮
	m2 := commMsgs([]string{"user", "assistant"})
	if _, err := s2.Commit(m2); err != nil {
		t.Fatalf("Commit 2: %v", err)
	}
	view = s2.ActiveView()
	if len(view) != 4 {
		t.Fatalf("expected 4 after second commit, got %d", len(view))
	}

	// rewind 到 checkpoint → 应回到 2 条
	if err := s2.SetHead(committed[1].MsgID); err != nil {
		t.Fatalf("SetHead: %v", err)
	}
	view = s2.ActiveView()
	if len(view) != 2 {
		t.Fatalf("after rewind after reopen, expected 2 in view, got %d", len(view))
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

	s := NewSession(SessionDeps{Store: st})
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
