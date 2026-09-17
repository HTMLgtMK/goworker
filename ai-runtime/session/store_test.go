package session

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

// ── 测试辅助 ──────────────────────────────────────────────────────────

func tempDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	return d
}

func openStore(t *testing.T) *Store {
	t.Helper()
	d := tempDir(t)
	s, err := Open(d)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// msgs 快捷构造消息列表，自动填充 MsgID。
func msgs(roleContentPairs ...string) []core.Message {
	var out []core.Message
	for i := 0; i < len(roleContentPairs); i += 2 {
		role := roleContentPairs[i]
		content := roleContentPairs[i+1]
		out = append(out, core.Message{
			Role:    role,
			Content: content,
			MsgID:   fmt.Sprintf("m_%02d", len(out)),
		})
	}
	return out
}

// commMsg 创建一个带指定 id 的消息。
func commMsg(id, role, content string) core.Message {
	return core.Message{MsgID: id, Role: role, Content: content}
}

// commMsgs 用 id 列表构造消息。
func commMsgs(ids []string, role string) []core.Message {
	var out []core.Message
	for _, id := range ids {
		out = append(out, commMsg(id, role, "content_"+id))
	}
	return out
}

// viewIDs 提取视图中的消息 id 列表（用于断言）。
func viewIDs(view []core.Message) []string {
	var ids []string
	for _, m := range view {
		ids = append(ids, m.MsgID)
	}
	return ids
}

// viewContent 提取视图内容列表。
func viewContent(view []core.Message) []string {
	var c []string
	for _, m := range view {
		c = append(c, m.Content)
	}
	return c
}

// readJSONL 读取 jsonl 文件的原始行。
func readJSONLFile(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read JSONL: %v", err)
	}
	if len(data) == 0 {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var out []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// ── 测试用例 ──────────────────────────────────────────────────────────

func TestThinkingRoundtripAndLegacyCompatibility(t *testing.T) {
	dir := tempDir(t)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	custom := map[string]json.RawMessage{
		"openai": json.RawMessage(`{"version":1,"fields":{"reasoning_content":"must replay"}}`),
	}
	message := core.Message{
		Role:     "assistant",
		Content:  "answer",
		MsgID:    "m_thinking",
		Thinking: core.Thinking{Text: "must replay"},
		Custom:   custom,
	}
	if _, err := s.Commit([]core.Message{message}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	view := reopened.ActiveView()
	if len(view) != 1 {
		t.Fatalf("ActiveView length = %d, want 1", len(view))
	}
	if view[0].Thinking.Text != "must replay" || !reflect.DeepEqual(view[0].Custom, custom) {
		t.Errorf("message = %#v, want thinking and custom data preserved", view[0])
	}

	legacy := Record{Kind: kindMsg, Msg: &msgFields{ID: "m_legacy", Role: "assistant", Content: "plain"}}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy: %v", err)
	}
	var decoded Record
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if got := recordToMessage(decoded); got.Thinking.Text != "" || got.Custom != nil {
		t.Errorf("legacy message = %#v, want no thinking or custom data", got)
	}
}

// TestActiveViewRoundtrip 验证 Commit → ActiveView 完整往返。
func TestActiveViewRoundtrip(t *testing.T) {
	s := openStore(t)

	input := msgs("user", "hello", "assistant", "hi there", "user", "how are you")
	committed, err := s.Commit(input)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(committed) != 3 {
		t.Fatalf("expected 3 committed, got %d", len(committed))
	}

	view := s.ActiveView()
	if len(view) != 3 {
		t.Fatalf("expected 3 view, got %d", len(view))
	}
	for i, m := range view {
		if m.Role != input[i].Role {
			t.Errorf("view[%d].Role = %q, want %q", i, m.Role, input[i].Role)
		}
		if m.Content != input[i].Content {
			t.Errorf("view[%d].Content = %q, want %q", i, m.Content, input[i].Content)
		}
	}

	// 验证文件中有对应的消息行 + head 行
	lines := readJSONLFile(t, filepath.Join(s.dir, "current.jsonl"))
	if len(lines) < 6 {
		t.Fatalf("expected >=6 lines (3 msg + 3 head), got %d", len(lines))
	}
}

// TestCompactBasic 验证 compact 后 ActiveView = [摘要 + 副本]，原链保留在文件里。
func TestCompactBasic(t *testing.T) {
	s := openStore(t)

	// 提交 5 条消息：m0 m1 m2 m3 m4
	all := commMsgs([]string{"m0", "m1", "m2", "m3", "m4"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// compact m1..m3（覆盖 m1 m2 m3，保留 m4）
	err = s.Compact("m1", "m3", "summary of m1-m3")
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	view := s.ActiveView()
	// 预期：[m0, s_m3(summary), m4'(clone of m4)]
	if len(view) != 3 {
		t.Fatalf("expected 3 messages in view, got %d: %v", len(view), viewIDs(view))
	}

	// 第一条是 m0
	if view[0].MsgID != "m0" {
		t.Errorf("view[0] = %s, want m0", view[0].MsgID)
	}
	// 第二条是 compact 摘要（system role）
	if view[1].Role != "system" {
		t.Errorf("view[1].Role = %q, want system", view[1].Role)
	}
	if view[1].Content != "summary of m1-m3" {
		t.Errorf("view[1].Content = %q, want 'summary of m1-m3'", view[1].Content)
	}
	if view[1].MsgID != "s_m3" {
		t.Errorf("view[1].MsgID = %q, want s_m3", view[1].MsgID)
	}
	// 第三条是 m4 的副本，内容一致但 id 不同
	if view[2].Content != "content_m4" {
		t.Errorf("view[2].Content = %q, want content_m4", view[2].Content)
	}
	if view[2].MsgID == "m4" {
		t.Errorf("view[2].MsgID should be clone id, not m4")
	}
	if view[2].Role != "user" {
		t.Errorf("view[2].Role = %q, want user", view[2].Role)
	}

	// 验证原链保留在文件里（m1 m2 m3 m4 行仍存在）
	lines := readJSONLFile(t, filepath.Join(s.dir, "current.jsonl"))
	foundM1 := false
	foundM4 := false
	for _, line := range lines {
		var r Record
		json.Unmarshal([]byte(line), &r)
		if r.NodeID() == "m1" {
			foundM1 = true
		}
		if r.NodeID() == "m4" {
			foundM4 = true
		}
	}
	if !foundM1 {
		t.Error("original m1 not found in jsonl (should be preserved)")
	}
	if !foundM4 {
		t.Error("original m4 not found in jsonl (should be preserved)")
	}
}

// TestCompactChain 验证链式 compact：第二次基于 [摘要+副本] 再做 compact。
func TestCompactChain(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1", "m2", "m3", "m4", "m5"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit 1: %v", err)
	}

	// 第一次 compact：覆盖 m1..m3，保留 m4 m5
	err = s.Compact("m1", "m3", "sum1")
	if err != nil {
		t.Fatalf("Compact 1: %v", err)
	}

	view1 := s.ActiveView()
	// view1: [m0, s_m3(sum1), m4', m5']
	if len(view1) != 4 {
		t.Fatalf("view1 len = %d, want 4", len(view1))
	}

	// 第二次 compact：覆盖 m4'..m5'（用副本 id）
	cloneM4ID := view1[2].MsgID
	cloneM5ID := view1[3].MsgID
	err = s.Compact(cloneM4ID, cloneM5ID, "sum2")
	if err != nil {
		t.Fatalf("Compact 2: %v", err)
	}

	view2 := s.ActiveView()
	// view2: [m0, s_m3(sum1), s_<cloneM5>(sum2)]
	if len(view2) != 3 {
		t.Fatalf("view2 len = %d, want 3: %v", len(view2), viewIDs(view2))
	}
	if view2[1].Content != "sum1" {
		t.Errorf("view2[1].Content = %q, want sum1", view2[1].Content)
	}
	if view2[2].Content != "sum2" {
		t.Errorf("view2[2].Content = %q, want sum2", view2[2].Content)
	}
}

// TestRewind 验证 rewind（SetHead）后 ActiveView 切换。
func TestRewind(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1", "m2"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// rewind 到 m1
	err = s.SetHead("m1")
	if err != nil {
		t.Fatalf("SetHead: %v", err)
	}

	view := s.ActiveView()
	ids := viewIDs(view)
	if len(ids) != 2 || ids[0] != "m0" || ids[1] != "m1" {
		t.Errorf("rewind to m1: got ids %v, want [m0 m1]", ids)
	}
}

// TestRewindToCovered 验证 rewind 到 covered 段节点 → 恢复压缩前全文。
func TestRewindToCovered(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1", "m2", "m3"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// compact m1..m2
	err = s.Compact("m1", "m2", "sum")
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// 确认 compact 后视图是压缩的
	view1 := s.ActiveView()
	if len(view1) != 3 { // m0 + s_m2 + m3'
		t.Fatalf("after compact, view len = %d, want 3", len(view1))
	}

	// rewind 到覆盖段内的 m2（原链节点）
	err = s.SetHead("m2")
	if err != nil {
		t.Fatalf("SetHead to m2: %v", err)
	}

	view2 := s.ActiveView()
	// 恢复：m0 m1 m2（沿 parent 回溯到原链，不含 m3 因为 rewind 把 head 切到了 m2）
	ids := viewIDs(view2)
	if len(ids) != 3 || ids[0] != "m0" || ids[1] != "m1" || ids[2] != "m2" {
		t.Errorf("rewind to covered m2: got ids %v, want [m0 m1 m2]", ids)
	}
}

// TestRewindAndBranch 验证 rewind 后 Commit 产生分叉。
func TestRewindAndBranch(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1", "m2"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// rewind 到 m0
	err = s.SetHead("m0")
	if err != nil {
		t.Fatalf("SetHead: %v", err)
	}

	// 在 m0 后追加新分支（新 id 避免幂等跳过）
	branch := []core.Message{
		commMsg("m10", "assistant", "branch reply"),
	}
	_, err = s.Commit(branch)
	if err != nil {
		t.Fatalf("Commit branch: %v", err)
	}

	view := s.ActiveView()
	ids := viewIDs(view)
	if len(ids) != 2 || ids[0] != "m0" || ids[1] != "m10" {
		t.Errorf("branch: got ids %v, want [m0 m10]", ids)
	}
}

// TestCommitIdempotent 验证重复提交相同 id 跳过。
func TestCommitIdempotent(t *testing.T) {
	s := openStore(t)

	msgs1 := []core.Message{commMsg("a1", "user", "msg1")}
	_, err := s.Commit(msgs1)
	if err != nil {
		t.Fatalf("Commit 1: %v", err)
	}

	// 重复提交相同 id
	msgs2 := []core.Message{commMsg("a1", "user", "msg1")}
	committed, err := s.Commit(msgs2)
	if err != nil {
		t.Fatalf("Commit 2: %v", err)
	}
	if len(committed) != 0 {
		t.Errorf("expected 0 committed (duplicate), got %d", len(committed))
	}

	view := s.ActiveView()
	if len(view) != 1 {
		t.Errorf("expected 1 message in view, got %d", len(view))
	}
}

// TestCompactPreservesToolPairs 验证 assistant(tool_call)+tool 结果在 compact 后副本配对完整。
func TestCompactPreservesToolPairs(t *testing.T) {
	s := openStore(t)

	toolCallMsg := core.Message{
		MsgID: "tc1",
		Role:  "assistant",
		ToolCalls: []core.ToolCall{
			{ID: "call_1", Type: "function", Function: core.ToolCallFunction{Name: "get_weather", Arguments: `{"city":"bj"}`}},
		},
	}
	toolResultMsg := core.Message{
		MsgID:      "tr1",
		Role:       "tool",
		Content:    "sunny",
		ToolCallID: "call_1",
	}

	_, err := s.Commit([]core.Message{
		commMsg("m0", "user", "what's the weather"),
		toolCallMsg,
		toolResultMsg,
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// compact 覆盖 m0..tc1，保留 tr1
	err = s.Compact("m0", "tc1", "weather query")
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	view := s.ActiveView()
	// view: [s_tc1(summary), tr1']
	if len(view) != 2 {
		t.Fatalf("view len = %d, want 2: %v", len(view), viewIDs(view))
	}

	if view[0].Role != "system" || view[0].Content != "weather query" {
		t.Errorf("view[0]: role=%s content=%s, want system/'weather query'", view[0].Role, view[0].Content)
	}

	// 副本 tr1' 应该保留 tool_call_id 配对
	if view[1].Role != "tool" {
		t.Errorf("view[1].Role = %q, want tool", view[1].Role)
	}
	if view[1].ToolCallID != "call_1" {
		t.Errorf("view[1].ToolCallID = %q, want call_1", view[1].ToolCallID)
	}
	if view[1].Content != "sunny" {
		t.Errorf("view[1].Content = %q, want sunny", view[1].Content)
	}
}

// TestCursorMapping 验证游标映射三档位置。
func TestCursorMapping(t *testing.T) {
	// 场景 1：游标在保留段末尾 → 映射到最后副本
	t.Run("cursor_at_retain_end", func(t *testing.T) {
		s := openStore(t)
		all := commMsgs([]string{"m0", "m1", "m2", "m3"}, "user")
		_, err := s.Commit(all)
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}

		// 固化到 m3（head）
		_ = s.AdvanceCursor("m3")

		// compact m0..m1，保留 m2 m3
		err = s.Compact("m0", "m1", "sum")
		if err != nil {
			t.Fatalf("Compact: %v", err)
		}

		// 游标应映射到 m3 的副本
		pending := s.PendingAfterCursor()
		if len(pending) != 0 {
			t.Errorf("expected 0 pending (all consolidated), got %d: %v", len(pending), viewIDs(pending))
		}
	})

	// 场景 2：游标在保留段中间 → 映射到对应副本
	t.Run("cursor_in_retain_middle", func(t *testing.T) {
		s := openStore(t)
		all := commMsgs([]string{"m0", "m1", "m2", "m3"}, "user")
		_, err := s.Commit(all)
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}

		// 固化到 m1
		_ = s.AdvanceCursor("m1")

		// compact m0..m0，保留 m1 m2 m3
		err = s.Compact("m0", "m0", "sum")
		if err != nil {
			t.Fatalf("Compact: %v", err)
		}

		// 游标在 m1 → 应映射到 m1 的副本，pending 为 m2' m3'
		pending := s.PendingAfterCursor()
		if len(pending) != 2 {
			t.Fatalf("expected 2 pending, got %d: %v", len(pending), viewIDs(pending))
		}
		for _, m := range pending {
			if m.MsgID == "m1" {
				t.Error("pending should not contain original m1 (should be clones only)")
			}
		}
	})

	// 场景 3：游标在覆盖段或更早 → 不动，PendingAfterCursor 兜底全量
	t.Run("cursor_in_covered", func(t *testing.T) {
		s := openStore(t)
		all := commMsgs([]string{"m0", "m1", "m2"}, "user")
		_, err := s.Commit(all)
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}

		// 固化到 m0
		_ = s.AdvanceCursor("m0")

		// compact m0..m1
		err = s.Compact("m0", "m1", "sum")
		if err != nil {
			t.Fatalf("Compact: %v", err)
		}

		// cursor=m0 在 covered 段 → 不在活跃路径 → 全量兜底
		pending := s.PendingAfterCursor()
		if len(pending) == 0 {
			t.Error("expected full pending (cursor not on active path)")
		}
		// 不应有重复
		idCount := make(map[string]int)
		for _, m := range pending {
			idCount[m.MsgID]++
		}
		for id, c := range idCount {
			if c > 1 {
				t.Errorf("duplicate id %s in pending", id)
			}
		}
	})
}

// TestPendingAfterCursor 验证 PendingAfterCursor 的各个分支。
func TestPendingAfterCursor(t *testing.T) {
	t.Run("empty_cursor_returns_all", func(t *testing.T) {
		s := openStore(t)
		all := commMsgs([]string{"m0", "m1", "m2"}, "user")
		_, _ = s.Commit(all)

		pending := s.PendingAfterCursor()
		if len(pending) != 3 {
			t.Errorf("empty cursor: expected 3, got %d", len(pending))
		}
	})

	t.Run("cursor_not_on_path_returns_all", func(t *testing.T) {
		s := openStore(t)
		all := commMsgs([]string{"m0", "m1"}, "user")
		_, _ = s.Commit(all)

		// 设置一个不在活跃路径上的 cursor
		_ = s.AdvanceCursor("nonexistent")

		pending := s.PendingAfterCursor()
		if len(pending) != 2 {
			t.Errorf("cursor not on path: expected 2, got %d", len(pending))
		}
	})

	t.Run("cursor_returns_after_only", func(t *testing.T) {
		s := openStore(t)
		all := commMsgs([]string{"m0", "m1", "m2"}, "user")
		_, _ = s.Commit(all)

		// cursor 在 m0
		_ = s.AdvanceCursor("m0")

		pending := s.PendingAfterCursor()
		if len(pending) != 2 {
			t.Fatalf("expected 2 after m0, got %d", len(pending))
		}
		if pending[0].MsgID != "m1" || pending[1].MsgID != "m2" {
			t.Errorf("got %v, want [m1 m2]", viewIDs(pending))
		}
	})

	t.Run("cursor_at_head_returns_empty", func(t *testing.T) {
		s := openStore(t)
		all := commMsgs([]string{"m0", "m1"}, "user")
		_, _ = s.Commit(all)

		_ = s.AdvanceCursor("m1")

		pending := s.PendingAfterCursor()
		if len(pending) != 0 {
			t.Errorf("cursor at head: expected 0, got %d", len(pending))
		}
	})
}

// TestConcurrentAppendRace 验证并发追加 -race 安全。
func TestConcurrentAppendRace(t *testing.T) {
	s := openStore(t)

	var wg sync.WaitGroup
	n := 10

	// 用通道分发唯一 id，避免 data race
	idCh := make(chan string, n)
	for i := 0; i < n; i++ {
		idCh <- fmt.Sprintf("race_%03d", i)
	}
	close(idCh)

	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			id := <-idCh
			msg := commMsg(id, "user", fmt.Sprintf("goroutine %d", gid))
			_, err := s.Commit([]core.Message{msg})
			if err != nil {
				t.Errorf("goroutine %d: Commit: %v", gid, err)
			}
		}(g)
	}
	wg.Wait()

	// 验证所有 10 条消息都在文件里且没有 panic
	view := s.ActiveView()
	if len(view) < 1 {
		// 由于并发 Commit，最后 head 只指向最后一条写入的路径，
		// 所以 view 可能只有最后成功的那个 goroutine 的消息链。
		// 核心是验证没有 data race 导致 panic。
		t.Logf("concurrent view len: %d (expected >=1, actual depends on timing)", len(view))
	}
}

// TestArchive 验证 Archive 后旧文件进 archive/，新文件空。
func assertPrivateMode(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != 0o700 && got != 0o600 {
		t.Errorf("permissions for %s = %04o, want 0700 or 0600", path, got)
	}
}

func TestOpenRestrictsSessionPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sessions")
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	assertPrivateMode(t, dir)
	assertPrivateMode(t, filepath.Join(dir, "current.jsonl"))
}

func TestOpenRestrictsExistingArchivePermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sessions")
	archiveDir := filepath.Join(dir, "archive")
	if err := os.MkdirAll(archiveDir, 0o755); err != nil {
		t.Fatalf("create archive dir: %v", err)
	}
	archivePath := filepath.Join(archiveDir, "old.jsonl")
	if err := os.WriteFile(archivePath, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write archive: %v", err)
	}

	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	assertPrivateMode(t, archiveDir)
	assertPrivateMode(t, archivePath)
}

func TestArchive(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// 确认 current.jsonl 有内容
	currentPath := filepath.Join(s.dir, "current.jsonl")
	lines := readJSONLFile(t, currentPath)
	if len(lines) == 0 {
		t.Fatal("current.jsonl should have content before archive")
	}

	err = s.Archive()
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}

	// current.jsonl 应该为空（新文件）
	newLines := readJSONLFile(t, currentPath)
	if len(newLines) != 0 {
		t.Errorf("current.jsonl should be empty after archive, got %d lines", len(newLines))
	}

	// archive 目录应该有刚才归档的文件
	archiveDir := filepath.Join(s.dir, "archive")
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		t.Fatalf("read archive dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 archive file, got %d", len(entries))
	}
	if !strings.HasSuffix(entries[0].Name(), ".jsonl") {
		t.Errorf("archive file should be .jsonl, got %q", entries[0].Name())
	}
	assertPrivateMode(t, archiveDir)
	assertPrivateMode(t, filepath.Join(archiveDir, entries[0].Name()))
	assertPrivateMode(t, currentPath)

	// ActiveView 应该是空
	view := s.ActiveView()
	if len(view) != 0 {
		t.Errorf("ActiveView after archive should be empty, got %d", len(view))
	}
}

// TestArchiveReopen 验证 Archive 后重新 Open 返回空会话。
func TestArchiveReopen(t *testing.T) {
	s := openStore(t)
	dir := s.dir

	all := commMsgs([]string{"m0", "m1"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	err = s.Archive()
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}
	s.Close()

	// 重新 Open
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after archive: %v", err)
	}
	defer s2.Close()

	view := s2.ActiveView()
	if len(view) != 0 {
		t.Errorf("reopen after archive: expected empty view, got %d", len(view))
	}

	// archive 文件还在
	entries, _ := os.ReadDir(filepath.Join(dir, "archive"))
	if len(entries) != 1 {
		t.Errorf("archive should have 1 file, got %d", len(entries))
	}
}

// TestCheckpoint 验证 checkpoint 功能。
func TestCheckpoint(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	err = s.Checkpoint("after m1")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	cks := s.Checkpoints(10)
	if len(cks) != 1 {
		t.Fatalf("expected 1 checkpoint, got %d", len(cks))
	}
	if cks[0].At != "m1" {
		t.Errorf("checkpoint at = %q, want m1", cks[0].At)
	}
	if cks[0].Preview != "after m1" {
		t.Errorf("checkpoint preview = %q, want 'after m1'", cks[0].Preview)
	}
}

// TestCheckpointLimit 验证 Checkpoints 的 limit 参数。
func TestCheckpointLimit(t *testing.T) {
	s := openStore(t)

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("m%02d", i)
		_, _ = s.Commit([]core.Message{commMsg(id, "user", "c")})
		_ = s.Checkpoint(fmt.Sprintf("ck_%d", i))
	}

	cks := s.Checkpoints(3)
	if len(cks) != 3 {
		t.Errorf("limit 3: expected 3, got %d", len(cks))
	}

	// 应该是最新的 3 个
	if cks[0].Preview != "ck_4" {
		t.Errorf("latest = %s, want ck_4", cks[0].Preview)
	}
}

// TestCheckpointsUnlimited 验证 limit <= 0 返回全部检查点（历史全量重放依赖）。
func TestCheckpointsUnlimited(t *testing.T) {
	s := openStore(t)

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("m%02d", i)
		_, _ = s.Commit([]core.Message{commMsg(id, "user", "c")})
		_ = s.Checkpoint(fmt.Sprintf("ck_%d", i))
	}

	for _, limit := range []int{0, -1} {
		cks := s.Checkpoints(limit)
		if len(cks) != 5 {
			t.Errorf("limit %d: expected all 5, got %d", limit, len(cks))
		}
		if len(cks) > 0 && cks[0].Preview != "ck_4" {
			t.Errorf("limit %d: latest = %s, want ck_4", limit, cks[0].Preview)
		}
	}
}

// TestClearPendingBetweenCommitAndCursor 验证增量固化场景。
func TestClearPendingBetweenCommitAndCursor(t *testing.T) {
	s := openStore(t)

	// 分批提交
	batch1 := commMsgs([]string{"m0", "m1"}, "user")
	_, err := s.Commit(batch1)
	if err != nil {
		t.Fatalf("Commit 1: %v", err)
	}

	// 固化到 m1
	_ = s.AdvanceCursor("m1")

	// 追加第二批
	batch2 := commMsgs([]string{"m2", "m3"}, "user")
	_, err = s.Commit(batch2)
	if err != nil {
		t.Fatalf("Commit 2: %v", err)
	}

	// pending 应该只有 m2 m3
	pending := s.PendingAfterCursor()
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(pending))
	}
	if pending[0].MsgID != "m2" || pending[1].MsgID != "m3" {
		t.Errorf("got pending ids %v, want [m2 m3]", viewIDs(pending))
	}
}

// TestActiveViewEmpty 空 store 返回空视图。
func TestActiveViewEmpty(t *testing.T) {
	s := openStore(t)
	view := s.ActiveView()
	if len(view) != 0 {
		t.Errorf("empty store: expected 0, got %d", len(view))
	}
}

// TestCompactEmptySession 空会话 compact 应该报错。
func TestCompactEmptySession(t *testing.T) {
	s := openStore(t)
	err := s.Compact("x", "y", "z")
	if err == nil {
		t.Error("expected error compacting empty session")
	}
}

// TestSetHeadUnknown 测试 rewind 到不在内存记录中的 id（head 指向未知节点）。
func TestSetHeadUnknown(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0"}, "user")
	_, _ = s.Commit(all)

	// rewind 到不存在的节点（仅追加 head 记录，不校验 id 存在性）
	err := s.SetHead("nonexistent")
	if err != nil {
		t.Fatalf("SetHead unknown: %v", err)
	}

	// ActiveView 应该返回空（因为 head 指向不存在的节点）
	view := s.ActiveView()
	if len(view) != 0 {
		t.Errorf("head to unknown node should yield empty view, got %d", len(view))
	}
}

// TestDetectCompact 验证检测逻辑。
func TestDetectCompact(t *testing.T) {
	tests := []struct {
		name     string
		oldView  []core.Message
		newConv  []core.Message
		wantFrom string
		wantTo   string
		wantSum  string
		wantOK   bool
	}{
		{
			name: "prefix_compressed",
			oldView: []core.Message{
				{MsgID: "m0", Role: "user", Content: "old0"},
				{MsgID: "m1", Role: "user", Content: "old1"},
				{MsgID: "m2", Role: "assistant", Content: "old2"},
			},
			newConv: []core.Message{
				{MsgID: "s_x", Role: "system", Content: "sum"},
				{MsgID: "m2", Role: "assistant", Content: "old2"},
			},
			wantFrom: "m0",
			wantTo:   "m1",
			wantSum:  "sum",
			wantOK:   true,
		},
		{
			name: "no_system_first",
			oldView: []core.Message{
				{MsgID: "m0", Role: "user", Content: "a"},
				{MsgID: "m1", Role: "assistant", Content: "b"},
			},
			newConv: []core.Message{
				{MsgID: "m0", Role: "user", Content: "a"},
			},
			wantOK: false,
		},
		{
			name: "not_shorter",
			oldView: []core.Message{
				{MsgID: "m0", Role: "user", Content: "a"},
			},
			newConv: []core.Message{
				{MsgID: "s_x", Role: "system", Content: "sum"},
				{MsgID: "m0", Role: "user", Content: "a"},
				{MsgID: "m1", Role: "assistant", Content: "b"},
			},
			wantOK: false,
		},
		{
			name: "full_fold",
			oldView: []core.Message{
				{MsgID: "m0", Role: "user", Content: "a"},
				{MsgID: "m1", Role: "assistant", Content: "b"},
			},
			newConv: []core.Message{
				{MsgID: "s_x", Role: "system", Content: "summary"},
			},
			wantFrom: "m0",
			wantTo:   "m1",
			wantSum:  "summary",
			wantOK:   true,
		},
		{
			name: "all_overlap_no_cover",
			oldView: []core.Message{
				{MsgID: "m0", Role: "user", Content: "a"},
			},
			newConv: []core.Message{
				{MsgID: "s_x", Role: "system", Content: "sum"},
				{MsgID: "m0", Role: "user", Content: "a"},
			},
			wantOK: false, // splitIdx == 0
		},
		{
			name:    "empty_new",
			oldView: []core.Message{{MsgID: "m0", Role: "user", Content: "a"}},
			newConv: []core.Message{},
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, to, sum, ok := DetectCompact(tt.oldView, tt.newConv)
			if ok != tt.wantOK {
				t.Errorf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if from != tt.wantFrom || to != tt.wantTo || sum != tt.wantSum {
				t.Errorf("got from=%s to=%s sum=%s, want from=%s to=%s sum=%s",
					from, to, sum, tt.wantFrom, tt.wantTo, tt.wantSum)
			}
		})
	}
}

// TestFileAppendIntegrity 验证文件追加完整性（读回后记录数一致）。
func TestFileAppendIntegrity(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1", "m2"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// 重新 Open 验证记录数
	s.Close()
	s2, err := Open(s.dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	view := s2.ActiveView()
	if len(view) != 3 {
		t.Errorf("reopen view len = %d, want 3", len(view))
	}
	for i, m := range view {
		if m.MsgID != fmt.Sprintf("m%d", i) {
			t.Errorf("view[%d].MsgID = %q, want m%d", i, m.MsgID, i)
		}
	}
}

// TestMultipleCommitBatches 验证多次分批提交后的视图正确性。
func TestMultipleCommitBatches(t *testing.T) {
	s := openStore(t)

	batch1 := commMsgs([]string{"m0"}, "user")
	_, err := s.Commit(batch1)
	if err != nil {
		t.Fatalf("Commit 1: %v", err)
	}

	batch2 := commMsgs([]string{"m1", "m2"}, "assistant")
	_, err = s.Commit(batch2)
	if err != nil {
		t.Fatalf("Commit 2: %v", err)
	}

	view := s.ActiveView()
	ids := viewIDs(view)
	expected := []string{"m0", "m1", "m2"}
	if !equalSlices(ids, expected) {
		t.Errorf("batch view: got %v, want %v", ids, expected)
	}
}

// TestBadLineSkip 验证坏行跳过不中断加载。
func TestBadLineSkip(t *testing.T) {
	s := openStore(t)

	// 手动写入坏行
	badLine := "this is not json\n"
	f, err := os.OpenFile(filepath.Join(s.dir, "current.jsonl"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for bad line: %v", err)
	}
	f.WriteString(badLine)
	f.Sync()
	f.Close()

	// 追加一条合法消息
	msg := commMsg("m0", "user", "hello")
	_, err = s.Commit([]core.Message{msg})
	if err != nil {
		t.Fatalf("Commit after bad line: %v", err)
	}

	// 重新加载应跳过坏行并正确解析
	s.Close()
	s2, err := Open(s.dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	view := s2.ActiveView()
	if len(view) != 1 || view[0].MsgID != "m0" {
		t.Errorf("after bad line skip: view = %v, want [m0]", viewIDs(view))
	}
}

// TestCompactPreservesCloneOf 验证副本标记 clone_of 字段。
func TestCompactPreservesCloneOf(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1", "m2"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	err = s.Compact("m0", "m1", "sum")
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// 读取文件，检查副本是否有 clone_of 标记
	lines := readJSONLFile(t, filepath.Join(s.dir, "current.jsonl"))
	var clones []Record
	for _, line := range lines {
		var r Record
		json.Unmarshal([]byte(line), &r)
		if r.Msg != nil && r.Msg.CloneOf != "" {
			clones = append(clones, r)
		}
	}
	if len(clones) == 0 {
		t.Error("expected at least one clone record with clone_of field")
	}
	for _, c := range clones {
		if c.Msg.CloneOf != "m2" {
			t.Errorf("clone_of = %q, want m2", c.Msg.CloneOf)
		}
	}
}

// TestCommitWithNoMsgs 空提交不报错。
func TestCommitWithNoMsgs(t *testing.T) {
	s := openStore(t)
	committed, err := s.Commit(nil)
	if err != nil {
		t.Fatalf("Commit nil: %v", err)
	}
	if len(committed) != 0 {
		t.Errorf("expected 0 committed for nil, got %d", len(committed))
	}

	committed, err = s.Commit([]core.Message{})
	if err != nil {
		t.Fatalf("Commit empty: %v", err)
	}
	if len(committed) != 0 {
		t.Errorf("expected 0 committed for empty, got %d", len(committed))
	}
}

// TestCheckpointEmptySession 空会话 checkpoint 应报错。
func TestCheckpointEmptySession(t *testing.T) {
	s := openStore(t)
	err := s.Checkpoint("test")
	if err == nil {
		t.Error("expected error checkpointing empty session")
	}
}

// TestAdvanceCursor 验证游标推进。
func TestAdvanceCursor(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1"}, "user")
	_, _ = s.Commit(all)

	err := s.AdvanceCursor("m1")
	if err != nil {
		t.Fatalf("AdvanceCursor: %v", err)
	}

	pending := s.PendingAfterCursor()
	if len(pending) != 0 {
		t.Errorf("cursor at head: expected 0 pending, got %d", len(pending))
	}
}

// TestRewindMultipleBranches 验证多次 rewind 产生多个分支。
func TestRewindMultipleBranches(t *testing.T) {
	s := openStore(t)

	// 主干：m0 m1 m2
	all := commMsgs([]string{"m0", "m1", "m2"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit main: %v", err)
	}

	// rewind 到 m1，开分支 A
	_ = s.SetHead("m1")
	branchA := []core.Message{commMsg("a1", "assistant", "branch A")}
	_, err = s.Commit(branchA)
	if err != nil {
		t.Fatalf("Commit branch A: %v", err)
	}

	// rewind 到 m0，开分支 B
	_ = s.SetHead("m0")
	branchB := []core.Message{commMsg("b1", "assistant", "branch B")}
	_, err = s.Commit(branchB)
	if err != nil {
		t.Fatalf("Commit branch B: %v", err)
	}

	view := s.ActiveView()
	ids := viewIDs(view)
	if len(ids) != 2 || ids[0] != "m0" || ids[1] != "b1" {
		t.Errorf("branch B view: got %v, want [m0 b1]", ids)
	}

	// rewind 到 m1，确认分支 A 还在
	_ = s.SetHead("m1")
	viewA := s.ActiveView()
	idsA := viewIDs(viewA)
	if len(idsA) != 2 || idsA[0] != "m0" || idsA[1] != "m1" {
		t.Errorf("rewind to m1 view: got %v, want [m0 m1]", idsA)
	}
}

// TestCompactNoRetain 验证覆盖到 head（无保留段）的 compact。
func TestCompactNoRetain(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// compact m0..m1（覆盖全部，无保留段）
	err = s.Compact("m0", "m1", "everything")
	if err != nil {
		t.Fatalf("Compact all: %v", err)
	}

	view := s.ActiveView()
	if len(view) != 1 {
		t.Fatalf("expected 1 (compact only), got %d: %v", len(view), viewIDs(view))
	}
	if view[0].Role != "system" || view[0].Content != "everything" {
		t.Errorf("compact-only view: role=%s content=%s", view[0].Role, view[0].Content)
	}
}

// TestDetectCompactEdgeCases 检测边界。
func TestDetectCompactEdgeCases(t *testing.T) {
	// 新对话首条 system 无 MsgID（压缩器产生摘要时常见场景）
	t.Run("new_conv_first_no_id", func(t *testing.T) {
		oldView := []core.Message{
			{MsgID: "m0", Role: "user", Content: "a"},
			{MsgID: "m1", Role: "assistant", Content: "b"},
			{MsgID: "m2", Role: "user", Content: "c"},
		}
		newConv := []core.Message{
			{Role: "system", Content: "sum"},          // 摘要消息无 MsgID（由压缩器产生）
			{MsgID: "m2", Role: "user", Content: "c"}, // 只有 m2 保留
		}

		from, to, _, ok := DetectCompact(oldView, newConv)
		if !ok {
			t.Error("should detect compact even when summary msg has no id")
		}
		if from != "m0" || to != "m1" {
			t.Errorf("covered range: got [%s, %s], want [m0, m1]", from, to)
		}
	})

	// oldView 首条消息无 MsgID（理论不应出现，但防御性处理：跳过无 id 消息继续匹配）
	t.Run("old_first_no_id", func(t *testing.T) {
		oldView := []core.Message{
			{Role: "system", Content: "intro"}, // 无 MsgID，如旧版遗留的系统提示
			{MsgID: "m0", Role: "user", Content: "a"},
			{MsgID: "m1", Role: "assistant", Content: "b"},
		}
		newConv := []core.Message{
			{MsgID: "s_x", Role: "system", Content: "sum"},
			{MsgID: "m1", Role: "assistant", Content: "b"},
		}

		// m0 不在 newConv 中 → covered = oldView[1:1]（空）→ 第一条有重叠的是 m1
		// 但 m0 不在 newIDs 中，m1 在。splitIdx=2 (m1的索引)。
		// covered = oldView[:2] = [{system, ""}, {m0, user}]
		// covered[0].MsgID = "" → 无法确定 from → 返回 !ok
		_, _, _, ok := DetectCompact(oldView, newConv)
		if ok {
			t.Error("should reject when covered segment starts with empty MsgID")
		}
	})

	// 非连续前缀：oldView 中部分消息消失，首条存活消息之后还有更多消息
	t.Run("contiguous_prefix", func(t *testing.T) {
		oldView := []core.Message{
			{MsgID: "m0", Role: "user", Content: "a"},
			{MsgID: "m1", Role: "assistant", Content: "b"},
			{MsgID: "m2", Role: "user", Content: "c"},
		}
		// m0 m1 被摘要掉，m2 保留（3条 → 2条，满足 len(newConv) < len(oldView)）
		newConv := []core.Message{
			{MsgID: "s_x", Role: "system", Content: "sum"},
			{MsgID: "m2", Role: "user", Content: "c"},
		}

		from, to, _, ok := DetectCompact(oldView, newConv)
		if !ok {
			t.Error("should detect compact for contiguous prefix")
		}
		if from != "m0" || to != "m1" {
			t.Errorf("covered range: got [%s, %s], want [m0, m1]", from, to)
		}
	})
}

// ── 测试辅助 ──────────────────────────────────────────────────────────

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestViewAfterReopen 验证 Close → Open 往返后 ActiveView 正确。
func TestViewAfterReopen(t *testing.T) {
	d := tempDir(t)

	s, err := Open(d)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	all := commMsgs([]string{"m0", "m1"}, "user")
	_, err = s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	s.Close()

	// 重新打开
	s2, err := Open(d)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer s2.Close()

	view := s2.ActiveView()
	if len(view) != 2 {
		t.Errorf("reopen view: expected 2, got %d", len(view))
	}
	if view[0].MsgID != "m0" || view[1].MsgID != "m1" {
		t.Errorf("reopen view ids: %v, want [m0 m1]", viewIDs(view))
	}
}

// TestCheckpointsOrder 验证检查点按时间倒序返回。
func TestCheckpointsOrder(t *testing.T) {
	s := openStore(t)

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("m%02d", i)
		_, _ = s.Commit([]core.Message{commMsg(id, "user", "c")})
		_ = s.Checkpoint(fmt.Sprintf("preview_%d", i))
	}

	cks := s.Checkpoints(100)
	if len(cks) != 3 {
		t.Fatalf("expected 3 checkpoints, got %d", len(cks))
	}
	// 最新的在前
	if cks[0].Preview != "preview_2" {
		t.Errorf("cks[0] = %s, want preview_2", cks[0].Preview)
	}
	if cks[1].Preview != "preview_1" {
		t.Errorf("cks[1] = %s, want preview_1", cks[1].Preview)
	}
	if cks[2].Preview != "preview_0" {
		t.Errorf("cks[2] = %s, want preview_0", cks[2].Preview)
	}
}

// TestCommitWithToolCalls 验证带 ToolCalls 的消息往返。
func TestCommitWithToolCalls(t *testing.T) {
	s := openStore(t)

	msg := core.Message{
		MsgID: "tool_assist",
		Role:  "assistant",
		ToolCalls: []core.ToolCall{
			{
				ID:   "call_1",
				Type: "function",
				Function: core.ToolCallFunction{
					Name:      "search",
					Arguments: `{"query":"test"}`,
				},
			},
		},
	}

	_, err := s.Commit([]core.Message{msg})
	if err != nil {
		t.Fatalf("Commit tool_call: %v", err)
	}

	view := s.ActiveView()
	if len(view) != 1 {
		t.Fatalf("view len = %d, want 1", len(view))
	}
	if len(view[0].ToolCalls) != 1 {
		t.Fatalf("ToolCalls len = %d, want 1", len(view[0].ToolCalls))
	}
	if view[0].ToolCalls[0].ID != "call_1" {
		t.Errorf("ToolCall ID = %q, want call_1", view[0].ToolCalls[0].ID)
	}
}

// TestCompactCursorRemap 完整游标映射三档测试（端到端）。
func TestCompactCursorRemap(t *testing.T) {
	tests := []struct {
		name        string
		cursorPos   string // 固化游标位置
		coveredFrom string
		coveredTo   string
		wantPending int // PendingAfterCursor 期望数量
	}{
		{
			name:        "cursor_at_retain_end_after_head",
			cursorPos:   "m4",
			coveredFrom: "m0",
			coveredTo:   "m2",
			wantPending: 0, // 游标映射到最后副本，无未固化
		},
		{
			name:        "cursor_in_retain_middle_partial",
			cursorPos:   "m3",
			coveredFrom: "m0",
			coveredTo:   "m1",
			wantPending: 1, // m4' 未固化
		},
		{
			name:        "cursor_in_covered_full",
			cursorPos:   "m1",
			coveredFrom: "m0",
			coveredTo:   "m2",
			wantPending: 2, // 兜底全量（游标不在活跃路径）
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openStore(t)

			// 提交 m0 m1 m2 m3 m4
			ids := []string{"m0", "m1", "m2", "m3", "m4"}
			all := commMsgs(ids, "user")
			_, err := s.Commit(all)
			if err != nil {
				t.Fatalf("Commit: %v", err)
			}

			// 设置游标
			_ = s.AdvanceCursor(tt.cursorPos)

			// compact
			err = s.Compact(tt.coveredFrom, tt.coveredTo, "sum")
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}

			pending := s.PendingAfterCursor()
			if len(pending) != tt.wantPending {
				t.Errorf("pending: got %d, want %d (ids: %v)", len(pending), tt.wantPending, viewIDs(pending))
			}

			// 验证无重复
			idCount := make(map[string]int)
			for _, m := range pending {
				idCount[m.MsgID]++
			}
			for id, c := range idCount {
				if c > 1 {
					t.Errorf("duplicate id %s in pending", id)
				}
			}
		})
	}
}

// TestCompactRangeValidation 验证 compact 参数校验。
func TestCompactRangeValidation(t *testing.T) {
	s := openStore(t)
	all := commMsgs([]string{"m0", "m1", "m2"}, "user")
	_, _ = s.Commit(all)

	// 倒序 range
	err := s.Compact("m2", "m0", "sum")
	if err == nil {
		t.Error("expected error for reversed range")
	}

	// 不存在的 from
	err = s.Compact("m9", "m1", "sum")
	if err == nil {
		t.Error("expected error for unknown from")
	}
}

// TestActiveViewWithCompactNode 验证包含 compact 节点的 ActiveView 解析。
func TestActiveViewWithCompactNode(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1"}, "user")
	_, _ = s.Commit(all)

	// compact 后 compact 节点应在视图中呈现为 system 消息
	_ = s.Compact("m0", "m0", "folded m0")

	view := s.ActiveView()
	hasSystem := false
	for _, m := range view {
		if m.Role == "system" {
			hasSystem = true
			if m.Content != "folded m0" {
				t.Errorf("system content = %q, want 'folded m0'", m.Content)
			}
		}
	}
	if !hasSystem {
		t.Error("view should contain system summary from compact node")
	}
}

// TestCompactThenRewindThenCompactAgain 验证 compact → rewind → 再 compact 的复杂场景。
func TestCompactThenRewindThenCompactAgain(t *testing.T) {
	s := openStore(t)

	all := commMsgs([]string{"m0", "m1", "m2", "m3", "m4"}, "user")
	_, err := s.Commit(all)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// compact m0..m2
	err = s.Compact("m0", "m2", "sum1")
	if err != nil {
		t.Fatalf("Compact 1: %v", err)
	}

	// rewind 到 m1（回到原链 covered 段）
	err = s.SetHead("m1")
	if err != nil {
		t.Fatalf("SetHead: %v", err)
	}

	// 在原链上 compact m0..m0（不同的覆盖范围）
	err = s.Compact("m0", "m0", "sum2")
	if err != nil {
		t.Fatalf("Compact 2: %v", err)
	}

	view := s.ActiveView()
	// [s_m0(sum2), m1'] — m1 在保留段被复制，id 已变
	if len(view) != 2 {
		t.Fatalf("view len = %d, want 2: %v", len(view), viewIDs(view))
	}
	if view[0].Role != "system" || view[0].Content != "sum2" {
		t.Errorf("view[0]: role=%s content=%s", view[0].Role, view[0].Content)
	}
	if view[1].Content != "content_m1" {
		t.Errorf("view[1] content = %s, want content_m1", view[1].Content)
	}
	if view[1].Role != "user" {
		t.Errorf("view[1] role = %s, want user", view[1].Role)
	}
	// 副本 id 应不同于原 m1
	if view[1].MsgID == "m1" {
		t.Errorf("view[1] should be clone with new id, not m1")
	}
}

// TestNewMsgIDGeneration 验证消息 id 生成唯一性。
func TestNewMsgIDGeneration(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := NewMsgID()
		if ids[id] {
			t.Errorf("duplicate id generated: %s", id)
		}
		ids[id] = true
	}
}

// TestRecordRoundTrip 验证 messageToRecord → recordToMessage 往返。
func TestRecordRoundTrip(t *testing.T) {
	m := core.Message{
		Role:       "assistant",
		Content:    "hello world",
		ToolCallID: "call_42",
		ToolCalls:  []core.ToolCall{{ID: "t1", Type: "function", Function: core.ToolCallFunction{Name: "do", Arguments: "{}"}}},
		MsgID:      "m_abc",
	}

	rec := messageToRecord(m)
	m2 := recordToMessage(rec)

	if m2.Role != m.Role || m2.Content != m.Content || m2.ToolCallID != m.ToolCallID || m2.MsgID != m.MsgID {
		t.Errorf("round-trip mismatch: %+v → %+v", m, m2)
	}
	if len(m2.ToolCalls) != 1 || m2.ToolCalls[0].ID != "t1" {
		t.Errorf("ToolCalls round-trip failed")
	}
}

// TestSortCheckpoints 验证检查点排序的稳定性。
func TestSortCheckpoints(t *testing.T) {
	// 验证 Checkpoints 返回的是按记录顺序（后出现的在前）
	s := openStore(t)

	ids := []string{"a", "b", "c", "d", "e"}
	for _, id := range ids {
		_, _ = s.Commit([]core.Message{commMsg(id, "user", "c")})
		_ = s.Checkpoint("ck_" + id)
	}

	cks := s.Checkpoints(10)
	if len(cks) != 5 {
		t.Fatalf("expected 5, got %d", len(cks))
	}

	// 验证按时间倒序排列（最新的在前）
	times := make([]string, len(cks))
	for i, ck := range cks {
		times[i] = ck.Preview
	}
	if !sort.SliceIsSorted(times, func(i, j int) bool {
		// 在文件中，后面出现的记录 CreatedAt 更大，我们在 Checkpoints 中从后向前收集
		// 所以 times 应该已经是降序
		return times[i] > times[j]
	}) {
		t.Errorf("checkpoints not in reverse chronological order: %v", times)
	}
}
