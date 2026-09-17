package session

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

// archivePathOf 返回会话目录下唯一的归档文件路径（测试夹具保证只有一个）。
func archivePathOf(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "archive"))
	if err != nil {
		t.Fatalf("read archive dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("archive entries = %d, want 1", len(entries))
	}
	return filepath.Join(dir, "archive", entries[0].Name())
}

// TestOpenArchiveViewRoundTrip 归档只读视图往返：Commit 若干轮 + checkpoint →
// Archive → OpenArchiveView 打开归档文件，断言 Head、ActiveView 内容/顺序与
// Checkpoints 锚点与归档前一致，且查看不改动归档文件字节。
func TestOpenArchiveViewRoundTrip(t *testing.T) {
	s := openStore(t)
	dir := s.dir

	// 轮 1：user → assistant(正文) → user；轮 2：assistant 带 tool_call → tool 结果
	if _, err := s.Commit([]core.Message{
		commMsg("m0", "user", "第一问"),
		commMsg("m1", "assistant", "第一答"),
		commMsg("m2", "user", "第二问"),
	}); err != nil {
		t.Fatalf("Commit round 1: %v", err)
	}
	if err := s.Checkpoint("round 1"); err != nil {
		t.Fatalf("Checkpoint round 1: %v", err)
	}
	if _, err := s.Commit([]core.Message{
		{
			MsgID: "m3", Role: "assistant",
			ToolCalls: []core.ToolCall{{
				ID: "call_1", Type: "function",
				Function: core.ToolCallFunction{Name: "read_file", Arguments: `{"path":"a.go"}`},
			}},
			Thinking: core.Thinking{Text: "想一下"},
		},
		{MsgID: "m4", Role: "tool", Content: "文件内容", ToolCallID: "call_1"},
	}); err != nil {
		t.Fatalf("Commit round 2: %v", err)
	}
	if err := s.Checkpoint("round 2"); err != nil {
		t.Fatalf("Checkpoint round 2: %v", err)
	}

	headBefore := s.Head()
	viewBefore := s.ActiveView()
	cksBefore := s.Checkpoints(0)

	if err := s.Archive(); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	path := archivePathOf(t, dir)
	rawBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read archive bytes: %v", err)
	}

	v, err := OpenArchiveView(path)
	if err != nil {
		t.Fatalf("OpenArchiveView: %v", err)
	}
	if v.Head() != headBefore {
		t.Errorf("archive head = %q, want %q", v.Head(), headBefore)
	}

	view := v.ActiveView()
	if len(view) != len(viewBefore) {
		t.Fatalf("archive view = %d messages, want %d", len(view), len(viewBefore))
	}
	for i := range viewBefore {
		if view[i].MsgID != viewBefore[i].MsgID || view[i].Role != viewBefore[i].Role || view[i].Content != viewBefore[i].Content {
			t.Errorf("archive view[%d] = {%s %s %q}, want {%s %s %q}", i,
				view[i].MsgID, view[i].Role, view[i].Content,
				viewBefore[i].MsgID, viewBefore[i].Role, viewBefore[i].Content)
		}
	}
	// tool 配对与 thinking 字段随视图保真（重放依赖它们）
	if view[3].ToolCalls[0].ID != "call_1" || view[4].ToolCallID != "call_1" {
		t.Errorf("tool pairing lost: assistant calls = %+v, tool id = %q", view[3].ToolCalls, view[4].ToolCallID)
	}
	if view[3].Thinking.Text != "想一下" {
		t.Errorf("view[3].Thinking = %q, want 想一下", view[3].Thinking.Text)
	}

	cks := v.Checkpoints(0)
	if len(cks) != len(cksBefore) {
		t.Fatalf("archive checkpoints = %d, want %d", len(cks), len(cksBefore))
	}
	for i := range cksBefore {
		if cks[i].At != cksBefore[i].At || cks[i].ID != cksBefore[i].ID || cks[i].Preview != cksBefore[i].Preview {
			t.Errorf("archive checkpoint[%d] = %+v, want %+v", i, cks[i], cksBefore[i])
		}
	}
	if got := v.Checkpoints(1); len(got) != 1 || got[0].At != cksBefore[0].At {
		t.Errorf("Checkpoints(1) = %+v, want latest only", got)
	}

	// 只读：查看后归档文件字节不变
	rawAfter, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("re-read archive bytes: %v", err)
	}
	if !bytes.Equal(rawBefore, rawAfter) {
		t.Error("archive file changed after OpenArchiveView (must be read-only)")
	}
}

// TestOpenArchiveViewWithCompact 覆盖 compact 场景：归档视图里 compact 节点转为
// system 摘要、保留段副本保真（与 Store.ActiveView 同语义）。
func TestOpenArchiveViewWithCompact(t *testing.T) {
	s := openStore(t)
	dir := s.dir

	if _, err := s.Commit(commMsgs([]string{"m0", "m1", "m2", "m3", "m4"}, "user")); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// compact m1..m3，保留 m4 → 视图 [m0, s_m3(summary), m4']
	if err := s.Compact("m1", "m3", "summary of m1-m3"); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Checkpoint("after compact"); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	headBefore := s.Head()
	viewBefore := s.ActiveView()

	if err := s.Archive(); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	v, err := OpenArchiveView(archivePathOf(t, dir))
	if err != nil {
		t.Fatalf("OpenArchiveView: %v", err)
	}
	if v.Head() != headBefore {
		t.Errorf("archive head = %q, want %q", v.Head(), headBefore)
	}
	view := v.ActiveView()
	if len(view) != len(viewBefore) || len(view) != 3 {
		t.Fatalf("archive view = %d messages, want 3: %v", len(view), viewIDs(view))
	}
	if view[0].MsgID != "m0" {
		t.Errorf("view[0].MsgID = %q, want m0 (compact 不截断更早历史)", view[0].MsgID)
	}
	if view[1].Role != "system" || view[1].Content != "summary of m1-m3" || view[1].MsgID != "s_m3" {
		t.Errorf("view[1] = {%s %s %q}, want compact summary s_m3", view[1].MsgID, view[1].Role, view[1].Content)
	}
	if view[2].Content != "content_m4" || view[2].MsgID == "m4" {
		t.Errorf("view[2] = {%s %q}, want clone of m4 with new id", view[2].MsgID, view[2].Content)
	}
	if cks := v.Checkpoints(0); len(cks) != 1 || cks[0].At != headBefore {
		t.Errorf("archive checkpoints = %+v, want one at %q", cks, headBefore)
	}
}

// TestOpenArchiveViewMissingFile 缺失路径报错（不同于 Store.Open 的新建语义）。
func TestOpenArchiveViewMissingFile(t *testing.T) {
	if _, err := OpenArchiveView(filepath.Join(t.TempDir(), "archive", "nope.jsonl")); err == nil {
		t.Error("OpenArchiveView on missing file = nil error, want error")
	}
}

// TestOpenArchiveViewEmptyFile 空归档文件返回空视图不报错（无 head）。
func TestOpenArchiveViewEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.jsonl")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write empty archive: %v", err)
	}
	v, err := OpenArchiveView(path)
	if err != nil {
		t.Fatalf("OpenArchiveView empty: %v", err)
	}
	if v.Head() != "" || len(v.ActiveView()) != 0 || len(v.Checkpoints(0)) != 0 {
		t.Errorf("empty archive view = {head %q, view %d, cks %d}, want empty", v.Head(), len(v.ActiveView()), len(v.Checkpoints(0)))
	}
}
