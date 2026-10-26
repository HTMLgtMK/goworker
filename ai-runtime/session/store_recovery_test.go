// store_recovery_test.go 验证崩溃尾记录截断修复（P1-2 存储层前置；完整版
// TestRestoreInterruptedRun 依赖 Run 状态持久化，按实施计划 §3/§4 划归 P2）。
package session

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

// TestPartialTailRecovery 验证：手工向 current.jsonl 追加半行 JSON（模拟
// appendRecord 的单次 Write 中途崩溃）→ Open 自动截断回最后一条合法记录、
// 既有消息 ActiveView 完整、Commit 继续可用、修复后重开历史无损。
func TestPartialTailRecovery(t *testing.T) {
	dir := tempDir(t)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Commit(commMsgs([]string{"m0", "m1"}, "user")); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, "current.jsonl")
	goodInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat before torn tail: %v", err)
	}
	goodSize := goodInfo.Size()

	// 模拟崩溃：半行 JSON——无结尾 '}'、无行尾 '\n'（appendRecord 的 Write
	// 只落了前半段）。
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open for torn tail: %v", err)
	}
	torn := `{"kind":"msg","id":"m_torn","role":"user","content":"never finished`
	if _, err := f.WriteString(torn); err != nil {
		f.Close()
		t.Fatalf("write torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close torn tail writer: %v", err)
	}
	tornInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after torn tail: %v", err)
	}
	if tornInfo.Size() <= goodSize {
		t.Fatalf("torn tail not appended: size = %d, want > %d", tornInfo.Size(), goodSize)
	}

	// Open 触发尾部修复：截断回最后一条合法记录并落盘。
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after torn tail: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	repairedInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after repair: %v", err)
	}
	if repairedInfo.Size() != goodSize {
		t.Fatalf("file size after repair = %d, want %d (truncated back to last valid record)", repairedInfo.Size(), goodSize)
	}

	// 既有消息 ActiveView 完整。
	if got := viewIDs(reopened.ActiveView()); !equalSlices(got, []string{"m0", "m1"}) {
		t.Fatalf("ActiveView after repair = %v, want [m0 m1]", got)
	}

	// Commit 继续可用。
	if _, err := reopened.Commit([]core.Message{commMsg("m2", "user", "after repair")}); err != nil {
		t.Fatalf("Commit after repair: %v", err)
	}
	if got := viewIDs(reopened.ActiveView()); !equalSlices(got, []string{"m0", "m1", "m2"}) {
		t.Fatalf("ActiveView after post-repair commit = %v, want [m0 m1 m2]", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close after repair: %v", err)
	}

	// 修复后重开：历史与追加记录都完整可读（修复没有留下脏字节）。
	again, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after repair: %v", err)
	}
	defer again.Close()
	if got := viewIDs(again.ActiveView()); !equalSlices(got, []string{"m0", "m1", "m2"}) {
		t.Fatalf("ActiveView after reopen = %v, want [m0 m1 m2]", got)
	}
}
