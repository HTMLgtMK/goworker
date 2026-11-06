package session

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
)

// digest_test.go 是 Plan A3 清单解析面（ScanSessionDigests）的验收：v2/v1 双
// 探测、legacy 根槽、archived/workspace/首条 user 事实提取、空/坏文件跳过。

// TestScanSessionDigestsCoversAllLayouts 验证扫描并集：per-id v2 目录、per-id
// 存量 current.jsonl 目录、legacy 根槽三类全部收录，事实字段逐项正确；
// 归档目录与杂散文件不入清单。
func TestScanSessionDigestsCoversAllLayouts(t *testing.T) {
	root := t.TempDir()

	// v2 会话：Create 形态（<id>/<id>.jsonl），带 workspace 声明 + user 消息。
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	v2, err := repo.Create(context.Background(), CreateOptions{CWD: "/tmp/wk-a"})
	if err != nil {
		t.Fatalf("Create v2: %v", err)
	}
	if _, err := v2.Commit([]core.Message{commMsg("m0", "user", "v2 hello")}); err != nil {
		t.Fatalf("Commit v2: %v", err)
	}
	if err := v2.SetTitle("Generated v2 title"); err != nil {
		t.Fatalf("SetTitle v2: %v", err)
	}
	v2ID := v2.SessionID()

	// 存量 current.jsonl 目录（v1 兼容形态）+ archived 终态标记。目录名是合法
	// session id，种子 metadata 以目录名落 current.jsonl（对齐
	// TestRepositoryOpenListDualProbe 的手工形态——Open 对空目录会生成新随机
	// id，破坏「目录名 == 文件内 id」不变量）。
	v1ID := NewSessionID()
	v1Dir := filepath.Join(root, v1ID)
	if err := os.MkdirAll(v1Dir, 0o700); err != nil {
		t.Fatalf("mkdir v1 dir: %v", err)
	}
	seed := fmt.Sprintf(`{"kind":"metadata","session_id":%q,"created_at":"2024-01-01T00:00:00Z"}`+"\n", v1ID)
	if err := os.WriteFile(filepath.Join(v1Dir, "current.jsonl"), []byte(seed), 0o600); err != nil {
		t.Fatalf("write v1 seed: %v", err)
	}
	st, err := Open(v1Dir)
	if err != nil {
		t.Fatalf("Open v1 slot: %v", err)
	}
	if _, err := st.Commit([]core.Message{commMsg("m0", "user", "v1 archived")}); err != nil {
		t.Fatalf("Commit v1: %v", err)
	}
	if err := st.MarkArchived(); err != nil {
		t.Fatalf("MarkArchived: %v", err)
	}
	v1Path := resolveDataFilePath(v1Dir) // 存量 current.jsonl 命中兼容探测
	v1Mod := modTime(t, v1Path)
	if err := st.Close(); err != nil {
		t.Fatalf("Close v1: %v", err)
	}

	// legacy 根槽：root 自身是会话槽。
	legacy, err := Open(root)
	if err != nil {
		t.Fatalf("Open legacy: %v", err)
	}
	if _, err := legacy.Commit([]core.Message{commMsg("m0", "user", "legacy hello")}); err != nil {
		t.Fatalf("Commit legacy: %v", err)
	}
	legacyID := legacy.SessionID()
	legacyMod := modTime(t, legacy.path)
	if err := legacy.Close(); err != nil {
		t.Fatalf("Close legacy: %v", err)
	}

	// 杂散内容：归档目录（存量轮转产物）与非目录文件不入清单。
	if err := os.MkdirAll(filepath.Join(root, v2ID, "archive"), 0o700); err != nil {
		t.Fatalf("mkdir archive: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "stray.jsonl"), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write stray: %v", err)
	}

	digests, err := ScanSessionDigests(root)
	if err != nil {
		t.Fatalf("ScanSessionDigests: %v", err)
	}
	byID := make(map[string]SessionDigest, len(digests))
	for _, d := range digests {
		byID[d.Metadata.SessionID] = d
	}
	if len(byID) != 3 {
		t.Fatalf("digests = %d entries (%v), want exactly 3 (v2 + v1 + legacy)", len(byID), idsOf(digests))
	}

	v2d, ok := byID[v2ID]
	if !ok {
		t.Fatalf("v2 session %s missing from scan", v2ID)
	}
	if v2d.Workspace != "/tmp/wk-a" || v2d.Archived || v2d.FirstUserText != "v2 hello" || v2d.Title != "Generated v2 title" {
		t.Fatalf("v2 digest = %+v, want workspace/archived/first-user/title aligned", v2d)
	}

	v1d, ok := byID[v1ID]
	if !ok {
		t.Fatalf("v1 session %s missing from scan", v1ID)
	}
	if !v1d.Archived || v1d.FirstUserText != "v1 archived" {
		t.Fatalf("v1 digest = %+v, want archived + first user text", v1d)
	}
	if !v1d.ModTime.Equal(v1Mod) {
		t.Errorf("v1 mtime = %v, want %v", v1d.ModTime, v1Mod)
	}

	ld, ok := byID[legacyID]
	if !ok {
		t.Fatalf("legacy session %s missing from scan", legacyID)
	}
	if ld.FirstUserText != "legacy hello" || ld.Archived {
		t.Fatalf("legacy digest = %+v, want first user text, not archived", ld)
	}
	if !ld.ModTime.Equal(legacyMod) {
		t.Errorf("legacy mtime = %v, want %v", ld.ModTime, legacyMod)
	}
}

// TestScanSessionDigestsSkipsUnaddressable 验证容错：空数据文件（无身份）跳过、
// root 不可读上抛。
func TestScanSessionDigestsSkipsUnaddressable(t *testing.T) {
	root := t.TempDir()
	emptyID := NewSessionID()
	if err := os.MkdirAll(filepath.Join(root, emptyID), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, emptyID, emptyID+".jsonl"), nil, 0o600); err != nil {
		t.Fatalf("write empty: %v", err)
	}
	digests, err := ScanSessionDigests(root)
	if err != nil {
		t.Fatalf("ScanSessionDigests: %v", err)
	}
	if len(digests) != 0 {
		t.Fatalf("digests = %v, want empty (身份缺失的目录不可寻址)", digests)
	}

	if _, err := ScanSessionDigests(filepath.Join(root, "missing-root")); err == nil {
		t.Error("scan on missing root = nil error, want error")
	}
}

// modTime 取数据文件 mtime（路径由调用方按 resolveDataFilePath 的决议给定；
// 存量 v1 形态目录名是合法 session id → v2 名）。
func modTime(t *testing.T, path string) time.Time {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.ModTime()
}

func idsOf(digests []SessionDigest) []string {
	out := make([]string, 0, len(digests))
	for _, d := range digests {
		out = append(out, d.Metadata.SessionID)
	}
	return out
}
