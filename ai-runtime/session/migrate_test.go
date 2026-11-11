package session

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

// 测试旧布局迁移、迁移后的数据不变量及幂等性。

// seedSessionDir 在 root 下手工种一个 per-id 会话目录：目录名 dirName，数据文件
// 落 fileName（v1 current.jsonl 或 v2 <dirName>.jsonl），metadata 身份为 metaID
// （测试可控目录名 ≠ 内部 id 的错位形态），附一条 user 消息使 head 非空。
// 返回数据文件路径。
func seedSessionDir(t *testing.T, root, dirName, fileName, metaID string) string {
	t.Helper()
	dir := filepath.Join(root, dirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	seed := `{"kind":"metadata","session_id":"` + metaID + `","created_at":"2024-01-01T00:00:00Z"}` + "\n" +
		`{"kind":"msg","message":{"role":"user","content":"迁移夹具","node_id":"n0"}}` + "\n"
	path := filepath.Join(dir, fileName)
	if err := os.WriteFile(path, []byte(seed), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// seedArchiveFile 复用 archiveview_test.go 的同名夹具（一次性 store 造数后把
// 数据文件字节搬进 root/archive/<name>，Store.Archive() 轮转时代的存量形态）。
// 本文件不再重复声明，调用处传 seed 闭包落一条 user 消息。

// archiveSeedID 返回归档夹具的 metadata 会话 id（迁移目标目录名的期望值）。
func archiveSeedID(t *testing.T, root, name string) string {
	t.Helper()
	m, ok := resolveSlotMetadata(filepath.Join(root, "archive", name))
	if !ok {
		t.Fatal("archive fixture has no identity")
	}
	return m.SessionID
}

// TestMigrateLegacyLayoutCoversAllFixtures 验证四类夹具一次迁齐：
// legacy 根槽（含 runs/ 随迁）、root 归档（archived 标记回归）、per-id v1 文件名、
// 目录名错位对齐、空壳删除；迁移后布局满足「目录名 == 内部 id == v2 文件名」
// 不变量，repo.Open / ScanSessionDigests 按新布局可达。
func TestMigrateLegacyLayoutCoversAllFixtures(t *testing.T) {
	root := t.TempDir()

	// ① legacy 根槽：真实 Open 造数（含一条 user 消息）+ runs/ journal 随迁验证。
	legacy, err := Open(root)
	if err != nil {
		t.Fatalf("open legacy slot: %v", err)
	}
	if _, err := legacy.Commit([]core.Message{{Role: "user", Content: "legacy 会话", MsgID: NewMsgID()}}); err != nil {
		t.Fatalf("commit legacy: %v", err)
	}
	legacyID := legacy.SessionID()
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "runs"), 0o700); err != nil {
		t.Fatalf("mkdir runs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "runs", "run-1.jsonl"), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write legacy run journal: %v", err)
	}

	// ② root 归档（两件：正常 + 不可寻址的坏件——后者按定案 #9 丢弃）。
	seedArchiveFile(t, root, "arch-1.jsonl", func(s *Store) {
		if _, err := s.Commit([]core.Message{{Role: "user", Content: "归档存量", MsgID: NewMsgID()}}); err != nil {
			t.Fatalf("commit archive seed: %v", err)
		}
	})
	archID := archiveSeedID(t, root, "arch-1.jsonl")
	if err := os.WriteFile(filepath.Join(root, "archive", "broken.jsonl"), []byte("not-json\n"), 0o600); err != nil {
		t.Fatalf("write broken archive: %v", err)
	}

	// ③ per-id v1：目录名合法、数据文件 current.jsonl。
	v1ID := NewSessionID()
	seedSessionDir(t, root, v1ID, "current.jsonl", v1ID)

	// ④ 目录名错位：目录 session_A、metadata id session_B。
	idA := NewSessionID()
	idB := NewSessionID()
	seedSessionDir(t, root, idA, idA+".jsonl", idB)

	// ⑤ 空壳：空数据文件目录 + 无数据文件的会话形态空目录（迁移崩溃残留）。
	shellID := NewSessionID()
	if err := os.MkdirAll(filepath.Join(root, shellID), 0o700); err != nil {
		t.Fatalf("mkdir shell: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, shellID, shellID+".jsonl"), nil, 0o600); err != nil {
		t.Fatalf("write empty shell: %v", err)
	}
	litterID := NewSessionID()
	if err := os.MkdirAll(filepath.Join(root, litterID), 0o700); err != nil {
		t.Fatalf("mkdir litter: %v", err)
	}
	// 非会话目录（runs 已在 ① 被种下、archive 已在 ②）不得被触碰。

	stats, err := MigrateLegacyLayout(root)
	if err != nil {
		t.Fatalf("MigrateLegacyLayout: %v", err)
	}
	// Aligned=3：v1 文件名对齐 ×1 + 错位目录的对齐（目录改名 + 数据文件 v2 名）×2。
	if stats.Legacy != 1 || stats.Archived != 1 || stats.Aligned != 3 || stats.Shells != 2 || stats.Dropped != 1 {
		t.Fatalf("stats = %+v, want legacy=1 archived=1 aligned=3 shells=2 dropped=1", stats)
	}

	// 布局不变量：目录名 == metadata id，数据文件为 v2 名；旧形态全部消失。
	for _, id := range []string{legacyID, archID, v1ID, idB} {
		if _, err := os.Stat(filepath.Join(root, id, id+".jsonl")); err != nil {
			t.Errorf("migrated session %s missing v2 data file: %v", id, err)
		}
	}
	for _, gone := range []string{
		filepath.Join(root, "current.jsonl"),
		filepath.Join(root, "archive"),
		filepath.Join(root, "runs"),
		filepath.Join(root, idA),
		filepath.Join(root, shellID),
		filepath.Join(root, litterID),
	} {
		if _, err := os.Stat(gone); !os.IsNotExist(err) {
			t.Errorf("stale layout entry %s still present (err=%v), want removed", gone, err)
		}
	}
	// runs/ journal 随 legacy 槽落位。
	if _, err := os.Stat(filepath.Join(root, legacyID, "runs", "run-1.jsonl")); err != nil {
		t.Errorf("legacy run journal not carried over: %v", err)
	}
	// archived 态回归：归档迁移件带 archived 标记；legacy/v1 迁移件不带。
	for id, wantArchived := range map[string]bool{archID: true, legacyID: false, v1ID: false, idB: false} {
		st, err := Open(filepath.Join(root, id))
		if err != nil {
			t.Fatalf("open migrated %s: %v", id, err)
		}
		if got := st.Archived(); got != wantArchived {
			t.Errorf("session %s archived = %v, want %v", id, got, wantArchived)
		}
		if got := st.SessionID(); got != id {
			t.Errorf("session %s metadata id = %q, want aligned", id, got)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close migrated %s: %v", id, err)
		}
	}
	// 迁移后 repo / 清单按新布局可达（决策 2 的 legacy 原地挂载退役）。
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	if _, err := repo.Open(legacyID); err != nil {
		t.Errorf("repo.Open(migrated legacy id) = %v, want per-id hit", err)
	}
	digests, err := ScanSessionDigests(root)
	if err != nil {
		t.Fatalf("ScanSessionDigests: %v", err)
	}
	if len(digests) != 4 {
		t.Errorf("digests = %d entries, want 4 (legacy + archived + v1 + aligned)", len(digests))
	}
	_ = repo.Close()
}

// TestMigrateLegacyLayoutIdempotent 验证中断重跑安全：首次迁移后再跑一遍，
// 摘要全零、布局零变化（rename-only 的幂等收敛——每一步对已迁移形态都是空操作）。
func TestMigrateLegacyLayoutIdempotent(t *testing.T) {
	root := t.TempDir()
	if _, err := Open(root); err != nil { // legacy 根槽造数
		t.Fatalf("open legacy: %v", err)
	}
	v1ID := NewSessionID()
	seedSessionDir(t, root, v1ID, "current.jsonl", v1ID)
	seedArchiveFile(t, root, "arch-1.jsonl", func(s *Store) {
		if _, err := s.Commit([]core.Message{{Role: "user", Content: "归档存量", MsgID: NewMsgID()}}); err != nil {
			t.Fatalf("commit archive seed: %v", err)
		}
	})

	listTree := func(t *testing.T) []string {
		t.Helper()
		var out []string
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			rel, rerr := filepath.Rel(root, path)
			if rerr != nil {
				return rerr
			}
			out = append(out, rel)
			return nil
		})
		if err != nil {
			t.Fatalf("walk: %v", err)
		}
		sort.Strings(out)
		return out
	}

	if _, err := MigrateLegacyLayout(root); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	before := listTree(t)

	stats, err := MigrateLegacyLayout(root)
	if err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if stats != (LayoutMigration{}) {
		t.Fatalf("second run stats = %+v, want all zero（已迁移形态对每一步都是空操作）", stats)
	}
	after := listTree(t)
	if !slices.Equal(before, after) {
		t.Fatalf("layout changed on second run:\nbefore=%v\nafter=%v", before, after)
	}
}

// TestMigrateLegacyLayoutConflictDropsSource 验证同 id 冲突的丢弃方向（定案 #9）：
// legacy 根槽的 metadata id 与既有 per-id 会话撞车时，根槽是重复方——连源清理，
// 在册 per-id 正本分毫不动（rename 覆盖会毁正本，宁丢重复）。
func TestMigrateLegacyLayoutConflictDropsSource(t *testing.T) {
	root := t.TempDir()
	dupID := NewSessionID()
	seedSessionDir(t, root, dupID, dupID+".jsonl", dupID) // 在册正本
	// 根槽种成同 id：手工写 current.jsonl（Open 会生成新随机 id，不可控）。
	slot := filepath.Join(root, "current.jsonl")
	seed := `{"kind":"metadata","session_id":"` + dupID + `","created_at":"2024-01-01T00:00:00Z"}` + "\n"
	if err := os.WriteFile(slot, []byte(seed), 0o600); err != nil {
		t.Fatalf("write conflict slot: %v", err)
	}

	stats, err := MigrateLegacyLayout(root)
	if err != nil {
		t.Fatalf("MigrateLegacyLayout: %v", err)
	}
	if stats.Dropped != 1 || stats.Legacy != 0 {
		t.Fatalf("stats = %+v, want dropped=1 legacy=0（冲突根槽丢弃）", stats)
	}
	if _, err := os.Stat(slot); !os.IsNotExist(err) {
		t.Errorf("conflict slot still present (err=%v), want removed", err)
	}
	if _, err := os.Stat(filepath.Join(root, dupID, dupID+".jsonl")); err != nil {
		t.Errorf("surviving session damaged: %v", err)
	}
}
