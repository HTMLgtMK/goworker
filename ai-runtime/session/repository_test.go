package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

// repositoryRootDirs 列出根目录下的直接子目录名（用于断言目录布局与
// 「目录名 == 会话 id」不变量）。
func repositoryRootDirs(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", root, err)
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	return dirs
}

// listContains 判断清单里是否含指定 session id。
func listContains(list []Metadata, id string) bool {
	for _, m := range list {
		if m.SessionID == id {
			return true
		}
	}
	return false
}

// TestRepositoryCreateOpenListDelete 覆盖四操作往返：
// Create → List 含新 id（目录名 == 文件内 SessionID 的种入不变量）→
// Open 返回同一实例 → Delete 各分支（in-use / 未知 id / 成功删除）。
// 另以第二个 Repository 重新打开同一 root，验证身份与历史跨重启可恢复。
func TestRepositoryCreateOpenListDelete(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}

	// ── Create：目录名 == 会话 id ──
	s1, err := repo.Create(ctx, CreateOptions{Title: "first", CWD: "/tmp/wk"})
	if err != nil {
		t.Fatalf("Create s1: %v", err)
	}
	if s1.SessionID() == "" {
		t.Fatal("Create: empty session id")
	}
	dirs := repositoryRootDirs(t, root)
	if len(dirs) != 1 || dirs[0] != s1.SessionID() {
		t.Fatalf("Create: root dirs = %v, want exactly [%s]", dirs, s1.SessionID())
	}
	if _, err := s1.Commit(msgs("user", "hello repo")); err != nil {
		t.Fatalf("Commit s1: %v", err)
	}

	// ── List：Create 后即含新 id ──
	list, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || !listContains(list, s1.SessionID()) {
		t.Fatalf("List after Create = %v, want exactly [%s]", list, s1.SessionID())
	}

	// ── Open：同 id 返回同一实例（决策 3）──
	s2, err := repo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create s2: %v", err)
	}
	got, err := repo.Open(s2.SessionID())
	if err != nil {
		t.Fatalf("Open s2: %v", err)
	}
	if got != s2 {
		t.Fatal("Open returned a different *Store instance for the same id")
	}
	list, err = repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 || !listContains(list, s1.SessionID()) || !listContains(list, s2.SessionID()) {
		t.Fatalf("List = %v, want ids [%s %s]", list, s1.SessionID(), s2.SessionID())
	}

	// ── Delete 分支：in-use / 未知 id / 畸形 id ──
	if err := repo.Delete(ctx, s2.SessionID()); !errors.Is(err, ErrSessionInUse) {
		t.Fatalf("Delete open session: err = %v, want ErrSessionInUse", err)
	}
	if err := repo.Delete(ctx, "session_doesnotexist"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Delete unknown id: err = %v, want ErrSessionNotFound", err)
	}
	if err := repo.Delete(ctx, "../evil"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Delete traversal id: err = %v, want ErrSessionNotFound", err)
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("repo.Close: %v", err)
	}

	// ── 跨重启：新 Repository 从同一 root 恢复身份与历史 ──
	repo2, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("reopen OpenRepository: %v", err)
	}
	defer repo2.Close()

	list, err = repo2.List(ctx)
	if err != nil {
		t.Fatalf("List after reopen: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("List after reopen = %v, want 2 sessions", list)
	}
	reopened, err := repo2.Open(s1.SessionID())
	if err != nil {
		t.Fatalf("Open s1 after restart: %v", err)
	}
	// 种入不变量的关键断言：文件内 metadata.SessionID 与目录名（即原 id）一致。
	if reopened.SessionID() != s1.SessionID() {
		t.Fatalf("reopened SessionID = %s, want original %s", reopened.SessionID(), s1.SessionID())
	}
	view := reopened.ActiveView()
	if len(view) != 1 || view[0].Content != "hello repo" {
		t.Fatalf("ActiveView after restart = %v, want 1 msg 'hello repo'", view)
	}
	if _, err := reopened.Commit([]core.Message{commMsg("m_reopen1", "user", "second round")}); err != nil {
		t.Fatalf("Commit after restart: %v", err)
	}
	if view := reopened.ActiveView(); len(view) != 2 || view[1].Content != "second round" {
		t.Fatalf("ActiveView after re-commit = %v, want 2 msgs", view)
	}

	// ── Delete 成功分支：未被本 repo 打开过的会话整目录移除 ──
	if err := repo2.Delete(ctx, s2.SessionID()); err != nil {
		t.Fatalf("Delete s2: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, s2.SessionID())); !os.IsNotExist(err) {
		t.Fatalf("session dir still present after Delete: stat err = %v", err)
	}
	if _, err := repo2.Open(s2.SessionID()); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Open deleted session: err = %v, want ErrSessionNotFound", err)
	}
	if list, err := repo2.List(ctx); err != nil || len(list) != 1 || !listContains(list, s1.SessionID()) {
		t.Fatalf("List after Delete = %v (err %v), want only s1", list, err)
	}
}

// TestConcurrentStoreOpen 验证进程内实例唯一（决策 3）：并发 Open 同一 id N 次，
// 全部返回同一 *Store 指针（输家在双检中关闭多余句柄，不产生双重打开的文件）。
// 会话经第二个 Repository 落盘后关闭，保证被测 repo 走的是缓存未命中的慢路径。
func TestConcurrentStoreOpen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	// 经 tmpRepo 造一个已落盘但不在被测 repo 缓存中的会话。
	tmpRepo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("tmp OpenRepository: %v", err)
	}
	seeded, err := tmpRepo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("tmp Create: %v", err)
	}
	id := seeded.SessionID()
	if err := tmpRepo.Close(); err != nil {
		t.Fatalf("tmpRepo.Close: %v", err)
	}

	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	defer repo.Close()

	const n = 16
	var (
		wg     sync.WaitGroup
		stores = make([]*Store, n)
		errs   = make([]error, n)
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stores[i], errs[i] = repo.Open(id)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Open[%d]: %v", i, err)
		}
	}
	for i := 1; i < n; i++ {
		if stores[i] != stores[0] {
			t.Fatalf("concurrent Open returned %d distinct instances; want a single *Store", i+1)
		}
	}
	// 唯一实例仍可正常写入（单一句柄，链路未坏）。
	if _, err := stores[0].Commit(msgs("user", "after race")); err != nil {
		t.Fatalf("Commit after concurrent open: %v", err)
	}
}

// TestLegacyRootMount 验证 legacy 布局的兼容挂载（决策 2）：用 session.Open +
// Commit + Archive 在 root 自身造出 legacy 槽（root/current.jsonl + root/archive/），
// 之后 Open(legacy id) 原地挂载、List 含它且不含归档、Delete 被拒绝，
// 且 Archive 仍可在该槽内轮转（/new 语义保持）、同槽换 id 后仍复用同一实例。
func TestLegacyRootMount(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	// ── 预置 legacy 布局：root 自身就是会话槽 ──
	legacy, err := Open(root)
	if err != nil {
		t.Fatalf("legacy Open: %v", err)
	}
	preArchiveID := legacy.SessionID()
	if _, err := legacy.Commit(msgs("user", "legacy hello")); err != nil {
		t.Fatalf("legacy Commit: %v", err)
	}
	if err := legacy.Archive(); err != nil {
		t.Fatalf("legacy Archive: %v", err)
	}
	legacyID := legacy.SessionID() // Archive 后 current 槽的新 metadata id
	if legacyID == preArchiveID {
		t.Fatal("Archive should rotate to a new session id")
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("legacy Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "current.jsonl")); err != nil {
		t.Fatalf("legacy layout missing root/current.jsonl: %v", err)
	}
	archived, err := os.ReadDir(filepath.Join(root, "archive"))
	if err != nil || len(archived) != 1 {
		t.Fatalf("legacy layout: archive entries = %v (err %v), want 1 file", archived, err)
	}

	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	defer repo.Close()

	// ── List：含 legacy 槽当前 id；归档里的旧 id 不计入 ──
	list, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !listContains(list, legacyID) {
		t.Fatalf("List = %v, want legacy id %s", list, legacyID)
	}
	if listContains(list, preArchiveID) {
		t.Fatalf("List = %v must not include archived id %s", list, preArchiveID)
	}

	// ── Open：legacy id 原地挂载到 root，不产生 per-id 目录 ──
	mounted, err := repo.Open(legacyID)
	if err != nil {
		t.Fatalf("Open legacy id: %v", err)
	}
	if mounted.SessionID() != legacyID {
		t.Fatalf("mounted SessionID = %s, want %s", mounted.SessionID(), legacyID)
	}
	for _, name := range repositoryRootDirs(t, root) {
		if name == legacyID {
			t.Fatalf("legacy mount must not create per-id dir %s", legacyID)
		}
	}

	// ── Delete：挂载中 → in-use 优先；未挂载的 repo → legacy 槽不可删 ──
	if err := repo.Delete(ctx, legacyID); !errors.Is(err, ErrSessionInUse) {
		t.Fatalf("Delete mounted legacy: err = %v, want ErrSessionInUse", err)
	}
	repo2, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("second OpenRepository: %v", err)
	}
	if err := repo2.Delete(ctx, legacyID); !errors.Is(err, ErrLegacyImmutable) {
		t.Fatalf("Delete legacy slot: err = %v, want ErrLegacyImmutable", err)
	}
	if err := repo2.Close(); err != nil {
		t.Fatalf("repo2.Close: %v", err)
	}

	// ── Archive 仍在 legacy 槽内轮转；换 id 后同槽复用同一实例（同目录别名）──
	if err := mounted.Archive(); err != nil {
		t.Fatalf("Archive on mounted legacy: %v", err)
	}
	newID := mounted.SessionID()
	if newID == legacyID {
		t.Fatal("Archive on mounted legacy should rotate to a new session id")
	}
	list, err = repo.List(ctx)
	if err != nil {
		t.Fatalf("List after rotation: %v", err)
	}
	if !listContains(list, newID) || listContains(list, legacyID) {
		t.Fatalf("List after rotation = %v, want [%s] without old %s", list, newID, legacyID)
	}
	again, err := repo.Open(newID)
	if err != nil {
		t.Fatalf("Open rotated id: %v", err)
	}
	if again != mounted {
		t.Fatal("Open after legacy rotation returned a second instance for the same slot")
	}
	if _, err := mounted.Commit([]core.Message{commMsg("m_legacy2", "user", "post rotation")}); err != nil {
		t.Fatalf("Commit on mounted legacy after rotation: %v", err)
	}
}
