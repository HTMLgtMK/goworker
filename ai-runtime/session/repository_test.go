package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// ── Plan A1 / A2.1①：CWD 接通、v2 双探测、CloseSession 逐出 ──────────────

// TestRepositoryCreateDeclaresWorkspace 验证 Create 接通 CWD（Plan A1「创建即声明」）：
// CWD 非空 → workspace 记录落 v2 数据文件、Workspace() 可读、跨重启可恢复；
// CWD 为空 → 不落记录（「未声明」= 不写记录，DeclareWorkspace 注释论证）；
// Title 仍被忽略（决策 1）。
func TestRepositoryCreateDeclaresWorkspace(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}

	s, err := repo.Create(ctx, CreateOptions{Title: "ignored title", CWD: "/tmp/wk/x"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := s.Workspace(); got != "/tmp/wk/x" {
		t.Fatalf("Workspace after Create = %q, want /tmp/wk/x", got)
	}

	// workspace 记录落在 v2 数据文件里（Create 产出 session_<hex> 目录）
	v2Path := filepath.Join(root, s.SessionID(), s.SessionID()+".jsonl")
	lines := readJSONLFile(t, v2Path)
	if len(lines) < 2 {
		t.Fatalf("v2 file %s has %d lines, want >= 2 (metadata + workspace)", v2Path, len(lines))
	}
	var last Record
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		t.Fatalf("unmarshal last line: %v", err)
	}
	if last.Kind != kindWorkspace || last.Ws == nil || last.Ws.Path != "/tmp/wk/x" {
		t.Fatalf("last record = %+v, want workspace with path /tmp/wk/x", last)
	}

	// 空 CWD：不落 workspace 记录
	bare, err := repo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create without CWD: %v", err)
	}
	if got := bare.Workspace(); got != "" {
		t.Fatalf("Workspace without CWD = %q, want empty", got)
	}

	// 跨重启：workspace 声明从记录恢复（A7 设计点 5 清单 cwd 的取数前提）
	if err := repo.Close(); err != nil {
		t.Fatalf("repo.Close: %v", err)
	}
	repo2, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("reopen OpenRepository: %v", err)
	}
	defer repo2.Close()
	got, err := repo2.Open(s.SessionID())
	if err != nil {
		t.Fatalf("Open after restart: %v", err)
	}
	if got.Workspace() != "/tmp/wk/x" {
		t.Fatalf("Workspace after restart = %q, want /tmp/wk/x (记录持久)", got.Workspace())
	}
}

// TestRepositoryOpenListDualProbe 验证 Open/List 的数据文件双探测（Plan A1）：
// ① Create 产出 v2 名后重开 Repository，Open/List 均命中（v2 探测）；
// ② 手工造 <id>/current.jsonl 存量形态，Open/List 同样命中（兼容探测），且读路径不创建 v2 文件（不隐性迁移，改名归 A4）。
func TestRepositoryOpenListDualProbe(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	// 经 tmpRepo 落盘一个 v2 会话
	tmpRepo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("tmp OpenRepository: %v", err)
	}
	created, err := tmpRepo.Create(ctx, CreateOptions{CWD: "/tmp/wk/probe"})
	if err != nil {
		t.Fatalf("tmp Create: %v", err)
	}
	id := created.SessionID()
	if err := tmpRepo.Close(); err != nil {
		t.Fatalf("tmpRepo.Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, id, id+".jsonl")); err != nil {
		t.Fatalf("fixture: v2 file missing: %v", err)
	}

	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	defer repo.Close()

	// ① v2 探测
	list, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !listContains(list, id) {
		t.Fatalf("List = %v, want v2 session %s", list, id)
	}
	got, err := repo.Open(id)
	if err != nil {
		t.Fatalf("Open v2 session: %v", err)
	}
	if got.SessionID() != id || got.Workspace() != "/tmp/wk/probe" {
		t.Fatalf("Open v2 session = {id %s ws %q}, want {id %s ws /tmp/wk/probe}",
			got.SessionID(), got.Workspace(), id)
	}

	// ② 存量 current.jsonl 兼容探测
	legacyID := NewSessionID()
	legacyDir := filepath.Join(root, legacyID)
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatalf("mkdir legacy dir: %v", err)
	}
	seed := fmt.Sprintf(`{"kind":"metadata","session_id":%q,"created_at":"2024-01-01T00:00:00Z"}`+"\n", legacyID)
	if err := os.WriteFile(filepath.Join(legacyDir, "current.jsonl"), []byte(seed), 0o600); err != nil {
		t.Fatalf("write legacy current.jsonl: %v", err)
	}
	list, err = repo.List(ctx)
	if err != nil {
		t.Fatalf("List after legacy dir: %v", err)
	}
	if !listContains(list, legacyID) {
		t.Fatalf("List = %v, want legacy-slot session %s", list, legacyID)
	}
	mounted, err := repo.Open(legacyID)
	if err != nil {
		t.Fatalf("Open legacy-slot session: %v", err)
	}
	if mounted.SessionID() != legacyID {
		t.Fatalf("Open legacy slot: SessionID = %q, want %q", mounted.SessionID(), legacyID)
	}
	if _, err := os.Stat(filepath.Join(legacyDir, legacyID+".jsonl")); !os.IsNotExist(err) {
		t.Errorf("compat probe must not create the v2 file (rename 归 A4): stat err = %v", err)
	}
}

// TestRepositoryCloseSessionReopen 验证 A2.1① 死实例修复：
// CloseSession 关闭实例并逐出缓存；再次 Open 走 flock 慢路径拿到活的新实例
// （锁已释放即可重开——session/load 重开需要的语义）；旧实例已 Close 不可写；
// 对不在缓存中的 id 幂等返回 nil。
func TestRepositoryCloseSessionReopen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	// 经 tmpRepo 落盘会话，保证被测 repo 的首次 Open 走慢路径
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

	s1, err := repo.Open(id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s2, err := repo.Open(id); err != nil || s2 != s1 {
		t.Fatalf("second Open = (%p, %v), want same instance %p", s2, err, s1)
	}

	// 幂等：不在缓存中的 id 返回 nil（调用方无需先探测）
	if err := repo.CloseSession("session_00000000000000000000000000000000"); err != nil {
		t.Fatalf("CloseSession(unknown id) = %v, want nil", err)
	}

	// 逐出 + 关闭
	if err := repo.CloseSession(id); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	// 再次逐出同样幂等（id 已不在缓存）
	if err := repo.CloseSession(id); err != nil {
		t.Fatalf("second CloseSession = %v, want nil (idempotent)", err)
	}
	// 旧实例已关闭：不再可写（句柄已释放）
	if _, err := s1.Commit(msgs("user", "dead write")); err == nil {
		t.Error("Commit on closed instance = nil error, want error")
	}

	// 重开：新实例、活实例（flock 已释放，慢路径可直接重开）
	s3, err := repo.Open(id)
	if err != nil {
		t.Fatalf("Open after CloseSession: %v", err)
	}
	if s3 == s1 {
		t.Fatal("Open after CloseSession returned the closed instance")
	}
	if _, err := s3.Commit(msgs("user", "alive again")); err != nil {
		t.Fatalf("Commit on reopened store: %v", err)
	}
	if view := s3.ActiveView(); len(view) != 1 || view[0].Content != "alive again" {
		t.Fatalf("ActiveView on reopened store = %v, want ['alive again']", viewContent(view))
	}
}

// TestRepositoryCloseSessionEvictsAliasEntries 验证别名条目一并逐出：
// legacy 槽轮转换 id 后同一 *Store 挂在多个 id 下（Open 的同目录别名机制），
// CloseSession(任一 id) 必须把全部别名条目逐出并只关闭一次——留任一条目都会
// 让该 id 的 Open 命中已关闭实例，死实例问题换个 id 复发。
func TestRepositoryCloseSessionEvictsAliasEntries(t *testing.T) {
	root := t.TempDir()

	// 造 legacy 槽夹具：root 自身即会话槽（同 TestLegacyRootMount）
	legacy, err := Open(root)
	if err != nil {
		t.Fatalf("legacy Open: %v", err)
	}
	if _, err := legacy.Commit(msgs("user", "alias fixture")); err != nil {
		t.Fatalf("legacy Commit: %v", err)
	}
	if err := legacy.Archive(); err != nil {
		t.Fatalf("legacy Archive: %v", err)
	}
	legacyID := legacy.SessionID()
	if err := legacy.Close(); err != nil {
		t.Fatalf("legacy Close: %v", err)
	}

	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	defer repo.Close()

	mounted, err := repo.Open(legacyID)
	if err != nil {
		t.Fatalf("Open legacy slot: %v", err)
	}
	// 槽内再轮转一次换 id → Open(新 id) 走别名机制复用同一实例
	if err := mounted.Archive(); err != nil {
		t.Fatalf("Archive on mounted legacy: %v", err)
	}
	newID := mounted.SessionID()
	again, err := repo.Open(newID)
	if err != nil {
		t.Fatalf("Open rotated id: %v", err)
	}
	if again != mounted {
		t.Fatal("fixture broken: rotated id did not reuse the mounted instance (alias)")
	}

	// CloseSession(旧 id) → 实例关闭、{旧 id, 新 id} 两个条目全部逐出
	if err := repo.CloseSession(legacyID); err != nil {
		t.Fatalf("CloseSession(alias id): %v", err)
	}
	if _, err := mounted.Commit(msgs("user", "dead alias")); err == nil {
		t.Error("Commit on closed alias instance = nil error, want error")
	}

	// 重开：新实例、活实例（legacy 根槽 current.jsonl 重新挂载）
	fresh, err := repo.Open(newID)
	if err != nil {
		t.Fatalf("Open newID after CloseSession: %v", err)
	}
	if fresh == mounted {
		t.Fatal("Open after CloseSession returned the closed alias instance")
	}
	if _, err := fresh.Commit(msgs("user", "fresh mount")); err != nil {
		t.Fatalf("Commit on fresh mount: %v", err)
	}
}
