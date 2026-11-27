// Repository manages session stores and per-run journals. Stores are cached by session ID;
// run journals use the Store's directory lock for cross-process exclusion.
package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Repository 级 sentinel 错误。调用方用 errors.Is 判定。
var (
	// ErrSessionNotFound：id 无法解析到任何会话（目录与 legacy 槽都不命中，或 id 非法）。
	ErrSessionNotFound      = errors.New("session: session not found")
	ErrSessionAlreadyExists = errors.New("session: session already exists")
	// ErrLockUnavailable signals that this platform cannot provide safe file locking.
	ErrLockUnavailable = errors.New("session: cross-process locking unavailable")
	// ErrSessionInUse：id 的 Store 仍在本 Repository 的 open 缓存中，拒绝删除。
	ErrSessionInUse = errors.New("session: session in use")
	// ErrLegacyImmutable prevents deleting the only copy of a legacy root session.
	ErrLegacyImmutable = errors.New("session: legacy root session is immutable")
	// ErrSessionNotOpen：custody 前置不满足——该会话的 Store 不在本 Repository
	// 的 open 缓存中。RunJournal 依附 Store in-cache（journal.go 文件头论证，
	// 不新增锁面），开不了 Store 就开不了 Journal。
	ErrSessionNotOpen = errors.New("session: session store not open")
	// ErrRunExists：同 runID 的 journal 文件已存在（O_EXCL 创建被拒）。RunID 的
	// RunID 跨重启碰撞由它兜底——变成可观测错误而非静默覆盖（run.go:100-104）。
	ErrRunExists = errors.New("session: run journal already exists")
)

// CreateOptions 是 Create 的选项。Title 仅作接口占位：本层不持久化标题，
// 标题由清单方按需扫描首条 user 消息得出。CWD 以 workspace 记录持久化；
// Metadata 是可选 ai-session 元数据，写在同一 JSONL metadata 行。
type CreateOptions struct {
	Title     string
	CWD       string
	CreatedAt time.Time
	UpdatedAt time.Time
	Metadata  map[string]string
}

// Repository 管理一个会话根目录下的全部会话存储。
type Repository struct {
	root string // 会话根目录，即 cfg.Session.Dir（config.go:190 默认 <config>/sessions）

	mu   sync.Mutex
	open map[string]*Store // 实例唯一缓存：同 id 重复 Open 返回同一 *Store
}

// OpenRepository 打开会话根目录仓库。目录不存在则创建（对齐 store.Open 的
// MkdirAll + Chmod，store.go:52-57），权限收紧到 0700。平台未提供锁实现时，
// 在触碰持久目录前失败关闭。
func OpenRepository(root string) (*Repository, error) {
	if err := requireLockSupport(); err != nil {
		return nil, fmt.Errorf("session: open repository: %w", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("session: create repo root %s: %w", root, err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("session: restrict repo root %s: %w", root, err)
	}
	return &Repository{root: root, open: make(map[string]*Store)}, nil
}

// lockSessionID acquires a stable repository-level lock for id. The lock directory lives
// outside the removable session directory, so failed creates and deletes never unlink a
// lock inode another Repository may already be waiting on.
func (r *Repository) lockSessionID(id string) (func(), error) {
	return r.lockSessionIDContext(context.Background(), id)
}

func (r *Repository) lockSessionIDContext(ctx context.Context, id string) (func(), error) {
	if ctx == nil {
		return nil, errors.New("session: nil lock context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lockRoot := filepath.Join(r.root, ".repository-locks")
	if err := os.MkdirAll(lockRoot, 0o700); err != nil {
		return nil, fmt.Errorf("session: create repository lock dir: %w", err)
	}
	hash := sha256.Sum256([]byte(id))
	lockDir := filepath.Join(lockRoot, hex.EncodeToString(hash[:]))
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		return nil, fmt.Errorf("session: create lock for %q: %w", id, err)
	}
	lock, err := acquireDirLockContext(ctx, lockDir)
	if err != nil {
		return nil, fmt.Errorf("session: lock repository ID %q: %w", id, err)
	}
	return func() {
		if err := releaseDirLock(lock); err != nil {
			slog.Warn("session: release repository ID lock failed", "id", id, "err", err)
		}
	}, nil
}

// Create 新建一个随机 ID 会话。指定 ID 的事务语义由 CreateWithID 统一实现。
func (r *Repository) Create(ctx context.Context, opts CreateOptions) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return r.CreateWithID(ctx, NewSessionID(), opts)
}

// CreateWithID persists one session at exactly id. An exclusive repository-level ID lock
// coordinates publication and rollback with Open, List, and Delete across Repository instances.
// CreateOptions.CWD is already validated by the caller; Metadata["cwd"] remains opaque unless
// the caller explicitly supplies CWD. The repository does not authorize workspace paths.
func (r *Repository) CreateWithID(ctx context.Context, id string, opts CreateOptions) (*Store, error) {
	return r.createWithID(ctx, id, opts, Open)
}

func (r *Repository) createWithID(ctx context.Context, id string, opts CreateOptions, openStore func(string) (*Store, error)) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validSessionID(id) {
		return nil, fmt.Errorf("session: create %q: %w", id, ErrSessionNotFound)
	}
	if opts.CreatedAt.IsZero() {
		opts.CreatedAt = time.Now().UTC()
	}
	if opts.UpdatedAt.IsZero() {
		opts.UpdatedAt = opts.CreatedAt
	}
	values := cloneStringMap(opts.Metadata)
	cwd := opts.CWD
	if cwd != "" {
		if values == nil {
			values = make(map[string]string)
		}
		values[aiSessionCWDMetadataKey] = cwd
	}
	_ = opts.Title // Title is derived from conversation content, not persisted as identity metadata.

	unlockID, err := r.lockSessionIDContext(ctx, id)
	if err != nil {
		return nil, err
	}
	defer unlockID()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, exists := r.open[id]; exists {
		return nil, fmt.Errorf("session: create %s: %w", id, ErrSessionAlreadyExists)
	}
	if m, ok := resolveSlotMetadata(filepath.Join(r.root, "current.jsonl")); ok && m.SessionID == id {
		return nil, fmt.Errorf("session: create %s: %w", id, ErrSessionAlreadyExists)
	}

	dir := filepath.Join(r.root, id)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("session: create %s: %w", dir, ErrSessionAlreadyExists)
		}
		return nil, fmt.Errorf("session: create dir %s: %w", dir, err)
	}
	created := true
	var store *Store
	cleanup := func(cause error) (*Store, error) {
		if store != nil {
			if closeErr := store.Close(); closeErr != nil {
				cause = errors.Join(cause, fmt.Errorf("session: close failed create %s: %w", id, closeErr))
			}
		}
		if created {
			if removeErr := os.RemoveAll(dir); removeErr != nil {
				cause = errors.Join(cause, fmt.Errorf("session: clean failed create %s: %w", id, removeErr))
			}
		}
		return nil, cause
	}

	if err := ctx.Err(); err != nil {
		return cleanup(err)
	}
	if err := seedSessionMetadata(dir, id, opts.CreatedAt, opts.UpdatedAt, values); err != nil {
		return cleanup(err)
	}
	if err := ctx.Err(); err != nil {
		return cleanup(err)
	}
	store, err = openStore(dir)
	if err != nil {
		return cleanup(err)
	}
	if err := ctx.Err(); err != nil {
		return cleanup(err)
	}
	if cwd != "" {
		if err := store.DeclareWorkspace(cwd); err != nil {
			return cleanup(fmt.Errorf("session: create %s: %w", id, err))
		}
	}
	if err := ctx.Err(); err != nil {
		return cleanup(err)
	}

	r.open[id] = store
	created = false
	return store, nil
}

// Open resolves an ID to a session directory or the legacy root slot, then caches
// the Store. The repository lock serializes cache lookup and Store.Open, avoiding
// duplicate in-process locks; directory aliases reuse the existing Store, while
// another process is rejected by the Store's non-blocking flock.
func (r *Repository) Open(id string) (*Store, error) {
	if !validSessionID(id) {
		return nil, fmt.Errorf("session: open %q: %w", id, ErrSessionNotFound)
	}
	unlockID, err := r.lockSessionID(id)
	if err != nil {
		return nil, err
	}
	defer unlockID()

	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.open[id]; ok {
		return s, nil
	}

	// 解析 id → 目标目录（数据文件双探测：先 v2 名后 current.jsonl）
	target := ""
	dir := filepath.Join(r.root, id)
	if _, err := os.Stat(resolveDataFilePath(dir)); err == nil {
		target = dir
	} else if m, ok := resolveSlotMetadata(filepath.Join(r.root, "current.jsonl")); ok && m.SessionID == id {
		target = r.root // legacy 根槽原地挂载（根槽恒为 current.jsonl，不做 v2 探测）
	}
	if target == "" {
		return nil, fmt.Errorf("session: open %s: %w", id, ErrSessionNotFound)
	}

	// 同目录别名：目标目录已被本进程以其他 id 挂载（存量「目录名 ≠
	// 内部 id」形态的复用路径，见函数头注释）。必须先于 Store.Open。
	for _, existing := range r.open {
		if existing.dir == target {
			r.open[id] = existing
			return existing, nil
		}
	}

	s, err := Open(target)
	if err != nil {
		return nil, err
	}
	r.open[id] = s
	return s, nil
}

// Peek returns the exact cached Store without opening files or rebuilding instances.
// Unlike Open, a cache miss is a plain false result; this prevents stale runtimes from
// resurrecting a closed Store and reacquiring its directory flock.
func (r *Repository) Peek(id string) (*Store, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.open[id]
	return s, ok
}

// List scans per-ID directories and the legacy root slot using the shared v2/v1 path and
// identity rules. It excludes archive files, resolves legacy IDs from metadata or records,
// and returns entries in os.ReadDir filename order.
func (r *Repository) List(ctx context.Context) ([]Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(r.root)
	if err != nil {
		return nil, fmt.Errorf("session: list %s: %w", r.root, err)
	}
	var out []Metadata
	for _, e := range entries {
		if !e.IsDir() || e.Name() == ".repository-locks" || e.Name() == ".repository-tombstones" {
			continue
		}
		unlockID, err := r.lockSessionIDContext(ctx, e.Name())
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			unlockID()
			return nil, err
		}
		m, ok := resolveSlotMetadata(resolveDataFilePath(filepath.Join(r.root, e.Name())))
		unlockID()
		if ok {
			out = append(out, m)
		}
	}
	// legacy 根槽：root 自身的 current.jsonl 也是一个活跃会话。
	if m, ok := resolveSlotMetadata(filepath.Join(r.root, "current.jsonl")); ok {
		out = append(out, m)
	}
	return out, nil
}

// Delete 删除会话。判定顺序：
//  1. open 缓存中存在 → ErrSessionInUse（打开着的 Store 不允许删文件）；
//  2. 存在 <root>/<id> → os.RemoveAll 整目录（含其 archive）；
//  3. legacy 根槽命中 id → ErrLegacyImmutable（不移动也不销毁 legacy 数据）；
//  4. 否则 ErrSessionNotFound。
func (r *Repository) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validSessionID(id) {
		return fmt.Errorf("session: delete %q: %w", id, ErrSessionNotFound)
	}
	unlockID, err := r.lockSessionIDContext(ctx, id)
	if err != nil {
		return err
	}
	defer unlockID()
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	_, inUse := r.open[id]
	r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if inUse {
		return fmt.Errorf("session: delete %s: %w", id, ErrSessionInUse)
	}

	dir := filepath.Join(r.root, id)
	if _, err := os.Stat(dir); err == nil {
		storeLock, err := acquireDirLock(dir)
		if errors.Is(err, ErrStoreLocked) {
			return fmt.Errorf("session: delete %s: %w", id, ErrSessionInUse)
		}
		if err != nil {
			return fmt.Errorf("session: lock before delete %s: %w", id, err)
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, releaseDirLock(storeLock))
		}

		tombstoneRoot := filepath.Join(r.root, ".repository-tombstones")
		if err := os.MkdirAll(tombstoneRoot, 0o700); err != nil {
			_ = releaseDirLock(storeLock)
			return fmt.Errorf("session: create tombstone dir: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, releaseDirLock(storeLock))
		}
		tombstone, err := os.MkdirTemp(tombstoneRoot, "deleted-")
		if err != nil {
			_ = releaseDirLock(storeLock)
			return fmt.Errorf("session: reserve delete tombstone: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(
				err,
				releaseDirLock(storeLock),
				wrapRepositoryCleanupError("remove unused delete tombstone", os.RemoveAll(tombstone)),
			)
		}
		retired := filepath.Join(tombstone, "store")
		if err := os.Rename(dir, retired); err != nil {
			_ = releaseDirLock(storeLock)
			_ = os.RemoveAll(tombstone)
			return fmt.Errorf("session: detach session %s for delete: %w", id, err)
		}
		unlockStoreErr := releaseDirLock(storeLock)
		removeErr := os.RemoveAll(tombstone)
		return errors.Join(
			wrapRepositoryCleanupError("release deleted store lock", unlockStoreErr),
			wrapRepositoryCleanupError("remove deleted store", removeErr),
		)
	}
	if m, ok := resolveSlotMetadata(filepath.Join(r.root, "current.jsonl")); ok && m.SessionID == id {
		return fmt.Errorf("session: delete %s: %w", id, ErrLegacyImmutable)
	}
	return fmt.Errorf("session: delete %s: %w", id, ErrSessionNotFound)
}

func wrapRepositoryCleanupError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("session: %s: %w", operation, err)
}

// Close 关闭 open 缓存中的全部 Store（逐个 Store.Close，store.go:134-148）并清空
// 缓存。错误以 errors.Join 聚合返回；Close 后本 Repository 不再持有任何实例。
func (r *Repository) Close() error {
	r.mu.Lock()
	stores := make([]*Store, 0, len(r.open))
	for _, s := range r.open {
		stores = append(stores, s)
	}
	r.open = make(map[string]*Store)
	r.mu.Unlock()

	var errs []error
	for _, s := range stores {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// CloseSession 关闭并逐出 id 对应的 Store。以 Store 指针匹配并一并移除别名，避免
// 任一缓存键继续引用已关闭实例。先在 r.mu 内逐出，再在锁外关闭 Store，避免持有
// 仓库锁进行文件 I/O；逐出与 Close 之间的并发 Open 可能因旧实例仍持锁而返回
// ErrStoreLocked，后续重试即可。缓存未命中时幂等返回 nil。
func (r *Repository) CloseSession(id string) error {
	r.mu.Lock()
	s, ok := r.open[id]
	if ok {
		for key, cached := range r.open {
			if cached == s { // 指针比较：同目录别名的全部条目一并逐出
				delete(r.open, key)
			}
		}
	}
	r.mu.Unlock()
	if !ok {
		return nil
	}

	if err := s.Close(); err != nil {
		return fmt.Errorf("session: close session %s: %w", id, err)
	}
	return nil
}

// ── 内部辅助 ──────────────────────────────────────────────────────────

// seedSessionMetadata creates the directory and writes its first metadata row, fixing
// the logical ID to the directory name. It uses the v2 filename for session-shaped
// directories and the legacy filename otherwise; JSONL bytes and 0600 permissions match
// Store.appendRecord so Store.Open can take over safely.
func seedSessionMetadata(dir, id string, createdAt, updatedAt time.Time, values map[string]string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("session: create dir %s: %w", dir, err)
	}
	data, err := json.Marshal(newMetadataRecord(id, createdAt, updatedAt, values))
	if err != nil {
		return fmt.Errorf("session: marshal seed metadata: %w", err)
	}
	data = append(data, '\n')
	path := filepath.Join(dir, newDataFileName(dir))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("session: seed metadata %s: %w", path, err)
	}
	return nil
}

// resolveSlotMetadata resolves a slot's ID using the same precedence as Store.Open and
// OpenArchiveView: first valid metadata, then a legacy ID from the first node or content
// hash. Empty or missing files are unaddressable. CreatedAt comes from metadata, or the
// earliest record for legacy data.
func resolveSlotMetadata(path string) (Metadata, bool) {
	recs, _, err := loadRecords(path)
	if err != nil {
		return Metadata{}, false
	}
	if m, ok := metadataFromRecords(recs); ok && m.SessionID != "" {
		m.Workspace = workspaceFromRecords(recs)
		return m, true
	}
	content, readErr := os.ReadFile(path)
	if readErr != nil {
		return Metadata{}, false // loadRecords 已成功过，读到失败说明文件正被并发移走
	}
	firstID := firstNodeID(recs)
	if firstID == "" && len(content) == 0 {
		return Metadata{}, false
	}
	return Metadata{
		SessionID: legacySessionID(firstID, content),
		CreatedAt: timeFromRecords(recs),
		UpdatedAt: timeFromRecords(recs),
		Workspace: workspaceFromRecords(recs),
	}, true
}

// validSessionID 拒绝空 id、相对引用与含路径分隔符的 id。Open/Delete 都以
// <root>/<id> 拼路径，Delete 更会 os.RemoveAll——恶意/畸形 id 必须在拼路径前卡住。
// 合法 id 实际只会是 session_<hex> / legacy_<hex>（id.go），此校验是防御性边界。
func validSessionID(id string) bool {
	if id == "" || id == "." || id == ".." {
		return false
	}
	if strings.ContainsRune(id, '/') || strings.ContainsRune(id, '\\') ||
		strings.ContainsRune(id, filepath.Separator) {
		return false
	}
	return id == filepath.Base(id) && id == filepath.Clean(id)
}

// closeLoser 关闭并发竞争中的输家实例。Close 失败只 warn（文件句柄泄漏属可观测
// 的次要问题，不影响胜者的正确性）。
func closeLoser(s *Store) {
	if err := s.Close(); err != nil {
		slog.Warn("session: discard duplicate store instance failed", "err", err)
	}
}
