// Repository manages session stores and per-run journals. Stores are cached by session ID;
// run journals use the Store's directory lock for cross-process exclusion.
package session

import (
	"context"
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
	ErrSessionNotFound = errors.New("session: session not found")
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

// CreateOptions 是 Create 的选项。Title 仅作接口占位：本层不持久化
// （不新增 metadata.json，避免第二个身份事实源）——标题由清单方按需扫描
// 首条 user 消息得出。CWD 已接通：cwd 兼任 workspace，
// Create 以 workspace 记录「创建即声明」持久化到会话 jsonl。
type CreateOptions struct {
	Title string
	CWD   string
}

// Repository 管理一个会话根目录下的全部会话存储。
type Repository struct {
	root string // 会话根目录，即 cfg.Session.Dir（config.go:190 默认 <config>/sessions）

	mu   sync.Mutex
	open map[string]*Store // 实例唯一缓存：同 id 重复 Open 返回同一 *Store
}

// OpenRepository 打开会话根目录仓库。目录不存在则创建（对齐 store.Open 的
// MkdirAll + Chmod，store.go:52-57），权限收紧到 0700。
func OpenRepository(root string) (*Repository, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("session: create repo root %s: %w", root, err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("session: restrict repo root %s: %w", root, err)
	}
	return &Repository{root: root, open: make(map[string]*Store)}, nil
}

// Create 新建一个会话：id = session.NewSessionID()，确认 <root>/<id>/ 不存在后
// 种入首行 metadata（保证目录名 == 文件内 SessionID，见文件头「附加不变量」），
// CWD 非空时追加 workspace 记录（「创建即声明」），再 Open 该目录并进
// 缓存。返回的 Store 的 SessionID() 即新会话 id。
func (r *Repository) Create(ctx context.Context, opts CreateOptions) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Title is ignored; an empty CWD leaves the workspace undeclared.
	// The host may inject a temporary execution directory without persisting it.
	_ = opts.Title

	id := NewSessionID()
	dir := filepath.Join(r.root, id)
	if _, err := os.Stat(dir); err == nil {
		// 128-bit 随机 id 撞名概率可忽略；真撞了让调用方重试一次 Create 即可。
		return nil, fmt.Errorf("session: create %s: dir already exists", dir)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("session: create %s: %w", dir, err)
	}

	if err := seedSessionMetadata(dir, id); err != nil {
		return nil, err
	}
	s, err := Open(dir)
	if err != nil {
		return nil, err
	}
	if opts.CWD != "" {
		if err := s.DeclareWorkspace(opts.CWD); err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("session: create %s: %w", id, err)
		}
	}

	// 防御性双检：全新随机 id 正常不会已有实例；万一命中以先注册者为准。
	r.mu.Lock()
	existing, ok := r.open[id]
	if !ok {
		r.open[id] = s
	}
	r.mu.Unlock()
	if ok {
		closeLoser(s)
		return existing, nil
	}
	return s, nil
}

// Open resolves an ID to a session directory or the legacy root slot, then caches
// the Store. The repository lock serializes cache lookup and Store.Open, avoiding
// duplicate in-process locks; directory aliases reuse the existing Store, while
// another process is rejected by the Store's non-blocking flock.
func (r *Repository) Open(id string) (*Store, error) {
	if !validSessionID(id) {
		return nil, fmt.Errorf("session: open %q: %w", id, ErrSessionNotFound)
	}

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
		if !e.IsDir() {
			continue
		}
		if m, ok := resolveSlotMetadata(resolveDataFilePath(filepath.Join(r.root, e.Name()))); ok {
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

	r.mu.Lock()
	_, inUse := r.open[id]
	r.mu.Unlock()
	if inUse {
		return fmt.Errorf("session: delete %s: %w", id, ErrSessionInUse)
	}

	dir := filepath.Join(r.root, id)
	if _, err := os.Stat(dir); err == nil {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("session: delete %s: %w", id, err)
		}
		return nil
	}
	if m, ok := resolveSlotMetadata(filepath.Join(r.root, "current.jsonl")); ok && m.SessionID == id {
		return fmt.Errorf("session: delete %s: %w", id, ErrLegacyImmutable)
	}
	return fmt.Errorf("session: delete %s: %w", id, ErrSessionNotFound)
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
func seedSessionMetadata(dir, id string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("session: create dir %s: %w", dir, err)
	}
	data, err := json.Marshal(newMetadataRecord(id, time.Now()))
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
