// repository.go 实现 SessionRepository：在 Store 之上提供多会话的四操作
// （Create/Open/List/Delete）与进程内实例唯一缓存。纯 SDK 层，不接线 daemon（P1-3 再接）。
//
// 目录布局：
//
//	<root>/<sessionID>/current.jsonl        新布局（P1 起创建，目录名即会话 id）
//	<root>/<sessionID>/runs/<runID>.jsonl   run 状态 journal（P2-2 起，每 run 一文件；legacy 槽落 <root>/runs/）
//	<root>/current.jsonl + <root>/archive/  legacy 布局（现有用户数据，只读兼容挂载）
//
// 三个关键决策（评审点，对应实施计划 §3 P1-1）：
//
//  1. 不新增 metadata.json：会话身份所需字段已有事实源——ID/CreatedAt 是 jsonl 内
//     不可变 metadata 记录（store.go:30-33，一次写入永不改写），Title 可由首条 user
//     消息扫描得出（daemon sessions.go:436-461 的 firstUserTitle），CWD 属会话运行态
//     （P1-4 再与 Session 绑定）。P1 拒绝引入第二个身份事实源：目录发现即索引——
//     <root> 下扫一遍就是全部活跃会话，不存在「目录在但索引丢了」或反向的漂移。
//     这比总方案第九节的 metadata.json 更保守，是有意为之。
//
//  2. 不移动 legacy 文件：<root>/current.jsonl 原地挂载为 legacy 会话（Open 命中
//     legacy 分支时直接 session.Open(root)），/new 在其上仍走 Store.Archive() 在该槽
//     内轮转（current.jsonl → root/archive/<ts>.jsonl），直到该槽被自然淘汰；新会话
//     一律进 <root>/<id>/。迁移用户数据不在 P1 范围内，原地挂载保证老用户升级后
//     历史无损、行为不变。
//
//  3. 实例唯一：Repository 对同一 SessionID 恒返回同一 *Store（总方案第九节
//     「Repository 对同一 SessionID 保证唯一的 Store 实例」），避免多次 Open 产生
//     不同文件句柄共同写同一文件。open 缓存即唯一事实源；跨进程互斥不在此层，
//     由 P1-2 的文件锁（flock + ErrStoreLocked）兜底。
//
// 附加不变量（由 Create 保证）：新布局下目录名 == 文件内 metadata.SessionID。
// Store.Open 对全新空文件会自行生成随机 id（store.go:92-94），与目录名无关；因此
// Create 在调用 Open 前先种下首行 metadata 记录（见 seedSessionMetadata），否则
// List（读文件 metadata）与 Open（按目录名解析）在跨进程重启后会对不上号。
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
	// ErrLegacyImmutable：legacy 根槽（<root>/current.jsonl）不可删除——它是现有
	// 用户数据的唯一活跃文件，删除等于毁历史；淘汰靠 Archive 轮转（决策 2）。
	ErrLegacyImmutable = errors.New("session: legacy root session is immutable")
	// ErrSessionNotOpen：custody 前置不满足——该会话的 Store 不在本 Repository
	// 的 open 缓存中。RunJournal 依附 Store in-cache（journal.go 文件头论证，
	// 勘察修正 C：不新增锁面），开不了 Store 就开不了 Journal。
	ErrSessionNotOpen = errors.New("session: session store not open")
	// ErrRunExists：同 runID 的 journal 文件已存在（O_EXCL 创建被拒）。P2-1 的
	// RunID 跨重启碰撞由它兜底——变成可观测错误而非静默覆盖（run.go:100-104）。
	ErrRunExists = errors.New("session: run journal already exists")
)

// CreateOptions 是 Create 的选项。Title/CWD 按 P1-1 计划仅作接口占位：本层不持久化
// （决策 1：不新增 metadata.json，避免第二个身份事实源）——Title 由清单方按需扫描
// 首条 user 消息得出，CWD 在 P1-4 作为会话运行态绑定。字段被接受但当前会被忽略。
type CreateOptions struct {
	Title string
	CWD   string
}

// Repository 管理一个会话根目录下的全部会话存储。见文件头注释的目录布局与三个决策。
type Repository struct {
	root string // 会话根目录，即 cfg.Session.Dir（config.go:190 默认 <config>/sessions）

	mu   sync.Mutex
	open map[string]*Store // 实例唯一缓存：同 id 重复 Open 返回同一 *Store（决策 3）
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
// 再 Open 该目录并进缓存。返回的 Store 的 SessionID() 即新会话 id。
func (r *Repository) Create(ctx context.Context, opts CreateOptions) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// opts.Title/CWD 在 P1 被有意忽略（决策 1），_ = opts 显式表明这不是遗漏。
	_ = opts

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

	// 防御性双检：全新随机 id 正常不会已有实例；万一命中以先注册者为准（决策 3）。
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

// Open 按 id 打开会话。解析顺序（未命中缓存时）：
//  1. <root>/<id>/current.jsonl 存在 → 新布局，Open(<root>/<id>)；
//  2. 否则 legacy 根槽：<root>/current.jsonl 存在且其 metadata.SessionID == id →
//     Open(root) 原地挂载（决策 2，不移动文件，Archive 仍在槽内轮转）；
//  3. 否则 ErrSessionNotFound。
//
// 命中缓存直接返回同一实例（决策 3）。P1-2 起 Store.Open 对会话目录加跨进程
// flock：同进程对已持锁目录的第二次 Open 会被拒绝（ErrStoreLocked），因此慢
// 路径全程在 r.mu 内串行——进程内恒返回同一实例、从不触碰本进程已持有的锁：
//   - 双检：并发 Open 同 id 时只有第一个真正执行 Store.Open，其余在锁内命中
//     缓存返回同一实例（决策 3；串行化后不再产生需要关闭的输家句柄）；
//   - 别名先于 Store.Open：legacy 槽 Archive 换 id 后，Open(新 id) 必须复用
//     已挂载实例，而不是对同一 <root>/current.jsonl 再开第二个句柄——目标目录
//     已被本进程另一 id 条目持锁时，先开后查必然被 flock 拒绝；
//   - 跨进程第二写者不受影响：另一进程持锁时 Store.Open 的 LOCK_NB 立即返回
//     ErrStoreLocked（宁拒勿双写，store.go ErrStoreLocked）。
func (r *Repository) Open(id string) (*Store, error) {
	if !validSessionID(id) {
		return nil, fmt.Errorf("session: open %q: %w", id, ErrSessionNotFound)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.open[id]; ok {
		return s, nil
	}

	// 解析 id → 目标目录
	target := ""
	dir := filepath.Join(r.root, id)
	if _, err := os.Stat(filepath.Join(dir, "current.jsonl")); err == nil {
		target = dir
	} else if m, ok := resolveSlotMetadata(filepath.Join(r.root, "current.jsonl")); ok && m.SessionID == id {
		target = r.root // legacy 根槽原地挂载（决策 2）
	}
	if target == "" {
		return nil, fmt.Errorf("session: open %s: %w", id, ErrSessionNotFound)
	}

	// 同目录别名：目标目录已被本进程以其他 id 挂载（legacy 槽 Archive 换 id
	// 后的复用路径，决策 3）。必须先于 Store.Open——见函数头注释。
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

// List 扫描 <root>/*/current.jsonl 的 metadata（复用 Store.Open 的
// metadataFromRecords/legacySessionID 机制，store.go:80-114）+ legacy 根槽。
// 归档文件不计入：只认 current.jsonl，<root>/<id>/archive/*.jsonl 与 legacy 的
// root/archive/ 天然被跳过（归档清单仍是 plugin 层扫描，见 sessions.go:42-64）。
// 新布局下目录名 == 文件内 SessionID（Create 种入保证）；legacy 槽的 id 来自文件
// metadata（或 legacy 合成规则）。返回顺序为目录名序（os.ReadDir 语义）。
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
		if m, ok := resolveSlotMetadata(filepath.Join(r.root, e.Name(), "current.jsonl")); ok {
			out = append(out, m)
		}
	}
	// legacy 根槽（决策 2）：root 自身的 current.jsonl 也是一个活跃会话。
	if m, ok := resolveSlotMetadata(filepath.Join(r.root, "current.jsonl")); ok {
		out = append(out, m)
	}
	return out, nil
}

// Delete 删除会话。判定顺序（按计划 §3 P1-1）：
//  1. open 缓存中存在 → ErrSessionInUse（打开着的 Store 不允许删文件）；
//  2. 存在 <root>/<id> → os.RemoveAll 整目录（含其 archive）；
//  3. legacy 根槽命中 id → ErrLegacyImmutable（决策 2：不移动也不销毁 legacy 数据）；
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

// ── 内部辅助 ──────────────────────────────────────────────────────────

// seedSessionMetadata 在新会话目录的 current.jsonl 写入首行 metadata 记录，
// 使 Store.Open 采纳目录名约定的 id（否则空文件会被赋予另一个随机 id，
// store.go:92-94，破坏「目录名 == 文件内 SessionID」不变量）。字节格式与
// Store.appendRecord 一致（json 单行 + '\n'，0600），后续由 Open 正常接管。
func seedSessionMetadata(dir, id string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("session: create dir %s: %w", dir, err)
	}
	data, err := json.Marshal(newMetadataRecord(id, time.Now()))
	if err != nil {
		return fmt.Errorf("session: marshal seed metadata: %w", err)
	}
	data = append(data, '\n')
	path := filepath.Join(dir, "current.jsonl")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("session: seed metadata %s: %w", path, err)
	}
	return nil
}

// resolveSlotMetadata 解析一个 jsonl 槽（current.jsonl）的会话身份，回退链与
// Store.Open（store.go:80-95）/ OpenArchiveView（archiveview.go:34-42）一致：
// 首个有效 metadata 记录优先；缺失时按首节点 id（或内容摘要）合成 legacy id。
// ok=false 表示槽不存在或无法给出稳定身份（空文件——Store.Open 会赋予随机 id，
// 不可寻址）。CreatedAt：有 metadata 记录用记录值；legacy 合成用文件内最早的
// 记录时间（与 OpenArchiveView 同策略；与 Store.Open 落盘的 time.Now 可能不同，
// 以落盘值为准的调用方应 Open 后读 Store.Metadata()）。
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
