// Package session 提供会话持久化的 jsonl 追加存储，支持 compact 树形组织与固化游标。
// 核心原则：纯追加，绝不改写旧行；ActiveView = 从 head 沿 parent 纯回溯。
package session

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
)

// Checkpoint 对外暴露的检查点摘要。
type Checkpoint struct {
	ID        string    `json:"id"`
	At        string    `json:"at"`
	Preview   string    `json:"preview"`
	CreatedAt time.Time `json:"created_at"`
}

// Store 管理会话的 jsonl 持久化存储。
type Store struct {
	mu sync.Mutex

	path string // current.jsonl 绝对路径
	dir  string // 会话目录

	file *os.File // 追加写入句柄，Open 后保持打开

	recs   []Record // 全部记录（仅加载时填充，后续追加写文件不维护内存全量——但为了回溯便捷保留）
	head   string   // 当前 head 节点 id
	cursor string   // 固化游标指向的消息 id
}

// Open 打开或创建 dir 下的会话存储。目录不存在则创建，jsonl 文件不存在则新建。
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("session: create dir %s: %w", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("session: restrict dir %s: %w", dir, err)
	}
	if err := restrictArchivePermissions(filepath.Join(dir, "archive")); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, "current.jsonl")

	// 加载已有记录
	recs, err := loadRecords(path)
	if err != nil {
		return nil, fmt.Errorf("session: load %s: %w", path, err)
	}

	// 以 append 模式打开文件句柄
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("session: open file %s: %w", path, err)
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return nil, fmt.Errorf("session: restrict file %s: %w", path, err)
	}

	s := &Store{
		path: path,
		dir:  dir,
		file: f,
		recs: recs,
	}

	// 扫描最后出现的 head / cursor
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		if s.head == "" && r.Kind == kindHead && r.Head != nil {
			s.head = r.Head.Tail
		}
		if s.cursor == "" && r.Kind == kindCursor && r.Cur != nil {
			s.cursor = r.Cur.MsgID
		}
		if s.head != "" && s.cursor != "" {
			break
		}
	}

	return s, nil
}

// Close 关闭文件句柄，释放资源。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		if err := s.file.Sync(); err != nil {
			slog.Warn("session: sync on close failed", "err", err)
		}
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("session: close file: %w", err)
		}
		s.file = nil
	}
	return nil
}

// ActiveView 从 head 沿 parent 回溯，构建当前活跃视图。
// compact 节点在视图里转为一条 Role="system" 的摘要消息。
func (s *Store) ActiveView() []core.Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.head == "" {
		return nil
	}

	// 内存中有完整记录时优先用内存（性能更好），否则从文件加载。
	// 但为了保持一致性，始终从内存记录构建（追加时 recs 未被更新，需要 reload）。
	// 折中方案：从 recs 构建 id 索引，若 head 不在索引里则重新加载。
	byID := make(map[string]Record, len(s.recs))
	for _, r := range s.recs {
		if r.Kind == kindMsg || r.Kind == kindCompact {
			byID[r.NodeID()] = r
		}
	}

	// 沿 parent 链回溯，收集节点（反向）
	var chain []Record
	cur := s.head
	visited := make(map[string]bool) // 防循环
	for cur != "" && !visited[cur] {
		visited[cur] = true
		r, ok := byID[cur]
		if !ok {
			slog.Warn("session: head points to unknown node", "head", s.head, "missing", cur)
			break
		}
		chain = append(chain, r)
		cur = r.ParentID()
	}

	// 反转得正向顺序
	slices.Reverse(chain)

	// 转换为 core.Message
	var msgs []core.Message
	for _, r := range chain {
		msgs = append(msgs, recordToMessage(r))
	}
	return msgs
}

// Commit 将一批消息追加落盘。已存在的 id 跳过（幂等）。
// 新消息链到当前 head 后，返回实际落盘的消息（补充了 id/时间戳）。
func (s *Store) Commit(msgs []core.Message) ([]core.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var committed []core.Message
	prev := s.head
	for i := range msgs {
		m := msgs[i]
		if m.MsgID == "" {
			slog.Warn("session: commit message without MsgID, skipping", "role", m.Role, "content_preview", truncate(m.Content, 40))
			continue
		}
		// 幂等去重：检查 id 是否已存在于内存记录中
		if s.hasID(m.MsgID) {
			slog.Debug("session: skip duplicate msg id", "id", m.MsgID)
			// 跳过重复，但仍需更新 prev 以保持链式结构（重复消息的 parent 指向它原本的前驱）
			// 注意：这里不做 parent 链的重新挂接，因为场景是同一批消息重复提交。
			// 跳过整个重复消息，prev 保持不变。
			continue
		}

		now := time.Now()
		if m.CreatedAt.IsZero() {
			m.CreatedAt = now
		}

		rec := messageToRecord(m)
		rec.Msg.Parent = prev
		rec.Msg.CreatedAt = now

		if err := s.appendRecord(rec); err != nil {
			return committed, fmt.Errorf("session: commit msg %s: %w", m.MsgID, err)
		}

		// 追加 head 记录
		headRec := newHeadRecord(m.MsgID, now)
		if err := s.appendRecord(headRec); err != nil {
			return committed, fmt.Errorf("session: commit head for %s: %w", m.MsgID, err)
		}

		s.recs = append(s.recs, rec, headRec)
		s.head = m.MsgID
		prev = m.MsgID
		committed = append(committed, m)
	}
	return committed, nil
}

// Compact 在 covered_from..covered_to 活跃路径连续段上做折叠。
// 追加 compact 摘要节点 + 保留段副本（新 id），head 移到最后副本，同时映射游标。
func (s *Store) Compact(coveredFrom, coveredTo, summary string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.head == "" {
		return fmt.Errorf("session: cannot compact empty session")
	}

	// 构建当前活跃路径的 id 有序列表 + parent 映射
	activeIDs, parentOf := s.activePathIDs()
	if len(activeIDs) == 0 {
		return fmt.Errorf("session: empty active path")
	}

	// 定位 covered 段在活跃路径中的位置
	fromIdx := -1
	toIdx := -1
	for i, id := range activeIDs {
		if id == coveredFrom {
			fromIdx = i
		}
		if id == coveredTo {
			toIdx = i
		}
	}
	_ = parentOf // 保留接口完整性，后续 rewind 分支检测可能用到
	if fromIdx < 0 || toIdx < 0 || fromIdx > toIdx {
		return fmt.Errorf("session: compact range [%s, %s] not found on active path or out of order", coveredFrom, coveredTo)
	}

	// covered 段前驱（compact 节点的 parent）
	compactParent := ""
	if fromIdx > 0 {
		compactParent = activeIDs[fromIdx-1]
	}

	// 保留段：covered_to 之后到 head 的活跃消息
	retainIDs := activeIDs[toIdx+1:]

	// 构建 id -> record 映射（从内存）
	byID := make(map[string]Record, len(s.recs))
	for _, r := range s.recs {
		if r.Kind == kindMsg || r.Kind == kindCompact {
			byID[r.NodeID()] = r
		}
	}

	// 生成 compact 节点 id（确定性：s_<coveredTo>）
	compactID := "s_" + coveredTo
	now := time.Now()

	// 追加 compact 节点
	compactRec := newCompactRecord(compactID, compactParent, summary, coveredFrom, coveredTo, now)
	if err := s.appendRecord(compactRec); err != nil {
		return fmt.Errorf("session: append compact record: %w", err)
	}
	s.recs = append(s.recs, compactRec)

	// 复制保留段，挂到 compact 节点下
	cloneMap := make(map[string]string) // 原 id → 克隆 id
	prevClone := compactID
	for _, origID := range retainIDs {
		orig, ok := byID[origID]
		if !ok || orig.Msg == nil {
			// 保留段只应含真实消息；id 找不到或混入 compact 节点都跳过（防御）
			slog.Warn("session: retain id not found or not a msg", "id", origID)
			continue
		}
		cloneID := NewMsgID()
		cloneRec := newMsgRecord(cloneID, prevClone, orig.Msg.Role, orig.Msg.Content,
			orig.Msg.ToolCalls, orig.Msg.ToolCallID, origID, now)
		cloneRec.Msg.Thinking = orig.Msg.Thinking
		cloneRec.Msg.Custom = orig.Msg.Custom
		if err := s.appendRecord(cloneRec); err != nil {
			return fmt.Errorf("session: append clone %s: %w", cloneID, err)
		}
		s.recs = append(s.recs, cloneRec)
		cloneMap[origID] = cloneID
		prevClone = cloneID
	}

	// 游标映射
	s.remapCursor(retainIDs, cloneMap, prevClone, activeIDs)

	// head 移到最后副本（如果无保留段则移到 compact 节点）
	newHead := compactID
	if prevClone != compactID {
		newHead = prevClone
	}

	headRec := newHeadRecord(newHead, now)
	if err := s.appendRecord(headRec); err != nil {
		return fmt.Errorf("session: append head after compact: %w", err)
	}
	s.recs = append(s.recs, headRec)
	s.head = newHead

	return nil
}

// SetHead 将 head 移到指定 id（rewind 操作）。
func (s *Store) SetHead(atID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	headRec := newHeadRecord(atID, now)
	if err := s.appendRecord(headRec); err != nil {
		return fmt.Errorf("session: set head to %s: %w", atID, err)
	}
	s.recs = append(s.recs, headRec)
	s.head = atID
	return nil
}

// Checkpoint 在当前位置打检查点（at = 当前 head）。
func (s *Store) Checkpoint(preview string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.head == "" {
		return fmt.Errorf("session: cannot checkpoint empty session")
	}

	now := time.Now()
	ckID := "ck_" + s.head
	rec := newCheckpointRecord(ckID, s.head, preview, now)
	if err := s.appendRecord(rec); err != nil {
		return fmt.Errorf("session: append checkpoint: %w", err)
	}
	s.recs = append(s.recs, rec)
	return nil
}

// Checkpoints 返回最近 limit 个检查点（按时间倒序）。
func (s *Store) Checkpoints(limit int) []Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()

	cks := make([]Checkpoint, 0, limit)
	for i := len(s.recs) - 1; i >= 0 && len(cks) < limit; i-- {
		r := s.recs[i]
		if r.Kind == kindCheckpoint && r.CK != nil {
			cks = append(cks, Checkpoint{
				ID:        r.CK.ID,
				At:        r.CK.At,
				Preview:   r.CK.Preview,
				CreatedAt: r.CK.CreatedAt,
			})
		}
	}
	return cks
}

// PendingAfterCursor 返回活跃路径上 cursor 之后（不含 cursor 自身）的真实消息。
// cursor 不在活跃路径 → 返回全量活跃真实消息（兜底）。
func (s *Store) PendingAfterCursor() []core.Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	active := s.activeViewLocked()
	if len(active) == 0 {
		return nil
	}

	// 找出活跃路径上所有真实消息（非 compact 摘要）
	type indexedMsg struct {
		idx int
		msg core.Message
	}
	var realMsgs []indexedMsg
	for i, m := range active {
		if m.MsgID != "" && isRealMessage(m) {
			realMsgs = append(realMsgs, indexedMsg{idx: i, msg: m})
		}
	}

	if s.cursor == "" {
		// 无游标 → 全量
		var out []core.Message
		for _, im := range realMsgs {
			out = append(out, im.msg)
		}
		return out
	}

	// 找 cursor 在活跃路径中的位置
	cursorIdx := -1
	for i, im := range realMsgs {
		if im.msg.MsgID == s.cursor {
			cursorIdx = i
			break
		}
	}

	if cursorIdx < 0 {
		// cursor 不在活跃路径 → 全量兜底
		var out []core.Message
		for _, im := range realMsgs {
			out = append(out, im.msg)
		}
		return out
	}

	// cursor 之后的消息
	var out []core.Message
	for _, im := range realMsgs[cursorIdx+1:] {
		out = append(out, im.msg)
	}
	return out
}

// AdvanceCursor 将固化游标推进到指定 id。
func (s *Store) AdvanceCursor(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	rec := newCursorRecord(id, now)
	if err := s.appendRecord(rec); err != nil {
		return fmt.Errorf("session: advance cursor to %s: %w", id, err)
	}
	s.recs = append(s.recs, rec)
	s.cursor = id
	return nil
}

// Archive 将 current.jsonl 重命名为 archive/<timestamp>.jsonl，然后打开新的空文件。
func (s *Store) Archive() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 关闭当前文件句柄
	if s.file != nil {
		if err := s.file.Sync(); err != nil {
			slog.Warn("session: sync before archive failed", "err", err)
		}
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("session: close before archive: %w", err)
		}
		s.file = nil
	}

	// 创建 archive 目录
	archiveDir := filepath.Join(s.dir, "archive")
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		return fmt.Errorf("session: create archive dir: %w", err)
	}
	if err := os.Chmod(archiveDir, 0o700); err != nil {
		return fmt.Errorf("session: restrict archive dir: %w", err)
	}

	// rename current.jsonl → archive/<unix_nano>.jsonl
	ts := fmt.Sprintf("%d", time.Now().UnixNano())
	dst := filepath.Join(archiveDir, ts+".jsonl")
	if err := os.Rename(s.path, dst); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("session: rename to archive: %w", err)
	}

	// 重开空文件
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("session: reopen after archive: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return fmt.Errorf("session: restrict reopened file: %w", err)
	}
	s.file = f
	s.recs = nil
	s.head = ""
	s.cursor = ""

	return nil
}

// ── 内部辅助 ──────────────────────────────────────────────────────────

func restrictArchivePermissions(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("session: read archive dir: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("session: restrict archive dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := os.Chmod(filepath.Join(dir, entry.Name()), 0o600); err != nil {
			return fmt.Errorf("session: restrict archive file %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// appendRecord 将一条记录序列化后追加到文件末尾并 fsync。
// 调用方必须持有 s.mu。
func (s *Store) appendRecord(r Record) error {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}
	data = append(data, '\n')
	if _, err := s.file.Write(data); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync after write: %w", err)
	}
	return nil
}

// hasID 检查 id 是否已存在于记录的 msg/compact 节点中。
func (s *Store) hasID(id string) bool {
	for _, r := range s.recs {
		if (r.Kind == kindMsg || r.Kind == kindCompact) && r.NodeID() == id {
			return true
		}
	}
	return false
}

// activePathIDs 返回当前活跃路径上所有 msg/compact 节点的 id 顺序列表。
func (s *Store) activePathIDs() (ids []string, parentOf map[string]string) {
	// 先构建 id → record 索引
	byID := make(map[string]Record, len(s.recs))
	for _, r := range s.recs {
		if r.Kind == kindMsg || r.Kind == kindCompact {
			byID[r.NodeID()] = r
		}
	}

	parentOf = make(map[string]string)
	var chain []string
	cur := s.head
	visited := make(map[string]bool)
	for cur != "" && !visited[cur] {
		visited[cur] = true
		r, ok := byID[cur]
		if !ok {
			break
		}
		chain = append(chain, cur)
		parentOf[cur] = r.ParentID()
		cur = r.ParentID()
	}
	slices.Reverse(chain)
	return chain, parentOf
}

// activeViewLocked 同 ActiveView 但不加锁（调用方持锁）。
func (s *Store) activeViewLocked() []core.Message {
	if s.head == "" {
		return nil
	}
	byID := make(map[string]Record, len(s.recs))
	for _, r := range s.recs {
		if r.Kind == kindMsg || r.Kind == kindCompact {
			byID[r.NodeID()] = r
		}
	}
	var chain []Record
	cur := s.head
	visited := make(map[string]bool)
	for cur != "" && !visited[cur] {
		visited[cur] = true
		r, ok := byID[cur]
		if !ok {
			break
		}
		chain = append(chain, r)
		cur = r.ParentID()
	}
	slices.Reverse(chain)
	var msgs []core.Message
	for _, r := range chain {
		msgs = append(msgs, recordToMessage(r))
	}
	return msgs
}

// remapCursor 在 compact 时映射游标：保留段原消息 → 对应副本。
// retainIDs: 保留段原消息 id 序列
// cloneMap: 原 id → 克隆 id
// lastClone: 最后一个副本 id（保留段末尾节点的副本，copyOf 的最后一条克隆）
func (s *Store) remapCursor(retainIDs []string, cloneMap map[string]string, lastClone string, _ []string) {
	if s.cursor == "" {
		return
	}

	// 游标在保留段中 → 指向对应副本
	for _, origID := range retainIDs {
		if s.cursor == origID {
			if cloneID, ok := cloneMap[origID]; ok {
				s.cursor = cloneID
			}
			return
		}
	}

	// 游标在保留段末尾之后（即 head 位置或更后）→ 指向最后副本
	// 判定：游标不在保留段里，且在活跃路径上处于保留段之后的位置
	if s.cursor == s.head {
		s.cursor = lastClone
		return
	}

	// 游标在覆盖段或更早 → 不动（落路径外，PendingAfterCursor 兜底全量）
}

// ── 文件操作 ──────────────────────────────────────────────────────────

// loadRecords 从 jsonl 文件加载全部记录，坏行跳过并 warn。
func loadRecords(path string) ([]Record, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var recs []Record
	sc := bufio.NewScanner(f)
	// 单行最大 1MB，超出视为坏行跳过
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			slog.Warn("session: skip bad jsonl line", "err", err, "line_preview", truncate(string(line), 80))
			continue
		}
		recs = append(recs, r)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	return recs, nil
}

// truncate 按 rune 截断字符串到 max 长度，中文安全。max <= 0 不截断（防 slice 越界 panic）。
func truncate(s string, max int) string {
	runes := []rune(s)
	if max <= 0 || len(runes) <= max {
		return s
	}
	return string(runes[:max]) + "…"
}

// ── detectCompact 纯函数 ──────────────────────────────────────────────

// DetectCompact 检测新旧对话之间是否发生了压缩。
// oldView: 压缩前活跃视图（带 MsgID）
// newConv: 压缩后对话（带 MsgID）
// 返回 covered_from, covered_to, summary 和是否检测到压缩。
//
// 条件：len(newConv) < len(oldView) 且 newConv[0].Role=="system"
// 找 oldView 中第一个仍出现在 newConv 的 id 位置 i，covered = oldView[:i]（必须连续前缀）。
func DetectCompact(oldView, newConv []core.Message) (from, to, summary string, ok bool) {
	if len(newConv) >= len(oldView) || len(newConv) == 0 {
		return "", "", "", false
	}
	if newConv[0].Role != "system" {
		return "", "", "", false
	}
	summary = newConv[0].Content

	// 构建 newConv 的 id 集合
	newIDs := make(map[string]bool, len(newConv))
	for _, m := range newConv {
		if m.MsgID != "" {
			newIDs[m.MsgID] = true
		}
	}

	// 找 oldView 中第一个仍在 newConv 里的消息位置
	splitIdx := -1
	for i, m := range oldView {
		if m.MsgID != "" && newIDs[m.MsgID] {
			splitIdx = i
			break
		}
	}

	if splitIdx < 0 {
		// 没有任何消息重叠 → 尝试覆盖全部
		if len(oldView) > 0 && oldView[0].MsgID != "" && oldView[len(oldView)-1].MsgID != "" {
			return oldView[0].MsgID, oldView[len(oldView)-1].MsgID, summary, true
		}
		return "", "", "", false
	}

	if splitIdx == 0 {
		// 第一条就重叠了，没有覆盖段
		return "", "", "", false
	}

	// covered = oldView[:splitIdx]，必须是连续前缀
	covered := oldView[:splitIdx]
	if len(covered) == 0 {
		return "", "", "", false
	}

	// 验证 covered 段消息都有 id
	if covered[0].MsgID == "" || covered[len(covered)-1].MsgID == "" {
		return "", "", "", false
	}

	return covered[0].MsgID, covered[len(covered)-1].MsgID, summary, true
}
