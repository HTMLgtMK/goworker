// 旧布局迁移及失败处理见 ../docs/README.md。
// 迁移必须在本进程打开任何 store 之前执行；调用方负责排除并发写入。
// 仓库没有根级跨进程迁移锁，部署时须保证同一数据目录由单个 daemon 独占。
package session

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// LayoutMigration summarizes the number of entries migrated, aligned, removed, or dropped.
type LayoutMigration struct {
	Legacy int // legacy 根槽迁入 per-id 目录（含 runs/ journal 随迁）
	// Archived counts root archives moved and marked read-only. Failures remain
	// retryable; an unmarked migrated archive is reported as idle until retried.
	Archived int
	Aligned  int // per-id 对齐：v1 文件名改 v2、目录名按 metadata id 对齐
	Shells   int // 空壳/不可寻址目录删除
	Dropped  int // 冲突/身份非法条目丢弃（连源清理 + warn）
}

// MigrateLegacyLayout migrates legacy session layouts. It is idempotent; root read errors
// are returned, while individual entry failures are logged and retried on a later run.
func MigrateLegacyLayout(root string) (LayoutMigration, error) {
	var stats LayoutMigration
	entries, err := os.ReadDir(root)
	if err != nil {
		return stats, fmt.Errorf("session: migrate layout %s: %w", root, err)
	}

	// 阶段 ①：legacy 根槽（<root>/current.jsonl）。先于 per-id 阶段——它产生的
	// 目标目录必须在本阶段落位，避免 per-id 阶段把崩溃残留的空壳目标目录误删。
	migrateLegacySlot(root, &stats)

	// 阶段 ②：root 归档文件（<root>/archive/*.jsonl，存量轮转产物）。
	migrateRootArchives(root, &stats)

	// 阶段 ③：per-id 目录对齐（v1 文件名、目录名 ≠ 内部 id、空壳删除）。
	for _, e := range entries {
		if !e.IsDir() {
			continue // 杂散文件不碰（非会话数据，不在迁移职责内）
		}
		migrateSessionDir(filepath.Join(root, e.Name()), &stats)
	}
	// 归档目录清空后摘除（迁移后 root/archive/ 清空）。非空时
	// Remove 失败（目录未空）——保留现场，无害。
	_ = os.Remove(filepath.Join(root, "archive"))
	return stats, nil
}

// migrateLegacySlot 迁移 legacy 根槽（阶段 ①）。
func migrateLegacySlot(root string, stats *LayoutMigration) {
	slot := filepath.Join(root, legacyDataFileName)
	if _, err := os.Stat(slot); err != nil {
		return // 无根槽（已迁移/从未存在）：幂等空操作
	}
	m, ok := migratableIdentity(slot)
	if !ok || !migratableID(m.SessionID) {
		// 不可寻址（空文件/坏行）或身份非法：连源清理。根槽是唯一活跃
		// 文件时丢弃即毁数据——但它要么可解析出身份（走迁移），要么本就不可
		// 寻址（Open 也挂载不了），删除是诚实处置而非数据损失。
		if err := os.Remove(slot); err != nil {
			slog.Warn("session: migrate legacy slot cleanup failed", "path", slot, "err", err)
			return
		}
		stats.Dropped++
		slog.Warn("session: migrated legacy slot dropped (unaddressable identity)", "path", slot)
		return
	}
	id := m.SessionID
	dir := filepath.Join(root, id)
	if _, err := os.Stat(filepath.Join(dir, id+".jsonl")); err == nil {
		// 目标数据文件已存在（同 id 的 per-id 会话在册）：冲突丢弃根槽
		// ——rename 覆盖会毁掉在册会话，宁丢重复勿毁正本。
		if err := os.Remove(slot); err != nil {
			slog.Warn("session: migrate legacy slot conflict cleanup failed", "path", slot, "err", err)
			return
		}
		stats.Dropped++
		slog.Warn("session: migrated legacy slot dropped (id conflict)", "id", id)
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		slog.Warn("session: migrate legacy slot mkdir failed", "dir", dir, "err", err)
		return // 现场保留，重跑幂等收敛
	}
	if err := os.Rename(slot, filepath.Join(dir, id+".jsonl")); err != nil {
		slog.Warn("session: migrate legacy slot rename failed", "from", slot, "err", err)
		return
	}
	// Move run journals with the legacy session so recovery and status retain their history.
	if err := os.Rename(filepath.Join(root, "runs"), filepath.Join(dir, "runs")); err != nil && !os.IsNotExist(err) {
		slog.Warn("session: migrate legacy runs dir failed", "from", filepath.Join(root, "runs"), "err", err)
	}
	stats.Legacy++
	slog.Info("session: migrated legacy root slot", "id", id)
}

// migrateRootArchives 迁移 root 归档文件（阶段 ②，存量归档的 73 条形态）。
func migrateRootArchives(root string, stats *LayoutMigration) {
	archiveDir := filepath.Join(root, "archive")
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		return // 无归档目录（已迁移/从未归档）：幂等空操作
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		path := filepath.Join(archiveDir, e.Name())
		m, ok := migratableIdentity(path)
		if !ok || !migratableID(m.SessionID) {
			if rmErr := os.Remove(path); rmErr != nil {
				slog.Warn("session: migrate archive cleanup failed", "path", path, "err", rmErr)
				continue
			}
			stats.Dropped++
			slog.Warn("session: migrated archive dropped (unaddressable identity)", "path", path)
			continue
		}
		id := m.SessionID
		dir := filepath.Join(root, id)
		target := filepath.Join(dir, id+".jsonl")
		if _, err := os.Stat(target); err == nil {
			// 同 id 会话已在册：归档件是重复（同一 metadata id 不可能有两个正本
			// ——Create 的 128-bit 随机 id 冲突仅理论可达），丢弃归档件。
			if rmErr := os.Remove(path); rmErr != nil {
				slog.Warn("session: migrate archive conflict cleanup failed", "path", path, "err", rmErr)
				continue
			}
			stats.Dropped++
			slog.Warn("session: migrated archive dropped (id conflict)", "id", id)
			continue
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			slog.Warn("session: migrate archive mkdir failed", "dir", dir, "err", err)
			continue
		}
		if err := os.Rename(path, target); err != nil {
			slog.Warn("session: migrate archive rename failed", "from", path, "err", err)
			continue
		}
		// 补 archived 终态标记（唯一允许的非 rename 动作，文件头已论证）：迁移
		// 无写者前提覆盖本写点；失败只 warn——数据已迁入，缺标记仅让该条目在
		// 清单降级为 idle（历史仍可 load），重跑幂等补标。
		st, err := Open(dir)
		if err == nil {
			err = st.MarkArchived()
			if cerr := st.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
		if err != nil {
			slog.Warn("session: migrate archive mark failed", "id", id, "err", err)
			continue
		}
		stats.Archived++
	}
}

// migrateSessionDir aligns one per-ID directory: remove empty session-shaped crash
// shells and unaddressable data directories; preserve non-session directories. For valid
// data, align directory names to metadata IDs via the three-step rename, then rename v1
// current.jsonl to the v2 basename when applicable.
func migrateSessionDir(dir string, stats *LayoutMigration) {
	base := filepath.Base(dir)
	dataPath := resolveDataFilePath(dir)
	if _, err := os.Stat(dataPath); err != nil {
		// 无数据文件：只清理「会话形态 + 空」的崩溃残留（os.Remove 仅空目录
		// 成功，含内容时失败保留现场——runs/ 等非会话目录天然不命中形态判定）。
		if sessionShapedBasename(base) {
			if err := os.Remove(dir); err == nil {
				stats.Shells++
				slog.Warn("session: migrated empty session dir removed", "dir", dir)
			}
		}
		return
	}
	m, ok := resolveSlotMetadata(dataPath)
	if !ok {
		// 不可寻址空壳：删除整个目录（数据文件不可解析 = 不可 load，
		// runs/ 等附属物随迁无主）。
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("session: migrate shell cleanup failed", "dir", dir, "err", err)
			return
		}
		stats.Shells++
		slog.Warn("session: migrated shell session dir removed", "dir", dir)
		return
	}
	// 目录名对齐：basename ≠ metadata id（存量「目录名 ≠ 内部 id」形态）。目标
	// 已存在 → 冲突丢弃源目录（同 id 双正本必有一份是非正本，保留
	// metadata 权威目标）。
	if base != m.SessionID && migratableID(m.SessionID) {
		if _, err := os.Stat(filepath.Join(filepath.Dir(dir), m.SessionID, m.SessionID+".jsonl")); err == nil {
			if err := os.RemoveAll(dir); err != nil {
				slog.Warn("session: migrate rename conflict cleanup failed", "dir", dir, "err", err)
				return
			}
			stats.Dropped++
			slog.Warn("session: migrated session dir dropped (id conflict)", "id", m.SessionID, "dir", dir)
			return
		}
		// 三段改名：数据文件先中性化为 current.jsonl（v2 名跟随旧目录名，直接
		// 搬目录会让它不可解析），再改目录名——之后的 v2 改名由下方统一步骤
		// 收口，任一时刻可解析（文件头论证）。
		if d2 := filepath.Join(dir, base+".jsonl"); d2 == dataPath {
			if err := os.Rename(dataPath, filepath.Join(dir, legacyDataFileName)); err != nil {
				slog.Warn("session: migrate neutral rename failed", "from", dataPath, "err", err)
				return
			}
			dataPath = filepath.Join(dir, legacyDataFileName)
		}
		if err := os.Rename(dir, filepath.Join(filepath.Dir(dir), m.SessionID)); err != nil {
			slog.Warn("session: migrate dir rename failed", "from", dir, "err", err)
			return
		}
		dir = filepath.Join(filepath.Dir(dir), m.SessionID)
		dataPath = filepath.Join(dir, legacyDataFileName) // 目录已改名，数据文件路径随之刷新
		stats.Aligned++
		slog.Info("session: migrated session dir aligned to metadata id", "id", m.SessionID, "from", base)
	}
	// v1 → v2 文件名对齐（存量兼容读路径的退役点）：仅会话形态目录名
	// （非会话形态恒用 current.jsonl，改名会破坏 resolveDataFilePath 决议）。
	// 目标已存在（双数据文件，防御分支）→ 旧名是影子副本，丢弃（rename 覆盖
	// 会毁数据，先删后改名不符 rename-only 纪律）。
	if filepath.Base(dataPath) == legacyDataFileName && sessionShapedBasename(filepath.Base(dir)) {
		target := filepath.Join(dir, filepath.Base(dir)+".jsonl")
		if _, err := os.Stat(target); err == nil {
			if err := os.Remove(dataPath); err != nil {
				slog.Warn("session: migrate v1 shadow cleanup failed", "path", dataPath, "err", err)
				return
			}
			stats.Dropped++
			slog.Warn("session: migrated v1 shadow file dropped", "path", dataPath)
			return
		}
		if err := os.Rename(dataPath, target); err != nil {
			slog.Warn("session: migrate v1 rename failed", "from", dataPath, "err", err)
			return
		}
		stats.Aligned++
	}
}

// migratableID 判定 metadata id 可否作为迁移目标目录名：既要过 validSessionID
// 的防穿越校验（拼路径前卡住），又要过会话形态严格判定（session_/legacy_ 前缀
// + hex）——v2 文件名决议只对会话形态目录名生效（store.go sessionShapedBasename），
// 非形态 id 迁出去也无法寻址，按同一规则丢弃。
func migratableID(id string) bool {
	return validSessionID(id) && sessionShapedBasename(id)
}

// migratableIdentity prefers metadata IDs; without metadata, only a first-node ID yields
// a stable legacy identity. Content-hash identities drift when migration appends the
// archived marker, breaking idempotency, so unaddressable garbage is dropped instead.
func migratableIdentity(path string) (Metadata, bool) {
	recs, _, err := loadRecords(path)
	if err != nil {
		return Metadata{}, false
	}
	if m, ok := metadataFromRecords(recs); ok && m.SessionID != "" {
		return m, true
	}
	if firstID := firstNodeID(recs); firstID != "" {
		return Metadata{SessionID: legacySessionID(firstID, nil), CreatedAt: timeFromRecords(recs)}, true
	}
	return Metadata{}, false
}
