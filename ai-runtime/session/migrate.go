// migrate.go 实现 Plan A4 的一次性布局迁移（daemon 启动时自动、幂等、rename-only）：
// 把存量混合布局迁到 A1 确立的唯一布局 <root>/<sessionID>/<sessionID>.jsonl——
//
//	① legacy 根槽：<root>/current.jsonl（有身份）→ 建 per-id 目录迁入（数据文件
//	   改名 <id>.jsonl，runs/ journal 目录随迁）；
//	② root 归档：<root>/archive/*.jsonl（Store.Archive() 轮转时代的存量，定案 #9）
//	   → 各自建 per-id 目录迁入 + 补 archived 终态标记（以 archived 态回归清单）；
//	③ per-id 存量对齐：v1 文件名 current.jsonl → <id>.jsonl；目录名 ≠ 文件内
//	   metadata id 的按 metadata id 对齐（rename 目录）；无消息空壳目录删除。
//
// 幂等（中断重跑安全）：每一步的中间态都可解析、可重入——rename 前探测目标已
// 存在即按冲突/重复处理（不覆盖）；archived 标记 MarkArchived 天然幂等（已有
// 记录 no-op）；文件/目录改名在同名文件系统内原子，崩溃只留下「迁移前后」两种
// 可解析状态。目录名对齐采用三段改名（数据文件先中性化为 current.jsonl → 目录
// 改名 → 文件改 v2 名），任一时刻数据文件都落在 resolveDataFilePath 可决议的
// 名字上，绝不产生不可寻址的中间态。
//
// 写纪律（为什么 archived 标记是唯一允许的非 rename 动作）：①③ 纯 rename——
// 数据字节零重写，纯追加原则不受损；② 的 archived 记录是 A1 的标记语义
// （append 一行 + fsync），rename 无法表达「终态标记」这一信息增量，且写发生在
// rename 落位之后、本进程无任何 store 打开（见下）——是迁移窗口内唯一写者。
//
// 调用前提（无写者论证，宿主负责满足）：迁移必须在 OpenRepository 之后、首个
// store 打开之前执行——此刻本进程缓存为空、无任何 flock 持有、无在途追加；
// 跨进程面生产形态为单 daemon 独占 configDir（既有 flock 纪律只保证 store 不
// 双开，双 daemon 并发本就在未定义之列），即便并发，POSIX rename 按 inode 语义
// 不撕裂已打开句柄的数据（旧 daemon 的追加继续落到改名后的文件），布局以迁移
// 结果为准。进程内单飞由调用点保证（daemon 仅在首个会话边界调用一次）。
//
// 失败语义（定案 #9：无线上用户，失败条目直接丢弃）：逐条独立处理——解析失败
// （数据文件不可寻址）、身份非法（metadata id 非会话形态）、目标冲突（同 id
// 已有会话）的条目连源清理 + warn 记账，绝不阻断启动；Rename/MarkArchived 的
// I/O 失败同样逐条 warn（保留现场，重跑幂等收敛），不上抛——迁移的整体成败
// 以 root 可读为界（不可读返回 error，调用方按启动失败降级处理）。
package session

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// LayoutMigration 是一次迁移的条数摘要（启动日志的素材，Plan A4「迁移日志输出
// 条数摘要」）。全部字段为条目计数，非字节量。
type LayoutMigration struct {
	Legacy int // legacy 根槽迁入 per-id 目录（含 runs/ journal 随迁）
	// Archived 是 root 归档文件迁入 per-id 并补终态标记的条数（以 archived 态
	// 回归清单）。MarkArchived I/O 失败的条目已迁入但缺标记（清单降级 idle），
	// 只 warn 不计入——失败可观测重跑可补（幂等）。
	Archived int
	Aligned  int // per-id 对齐：v1 文件名改 v2、目录名按 metadata id 对齐
	Shells   int // 空壳/不可寻址目录删除
	Dropped  int // 冲突/身份非法条目丢弃（连源清理 + warn）
}

// MigrateLegacyLayout 执行一次布局迁移（幂等，可重复调用；见文件头）。
// root 不可读返回 error（启动链路据此降级）；逐条失败只 warn 计数，不上抛。
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
	// 归档目录清空后摘除（A3 遗留闭合：迁移后 root/archive/ 清空）。非空时
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
		// 不可寻址（空文件/坏行）或身份非法：定案 #9 连源清理。根槽是唯一活跃
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
		// 目标数据文件已存在（同 id 的 per-id 会话在册）：冲突丢弃根槽（定案 #9）
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
	// runs/ journal 随迁（P2-2 起 legacy 槽的 journal 落 <root>/runs/，store.dir
	// 迁移后 journal 面改认 <root>/<id>/runs/——不同步搬走会让 P2-7 恢复扫描与
	// RunStatus 的 journal 权威分支丢失全部历史 run 记录）。目标已存在（理论
	// 不可达：同 id 冲突在上方已丢弃）时保留现场不合并，warn 交由人工裁决。
	if err := os.Rename(filepath.Join(root, "runs"), filepath.Join(dir, "runs")); err != nil && !os.IsNotExist(err) {
		slog.Warn("session: migrate legacy runs dir failed", "from", filepath.Join(root, "runs"), "err", err)
	}
	stats.Legacy++
	slog.Info("session: migrated legacy root slot", "id", id)
}

// migrateRootArchives 迁移 root 归档文件（阶段 ②，定案 #9 的存量 73 条形态）。
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

// migrateSessionDir 对齐单个 per-id 目录（阶段 ③）：
//   - 无数据文件：会话形态目录名 + 空目录 = 迁移崩溃残留（① 的 MkdirAll 与
//     rename 之间的窗口），Remove 清理；非会话形态目录（runs/、archive/、用户
//     自建）不碰——它们不是会话槽，删除越权。
//   - 数据文件不可寻址（空/坏行）：空壳删除（RemoveAll，定案 #9——不可寻址即
//     不可 load，保留只是垃圾）。
//   - 身份可解析：目录名 ≠ metadata id → 对齐（三段改名，文件头论证）；数据
//     文件是 v1 名（current.jsonl）且目录名为会话形态 → 改 v2 名。
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
		// 不可寻址空壳（定案 #9）：删除整个目录（数据文件不可解析 = 不可 load，
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
	// 已存在 → 冲突丢弃源目录（定案 #9；同 id 双正本必有一份是非正本，保留
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
	// v1 → v2 文件名对齐（Plan A1 的存量兼容读路径退役点）：仅会话形态目录名
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
// 非形态 id 迁出去也无法寻址，按定案 #9 丢弃。
func migratableID(id string) bool {
	return validSessionID(id) && sessionShapedBasename(id)
}

// migratableIdentity 解析槽位身份并要求「稳定」（比 resolveSlotMetadata 多一道
// 判据）：metadata 记录优先；缺失时只有首节点 id 可依赖——legacySessionID 在
// firstNodeID 非空时忽略 content（id.go 注释：首节点 id 跨归档稳定），由此合成
// 的 legacy_<hex> 与 digest 清单扫描（resolveSlotMetadata）对同一文件恒同值；
// 纯垃圾内容（无可解析记录、无首节点）走 content 摘要分支，id 会随任何追加
// 漂移——迁移的 archived 标记写点本身就会改内容，用它会产出「目录名 ≠ 下次
// 扫描身份」的永久错位，幂等性被破坏。这类文件没有可恢复的记录，按定案 #9
// 丢弃而非迁移。
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
