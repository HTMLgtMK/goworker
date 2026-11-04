// digest.go 提供会话清单的一次性只读解析面（Plan A3）：ScanSessionDigests 扫描
// root 下全部会话（per-id 目录 + legacy 根槽），对每个会话一次 loadRecords 提取
// 清单所需的全部事实（身份/首条 user 文本/workspace 声明/archived 终态标记/数据
// 文件 mtime），供 daemon 侧 session/list 重写消费。
//
// 为什么不是 repo.List + 逐会话 repo.Open：清单需要 title/updatedAt/cwd/state
// 等超出 Metadata 的事实，而 Open 会把每个会话的 Store 实例塞进实例唯一缓存并
// 持有跨进程 flock——清单是客户端高频操作，50 个会话全量常驻缓存（句柄 + 锁）
// 不可接受；SessionArchived 的「探测性打开」取舍（A2b1）在单会话探测上成立，
// 在全量清单上不成立。本文件与 OpenArchiveView 同级：只读打开、解析后即弃，
// 不进实例唯一缓存、不取写句柄、不加目录锁——清单扫描与运行中的 store 追加
// 并发安全（坏行跳过与 loadRecords 同策略；mtime 取 Stat 快照）。
//
// 与 repo.List 的分工：repo.List 保持 Metadata 面（身份清单，既有消费方）；
// 本文件是清单专用富事实面。两者的目录扫描逻辑（双探测 + legacy 根槽）同源
// （都走 resolveDataFilePath/resolveSlotMetadata），不存在第二套布局决议。
package session

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SessionDigest 是单个会话的清单事实快照（Plan A3）。全部字段为解析时刻的
// 只读快照；Title 原料（FirstUserText）不做截断/单行化——展示策略归调用方
// （daemon 的 truncateTitle），本层不引入展示语义。
type SessionDigest struct {
	Metadata Metadata // 会话身份（metadata 记录优先，legacy 合成回退）
	// FirstUserText 是首条 user 消息的原文（清单 title 的原料；无 user 消息
	// 或全是坏行的空会话为空串）。
	FirstUserText string
	// Workspace 是最后一条 workspace 声明（last-wins，Plan A1；从未声明为空）。
	Workspace string
	// Archived 报告是否已落 archived 终态标记（Plan A1 原位终态，只读保留）。
	Archived bool
	// ModTime 是数据文件的修改时间（清单 updatedAt 的原料；Stat 失败为零值）。
	ModTime time.Time
}

// ScanSessionDigests 扫描 root 下全部会话的清单事实（Plan A3 的 session/list
// 数据源）。扫描序与 repo.List 同源：
//   - per-id 目录：<root>/<dir>/ 的数据文件按 v2/v1 双探测存在且可给出稳定
//     身份（metadata 优先，缺失按首节点/内容摘要合成 legacy id）→ 收录；
//   - legacy 根槽：<root>/current.jsonl 存在且可解析身份 → 收录（A4 一次性
//     迁移前维持兼容挂载语义）。
//
// 归档目录（<root>/archive/ 与 <root>/<id>/archive/）不扫描：Plan A1 起 archived
// 是原位终态标记（MarkArchived 不搬文件），显式归档与 A4 迁移的会话都停在
// 数据文件位置；存量轮转归档（Store.Archive() 的历史产物）已由 A4 一次性迁移
// （migrate.go，daemon 启动链调用）以 archived 态迁回 per-id 目录后回归清单，
// 本扫描层对未迁移的旧快照维持「归档目录不扫描」的口径。
//
// 读错误（root 不可读）上抛；单个会话的解析失败（坏行/Stat 失败）按坏行策略
// 跳过该会话（与 loadRecords 的容错一致，清单可用性优先于单条完整性）。
func ScanSessionDigests(root string) ([]SessionDigest, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("session: scan digests %s: %w", root, err)
	}
	out := make([]SessionDigest, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		path := resolveDataFilePath(dir)
		if d, ok := digestFromPath(path); ok {
			out = append(out, d)
		}
	}
	// legacy 根槽（决策 2）：root 自身的 current.jsonl 也是一个活跃会话。
	if d, ok := digestFromPath(filepath.Join(root, legacyDataFileName)); ok {
		out = append(out, d)
	}
	return out, nil
}

// digestFromPath 对单个数据文件做一次性只读解析并提取清单事实。ok=false 表示
// 文件不存在或无法给出稳定身份（空文件——Store.Open 会赋予随机 id，不可寻址）。
func digestFromPath(path string) (SessionDigest, bool) {
	recs, _, err := loadRecords(path)
	if err != nil {
		return SessionDigest{}, false
	}
	m, hasMeta := metadataFromRecords(recs)
	if !hasMeta || m.SessionID == "" {
		// 身份回退链与 resolveSlotMetadata 同源：metadata 缺失按首节点/内容
		// 摘要合成 legacy id；空文件不可寻址。
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return SessionDigest{}, false
		}
		firstID := firstNodeID(recs)
		if firstID == "" && len(content) == 0 {
			return SessionDigest{}, false
		}
		m = Metadata{
			SessionID: legacySessionID(firstID, content),
			CreatedAt: timeFromRecords(recs),
		}
	}
	d := SessionDigest{
		Metadata:  m,
		Workspace: workspaceFromRecords(recs),
		Archived:  archivedFromRecords(recs),
	}
	// 首条 user 消息原文（清单 title 原料）：kind/role 字面量即磁盘格式契约
	// （record_format_test 兜底）。
	for _, r := range recs {
		if r.Kind == kindMsg && r.Msg != nil && r.Msg.Role == "user" {
			d.FirstUserText = r.Msg.Content
			break
		}
	}
	if fi, statErr := os.Stat(path); statErr == nil {
		d.ModTime = fi.ModTime()
	}
	return d, true
}
