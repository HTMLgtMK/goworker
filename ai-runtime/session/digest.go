// ScanSessionDigests 只读扫描 per-id 目录与 legacy 根槽，提取清单所需的会话事实。
// 它不创建 Store、不持有写句柄或目录锁；单会话解析错误按坏行策略跳过。目录解析
// 与 Repository 共用同一组路径和身份解析函数。
package session

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SessionDigest 是单个会话的清单事实快照。全部字段为解析时刻的
// 只读快照；Title 原料（FirstUserText）不做截断/单行化——展示策略归调用方
// （daemon 的 truncateTitle），本层不引入展示语义。
type SessionDigest struct {
	Metadata Metadata // 会话身份（metadata 记录优先，legacy 合成回退）
	// FirstUserText 是首条 user 消息的原文（清单 title 的原料；无 user 消息
	// 或全是坏行的空会话为空串）。
	FirstUserText string
	// Workspace 是最后一条 workspace 声明（last-wins；从未声明为空）。
	Workspace string
	// Archived 报告是否已落 archived 终态标记（原位终态，只读保留）。
	Archived bool
	// ModTime 是数据文件的修改时间（清单 updatedAt 的原料；Stat 失败为零值）。
	ModTime time.Time
}

// ScanSessionDigests 以与 repo.List 相同的路径决议扫描清单事实：探测 per-id
// 目录的 v2/v1 文件并优先取 metadata 身份，缺失时按首节点或内容摘要合成 legacy id；
// 迁移前仍收录可解析的 legacy 根槽 current.jsonl。归档目录不扫描：当前归档为原位
// archived 标记，历史轮转归档由启动迁移恢复到 per-id 目录。root 读取错误上抛，
// 单会话坏行或 Stat 失败则跳过，保持 loadRecords 的容错边界。
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
	// legacy 根槽：root 自身的 current.jsonl 也是一个活跃会话。
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
