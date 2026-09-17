// archiveview.go 提供归档 jsonl 的只读视图：OpenArchiveView 一次性解析归档文件，
// 构建 ActiveView 与 checkpoint 锚点，供 session/load 的归档重放（只读查看）使用。
// 视图不持写句柄、不 rename、不触碰 current —— 归档文件在 Archive 之后不可变，
// 只读解析与追加方天然无竞争（坏行/半行按 loadRecords 同策略跳过）。
package session

import (
	"fmt"
	"os"

	"github.com/tinguo/goworker/ai-core/core"
)

// ArchiveView 是归档 jsonl 的只读视图。recs/head 在打开时解析定案，之后只读：
// 归档文件不可变（Archive 已切换到新 current），无需锁与刷新。
type ArchiveView struct {
	recs []Record
	head string
}

// OpenArchiveView 只读打开归档 jsonl 并构建视图。文件不存在或不可读返回错误
// （调用方按 unknown 会话处理；loadRecords 对缺失文件返回空集的语义是给
// Store.Open 的新建路径用的，这里必须显式区分）；文件存在但为空（无 head）
// 返回空视图不报错。
func OpenArchiveView(path string) (*ArchiveView, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("session: open archive view %s: %w", path, err)
	}
	recs, err := loadRecords(path)
	if err != nil {
		return nil, fmt.Errorf("session: open archive view %s: %w", path, err)
	}
	v := &ArchiveView{recs: recs}
	// head 取最后一条 head 记录（与 Store.Open 同策略：文件行序即追加序）
	for i := len(recs) - 1; i >= 0; i-- {
		if r := recs[i]; r.Kind == kindHead && r.Head != nil {
			v.head = r.Head.Tail
			break
		}
	}
	return v, nil
}

// Head 返回归档会话的 head 节点 id（空文件/无 head 记录返回空串）。
func (v *ArchiveView) Head() string { return v.head }

// ActiveView 从 head 沿 parent 回溯构建归档会话的活跃视图，与 Store.ActiveView
// 共用 buildActiveView（compact 转 system 摘要、穿过 compact 回溯的语义一致）。
func (v *ArchiveView) ActiveView() []core.Message {
	return buildActiveView(v.recs, v.head)
}

// Checkpoints 返回归档会话的检查点列表，与 Store.Checkpoints 共用
// checkpointsFromRecords（倒序，limit > 0 截取最近 limit 个）。
func (v *ArchiveView) Checkpoints(limit int) []Checkpoint {
	return checkpointsFromRecords(v.recs, limit)
}
