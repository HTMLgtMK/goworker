package logger

import (
	"os"
	"path/filepath"
	"time"
)

// CleanupOld 删除 dir 下 mtime 超过 maxAge 的 *.log 文件，返回删除数量。
//
// active 是当前正在写入的日志文件路径，始终跳过——删除活跃文件
// 会导致后续写盘直接失败。目录不存在时视为空目录，返回 0。
func CleanupOld(dir, active string, maxAge time.Duration) (int, error) {
	if maxAge <= 0 {
		return 0, nil
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(matches) == 0 {
		return 0, err
	}

	cutoff := time.Now().Add(-maxAge)
	removed := 0
	for _, f := range matches {
		abs, _ := filepath.Abs(f)
		if abs == active {
			continue // 活跃文件永不清理
		}
		fi, err := os.Stat(f)
		if err != nil {
			continue // 刚被轮转走/权限问题，跳过
		}
		if fi.ModTime().Before(cutoff) {
			if err := os.Remove(f); err == nil {
				removed++
			}
		}
	}
	return removed, nil
}
