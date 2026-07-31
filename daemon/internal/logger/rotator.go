package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RotatingWriter 是按大小轮转的 io.Writer。
//
// 写入串行化：写前检查已写字节数，超过 maxSize 就把当前文件
// rename 成带时间戳的名字（app.log → app.2026-07-31T15-04-05.123.log）
// 再开新文件。单次 Write 在锁内原子完成，不会在轮转中被截断。
type RotatingWriter struct {
	mu       sync.Mutex
	path     string
	dir      string
	base     string // 去掉扩展名的文件名前缀，如 "app"
	ext      string
	maxSize  int64
	size     int64 // 当前文件已写字节数，打开时从 stat 恢复
	file     *os.File
	onRotate func() // 每次轮转后的回调（触发按天清理），可为 nil
}

// NewRotatingWriter 创建按 maxSizeBytes 轮转的 writer，maxSizeBytes<=0 时默认 10MB。
// onRotate 在每次轮转后调用，retention 清理通过它挂进来。
func NewRotatingWriter(path string, maxSizeBytes int64, onRotate func()) (*RotatingWriter, error) {
	if maxSizeBytes <= 0 {
		maxSizeBytes = 10 << 20 // 默认 10MB
	}
	w := &RotatingWriter{
		path:     path,
		dir:      filepath.Dir(path),
		base:     strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)),
		ext:      filepath.Ext(path),
		maxSize:  maxSizeBytes,
		onRotate: onRotate,
	}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

// open 打开（或创建）当前日志文件，并 stat 恢复已写字节数。
// O_APPEND 保证多进程/崩溃重启后写入不互相覆盖。
func (w *RotatingWriter) open() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("stat log file: %w", err)
	}
	w.file = f
	w.size = fi.Size()
	return nil
}

// Write 实现 io.Writer。
func (w *RotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.size >= w.maxSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

// rotate 关闭当前文件、rename 成时间戳备份、再开新文件。
// 极端情况下（同毫秒多次轮转）目标已存在时追加序号，避免覆盖丢日志。
func (w *RotatingWriter) rotate() error {
	renamed := w.nextRotateName()

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close before rotate: %w", err)
	}
	if err := os.Rename(w.path, renamed); err != nil {
		return fmt.Errorf("rotate: %w", err)
	}
	if w.onRotate != nil {
		w.onRotate()
	}
	return w.open()
}

// nextRotateName 生成轮转文件名：时间戳基准，已存在则追加 .1/.2 序号。
func (w *RotatingWriter) nextRotateName() string {
	ts := time.Now().Format("2006-01-02T15-04-05.000")
	base := filepath.Join(w.dir, w.base+"."+ts)
	for i := 0; ; i++ {
		name := base + w.ext
		if i > 0 {
			name = fmt.Sprintf("%s.%d%s", base, i, w.ext)
		}
		if _, err := os.Lstat(name); os.IsNotExist(err) {
			return name
		}
	}
}

// Close 关闭当前日志文件。
func (w *RotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
