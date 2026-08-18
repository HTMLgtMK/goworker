package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Logger 是 goworker 的结构化日志句柄，包装 *slog.Logger。
type Logger struct {
	*slog.Logger
	rotator *RotatingWriter // 文件轮转器；未配置文件时为 nil
}

// Setup 按配置创建 Logger。
//
//   - Level 非法时回落 info（见 ParseLevel）
//   - File 为空时只写 stderr
//   - File 非空时双写 stderr + 文件，并按 MaxSizeMB/MaxAgeDays 轮转与清理
//
// 文件路径不可写时返回错误，由调用方决定是否 fatal。
func Setup(cfg Config) (*Logger, error) {
	cfg = applyDefaults(cfg)

	var writer io.Writer = os.Stderr
	var rotator *RotatingWriter

	if cfg.File != "" {
		// 0700：日志目录默认仅属主可进，避免同机其他用户读到敏感日志
		if err := os.MkdirAll(filepath.Dir(cfg.File), 0700); err != nil {
			return nil, fmt.Errorf("create log dir: %w", err)
		}
		maxAge := time.Duration(cfg.MaxAgeDays) * 24 * time.Hour
		r, err := NewRotatingWriter(cfg.File, int64(cfg.MaxSizeMB)<<20, func() {
			// 轮转后顺手清掉超龄文件
			CleanupOld(filepath.Dir(cfg.File), cfg.File, maxAge)
		})
		if err != nil {
			return nil, fmt.Errorf("init rotating writer: %w", err)
		}
		rotator = r
		// 启动时也清一次，处理进程重启前的遗留文件
		CleanupOld(filepath.Dir(cfg.File), cfg.File, maxAge)
		writer = io.MultiWriter(os.Stderr, r)
	}

	s := slog.New(slog.NewTextHandler(writer, &slog.HandlerOptions{Level: ParseLevel(cfg.Level)}))
	return &Logger{Logger: s, rotator: rotator}, nil
}

// Close 关闭日志文件句柄；未配置文件时为空操作。
func (l *Logger) Close() error {
	if l.rotator != nil {
		return l.rotator.Close()
	}
	return nil
}
