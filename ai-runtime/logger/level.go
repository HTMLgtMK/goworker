// Package logger 提供 goworker 的结构化日志。
//
// 基于标准库 log/slog，支持等级过滤、写入日志文件、
// 按大小轮转与按天数清理。
package logger

import (
	"log/slog"
	"strings"
)

// levelNames 是配置文件里可用的等级字符串到 slog.Level 的映射。
var levelNames = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

// ParseLevel 把字符串解析为 slog.Level，大小写不敏感、容忍前后空格。
// 非法或空值回落到 info —— 日志系统不能因为配置写错就起不来。
func ParseLevel(s string) slog.Level {
	if l, ok := levelNames[normalize(s)]; ok {
		return l
	}
	return slog.LevelInfo
}

// ValidLevel 报告字符串是否为合法等级。
func ValidLevel(s string) bool {
	_, ok := levelNames[normalize(s)]
	return ok
}

// LevelName 返回 slog.Level 的可读名（debug/info/warn/error）。
func LevelName(l slog.Level) string {
	switch l {
	case slog.LevelDebug:
		return "debug"
	case slog.LevelWarn:
		return "warn"
	case slog.LevelError:
		return "error"
	default:
		return "info"
	}
}

// normalize 统一小写并去空格，避免配置里写 "INFO" 或 " info " 就翻车。
func normalize(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}
