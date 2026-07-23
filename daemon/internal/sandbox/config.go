// Package sandbox 提供 shell 命令安全检查，不依赖 agent 或 engine，可被任何插件复用。
package sandbox

import (
	"fmt"
	"regexp"
)

// Mode 沙箱运行模式。
type Mode string

const (
	ModeNormal   Mode = "normal"   // 默认：risky 命令需要确认
	ModeStrict   Mode = "strict"   // 拒绝所有 risky 命令
	ModeReadOnly Mode = "readonly" // 只读模式，拒绝所有写操作
	ModeOff      Mode = "off"      // 关闭沙箱（不推荐）
)

// Config 沙箱配置。
type Config struct {
	DeniedPatterns  []*regexp.Regexp // 拒绝执行的命令匹配
	RiskyPatterns   []*regexp.Regexp // 需要用户确认的匹配
	AllowedWorkDir  string           // 限制工作目录，空 = 不限制
	AllowList       []*regexp.Regexp // 非空时只允许匹配的命令
	ReadOnly        bool             // 只读模式
	MaxOutputBytes  int              // 最大输出字节数，0=不限制
	ConfirmationFn  func(cmd string) bool // 确认回调，返回 true=放行
	Mode            Mode             // 沙箱模式
}

// Check 检查命令 cmd 是否符合沙箱规则。
//   - 返回 nil 表示放行
//   - 返回 error 表示被拒绝（含拒绝原因）
func Check(cmd string, cfg *Config) error {
	if cfg == nil {
		return nil
	}
	if cfg.Mode == ModeOff {
		return nil
	}

	// Denylist 检查
	for _, p := range cfg.DeniedPatterns {
		if p.MatchString(cmd) {
			return fmt.Errorf("sandbox: command denied by pattern %q", p.String())
		}
	}

	// AllowList 检查（仅在非空时启用）
	if len(cfg.AllowList) > 0 {
		allowed := false
		for _, p := range cfg.AllowList {
			if p.MatchString(cmd) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("sandbox: command not in allowlist")
		}
	}

	// 只读模式：检查是否有写操作特征
	if cfg.ReadOnly {
		if hasWriteOps(cmd) {
			return fmt.Errorf("sandbox: write operations not allowed in readonly mode")
		}
	}

	// Risk 检测 + 确认
	if cfg.Mode == ModeStrict {
		for _, p := range cfg.RiskyPatterns {
			if p.MatchString(cmd) {
				return fmt.Errorf("sandbox: risky command denied in strict mode")
			}
		}
	} else if cfg.Mode == ModeNormal && cfg.ConfirmationFn != nil {
		for _, p := range cfg.RiskyPatterns {
			if p.MatchString(cmd) {
				if !cfg.ConfirmationFn(cmd) {
					return fmt.Errorf("sandbox: command rejected by user")
				}
				break // 只确认一次
			}
		}
	}

	return nil
}

// hasWriteOps 判断命令是否包含写操作。
func hasWriteOps(cmd string) bool {
	// 写入特征：重定向 >、dd、mkfs、rm、mv、chmod、chown
	patterns := []string{
		`>\s+`, `dd\s+`, `mkfs\.`, `rm\s+`, `mv\s+`, `chmod\s+`, `chown\s+`,
		`touch\s+`, `ln\s+`, `cp\s+`,
	}
	for _, p := range patterns {
		if regexp.MustCompile(p).MatchString(cmd) {
			return true
		}
	}
	return false
}
