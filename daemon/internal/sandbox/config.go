// Package sandbox 提供 shell 命令安全检查，不依赖 agent 或 engine，可被任何插件复用。
package sandbox

import (
	"fmt"
	"os"
	"regexp"

	"github.com/tinguo/goworker/daemon/internal/config"
)

// Mode 沙箱运行模式。
type Mode string

const (
	ModeNormal   Mode = "normal"   // 默认：risky 命令需要 agent 向用户请求确认
	ModeStrict   Mode = "strict"   // 拒绝所有 risky 命令
	ModeReadOnly Mode = "readonly" // 只读模式，拒绝所有写操作
	ModeOff      Mode = "off"      // 关闭沙箱（不推荐）
)

// Config 沙箱配置。
type Config struct {
	DeniedPatterns []*regexp.Regexp  // 拒绝执行的命令匹配
	RiskyPatterns  []*regexp.Regexp  // 需要用户确认的匹配
	PatternDescs   map[string]string // pattern → 人类可读描述（来自配置或内置默认）
	AllowedWorkDir string            // 限制工作目录，空 = 不限制
	AllowList      []*regexp.Regexp  // 非空时只允许匹配的命令
	ReadOnly       bool              // 只读模式
	MaxOutputBytes int               // 最大输出字节数，0=不限制
	Mode           Mode              // 沙箱模式
}

// NewFromConfig 从全局配置构建 sandbox.Config，编译正则并填充默认值。
func NewFromConfig(cfg *config.SandboxConfig) *Config {
	denied := cfg.DeniedPatterns
	if len(denied) == 0 {
		denied = DefaultDeniedPatterns
	}

	// 从 RiskPatternConfig 提取 pattern 字符串和用户自定的 desc
	var riskyStrs []string
	var patternDescs map[string]string
	if len(cfg.RiskyPatterns) == 0 {
		riskyStrs = DefaultRiskyPatterns
	} else {
		riskyStrs = make([]string, len(cfg.RiskyPatterns))
		patternDescs = make(map[string]string, len(cfg.RiskyPatterns))
		for i, r := range cfg.RiskyPatterns {
			riskyStrs[i] = r.Pattern
			if r.Desc != "" {
				patternDescs[r.Pattern] = r.Desc
			}
		}
	}

	workDir := cfg.AllowedWorkDir
	if workDir == "" {
		if wd, err := os.Getwd(); err == nil {
			workDir = wd
		}
	}

	return &Config{
		Mode:           Mode(cfg.Mode),
		AllowedWorkDir: workDir,
		DeniedPatterns: MustCompile(denied),
		RiskyPatterns:  MustCompile(riskyStrs),
		PatternDescs:   patternDescs,
		ReadOnly:       Mode(cfg.Mode) == ModeReadOnly,
	}
}

// NeedsConfirmationError 由 Check 返回，表示命令匹配风险模式，调用方应请求用户确认。
// 这是 HITL (Human-in-the-Loop) 的触发信号，不是拒绝。
type NeedsConfirmationError struct {
	Command string // 触发检查的命令
	Pattern string // 匹配的正则模式
	Reason  string // 人类可读的描述
}

func (e *NeedsConfirmationError) Error() string {
	return fmt.Sprintf("risky command: %s (%q)", e.Reason, e.Pattern)
}

// Check 检查命令 cmd 是否符合沙箱规则。
//   - 返回 nil 表示放行
//   - 返回 *NeedsConfirmationError 表示需要用户确认
//   - 返回其他 error 表示被拒绝
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

	// Risk 检测
	if cfg.Mode == ModeStrict {
		for _, p := range cfg.RiskyPatterns {
			if p.MatchString(cmd) {
				return fmt.Errorf("sandbox: risky command denied in strict mode")
			}
		}
	} else if cfg.Mode == ModeNormal {
		for _, p := range cfg.RiskyPatterns {
			if p.MatchString(cmd) {
				patStr := p.String()
				reason := PatternDesc(patStr)
				if cfg.PatternDescs != nil {
					if d, ok := cfg.PatternDescs[patStr]; ok {
						reason = d
					}
				}
				return &NeedsConfirmationError{Command: cmd, Pattern: patStr, Reason: reason}
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
