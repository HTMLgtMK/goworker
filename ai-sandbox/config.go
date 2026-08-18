package sandbox

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// NewFromConfig 从全局配置构建 sandbox.Config，编译正则并填充默认值。
func NewFromConfig(cfg *SandboxConfig) *Config {
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

	// 配置追加的只读安全命令
	var safeCmds map[string]bool
	if len(cfg.SafeCommands) > 0 {
		safeCmds = make(map[string]bool, len(cfg.SafeCommands))
		for _, c := range cfg.SafeCommands {
			safeCmds[c] = true
		}
	}

	return &Config{
		Mode:           Mode(cfg.Mode),
		AllowedWorkDir: workDir,
		DeniedPatterns: MustCompile(denied),
		RiskyPatterns:  MustCompile(riskyStrs),
		PatternDescs:   patternDescs,
		ReadOnly:       Mode(cfg.Mode) == ModeReadOnly,
		SafeCommands:   safeCmds,
		AllowRules:     compileAllowRules(cfg.AllowRules),
	}
}

// compileAllowRules 把 YAML 规则编译为内部 AllowRule。非法规则（解析不了等级/空匹配）跳过——
// 配置错误不应让 daemon 崩，跳过并保持保守（该命令继续走常规评估）。
func compileAllowRules(rules []AllowRuleConfig) []AllowRule {
	out := make([]AllowRule, 0, len(rules))
	for _, r := range rules {
		lv, err := ParseRiskLevel(r.MaxRisk)
		if err != nil {
			continue
		}
		tokens := strings.Fields(r.Match)
		if len(tokens) == 0 {
			continue
		}
		out = append(out, AllowRule{
			MatchTokens: tokens,
			MaxRisk:     lv,
			Effects:     parseEffects(r.Effects),
			Desc:        r.Desc,
		})
	}
	return out
}

// parseEffects 把副作用名列表解析为位集；未知名字静默忽略。
func parseEffects(names []string) Effects {
	var e Effects
	for _, n := range names {
		switch n {
		case "file_read":
			e = e.Add(EffectFileRead)
		case "file_write":
			e = e.Add(EffectFileWrite)
		case "network":
			e = e.Add(EffectNetwork)
		case "privileged":
			e = e.Add(EffectPrivileged)
		case "destructive":
			e = e.Add(EffectDestructive)
		case "secret_access":
			e = e.Add(EffectSecretAccess)
		case "process_spawn":
			e = e.Add(EffectProcessSpawn)
		case "code_execution":
			e = e.Add(EffectCodeExecution)
		}
	}
	return e
}

func (e *NeedsConfirmationError) Error() string {
	return fmt.Sprintf("risky command: %s (%q)", e.Reason, e.Pattern)
}

// Check 检查命令 cmd 是否符合沙箱规则（兼容 shim，新逻辑见 Assess + Evaluate）。
//   - 返回 nil 表示放行
//   - 返回 *NeedsConfirmationError 表示需要用户确认
//   - 返回其他 error 表示被拒绝
func Check(cmd string, cfg *Config) error {
	if cfg == nil || cfg.Mode == ModeOff {
		return nil
	}
	return Evaluate(CommandRequest{Command: cmd}, cfg).Error()
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
