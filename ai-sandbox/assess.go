package sandbox

import (
	"path/filepath"
	"strings"
)

// writeCommandLevels 结构化写命令表：命令 → 风险等级 + 副作用。
// 这是"规则 ≠ 模型"方向：结构化命令知识替代纯字符串正则，同时喂给 Effects。
// 分级依据：写工作区=R2、破坏/移动/权限变更=R3、提权/进程控制=R4、denylist 级=R5。
var writeCommandLevels = map[string]struct {
	level   RiskLevel
	effects Effects
}{
	"rm":       {RiskR3, Effects(EffectDestructive)},
	"mv":       {RiskR3, Effects(EffectFileWrite)},
	"dd":       {RiskR3, Effects(EffectDestructive)},
	"cp":       {RiskR2, Effects(EffectFileWrite)},
	"touch":    {RiskR2, Effects(EffectFileWrite)},
	"mkdir":    {RiskR2, Effects(EffectFileWrite)},
	"rmdir":    {RiskR2, Effects(EffectFileWrite)},
	"ln":       {RiskR2, Effects(EffectFileWrite)},
	"tee":      {RiskR2, Effects(EffectFileWrite)},
	"truncate": {RiskR2, Effects(EffectFileWrite)},
	"chmod":    {RiskR4, Effects(EffectPrivileged)},
	"chown":    {RiskR4, Effects(EffectPrivileged)},
	"kill":     {RiskR4, Effects(EffectProcessSpawn)},
	"pkill":    {RiskR4, Effects(EffectProcessSpawn)},
	"killall":  {RiskR4, Effects(EffectProcessSpawn)},
	"mkfs":     {RiskR5, Effects(EffectDestructive)},
	"shutdown": {RiskR4, Effects(EffectProcessSpawn)},
	"reboot":   {RiskR4, Effects(EffectProcessSpawn)},
}

// readNetworkCmds 只读但跨网络的命令 → 审计标 Network 副作用。
var readNetworkCmds = map[string]bool{"curl": true, "wget": true}

// Assess 对整条命令输出风险评估。纯函数，无 I/O。
// 检测顺序（只升不降，绝不把已拒绝的命令放行）：
//
//	denylist → 三态分类(safe/unsafe) → 写命令表 → readonly 扫描 → risky pattern → unclassified
func Assess(cmd string, cfg *Config) RiskAssessment {
	if cfg == nil || cfg.Mode == ModeOff {
		return RiskAssessment{Level: RiskR0, Source: SourceBypass, Confidence: 1}
	}

	// 1. denylist：硬拒。伪设备豁免只作用于内置 `>\s*/dev/` 规则。
	for _, p := range cfg.DeniedPatterns {
		if !p.MatchString(cmd) {
			continue
		}
		if isBuiltinDevWritePattern(p) && pseudoDevRedirectOnly(cmd) {
			continue
		}
		// denylist 保守双标副作用：破坏 + 代码执行都可能。审计偏保守无害，决策不受影响（恒 deny）。
		return RiskAssessment{
			Level:      RiskR5,
			Effects:    Effects(EffectDestructive).Add(EffectCodeExecution),
			Reasons:    []Reason{{Code: "deny_pattern", Pattern: p.String(), Detail: "拒绝执行的危险命令"}},
			Source:     SourceRule,
			Confidence: 1,
		}
	}

	// 1.5 命令注入向量（子shell/解释器 -c/eval/heredoc）。denylist 之后、classify 之前——
	//    否则 `echo hi` 会因分类 safe 提前返回，绕过对后段注入的检查。只升不降。
	if lv, eff, ok := assessInjection(cmd); ok {
		return RiskAssessment{
			Level:      lv,
			Effects:    eff,
			Reasons:    []Reason{{Code: "injection", Detail: "检测到命令注入向量"}},
			Source:     SourceHeuristic,
			Confidence: 1,
		}
	}

	// 2. 三态分类：safe 直接放行（R1）；unsafe（带写参数/重定向）交给 assessUnsafe。
	if verdict, reason := classifyCmd(cmd, cfg.SafeCommands); verdict == verdictSafe {
		return RiskAssessment{
			Level:      RiskR1,
			Effects:    assessReadEffects(cmd),
			Reasons:    []Reason{{Code: "safe_readonly", Detail: reason}},
			Source:     SourceClassifier,
			Confidence: 1,
		}
	} else if verdict == verdictUnsafe {
		return assessUnsafe(cmd)
	}

	// 3a. 结构化写命令表命中（rm/mv/touch/chmod...）→ 表内等级 + 副作用。
	if lv, eff, ok := assessWriteCommand(cmd); ok {
		return RiskAssessment{
			Level:      lv,
			Effects:    eff,
			Reasons:    []Reason{{Code: "write_command", Detail: "写操作命令"}},
			Source:     SourceHeuristic,
			Confidence: 1,
		}
	}

	// 3b. readonly 模式：覆盖 verdictUnknown 的写特征（cp/mv 等）。
	if cfg.ReadOnly && hasWriteOps(cmd) {
		return RiskAssessment{
			Level:      RiskR2,
			Effects:    Effects(EffectFileWrite),
			Reasons:    []Reason{{Code: "write_op", Detail: "只读模式检测到写操作"}},
			Source:     SourceHeuristic,
			Confidence: 1,
		}
	}

	// 3c. risky pattern 命中 → 等级/副作用映射（默认 R2）。
	if lv, eff, pat, ok := assessRiskyPattern(cmd, cfg); ok {
		return RiskAssessment{
			Level:      lv,
			Effects:    eff,
			Reasons:    []Reason{{Code: "risky_pattern", Pattern: pat, Detail: PatternDesc(pat)}},
			Source:     SourceRule,
			Confidence: 1,
		}
	}

	// 3d. 什么都没命中 → R6 unclassified，默认不信任（Unknown ≠ Safe）。
	return RiskAssessment{
		Level:      RiskR6,
		Reasons:    []Reason{{Code: "unclassified", Detail: "无法分类的命令，默认不信任"}},
		Source:     SourceUnclassified,
		Confidence: 0,
	}
}

// assessReadEffects 给只读命令标副作用：读操作 + 跨网络命令。
func assessReadEffects(cmd string) Effects {
	e := Effects(EffectFileRead)
	for _, seg := range splitShellSegments(cmd) {
		name, _ := splitMain(seg)
		if name == "" {
			continue
		}
		if readNetworkCmds[filepath.Base(name)] {
			e = e.Add(EffectNetwork)
		}
	}
	return e
}

// assessUnsafe 处理已判定 unsafe 的命令（带写参数/重定向）：副作用 + floor 等级。
func assessUnsafe(cmd string) RiskAssessment {
	effects := Effects(0)
	if hasFileRedirect(cmd) {
		effects = effects.Add(EffectFileWrite)
	}
	level := RiskR2 // unsafe 至少 R2

	for _, seg := range splitShellSegments(cmd) {
		name, args := splitMain(seg)
		if name == "" {
			continue
		}
		base := filepath.Base(name)
		if w, ok := writeCommandLevels[base]; ok {
			effects = effects | w.effects
			if w.level > level {
				level = w.level
			}
		}
		switch base {
		case "curl":
			if curlHasWrite(args) {
				effects = effects.Add(EffectNetwork)
				if curlSavesFile(args) {
					effects = effects.Add(EffectFileWrite)
				}
			}
		case "wget":
			if wgetHasWrite(args) {
				effects = effects.Add(EffectNetwork)
				if wgetSavesFile(args) {
					effects = effects.Add(EffectFileWrite)
				}
			}
		case "sed", "awk":
			if hasArg(args, "-i", "--in-place") {
				effects = effects.Add(EffectFileWrite)
			}
		case "git":
			// 能走到 unsafe 说明 gitReadonly 已判 false（写子命令）。
			// push 更新远端 + 本地 refs；--force 强制覆盖 → destructive。
			eff := Effects(EffectNetwork).Add(EffectFileWrite)
			if hasArg(args, "--force", "-f", "--force-with-lease") {
				eff = eff.Add(EffectDestructive)
				level = maxLevel(level, RiskR3)
			}
			effects = effects | eff
			level = maxLevel(level, RiskR2)
		}
	}

	return RiskAssessment{
		Level:      level,
		Effects:    effects,
		Reasons:    []Reason{{Code: "unsafe_args", Detail: "命令带写操作参数"}},
		Source:     SourceClassifier,
		Confidence: 1,
	}
}

// assessWriteCommand 在主命令命中写命令表时返回其等级 + 副作用。
func assessWriteCommand(cmd string) (RiskLevel, Effects, bool) {
	for _, seg := range splitShellSegments(cmd) {
		name, _ := splitMain(seg)
		if name == "" {
			continue
		}
		if w, ok := writeCommandLevels[filepath.Base(name)]; ok {
			return w.level, w.effects, true
		}
	}
	return 0, 0, false
}

// curlSavesFile 判断 curl 是否保存文件到本地（-o/-O/-T 及合并短 flag）。
func curlSavesFile(args []string) bool {
	for _, a := range args {
		switch {
		case a == "-o" || a == "--output" || a == "-O" || a == "--remote-name" ||
			a == "--remote-name-all" || a == "-T" || a == "--upload-file":
			return true
		case strings.HasPrefix(a, "--output=") || strings.HasPrefix(a, "--upload-file="):
			return true
		}
		if len(a) > 2 && a[0] == '-' && a[1] != '-' {
			if short := a[:2]; short == "-o" || short == "-O" || short == "-T" {
				return true
			}
		}
	}
	return false
}

// wgetSavesFile 判断 wget 是否保存文件到本地（-O/--output-document 及合并短 flag）。
func wgetSavesFile(args []string) bool {
	for _, a := range args {
		switch {
		case a == "-O" || a == "--output-document":
			return true
		case strings.HasPrefix(a, "--output-document="):
			return true
		}
		if len(a) > 2 && a[0] == '-' && a[1] != '-' && a[:2] == "-O" {
			return true
		}
	}
	return false
}

// assessRiskyPattern 命中 risky 模式时返回等级 + 副作用 + 模式原文。
// 未映射的模式保守默认 R2。
func assessRiskyPattern(cmd string, cfg *Config) (RiskLevel, Effects, string, bool) {
	for _, p := range cfg.RiskyPatterns {
		if !p.MatchString(cmd) {
			continue
		}
		patStr := p.String()
		lv := riskPatternLevel[patStr]
		if lv == 0 {
			lv = RiskR2
		}
		return lv, riskPatternEffects[patStr], patStr, true
	}
	return 0, 0, "", false
}
