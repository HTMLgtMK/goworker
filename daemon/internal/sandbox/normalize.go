package sandbox

import (
	"path/filepath"
	"regexp"
	"strings"
)

// interpreterCmds 解释器命令：带 -c/-e 参数 = 执行任意代码。
var interpreterCmds = map[string]bool{
	"bash": true, "sh": true, "zsh": true, "ksh": true, "dash": true,
	"python": true, "python2": true, "python3": true,
	"perl": true, "ruby": true, "node": true, "php": true, "lua": true,
}

// dangerousSubMarkers 命令替换内容里的危险标记。命中即视为注入向量 → 升级 R4。
var dangerousSubMarkers = []string{
	"rm ", "mv ", "dd ", "sudo ", "chmod ", "chown ", "kill ", "pkill ",
	"mkfs", ">>", "> ", "|", "sh ", "bash", "curl", "wget", "eval",
	"python -c", "perl -e", "reboot", "shutdown",
}

// subshellRe 匹配命令替换：$(...) 或反引号内容（不做嵌套解析——检测级够用）。
var subshellRe = regexp.MustCompile(`\$\(([^)]*)\)|` + "`([^`]*)`")

// assessInjection 检测命令注入向量，命中返回升级后的等级 + CodeExecution。
// 检测点：解释器 -c/-e、eval 主命令、命令替换（内容含危险标记）、heredoc。
// 只升不降——不会把已拒绝的命令放行；denylist 在调用方已先行。
func assessInjection(cmd string) (RiskLevel, Effects, bool) {
	// 1. 解释器代码执行：bash -c '...' / python3 -c / perl -e
	for _, seg := range splitShellSegments(cmd) {
		name, args := splitMain(seg)
		if name == "" {
			continue
		}
		if interpreterCmds[filepath.Base(name)] && hasExecFlag(args, "-c", "-e", "--command") {
			return RiskR4, Effects(EffectCodeExecution), true
		}
	}

	// 2. eval 主命令：eval 是 shell 里的任意代码执行入口。
	if lv, eff, ok := evalCommand(cmd); ok {
		return lv, eff, true
	}

	// 3. 命令替换/子shell：内容含危险标记才升级（$(date) 这类安全内容不误报）。
	if code := extractSubshellCode(cmd); code != "" && containsDanger(code) {
		return RiskR4, Effects(EffectCodeExecution).Add(EffectProcessSpawn), true
	}

	// 4. heredoc：<< 输入重定向可能喂给会执行的命令。
	if hasHeredoc(cmd) {
		return RiskR2, Effects(EffectFileWrite).Add(EffectProcessSpawn), true
	}

	return 0, 0, false
}

// hasExecFlag 判断参数里是否含 -c/-e 这类执行标志。
func hasExecFlag(args []string, flags ...string) bool {
	for _, a := range args {
		for _, f := range flags {
			if a == f {
				return true
			}
		}
	}
	return false
}

// evalCommand 检测 eval 作为主命令。
func evalCommand(cmd string) (RiskLevel, Effects, bool) {
	segs := splitShellSegments(cmd)
	if len(segs) == 0 {
		return 0, 0, false
	}
	name, _ := splitMain(segs[0])
	if name == "" {
		return 0, 0, false
	}
	if filepath.Base(name) == "eval" {
		return RiskR4, Effects(EffectCodeExecution), true
	}
	return 0, 0, false
}

// extractSubshellCode 提取命令替换内容。跳过单引号内字面量（” 内不执行）。
func extractSubshellCode(cmd string) string {
	var inSingle bool
	var cleaned strings.Builder
	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		switch {
		case c == '\'' && !inSingle:
			inSingle = true
		case c == '\'' && inSingle:
			inSingle = false
		case !inSingle:
			cleaned.WriteByte(c)
		}
	}
	m := subshellRe.FindStringSubmatch(cleaned.String())
	if m == nil {
		return ""
	}
	if m[1] != "" {
		return m[1]
	}
	return m[2]
}

// containsDanger 判断命令替换内容是否含危险标记。
func containsDanger(code string) bool {
	for _, m := range dangerousSubMarkers {
		if strings.Contains(code, m) {
			return true
		}
	}
	return false
}

// hasHeredoc 引号感知检测 heredoc 输入重定向 `<<`。
func hasHeredoc(cmd string) bool {
	var inSingle, inDouble, esc bool
	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		switch {
		case esc:
			esc = false
		case c == '\\':
			esc = true
		case c == '\'' && !inDouble:
			inSingle = !inSingle
		case c == '"' && !inSingle:
			inDouble = !inDouble
		case c == '<' && !inSingle && !inDouble:
			if i+1 < len(cmd) && cmd[i+1] == '<' {
				return true
			}
		}
	}
	return false
}
