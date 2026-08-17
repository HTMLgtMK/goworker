package sandbox

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// 只读安全命令判定。三态分类：
//
//	verdictSafe    → 命中只读白名单且无写操作，直接放行，不触发 HITL
//	verdictUnsafe  → 明确识别为写操作（带写参数 / 文件重定向），交给 HITL 确认或拒绝
//	verdictUnknown → 无法判定的命令，走原有 risky pattern 逻辑
//
// denylist 在 Check 里优先：即使主命令在只读白名单（如 curl），管道到 bash/sh 这类仍先被拒。

type safeVerdict int

const (
	verdictUnknown safeVerdict = iota
	verdictSafe
	verdictUnsafe
)

// safeReadonlyCmds 默认只读安全命令 → 无需额外参数校验。
// 注意：env 不在其中 —— env 会执行其后续参数，必须当作包装器单独处理。
var safeReadonlyCmds = map[string]bool{
	"cat": true, "head": true, "tail": true, "ls": true, "grep": true,
	"egrep": true, "fgrep": true, "wc": true, "sort": true, "uniq": true,
	"cut": true, "diff": true, "stat": true, "file": true, "du": true,
	"df": true, "date": true, "whoami": true, "pwd": true, "echo": true,
	"printf": true, "which": true, "true": true, "false": true,
	"jq": true,
}

// flagCheckCmds 需要参数级只读校验的命令（带写参数时判 unsafe）。
var flagCheckCmds = map[string]bool{
	"curl": true, "wget": true, "sed": true, "awk": true, "find": true,
	"git": true, "go": true,
}

// classifyCmd 对整条命令做三态分类。extra 为配置追加的只读命令名。
// 命令按 |、||、&&、; 拆段后逐段判定，任一段 unsafe 则整体 unsafe。
func classifyCmd(cmd string, extra map[string]bool) (safeVerdict, string) {
	segs := splitShellSegments(cmd)
	if len(segs) == 0 {
		return verdictUnknown, ""
	}
	for _, seg := range segs {
		if hasFileRedirect(seg) {
			return verdictUnsafe, "输出重定向到文件"
		}
		v, reason := classifySegment(seg, extra)
		if v != verdictSafe {
			return v, reason
		}
	}
	return verdictSafe, ""
}

// classifySegment 判定单个命令段。
func classifySegment(seg string, extra map[string]bool) (safeVerdict, string) {
	name, args := splitMain(seg)
	if name == "" {
		return verdictUnknown, ""
	}
	base := filepath.Base(name) // ./bin/tool、/usr/bin/curl 归一为命令名

	// env 是命令包装器：剥掉 flag 和 VAR=x 赋值，解析真实执行的命令再判定。
	// 不能把 env 本身当只读命令，否则 `env curl -o /tmp/x` 会绕过写参数校验。
	if base == "env" {
		realName, realArgs, ok := envResolve(args)
		if !ok {
			return verdictUnknown, "" // 解析不出真实命令，交给 risky 层兜底
		}
		return classifyCommand(filepath.Base(realName), realArgs, extra)
	}
	return classifyCommand(base, args, extra)
}

// classifyCommand 判定已解析出的真实命令。
func classifyCommand(base string, args []string, extra map[string]bool) (safeVerdict, string) {
	// 用户声明的只读命令：支持 "kubectl" 裸名或 "kubectl get" 命令+子命令。
	// 命中后仍需过 flag 校验——不能因用户声明就跳过写参数检查（`safe_commands: ["curl"]` 不放过 curl -o）。
	declared := extra[base] || (len(args) > 0 && extra[base+" "+args[0]])
	if declared {
		if flagCheckCmds[base] {
			if validateFlags(base, args) {
				return verdictSafe, ""
			}
			return verdictUnsafe, fmt.Sprintf("%s 带写操作参数", base)
		}
		return verdictSafe, ""
	}
	if safeReadonlyCmds[base] {
		return verdictSafe, ""
	}
	if flagCheckCmds[base] {
		if validateFlags(base, args) {
			return verdictSafe, ""
		}
		return verdictUnsafe, fmt.Sprintf("%s 带写操作参数", base)
	}
	// 用户声明过该命令族但未声明当前子命令 → 需要确认，不放行。
	// 例：safe_commands 声明了 "kubectl get"，则 kubectl delete/apply/edit 都要确认。
	if len(args) > 0 && declaredBase(extra, base) {
		return verdictUnsafe, fmt.Sprintf("%s %s 未在 safe_commands 中声明", base, args[0])
	}
	return verdictUnknown, ""
}

// declaredBase 判断 safe_commands 是否声明过 base 这个命令族（裸名或以 base 开头的子命令）。
func declaredBase(extra map[string]bool, base string) bool {
	for k := range extra {
		if k == base || strings.HasPrefix(k, base+" ") {
			return true
		}
	}
	return false
}

// envResolve 从 env 的参数中解析出真实执行的命令及其参数。
// env [OPTION]... [VAR=VALUE]... COMMAND [ARG]...
// 返回 ok=false 表示解析不出命令（可能只剩 flag/赋值），交给上层兜底。
func envResolve(args []string) (name string, rest []string, ok bool) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case strings.Contains(a, "="):
			continue // VAR=VALUE 赋值
		case a == "-i" || a == "--ignore-environment":
			continue // 无值 flag
		case a == "-u" || a == "--unset" || a == "-C" || a == "--chdir" || a == "-S" || a == "--split-string":
			i++ // 带值 flag：跳过它的值（如 -u HOME）
		case a == "--":
			if i+1 < len(args) {
				return args[i+1], args[i+2:], true
			}
		default:
			return a, args[i+1:], true // 第一个非 flag 非赋值 token 即命令
		}
	}
	return "", nil, false
}

// splitMain 提取段的主命令名与参数，跳过 `FOO=bar` 这类 env 赋值前缀。
func splitMain(seg string) (string, []string) {
	fields := strings.Fields(seg)
	if len(fields) == 0 {
		return "", nil
	}
	i := 0
	for i < len(fields) && strings.Contains(fields[i], "=") {
		i++
	}
	if i >= len(fields) {
		return "", nil
	}
	return fields[i], fields[i+1:]
}

// splitShellSegments 按 |、||、&&、; 拆分命令段。
// 引号（'、"、\）内的分隔符忽略；`2>&1`、`>&` 这类 fd 重定向不拆分。
func splitShellSegments(cmd string) []string {
	var segs []string
	var cur strings.Builder
	var inSingle, inDouble, esc bool
	var last rune // 上一个非空白字符
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" {
			segs = append(segs, s)
		}
		cur.Reset()
	}
	for _, r := range cmd {
		switch {
		case esc:
			cur.WriteRune(r)
			esc = false
			continue
		case r == '\\':
			cur.WriteRune(r)
			esc = true
		case r == '\'' && !inDouble:
			inSingle = !inSingle
			cur.WriteRune(r)
		case r == '"' && !inSingle:
			inDouble = !inDouble
			cur.WriteRune(r)
		// 换行也是命令分隔符：`echo hi\nsudo rm -rf /tmp` 若只按首段判安全会漏掉后段高危命令。
		// 引号内的换行（多行字符串）不拆。
		case (r == '\n' || r == '\r') && !inSingle && !inDouble:
			flush()
		case (r == '|' || r == ';') && !inSingle && !inDouble:
			flush()
		case r == '&' && !inSingle && !inDouble:
			// & 出现在重定向上下文（2>&1、>&2）时不拆，交给 hasFileRedirect 判定
			if last == '>' || last == '<' {
				cur.WriteRune(r)
			} else {
				flush() // && 逻辑或后台执行，拆段
			}
		default:
			cur.WriteRune(r)
			if r != ' ' && r != '\t' {
				last = r
			}
		}
	}
	flush()
	return segs
}

// hasFileRedirect 检测段内是否包含写入文件的重定向（>、>>、&>、>|）。
// 忽略 fd 复制（2>&1、>&N）、> /dev/null。引号内的 > 是字面量，不算。
func hasFileRedirect(seg string) bool {
	var inSingle, inDouble, esc bool
	for i := 0; i < len(seg); i++ {
		c := seg[i]
		switch {
		case esc:
			esc = false
		case c == '\\':
			esc = true
		case c == '\'' && !inDouble:
			inSingle = !inSingle
		case c == '"' && !inSingle:
			inDouble = !inDouble
		case (c == '>' || c == '|') && !inSingle && !inDouble:
			// > 开头才可能是重定向；单独 | 不算（已拆段）
			if c == '|' {
				continue
			}
			// 跳过连续 >> 和空白
			j := i + 1
			for j < len(seg) && (seg[j] == '>' || seg[j] == ' ' || seg[j] == '\t') {
				j++
			}
			if j >= len(seg) {
				continue // 孤立的 >，忽略
			}
			d := seg[j]
			if d == '&' {
				// >&N：fd 复制，无害；>& file：写文件
				k := j + 1
				for k < len(seg) && (seg[k] == ' ' || seg[k] == '\t') {
					k++
				}
				if k < len(seg) && seg[k] >= '0' && seg[k] <= '9' {
					continue // 2>&1 / >&1
				}
				return true // >& file
			}
			if d == '/' {
				// /dev/null、/dev/stdout 等伪设备无害；真实设备/普通路径算写文件
				if strings.HasPrefix(seg[j:], "/dev/") && pseudoDevRedirectOnly(seg[i:]) {
					continue
				}
				return true // > /some/path
			}
			return true // > out.txt
		}
	}
	return false
}

// pseudoDevNames 无害伪设备：写它们不产生真实磁盘副作用。
var pseudoDevNames = map[string]bool{
	"null": true, "stdout": true, "stderr": true, "zero": true,
	"full": true, "random": true, "urandom": true, "tty": true, "tty0": true,
}

// pseudoDevRedirectOnly 判断命令中所有 `> /dev/...` 重定向目标是否都是无害伪设备。
// 用于豁免 denylist 的 `>\s*/dev/` 规则：`> /dev/null` 允许，`> /dev/sda` 拒绝。
// 命令里不写 /dev/（返回 false）不参与豁免判断。
func pseudoDevRedirectOnly(cmd string) bool {
	re := regexp.MustCompile(`>\s*\/dev\/([^\s;&|]+)`)
	matches := re.FindAllStringSubmatch(cmd, -1)
	if len(matches) == 0 {
		return false
	}
	for _, m := range matches {
		dev := strings.TrimSuffix(m[1], "/")
		if pseudoDevNames[dev] {
			continue
		}
		if strings.HasPrefix(dev, "fd/") { // /dev/fd/0 等
			continue
		}
		return false // 遇到真实设备，不豁免
	}
	return true
}

// validateFlags 对特定命令做参数级只读校验。返回 false 表示有写操作。
func validateFlags(base string, args []string) bool {
	switch base {
	case "sed", "awk":
		// -i / --in-place 原地改文件
		return !hasArg(args, "-i", "--in-place")
	case "find":
		// -delete/-exec/-execdir/-ok 会写或执行
		return !hasArg(args, "-delete", "-exec", "-execdir", "-ok")
	case "curl":
		return !curlHasWrite(args)
	case "wget":
		return !wgetHasWrite(args)
	case "git":
		return gitReadonly(args)
	case "go":
		return goReadonly(args)
	}
	return true
}

// hasArg 检测参数列表是否含指定 flag，兼容 -i、--in-place、-i.bak、--in-place=x。
func hasArg(args []string, names ...string) bool {
	for _, a := range args {
		for _, n := range names {
			if a == n || strings.HasPrefix(a, n+"=") {
				return true
			}
			if len(n) == 2 && strings.HasPrefix(a, n) && len(a) > 2 {
				return true // 短 flag 合并：-i.bak
			}
		}
	}
	return false
}

// curlHasWrite 检测 curl 的写操作参数（保存文件、上传、POST body、非只读方法）。
func curlHasWrite(args []string) bool {
	writeFlags := []string{
		"-o", "--output", "-O", "--remote-name", "--remote-name-all",
		"-d", "--data", "--data-raw", "--data-ascii", "--data-binary", "--data-urlencode",
		"-F", "--form", "-T", "--upload-file", "--cookie-jar",
	}
	for i, a := range args {
		for _, f := range writeFlags {
			if a == f || strings.HasPrefix(a, f+"=") {
				return true
			}
		}
		if len(a) > 2 && a[0] == '-' && a[1] != '-' {
			if short := a[:2]; short == "-o" || short == "-O" || short == "-d" || short == "-F" || short == "-T" {
				return true // 合并短 flag：-oout、-ddata
			}
		}
		// -X / --request 指定 HTTP 方法，非只读方法视为写。
		// 覆盖 -X POST（两参数）、-XPOST（合并）、--request POST、--request=DELETE。
		if a == "-X" || a == "--request" {
			if i+1 < len(args) {
				if !isReadMethod(args[i+1]) {
					return true
				}
			}
			continue
		}
		if strings.HasPrefix(a, "-X") && len(a) > 2 {
			return !isReadMethod(a[2:]) // -XPOST 合并形式
		}
		if strings.HasPrefix(a, "--request=") {
			return !isReadMethod(a[len("--request="):]) // --request=DELETE
		}
	}
	return false
}

// isReadMethod 判断 HTTP 方法是否为只读。
func isReadMethod(m string) bool {
	switch strings.ToUpper(m) {
	case "GET", "HEAD", "OPTIONS", "TRACE":
		return true
	}
	return false
}

// wgetHasWrite 检测 wget 的写操作参数。
func wgetHasWrite(args []string) bool {
	writeFlags := []string{
		"-O", "-o", "--output-document", "--output-file",
		"--post-data", "--post-file", "--save-headers",
	}
	for _, a := range args {
		for _, f := range writeFlags {
			if a == f || strings.HasPrefix(a, f+"=") {
				return true
			}
		}
		if len(a) > 2 && a[0] == '-' && a[1] != '-' {
			if short := a[:2]; short == "-O" || short == "-o" {
				return true
			}
		}
	}
	return false
}

// gitReadonly 仅放行只读子命令。
var gitReadonlySubs = map[string]bool{
	"status": true, "log": true, "diff": true, "show": true, "branch": true,
	"ls-files": true, "rev-parse": true, "remote": true, "describe": true,
	"tag": true, "version": true, "help": true, "blame": true, "grep": true,
}

func gitReadonly(args []string) bool {
	if len(args) == 0 {
		return false
	}
	return gitReadonlySubs[args[0]]
}

// goReadonly 仅放行只读子命令（go build/test 写产物，不算只读）。
var goReadonlySubs = map[string]bool{"version": true, "env": true, "list": true, "doc": true}

func goReadonly(args []string) bool {
	if len(args) == 0 {
		return false
	}
	return goReadonlySubs[args[0]]
}
