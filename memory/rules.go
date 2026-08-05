package memory

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// InstructionSet 是声明式指令的结果载体，只有两个字段：
//   - snapshot：注入 system prompt 的文本，构造后冻结
//   - profile：  写 USER.md 的把手（agent 经 profile 工具写入）
//
// 刷新 = 重新 LoadInstructions 换新对象，不做就地更新。冻结语义因此天然成立：
// profile 写入只改 USER.md 文件，snapshot 一动不动。
type InstructionSet struct {
	snapshot string   // 注入文本（不可变）
	profile  *Profile // USER.md 写模型（可变）
}

// Cap 是声明式指令的容量配置。
type Cap struct {
	UserMaxChars   int // USER.md 容量上限（rune），0 = 默认 1500
	AgentsMaxChars int // AGENTS.md 合并注入上限，0 = 默认 4096
}

// LoadInstructions 读三份指令文件、渲染出注入文本，返回结果载体。
// 每层独立容错：缺文件当空层；真读失败（权限/IO）降级该层并告警，不拖垮其他层。
func LoadInstructions(dir, cwd string, caps Cap) (*InstructionSet, error) {
	userPath := filepath.Join(dir, "USER.md")
	globalAgentPath := filepath.Join(dir, "AGENTS.md")

	profile := newProfile(userPath, caps.UserMaxChars)
	if err := profile.reload(); err != nil && !os.IsNotExist(err) {
		slog.Warn("instructions: USER.md read failed, using empty profile", "path", userPath, "err", err)
	}
	// 项目 AGENTS.md：cwd 向上找最近的一个。若恰好命中全局文件（cwd 在 config
	// 目录下运行时）则跳过 —— 全局已单独读，避免重复注入。
	projPath := findUpward(cwd, "AGENTS.md")
	if projPath == globalAgentPath {
		projPath = ""
	}
	agents := mergeAgentFiles(globalAgentPath, projPath, defaultInt(caps.AgentsMaxChars, 4096))
	return &InstructionSet{
		snapshot: render(profile.Content(), agents, profile.maxChars),
		profile:  profile,
	}, nil
}

// mergeAgentFiles 读全局+项目 AGENTS.md 并合并。缺文件当空层，真读失败降级该层并告警。
func mergeAgentFiles(globalPath, projPath string, maxChars int) string {
	global := readFileOrEmpty(globalPath, "global AGENTS.md")
	var proj string
	if projPath != "" {
		proj = readFileOrEmpty(projPath, "project AGENTS.md")
	}
	return mergeAgents(global, proj, maxChars)
}

// readFileOrEmpty 读文件。IsNotExist 与真错误都返回空串，但真错误打告警（不静默吞）。
func readFileOrEmpty(path, what string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("instructions: "+what+" read failed, skipping", "path", path, "err", err)
		}
		return ""
	}
	return string(data)
}

// Snapshot 返回注入文本。
func (s *InstructionSet) Snapshot() string { return s.snapshot }

// Profile 返回 USER.md 写模型。
func (s *InstructionSet) Profile() *Profile { return s.profile }

// render 拼注入块：base → [Instructions]（agents）→ [User]（user）。
// 顺序即优先级：用户画像放最后 = 冲突时用户说了算（Hermes 语义）。
// user 是手写文件可能超限，注入时防御性截断，但头部如实显示超了多少。
func render(user, agents string, maxUserChars int) string {
	var b strings.Builder
	if a := strings.TrimSpace(agents); a != "" {
		b.WriteString("\n[Instructions] The following instructions come from AGENTS.md (global first, project after; project takes precedence on conflict):\n")
		b.WriteString(a)
		b.WriteString("\n")
	}
	if u := strings.TrimSpace(user); u != "" {
		used := runeLen(u)
		if maxUserChars > 0 && used > maxUserChars {
			u = TruncateRunes(u, maxUserChars)
		}
		fmt.Fprintf(&b, "\n[User] Your user profile, from USER.md (used %d/%d chars):\n", used, maxUserChars)
		b.WriteString(u)
		b.WriteString("\n")
	}
	return b.String()
}

// mergeAgents 拼接全局+项目 AGENTS.md（全局在前、项目在后）。
// 项目优先：超限时从头部（全局）开始丢，保住排在后边的项目指令 —— 渲染头声明的
// "project takes precedence on conflict" 靠项目排最后实现，若从尾部截断会恰好
// 砍掉优先级最高的那段。
func mergeAgents(global, project string, maxChars int) string {
	out := strings.TrimSpace(global)
	if p := strings.TrimSpace(project); p != "" {
		if out != "" {
			out += "\n\n"
		}
		out += p
	}
	if maxChars > 0 && runeLen(out) > maxChars {
		out = truncateLeading(out, maxChars)
	}
	return out
}

// ---- 小工具 ----

func defaultInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// findUpward 从 start 一路向上找最近含 filename 的目录，返回该文件路径；找不到返回 ""。
func findUpward(start, filename string) string {
	dir := start
	for {
		candidate := filepath.Join(dir, filename)
		if fi, err := os.Stat(candidate); err == nil && !fi.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func runeLen(s string) int { return utf8.RuneCountInString(s) }

// TruncateRunes 保留前 n 个 rune，超长加 "…" 后缀。给 middlewares 的注入截断复用，
// 避免两条注入链各自演化的截断逻辑分叉。
func TruncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// truncateLeading 保留尾部 max 个 rune（优先级最高的内容在后），从头部丢，加 "…" 前缀。
func truncateLeading(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return "…" + string(r[len(r)-max:])
}
