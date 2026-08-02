// Package skills 提供可复用指令包（skill）的发现与解析。
//
// 一个 skill 就是一个目录下的 SKILL.md：frontmatter 声明 name/description，
// 正文是给 LLM 的指令。agent 把每个 skill 注册成工具，按需加载内容。
package skills

// Skill 是一个可复用的指令包。
type Skill struct {
	Name        string // 唯一标识（同时是合法工具名后缀，见 parse 的校验）
	Description string // 给 LLM 判断何时加载该技能
	Content     string // SKILL.md 正文（去掉 frontmatter）
	Source      string // 来源目录，调试用
}
