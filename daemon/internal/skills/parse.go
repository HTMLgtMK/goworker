package skills

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// SKILLMD 是 skill 定义文件名。
const SKILLMD = "SKILL.md"

// frontmatterRe 匹配开头的 --- frontmatter 块。
// (?s) 让 . 匹配换行；\r?\n 容忍 CRLF；正文可能为空。
// BOM 在读取后显式剥掉，不放正则里（正则要直观）。
var frontmatterRe = regexp.MustCompile(`(?s)^---\r?\n(.*?)\r?\n---\r?\n?(.*)$`)

// skillNameRe 限定 name 只含工具名合法字符，且最长 32。
// skill 会注册成 "skill_"+name 的 LLM 工具（OpenAI 要求 ^[a-zA-Z0-9_-]{1,64}$），
// 留足前缀余量，在源头卡死而非等到注册时炸。
var skillNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,32}$`)

type metadata struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

// parseSKILLMD 解析单个 SKILL.md 文件。
// ok=false 表示文件存在但没有合法 frontmatter（不是 skill 文件）。
func parseSKILLMD(path string) (Skill, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Skill{}, false, err
	}
	// 剥掉 UTF-8 BOM，避免 Windows 编辑器产物匹配不上 frontmatter
	data = []byte(strings.TrimPrefix(string(data), "\ufeff"))

	m := frontmatterRe.FindSubmatch(data)
	if m == nil {
		return Skill{}, false, nil
	}

	var meta metadata
	if err := yaml.Unmarshal(m[1], &meta); err != nil {
		return Skill{}, false, fmt.Errorf("parse frontmatter %s: %w", path, err)
	}

	name := strings.TrimSpace(meta.Name)
	if name == "" {
		return Skill{}, false, fmt.Errorf("skill %s: frontmatter 缺 name", path)
	}
	if !skillNameRe.MatchString(name) {
		return Skill{}, false, fmt.Errorf("skill %s: name %q 含非法字符（仅允许字母数字_ -）", path, name)
	}

	return Skill{
		Name:        name,
		Description: strings.TrimSpace(meta.Description),
		Content:     strings.TrimSpace(string(m[2])),
		Source:      filepath.Dir(path),
	}, true, nil
}
