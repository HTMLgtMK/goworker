package skills

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Discover 扫描多个根目录，返回全部可用 skill 及加载中的错误。
//
// 每个根目录下，一层子目录 + 其中 SKILL.md 构成一个 skill。
// 同名 skill 后者覆盖前者（调用方把项目级目录放最后，实现项目级优先）。
// 单个坏 skill（frontmatter 解析失败、name 非法）只跳过并记入 errs，
// 不拖垮其他 skill —— 目录不存在、缺 SKILL.md 的普通目录则完全静默跳过。
func Discover(dirs ...string) ([]Skill, []error) {
	found := make(map[string]Skill)
	var order []string // 记录首次出现顺序，保证输出稳定（map 迭代无序）
	var errs []error

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			errs = append(errs, fmt.Errorf("scan skills dir %s: %w", dir, err))
			continue
		}

		for _, e := range entries {
			if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			skill, ok, err := parseSKILLMD(filepath.Join(dir, e.Name(), SKILLMD))
			if err != nil {
				// 目录里没 SKILL.md 是正常情况（组织用目录），跳过而非报错
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				errs = append(errs, err)
				continue
			}
			if !ok {
				continue
			}
			if _, exists := found[skill.Name]; !exists {
				order = append(order, skill.Name)
			}
			found[skill.Name] = skill
		}
	}

	out := make([]Skill, 0, len(order))
	for _, name := range order {
		out = append(out, found[name])
	}
	return out, errs
}
