package stdin

import (
	"sort"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// 本文件是命令 autocomplete 的纯逻辑模块（specs/002-command-autocomplete）：
// 候选构建（engine.Commands() → 别名归并）、前缀匹配、cycle 状态机。
// 不碰终端 —— editor 只做按键分派与渲染，这里的逻辑可完全表驱动测试。

// tabOutcome 是一次 Tab/Shift-Tab 的结果类别。
type tabOutcome int

const (
	tabNoMatch tabOutcome = iota // 无匹配候选：编辑器响铃，文本不动
	tabUnique                    // 唯一候选：text 已补全并带尾随空格
	tabCycling                   // 多候选循环：text 为当前候选，list 非空
)

// candidate 是一个可补全项。completion 恒为 canonical 命令名（含前导 /）。
type candidate struct {
	completion  string
	description string
	tokens      []string // 可匹配的 token（canonical + 别名，全小写）
}

// completer 持有全量候选与 cycle 状态。仅在被 ReadLine 调用的 goroutine 上使用，无并发。
type completer struct {
	candidates []candidate
	cycling    bool
	cycleIdx   int
	list       []candidate // cycle 中的候选集
	current    string      // 最近一次产出的文本；cycle 期间输入漂移即退出重算
}

// newCompleter 从命令清单构建候选（含别名归并），按命令名字典序稳定排序。
func newCompleter(cmds []plugin.Command) *completer {
	sorted := make([]plugin.Command, len(cmds))
	copy(sorted, cmds)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	cands := make([]candidate, 0, len(sorted))
	for _, cmd := range sorted {
		tokens := []string{strings.ToLower(cmd.Name)}
		for _, a := range cmd.Aliases {
			tokens = append(tokens, strings.ToLower(a))
		}
		cands = append(cands, candidate{
			completion:  cmd.Name,
			description: cmd.Description,
			tokens:      tokens,
		})
	}
	// /quit 不走 engine（Run 主循环直接拦截），候选里补上，别名一并归并
	cands = append(cands, candidate{
		completion:  "/quit",
		description: "退出 goworker",
		tokens:      []string{"/quit", "/exit", "/q"},
	})
	return &completer{candidates: cands}
}

// reset 退出 cycle 模式（任意编辑键触发）。
func (c *completer) reset() {
	c.cycling = false
	c.list = nil
}

// complete 处理一次 Tab（forward=true）/ Shift-Tab（false）。
// 调用前提：editor 已判定行以 / 开头、光标在首个词的行尾。
// 返回结果类别、补全后的完整文本、cycle 候选列表与当前高亮下标。
func (c *completer) complete(input string, forward bool) (tabOutcome, string, []candidate, int) {
	// cycle 期间输入被改动（漂移保护）：退出 cycle 重新匹配
	if c.cycling && input != c.current {
		c.reset()
	}

	if c.cycling {
		n := len(c.list)
		if forward {
			c.cycleIdx = (c.cycleIdx + 1) % n
		} else {
			c.cycleIdx = (c.cycleIdx - 1 + n) % n
		}
		text := c.list[c.cycleIdx].completion
		c.current = text
		return tabCycling, text, c.list, c.cycleIdx
	}

	matches := c.match(input)
	switch {
	case len(matches) == 0:
		return tabNoMatch, input, nil, 0
	case len(matches) == 1:
		// 唯一候选直接补全 + 尾随空格（参数从空格后开始）
		text := matches[0].completion + " "
		c.current = text
		return tabUnique, text, nil, 0
	default:
		c.cycling = true
		c.list = matches
		c.cycleIdx = 0
		c.current = matches[0].completion
		return tabCycling, c.current, matches, 0
	}
}

// match 返回前缀命中（canonical 名或任一别名，大小写不敏感）的候选，
// 保持 candidates 的字典序 —— map 无序，顺序稳定性全靠构建期排序。
func (c *completer) match(input string) []candidate {
	prefix := strings.ToLower(input) // input 以 / 开头（editor 已判定）
	var out []candidate
	for _, cand := range c.candidates {
		for _, tok := range cand.tokens {
			if strings.HasPrefix(tok, prefix) {
				out = append(out, cand)
				break
			}
		}
	}
	return out
}
