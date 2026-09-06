package stdin

import (
	"regexp"
	"strings"
)

// trailingJunk 匹配行尾的空白与 SGR 序列混合串：glamour 的行尾 padding
// 是"每个空格单独包一层颜色序列"，纯 TrimRight 剥不掉。
var trailingJunk = regexp.MustCompile(`(?:\[[0-9;]*m|[ \t])+$`)

// layoutBlock 把 glamour 渲染好的内容装配到锚点列下，实现"锚点独占一列、
// 续行从锚点间距起"的悬挂缩进：
//
//	● 首行内容……
//	  续行对齐到锚点列……
//	    列表自身的层级缩进叠加在锚点列之上
//
// 排版权在前端：glamour 只负责块内容的语法渲染（颜色/列表符号/代码高亮），
// 行装配（去 padding、压空行、锚点对齐）统一在这里做。
func layoutBlock(m blockMarker, lines []string) string {
	var b strings.Builder
	pad := strings.Repeat(" ", m.indent)
	for i, line := range lines {
		if i == 0 {
			b.WriteString(m.glyph)
		} else {
			b.WriteString("\n")
			if line != "" {
				// 空行不垫缩进空格，避免行尾幽灵空白
				b.WriteString(pad)
			}
		}
		b.WriteString(line)
	}
	return b.String()
}

// normalizeRendered 整理 glamour 输出的行：
//   - 去每行尾部空格：glamour 会把行 padding 到全宽，叠加前端缩进后总宽
//     必然超出终端，触发折行错位（排版碎片飞到屏幕右缘的根源）
//   - 去首尾空行、连续空行压缩为一个：块间留一个空行，间距由前端统一控制
func normalizeRendered(rendered string) []string {
	raw := strings.Split(rendered, "\n")
	lines := make([]string, 0, len(raw))
	for _, l := range raw {
		l = trailingJunk.ReplaceAllString(l, "")
		if stripANSI(l) == "" {
			// 纯 SGR + 空白的行视为空行，统一存成干净空串
			l = ""
		}
		if l == "" && (len(lines) == 0 || lines[len(lines)-1] == "") {
			continue
		}
		lines = append(lines, l)
	}
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}
