package stdin

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync"

	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/glamour/ansi"
)

// 简约风格 — 只保留结构标记，不改变终端默认颜色
//
// 设计原则：
//   - 标题：加粗（视觉区分，不改色）
//   - 行内代码：低调背景，不换色
//   - 代码块：浅背景 + 等宽，无 chroma 语法高亮
//   - 引用：暗淡斜体
//   - 列表：缩进 + 圆点
//   - 链接：下划线
//   - 表格：精简
//   - 无彩色边框/背景，不喧宾夺主
var minimalStyleJSON = mustMarshalJSON(ansi.StyleConfig{
	Document: ansi.StyleBlock{
		StylePrimitive: ansi.StylePrimitive{
			BlockPrefix: "\n",
			BlockSuffix: "\n",
		},
		Margin: uintPtr(0),
	},
	BlockQuote: ansi.StyleBlock{
		StylePrimitive: ansi.StylePrimitive{
			Italic: boolPtr(true),
			Faint:  boolPtr(true),
		},
		Indent:      uintPtr(1),
		IndentToken: strPtr("▎"),
	},
	Heading: ansi.StyleBlock{
		StylePrimitive: ansi.StylePrimitive{
			Bold: boolPtr(true),
		},
	},
	H1: ansi.StyleBlock{
		StylePrimitive: ansi.StylePrimitive{
			Bold:      boolPtr(true),
			Underline: boolPtr(true),
		},
	},
	Strong: ansi.StylePrimitive{
		Bold: boolPtr(true),
	},
	Emph: ansi.StylePrimitive{
		Italic: boolPtr(true),
	},
	HorizontalRule: ansi.StylePrimitive{
		Faint:  boolPtr(true),
		Format: "\n────\n",
	},
	Item: ansi.StylePrimitive{
		BlockPrefix: "• ",
	},
	Link: ansi.StylePrimitive{
		Underline: boolPtr(true),
	},
	Code: ansi.StyleBlock{
		StylePrimitive: ansi.StylePrimitive{
			BackgroundColor: strPtr("#2d2d2d"),
		},
	},
	CodeBlock: ansi.StyleCodeBlock{
		StyleBlock: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{
				BackgroundColor: strPtr("#1a1a1a"),
			},
			Margin: uintPtr(0),
		},
		Chroma: &ansi.Chroma{
			Text: ansi.StylePrimitive{Color: strPtr("#e0e0e0")},
		},
	},
	List: ansi.StyleList{
		StyleBlock: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{},
		},
		LevelIndent: 2,
	},
	Table: ansi.StyleTable{
		StyleBlock: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{},
		},
	},
})

// getRenderer 按需创建 glamour 渲染器，终端宽度变化时自动重建。
var (
	rendererMu     sync.Mutex
	rendererWidth  int
	cachedRenderer *glamour.TermRenderer
)

func getRenderer(width int) *glamour.TermRenderer {
	rendererMu.Lock()
	defer rendererMu.Unlock()

	if cachedRenderer != nil && rendererWidth == width {
		return cachedRenderer
	}

	r, err := glamour.NewTermRenderer(
		glamour.WithStylesFromJSONBytes(minimalStyleJSON),
		glamour.WithWordWrap(width),
	)
	if err != nil {
		return nil
	}
	cachedRenderer = r
	rendererWidth = width
	return r
}

// hrPattern 匹配 glamour 渲染后的水平分割线（纯文本 `─{4}` + 尾部空格，无 ANSI 封装）。
var hrPattern = regexp.MustCompile(`(?m)^─{3,} *$`)

// postProcessAnsi 后处理 glamour 的 ANSI 输出：修复 HR 宽度 + 表头 cell 间距 + 去除前导空白行。
func postProcessAnsi(s string, width int) string {
	s = hrPattern.ReplaceAllString(s, "\x1b[2m"+strings.Repeat("─", width)+"\x1b[0m")
	s = normalizeTableCells(s)
	s = trimBlankLines(s)
	return s
}

// trimBlankLines 去除 glamour 输出中前后的全空白行（Document style 的 margin/prefix 残留），
// 但保留内容行内部的 leading/trailing 空格（表头 cell 前导空格不能丢）。
func trimBlankLines(s string) string {
	lines := strings.Split(s, "\n")
	start := 0
	for start < len(lines) && strings.TrimSpace(lines[start]) == "" {
		start++
	}
	end := len(lines)
	for end > start && strings.TrimSpace(lines[end-1]) == "" {
		end--
	}
	return strings.Join(lines[start:end], "\n")
}

// normalizeTableCells 统一表格每列 cell 的前导空格数，修复 glamour lipgloss
// 对表头与数据行的 margin 不一致问题——有的 cell 1 个空格，有的 2 个。
// 策略：每列 cell 统一 strip 前导空格（保留 cell 内容），然后靠 │ 分隔。
func normalizeTableCells(s string) string {
	lines := strings.Split(s, "\n")
	i := 0
	for i < len(lines) {
		if !strings.Contains(lines[i], "│") {
			i++
			continue
		}
		start := i
		for i < len(lines) && strings.Contains(lines[i], "│") {
			i++
		}
		table := lines[start:i]
		for j, line := range table {
			if strings.Contains(line, "─") || strings.Contains(line, "┼") {
				continue // 分隔线不改
			}
			cells := strings.Split(line, "│")
			for k := range cells {
				if k == 0 {
					// 首列：去掉 glamour margin 加的前导空格
					cells[k] = strings.TrimLeft(cells[k], " ")
				} else {
					// 后续列：保留 "│ " 分隔格式，去掉多余前导空格
					cells[k] = " " + strings.TrimLeft(cells[k], " ")
				}
			}
			table[j] = strings.Join(cells, "│")
		}
	}
	return strings.Join(lines, "\n")
}

// RenderMarkdown 渲染 markdown 为带简约 ANSI 风格的终端输出。
// termWidth 为终端列数，用于 word wrap 和水平分割线宽度。
func RenderMarkdown(text string, termWidth int) string {
	r := getRenderer(termWidth)
	if r == nil {
		return text
	}
	out, err := r.Render(text)
	if err != nil {
		return text
	}
	return postProcessAnsi(out, termWidth)
}

// ---- helpers ----

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }
func uintPtr(u uint) *uint    { return &u }

func mustMarshalJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
