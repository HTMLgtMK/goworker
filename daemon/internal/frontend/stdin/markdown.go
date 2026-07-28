package stdin

import (
	"encoding/json"

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

var minimalRenderer *glamour.TermRenderer

func init() {
	r, err := glamour.NewTermRenderer(
		glamour.WithStylesFromJSONBytes(minimalStyleJSON),
		glamour.WithWordWrap(120),
	)
	if err == nil {
		minimalRenderer = r
	}
}

// RenderMarkdown 渲染 markdown 为带简约 ANSI 风格的终端输出。
func RenderMarkdown(text string) string {
	if minimalRenderer == nil {
		return text
	}
	out, err := minimalRenderer.Render(text)
	if err != nil {
		return text
	}
	return out
}

// ---- helpers ----

func strPtr(s string) *string  { return &s }
func boolPtr(b bool) *bool     { return &b }
func uintPtr(u uint) *uint     { return &u }

func mustMarshalJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
