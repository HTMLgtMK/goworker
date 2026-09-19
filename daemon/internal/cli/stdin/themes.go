package stdin

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/charmbracelet/glamour/ansi"
)

// 内置主题的 JSON 样式定义。
var (
	lightStyleJSON = mustMarshalJSON(ansi.StyleConfig{
		Document: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{
				// 正文色放 Document 而不是 Text：glamour 的 Text 样式会覆盖所有内联文字
				//（包括标题内容），放 Document 才能在标题里用 Heading 自己的颜色。
				Color: strPtr("#141413"),
			},
			// Margin 0：排版缩进统一由前端的锚点列负责，glamour 只做内容渲染。
			// 主题自带 margin 会在锚点列之上叠加第二层缩进（内容看起来悬浮/居中）。
			Margin: uintPtr(0),
		},
		// Text 必须留空，否则标题内容会被强制染成正文色，Heading.Color 形同虚设。
		Text: ansi.StylePrimitive{},
		Strong: ansi.StylePrimitive{
			Bold:  boolPtr(true),
			Color: strPtr("#000000"),
		},
		Emph: ansi.StylePrimitive{
			Italic: boolPtr(true),
			Color:  strPtr("#3A3A38"),
		},
		// 标题：深色 + 加粗，浅色终端上足够醒目。前缀按级别放 H1~H6，
		// 避免 base Heading 的 block_prefix 让所有级别都显示 "## "。
		Heading: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{
				Bold:  boolPtr(true),
				Color: strPtr("#9C4A24"),
			},
		},
		H1: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "# "}},
		H2: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "## "}},
		H3: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "### "}},
		H4: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "#### "}},
		H5: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "##### "}},
		H6: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "###### "}},
		Link: ansi.StylePrimitive{
			Color:     strPtr("#3668A0"),
			Underline: boolPtr(true),
		},
		LinkText: ansi.StylePrimitive{
			Color: strPtr("#3668A0"),
		},
		Code: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{
				Color:           strPtr("#A53A2E"),
				BackgroundColor: strPtr("#E8E6DC"),
				BlockPrefix:     " ",
				BlockSuffix:     " ",
			},
		},
		CodeBlock: ansi.StyleCodeBlock{
			StyleBlock: ansi.StyleBlock{
				StylePrimitive: ansi.StylePrimitive{
					Color:           strPtr("#141413"),
					BackgroundColor: strPtr("#F3F1E8"),
				},
				Margin: uintPtr(1),
			},
			Chroma: &ansi.Chroma{
				Text:          ansi.StylePrimitive{Color: strPtr("#141413")},
				Keyword:       ansi.StylePrimitive{Color: strPtr("#A53A2E")},
				LiteralString: ansi.StylePrimitive{Color: strPtr("#4A7038")},
				Comment:       ansi.StylePrimitive{Color: strPtr("#B0AEA5")},
				LiteralNumber: ansi.StylePrimitive{Color: strPtr("#8A6A10")},
			},
		},
		Item: ansi.StylePrimitive{
			Color:       strPtr("#141413"),
			BlockPrefix: "• ",
		},
		BlockQuote: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{
				Italic: boolPtr(true),
				Faint:  boolPtr(true),
			},
			Indent:      uintPtr(1),
			IndentToken: strPtr("| "),
		},
		HorizontalRule: ansi.StylePrimitive{
			Faint:  boolPtr(true),
			Format: "\n---\n",
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

	defaultStyleJSON = mustMarshalJSON(ansi.StyleConfig{
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
				Prefix:    "# ",
			},
		},
		H2: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "## "}},
		H3: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "### "}},
		H4: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "#### "}},
		H5: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "##### "}},
		H6: ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: "###### "}},
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
)

// builtinThemes 注册表：内置主题名 → JSON 样式。
var builtinThemes = map[string][]byte{
	"default": defaultStyleJSON,
	"light":   lightStyleJSON,
}

// SetTheme 设置当前 glamour 主题。
//
// name 可以是：
//   - 内置主题名：  "default"（无颜色）、"light"（浅色暖调）
//   - JSON 文件路径：自动加载并解析
//
// 切换后缓存渲染器自动失效，下次 RenderMarkdown 使用新主题。
func SetTheme(name string) error {
	rendererMu.Lock()
	defer rendererMu.Unlock()

	// 1) 内置主题
	if j, ok := builtinThemes[name]; ok {
		minimalStyleJSON = j
		cachedRenderer = nil
		return nil
	}

	// 2) 文件路径——展开 ~ 并加载
	expanded := name
	if len(expanded) > 0 && expanded[0] == '~' {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("展开 ~ 失败: %w", err)
		}
		expanded = filepath.Join(home, expanded[1:])
	}
	j, err := os.ReadFile(expanded)
	if err != nil {
		return fmt.Errorf("未知主题 %q（不是内置主题，也不是有效文件路径）", name)
	}
	minimalStyleJSON = j
	cachedRenderer = nil
	return nil
}
