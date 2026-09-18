package stdin

import "strings"

// blockMarker 定义编组块的锚点：首行前缀 glyph + 延续行缩进列数 indent。
// 注意：indent 必须是 glyph 的终端显示宽度（列数），不是字节长度——
// ●/⎿ 是 3 字节宽，len() 会算错导致延续行错位。
type blockMarker struct {
	glyph  string
	indent int
}

var (
	markerText     = blockMarker{glyph: "● ", indent: 2}
	markerTool     = blockMarker{glyph: "  ⎿  ", indent: 5}
	markerThinking = blockMarker{glyph: "✻ ", indent: 2} // thinking 专用锚点，与正文 ● 区分
)

// block 将 content 编组成带锚点标记的文本块：
// 首行加 glyph 前缀，延续行按 indent 个空格对齐，维持垂直参考线。
func block(m blockMarker, content string) string {
	return m.glyph + strings.ReplaceAll(content, "\n", "\n"+strings.Repeat(" ", m.indent))
}
