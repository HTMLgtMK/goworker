package stdin

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync"

	"github.com/charmbracelet/glamour"
	"github.com/muesli/termenv"
)

var minimalStyleJSON []byte // 当前活跃主题的 JSON 样式，由 SetTheme 在启动时设置

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
		// glamour v1.0.0 默认硬编码 TrueColor，不管终端支不支持 24 位色。
		// 改成按终端实际能力走，macOS Terminal.app（ANSI256）不再收到一堆
		// 无法解析的 RGB 序列导致颜色乱掉。
		glamour.WithColorProfile(colorProfile()),
	)
	if err != nil {
		return nil
	}
	cachedRenderer = r
	rendererWidth = width
	return r
}

// colorProfile 返回终端实际支持的颜色深度，供 glamour 渲染器使用。
// 探测失败（非 TTY / TERM 未知，termenv 返回 Ascii）时退回 TrueColor，
// 保证输出永远带颜色——否则用户把输出管道重定向到文件时颜色会被整段剥掉。
func colorProfile() termenv.Profile {
	if p := termenv.ColorProfile(); p != termenv.Ascii {
		return p
	}
	return termenv.TrueColor
}

// hrPattern 匹配 glamour 渲染后的水平分割线（纯文本 `─{4}` + 尾部空格，无 ANSI 封装）。
var hrPattern = regexp.MustCompile(`(?m)^─{3,} *$`)

// postProcessAnsi 后处理 glamour 的 ANSI 输出：修复 HR 宽度 + 去除前导空白行。
//
// 注意：表格不做任何 cell 归一化。glamour v1.0.0 的表格原生输出就是对齐的——
// 之前 normalizeTableCells 把数据行前导 margin 剥掉却放过了分隔行，反而让 ┼ 与 │ 错位。
func postProcessAnsi(s string, width int) string {
	s = hrPattern.ReplaceAllString(s, "\x1b[2m"+strings.Repeat("─", width)+"\x1b[0m")
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
