package agent

import (
	"fmt"
	"strings"

	"github.com/tinguo/goworker/ai-core/core"
)

// 本文件是展示层小工具：消息描述、文本折叠/截断、token 格式化、schema 兜底。
// 这些 helper 跨 SDK 与宿主 plugin 复用，统一导出（宿主在 daemon 层用 agent.Truncate 等）。

// DescribeMessage 把一条消息压成单行描述，供 /history 逐条展示。
// 工具调用展开成 name(args)，多行内容折叠成一行，长内容截断。
func DescribeMessage(m core.Message) string {
	switch {
	case len(m.ToolCalls) > 0:
		var parts []string
		for _, tc := range m.ToolCalls {
			parts = append(parts, fmt.Sprintf("%s(%s)", tc.Function.Name, tc.Function.Arguments))
		}
		return Truncate(strings.Join(parts, " | "), 160)
	case m.Content == "":
		return "(empty)"
	default:
		return Truncate(OneLine(m.Content), 160)
	}
}

// OneLine 把多行文本折叠成单行，换行替换为 ⏎，避免长工具输出铺满屏幕。
func OneLine(s string) string {
	return strings.ReplaceAll(s, "\n", " ⏎ ")
}

// Truncate 按 rune 截断长文本，避免字节切半中文。n <= 0 时不截断。
func Truncate(s string, n int) string {
	r := []rune(s)
	if n <= 0 || len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// Humanize 把 token 数格式化为千分位缩写，12345 -> "12.3k"。
// 与 frontend/stdin 的 humanize 保持同一套规则，避免跨包依赖。
func Humanize(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// renderKind 将 core.Token 类型映射为 runtime agent 的渲染分类，剥离渲染逻辑。
// 接收者已去掉（函数体不依赖插件状态），供 Session.Run 直接调用。
func renderKind(t core.Token) (RenderKind, string) {
	switch t.Type {
	case core.TokenTypeThinking:
		return KindThinking, t.Content
	case core.TokenTypeToolCall:
		return KindToolCall, t.Content
	case core.TokenTypeToolResult:
		return KindToolResult, t.Content
	default:
		// TokenTypeText 及未知类型一律按 markdown 渲染
		return KindText, t.Content
	}
}

// NormalizeSchema 兜底空 schema 为 OpenAI 工具要求的 object 结构。
// spec/skill/MCP 三种工具构造统一走这里，避免 schema 处理分叉。
func NormalizeSchema(p map[string]any) map[string]any {
	if len(p) == 0 {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return p
}
