package agent

import (
	"fmt"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// 本文件是展示层小工具：消息描述、文本折叠/截断、token 格式化、schema 兜底。

// describeMessage 把一条消息压成单行描述，供 /history 逐条展示。
// 工具调用展开成 name(args)，多行内容折叠成一行，长内容截断。
func describeMessage(m core.Message) string {
	switch {
	case len(m.ToolCalls) > 0:
		var parts []string
		for _, tc := range m.ToolCalls {
			parts = append(parts, fmt.Sprintf("%s(%s)", tc.Function.Name, tc.Function.Arguments))
		}
		return truncate(strings.Join(parts, " | "), 160)
	case m.Content == "":
		return "(empty)"
	default:
		return truncate(oneLine(m.Content), 160)
	}
}

// oneLine 把多行文本折叠成单行，换行替换为 ⏎，避免长工具输出铺满屏幕。
func oneLine(s string) string {
	return strings.ReplaceAll(s, "\n", " ⏎ ")
}

// truncate 按 rune 截断长文本，避免字节切半中文。n <= 0 时不截断。
func truncate(s string, n int) string {
	r := []rune(s)
	if n <= 0 || len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// humanize 把 token 数格式化为千分位缩写，12345 -> "12.3k"。
// 与 frontend/stdin 的 humanize 保持同一套规则，避免跨包依赖。
func humanize(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// renderKind 将 core.Token 类型映射为 spec.RenderKind，剥离渲染逻辑。
func (p *AgentPlugin) renderKind(t core.Token) (spec.RenderKind, string) {
	switch t.Type {
	case core.TokenTypeToolCall:
		return spec.KindToolCall, t.Content
	case core.TokenTypeToolResult:
		return spec.KindToolResult, t.Content
	default:
		// TokenTypeText 及未知类型一律按 markdown 渲染
		return spec.KindText, t.Content
	}
}

// normalizeSchema 兜底空 schema 为 OpenAI 工具要求的 object 结构。
// spec/skill/MCP 三种工具构造统一走这里，避免 schema 处理分叉。
func normalizeSchema(p map[string]any) map[string]any {
	if len(p) == 0 {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return p
}
