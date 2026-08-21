package agent

import (
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/middlewares"
)

// SDK 展示层 helper 的单元测试：消息描述、文本截断、记忆块过滤。

func TestDescribeMessage_ToolCallExpandsName(t *testing.T) {
	m := core.Message{
		Role: "assistant",
		ToolCalls: []core.ToolCall{{
			Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"ls -la"}`},
		}},
	}
	got := DescribeMessage(m)
	if !strings.Contains(got, "bash(") || !strings.Contains(got, "ls -la") {
		t.Errorf("DescribeMessage = %q, want tool call name+args", got)
	}
	if strings.Contains(got, "\n") {
		t.Errorf("DescribeMessage should be single line, got %q", got)
	}
}

func TestDescribeMessage_FoldsMultiline(t *testing.T) {
	m := core.Message{Role: "tool", Content: "line1\nline2\nline3"}
	got := DescribeMessage(m)
	if strings.Contains(got, "\n") {
		t.Errorf("multiline not folded: %q", got)
	}
	if !strings.Contains(got, "⏎") {
		t.Errorf("fold marker missing: %q", got)
	}
}

func TestTruncate(t *testing.T) {
	if got := Truncate("short", 10); got != "short" {
		t.Errorf("Truncate(short) = %q", got)
	}
	long := "中文内容特别长，用来验证不会切半字符"
	got := Truncate(long, 6)
	if len([]rune(got)) != 7 { // 6 rune + …
		t.Errorf("Truncate got %d runes, want 7: %q", len([]rune(got)), got)
	}
	if got := Truncate("x", 0); got != "x" {
		t.Errorf("Truncate n=0 should not Truncate: %q", got)
	}
}

func TestStripMemoryBlocks(t *testing.T) {
	msgs := []core.Message{
		{Role: "system", Content: middlewares.MemoryBlockPrefix + " 来自之前的会话，仅供参考"},
		{Role: "user", Content: "hi"},
		{Role: "system", Content: "压缩摘要：上次任务状态"}, // 普通 system 消息不受影响
	}
	got := stripMemoryBlocks(msgs)
	if len(got) != 2 {
		t.Fatalf("got %d messages, want 2 (%+v)", len(got), got)
	}
	if got[0].Role != "user" || got[0].Content != "hi" {
		t.Errorf("user message lost: %+v", got[0])
	}
	if got[1].Content != "压缩摘要：上次任务状态" {
		t.Errorf("non-memory system message should survive: %+v", got[1])
	}
}
