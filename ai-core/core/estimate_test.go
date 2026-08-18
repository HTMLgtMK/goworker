package core

import (
	"strings"
	"testing"
)

func TestEstimateTokens_Empty(t *testing.T) {
	// 空切片序列化为 "[]"（2 字节）→ 0；nil 序列化为 "null" → 1，都不算数
	if n := EstimateTokens([]Message{}); n != 0 {
		t.Errorf("EstimateTokens([]) = %d, want 0", n)
	}
	if n := EstimateTokens(nil); n > 4 {
		t.Errorf("EstimateTokens(nil) = %d, want ~1", n)
	}
}

func TestEstimateTokens_ScalesWithContent(t *testing.T) {
	small := []Message{{Role: "user", Content: "hi"}}
	big := []Message{
		{Role: "user", Content: strings.Repeat("x", 200)},
		{Role: "assistant", Content: strings.Repeat("y", 200)},
		{Role: "tool", Content: strings.Repeat("z", 200), ToolCallID: "c1"},
	}
	if n := EstimateTokens(small); n == 0 {
		t.Error("EstimateTokens(small) should be > 0")
	}
	if EstimateTokens(big) <= EstimateTokens(small) {
		t.Errorf("EstimateTokens(big)=%d should exceed EstimateTokens(small)=%d", EstimateTokens(big), EstimateTokens(small))
	}
}
