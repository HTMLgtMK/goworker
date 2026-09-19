package stdin

import "testing"

// TestBlock 锁定编组对齐规则：
// 首行加 glyph 前缀，延续行按 indent 列空格对齐。
// "tool multi line aligned" 用例直接焊死字节宽度 vs 显示宽度这个坑——
// markerTool 的 glyph 是 7 字节（⎿ 3 字节 + 4 空格），但显示宽度只有 5 列，
// 若用 len() 派生缩进会得 7 格，延续行必错位。
func TestBlock(t *testing.T) {
	tests := []struct {
		name    string
		m       blockMarker
		content string
		want    string
	}{
		{"text single line", markerText, "hi", "● hi"},
		{"text multi line", markerText, "a\nb", "● a\n  b"},
		{"tool single line", markerTool, "ls -la", "  ⎿  ls -la"},
		{"tool multi line aligned", markerTool, "a\nb", "  ⎿  a\n     b"},
		{"empty content", markerText, "", "● "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := block(tt.m, tt.content); got != tt.want {
				t.Errorf("block() = %q, want %q", got, tt.want)
			}
		})
	}
}
