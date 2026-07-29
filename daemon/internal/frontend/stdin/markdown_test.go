package stdin

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rivo/uniseg"
)

func TestMain(m *testing.M) {
	// 测试前初始化默认主题
	SetTheme("light")
	os.Exit(m.Run())
}

func ansiStrip(s string) string {
	var b strings.Builder
	inEscape := false
	for i := 0; i < len(s); i++ {
		if inEscape {
			if s[i] == 'm' {
				inEscape = false
			}
			continue
		}
		if s[i] == '\x1b' && i+1 < len(s) && s[i+1] == '[' {
			inEscape = true
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func TestTableAlignment(t *testing.T) {
	tests := []struct {
		name string
		md   string
	}{
		{"simple", "| 项目 | 温度 | 湿度 |\n|------|------|------|\n| 长沙 | 28°C | 84% |\n"},
		{"with bold", "| **项目** | **温度** |\n|------|------|\n| **长沙** | 28°C |\n"},
		{"realistic", "| 项目 | 数据 |\n|------|------|\n| **天气** | ⛅ 局部多云 |\n| **温度** | **28°C**（体感约 33°C） |\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := RenderMarkdown(tt.md, 100)
			out = "● " + strings.ReplaceAll(out, "\n", "\n  ")

			lines := strings.Split(out, "\n")
			var allPositions [][]int

			for _, line := range lines {
				if !strings.Contains(line, "│") {
					continue
				}
				if strings.Contains(line, "─") || strings.Contains(line, "┼") {
					continue
				}

				parts := strings.Split(line, "│")
				var positions []int
				cumVis := 0
				for i, p := range parts {
					clean := ansiStrip(p)
					w := uniseg.StringWidth(clean)
					cumVis += w
					if i < len(parts)-1 {
						// 不是最后一列，记录 │ 的位置
						positions = append(positions, cumVis)
					}
					cumVis++ // │ 占 1 格
				}
				if len(positions) > 0 {
					allPositions = append(allPositions, positions)
				}
			}

			if len(allPositions) < 2 {
				t.Fatal("expected at least 2 rows with separators")
			}

			baseline := allPositions[0]
			allAligned := true
			for r := 1; r < len(allPositions); r++ {
				if len(allPositions[r]) != len(baseline) {
					t.Errorf("row %d: %d seps vs expected %d", r, len(allPositions[r]), len(baseline))
					allAligned = false
					continue
				}
				for c := range baseline {
					if allPositions[r][c] != baseline[c] {
						t.Errorf("row %d col %d: │ at vis %d, expected %d",
							r, c, allPositions[r][c], baseline[c])
						allAligned = false
					}
				}
			}
			if allAligned {
				fmt.Printf("  %s: ✓ all %d rows, %d │ aligned\n", tt.name, len(allPositions), len(baseline))
			}
		})
	}
}
