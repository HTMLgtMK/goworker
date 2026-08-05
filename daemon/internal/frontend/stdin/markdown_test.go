package stdin

import (
	"regexp"
	"strings"
	"testing"

	"github.com/mattn/go-runewidth"
	"github.com/muesli/termenv"
)

var ansiStripper = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// borderCols 返回一行中表格边框字符（│/┼）的 display 列。
// 表格线在真实终端是窄字符（宽 1），runewidth 会把 box drawing 误判为宽 2，这里强制按 1 算。
func borderCols(s string) []int {
	var pos []int
	w := 0
	for _, ch := range s {
		if ch == '│' || ch == '┼' {
			pos = append(pos, w)
		}
		if ch == '─' || ch == '┼' || ch == '│' {
			w++
		} else {
			w += runewidth.RuneWidth(ch)
		}
	}
	return pos
}

func TestRenderMarkdown_TableAligned(t *testing.T) {
	SetTheme("light")
	out := RenderMarkdown("| a | b | 城市 |\n|---|---|---|\n| 1 | 2 | 北京 |\n| long | x | 深 |\n", 80)

	var first []int
	for _, line := range strings.Split(out, "\n") {
		if !strings.ContainsAny(line, "│┼") {
			continue
		}
		cols := borderCols(ansiStripper.ReplaceAllString(line, ""))
		if len(cols) == 0 {
			continue
		}
		if first == nil {
			first = cols
		} else if !equalIntSlice(first, cols) {
			t.Errorf("表格边框未对齐: 数据/分隔行 cols=%v, 首行=%v\n  行: %s",
				cols, first, ansiStripper.ReplaceAllString(line, ""))
		}
	}
}

func TestRenderMarkdown_LightHeadingBoldAndDark(t *testing.T) {
	SetTheme("light")
	out := RenderMarkdown("## 二级标题", 80)

	// 标题必须加粗 + 主题深色 #9C4A24 = (156,73,36)。
	// termenv 会把 bold 合并进颜色序列（38;2;156;73;36;1m 末尾的 ;1 即加粗），
	// 而非输出独立的 \x1b[1m，所以直接断言合并后的完整 SGR。
	if !strings.Contains(out, "\x1b[38;2;156;73;36;1m") {
		t.Errorf("标题未按「深色 #9C4A24 + 加粗」渲染，期望 38;2;156;73;36;1m: %q", out)
	}
	// 标题颜色不能是正文色 #141413 = (20,20,19)（Text 覆盖 bug 的回归特征）
	if strings.Contains(out, "38;2;20;20;19") {
		t.Errorf("标题不应染成正文色 #141413: %q", out)
	}
	if strings.Contains(out, "38;2;20;20;19") {
		t.Errorf("标题不应染成正文色 #141413: %q", out)
	}
}

func TestColorProfile_FallsBackToTrueColorOnNonTTY(t *testing.T) {
	// 测试环境 stdout 非 TTY，termenv.ColorProfile() 返回 Ascii；
	// colorProfile() 必须回退 TrueColor，否则颜色会被整段剥掉。
	if p := colorProfile(); p != termenv.TrueColor {
		t.Fatalf("colorProfile() = %v, want TrueColor（非 TTY 回退）", p)
	}
}

func equalIntSlice(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
