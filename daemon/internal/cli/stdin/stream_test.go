package stdin

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/cli/statusbar"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// newTestStream 构造输出捕获型的渲染器（状态栏不活跃，bar 分支全部跳过）。
func newTestStream() (*streamRenderer, *bytes.Buffer) {
	var out bytes.Buffer
	f := &StdinFrontend{sb: statusbar.New(), stack: NewConsumerStack()}
	s := newStreamRenderer(f)
	s.out = &out
	s.errOut = &out
	return s, &out
}

func TestStreamRenderer_LineLevelFlushAndFinalizeAtEnd(t *testing.T) {
	s, out := newTestStream()

	s.append(model.KindText, "hello ")
	if out.Len() != 0 {
		t.Fatalf("partial line printed early: %q", out.String())
	}
	s.append(model.KindText, "world\nnext\n")
	// 整行已上屏（含 ● 锚点与分隔换行），尾部无残留
	got := out.String()
	if !strings.Contains(got, rawNL+markerText.glyph+"hello world"+rawNL) {
		t.Errorf("first line output = %q", got)
	}
	if !strings.Contains(got, "next"+rawNL) {
		t.Errorf("second line output = %q", got)
	}

	// 空行不再触发分段定稿（整段定稿语义）：只是内容里的一行，rows 继续累计
	out.Reset()
	s.append(model.KindText, "\n")
	if s.rows != 3 {
		t.Errorf("rows after blank line = %d, want 3 (2 content + 1 blank)", s.rows)
	}
	if strings.Contains(out.String(), "\x1b[") {
		t.Errorf("blank line must not trigger erase: %q", out.String())
	}

	// finish 才定稿：擦除全部 3 行，整条消息一次渲染替换
	out.Reset()
	s.finish()
	if !strings.Contains(out.String(), "\r\033[3A\033[J") {
		t.Errorf("missing erase sequence for 3 rows: %q", out.String())
	}
	if strings.Count(out.String(), markerText.glyph) != 1 {
		t.Errorf("finalized message should carry exactly one ● anchor: %q", out.String())
	}
	if s.started || s.rows != 0 {
		t.Errorf("state not reset: started=%v rows=%d", s.started, s.rows)
	}
}

func TestStreamRenderer_FinalizeFlushesPendingAndMarksDone(t *testing.T) {
	s, out := newTestStream()
	s.append(model.KindText, "no trailing newline")
	if out.Len() != 0 {
		t.Fatalf("pending printed early: %q", out.String())
	}
	s.finish()
	if !strings.Contains(out.String(), "no trailing newline") {
		t.Errorf("pending not flushed on finish: %q", out.String())
	}
	if s.started {
		t.Error("stream still started after finish")
	}

	// finish 后回到提交行协议：下一次流需要分隔换行
	out.Reset()
	s.append(model.KindText, "second\n")
	if !strings.HasPrefix(out.String(), rawNL) {
		t.Errorf("second stream missing separator: %q", out.String())
	}
}

func TestStreamRenderer_ThinkingGrayAndKindSwitch(t *testing.T) {
	s, out := newTestStream()

	s.append(model.KindThinking, "reason line\n")
	if !strings.Contains(out.String(), thinkingColor) {
		t.Errorf("thinking line not gray: %q", out.String())
	}
	if !strings.Contains(out.String(), markerThinking.glyph) || strings.Contains(out.String(), markerText.glyph) {
		t.Errorf("thinking raw line should use ✻ anchor, not ●: %q", out.String())
	}
	out.Reset()
	// kind 切换：定稿 thinking 块后，text 流重新开始，块间恰好空一行
	s.append(model.KindText, "answer\n")
	got := out.String()
	if !strings.Contains(got, rawNL+rawNL+markerText.glyph+"answer") {
		t.Errorf("text should start after a blank line below thinking block: %q", got)
	}
	// 定稿的 thinking 块整体灰化且不带 glamour 正文深色
	if !strings.Contains(got, "✻ Thinking") {
		t.Errorf("thinking block missing ✻ Thinking header: %q", got)
	}
	if strings.Contains(got, "\x1b[38;2;20;20;19m") {
		t.Errorf("thinking block must not contain body text color: %q", got)
	}
	if s.kind != model.KindText {
		t.Errorf("kind = %v, want text", s.kind)
	}
}

func TestStreamRenderer_BlankLeadingLineNoOrphanMarker(t *testing.T) {
	s, out := newTestStream()
	// agent note 以 "\n" 开头：首行空行跳过不上屏，不得留下孤立 ●
	s.append(model.KindText, "\n⚠ note\n")
	got := out.String()
	if strings.Contains(got, markerText.glyph+rawNL) {
		t.Errorf("orphan marker for empty paragraph: %q", got)
	}
	if !strings.Contains(got, "⚠ note") {
		t.Errorf("note line missing: %q", got)
	}
}

func TestStreamRenderer_WrappedLineRowCountsPhysicalRows(t *testing.T) {
	s, out := newTestStream()
	s.f.termWidth.Store(20) // 窄终端放大折行效果

	// 首行：●(2) + 45 列 ASCII = 47 列 → 3 物理行；次行 4 列 → 1 行
	s.append(model.KindText, strings.Repeat("a", 45)+"\nnext\n")
	if s.rows != 4 {
		t.Fatalf("rows = %d, want 4 (3 wrapped + 1)", s.rows)
	}
	// CJK 宽字符按显示宽度算：10 个汉字 = 20 列，恰好占满 1 物理行（锚点只在
	// 消息首行，本行无前缀）；空行 1 行
	s.append(model.KindText, strings.Repeat("长", 10)+"\n\n")
	if s.rows != 6 {
		t.Errorf("rows = %d, want 6 (3+1+1+1)", s.rows)
	}

	// finish 定稿：擦除整条消息累计的 6 个物理行
	out.Reset()
	s.finish()
	if s.rows != 0 {
		t.Errorf("rows after finalize = %d, want 0", s.rows)
	}
	if !strings.Contains(out.String(), "\r\033[6A\033[J") {
		t.Errorf("erase should cover 6 physical rows: %q", out.String())
	}
}

func TestPhysicalRows(t *testing.T) {
	cases := []struct {
		cols, width, want int
	}{
		{0, 80, 1},   // 空行占一行
		{1, 80, 1},   // 不满一行
		{80, 80, 1},  // 恰好整行
		{81, 80, 2},  // 折行
		{161, 80, 3}, // 折两行
		{47, 20, 3},  // 窄终端
		{10, 0, 1},   // termWidth 未知兜底
	}
	for _, c := range cases {
		if got := physicalRows(c.cols, c.width); got != c.want {
			t.Errorf("physicalRows(%d, %d) = %d, want %d", c.cols, c.width, got, c.want)
		}
	}
}
