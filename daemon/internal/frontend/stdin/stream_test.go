package stdin

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
	"github.com/tinguo/goworker/daemon/internal/plugin"
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

func TestStreamRenderer_LineLevelFlushAndParagraphFinalize(t *testing.T) {
	s, out := newTestStream()

	s.append(plugin.KindText, "hello ")
	if out.Len() != 0 {
		t.Fatalf("partial line printed early: %q", out.String())
	}
	s.append(plugin.KindText, "world\nnext\n")
	// 整行已上屏（含 ● 锚点与分隔换行），尾部无残留
	got := out.String()
	if !strings.Contains(got, rawNL+markerText.glyph+"hello world"+rawNL) {
		t.Errorf("first line output = %q", got)
	}
	if !strings.Contains(got, "next"+rawNL) {
		t.Errorf("second line output = %q", got)
	}
	if s.rows != 2 || s.pending != "" {
		t.Fatalf("rows=%d pending=%q, want 2/\"\"", s.rows, s.pending)
	}

	// 空行触发段落定稿：擦除序列出现，渲染块替换
	out.Reset()
	s.append(plugin.KindText, "\n")
	if s.rows != 0 || !s.fresh {
		t.Errorf("paragraph not reset: rows=%d fresh=%v", s.rows, s.fresh)
	}
	// 空行本身也占一行：2 行内容 + 1 行边界 = 3 行擦除
	if !strings.Contains(out.String(), "\r\033[3A\033[J") {
		t.Errorf("missing erase sequence for 3 rows: %q", out.String())
	}
}

func TestStreamRenderer_FinalizeFlushesPendingAndMarksDone(t *testing.T) {
	s, out := newTestStream()
	s.append(plugin.KindText, "no trailing newline")
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
	s.append(plugin.KindText, "second\n")
	if !strings.HasPrefix(out.String(), rawNL) {
		t.Errorf("second stream missing separator: %q", out.String())
	}
}

func TestStreamRenderer_ThinkingGrayAndKindSwitch(t *testing.T) {
	s, out := newTestStream()

	s.append(plugin.KindThinking, "reason line\n")
	if !strings.Contains(out.String(), thinkingColor) {
		t.Errorf("thinking line not gray: %q", out.String())
	}
	out.Reset()
	// kind 切换：先定稿 thinking 段落，text 重新开始（不需要分隔换行）
	s.append(plugin.KindText, "answer\n")
	got := out.String()
	if strings.HasPrefix(got, rawNL) {
		t.Errorf("kind switch should not re-separate: %q", got)
	}
	// text 行本身不带灰色（thinking 段落的定稿块除外）
	if idx := strings.Index(got, "● answer"); idx >= 0 && strings.Contains(got[idx:], thinkingColor) {
		t.Errorf("text line should not be gray: %q", got)
	}
	if s.kind != plugin.KindText {
		t.Errorf("kind = %v, want text", s.kind)
	}
}

func TestStreamRenderer_BlankLeadingLineNoOrphanMarker(t *testing.T) {
	s, out := newTestStream()
	// agent note 以 "\n" 开头：首行即空行 → 段落边界，无内容不得留下孤立 ●
	s.append(plugin.KindText, "\n⚠ note\n")
	got := out.String()
	if strings.Contains(got, markerText.glyph+rawNL) {
		t.Errorf("orphan marker for empty paragraph: %q", got)
	}
	if !strings.Contains(got, "⚠ note") {
		t.Errorf("note line missing: %q", got)
	}
}
