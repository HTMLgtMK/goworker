package stdin

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// streamRenderer 负责流式 token 的增量上屏与定稿替换。
//
// 策略（行级流式）：增量文本先以原始形式逐行上屏（不跑 markdown 渲染），
// 段落边界（空行）或流结束时，擦除已上屏的原始行，用 glamour 渲染结果整块
// 替换——claude-code 风格。粒度选行而不是字符：行边界与状态栏"独占一行"
// 协议天然兼容，无需光标列寻址（折行/宽字符下列计算存在终端歧义）。
//
// 终端协议与 Write() 一致：状态栏活跃时每次上屏 = 清栏 → 写内容 → 换行 →
// 重绘栏，状态栏始终独占已写内容的下一行；由此擦除时的光标位置是确定的
// （栏行或最后内容行的下一行行首），上移 rows 行即回到段落起始行。
type streamRenderer struct {
	f *StdinFrontend

	// 输出出口，默认终端；测试注入 buffer 捕获
	out    io.Writer // 内容（stdout）
	errOut io.Writer // 换行/控制序列（stderr）

	kind    plugin.RenderKind // 正在流式渲染的 token 类型
	pending string            // 未凑齐整行的尾部增量（不上屏）
	text    strings.Builder   // 当前段落累积的原始文本（含已上屏行与 pending）
	rows    int               // 当前段落已上屏的原始文本行数
	fresh   bool              // 当前段落尚未上屏任何内容
	needSep bool              // 下一次上屏前是否需要分隔换行（提交行 → 内容区）
	started bool              // 是否处于流式渲染中（两次 finish 之间）
}

func newStreamRenderer(f *StdinFrontend) *streamRenderer {
	return &streamRenderer{f: f, needSep: true, out: os.Stdout, errOut: os.Stderr}
}

func (s *streamRenderer) bind(f *StdinFrontend) { s.f = f }

// append 追加一段流式增量（text/thinking）。kind 切换时先定稿当前段落。
func (s *streamRenderer) append(kind plugin.RenderKind, content string) {
	if s.started && s.kind != kind {
		s.finalizeParagraph()
		s.kind = kind
	}
	if !s.started {
		s.started = true
		s.kind = kind
		s.fresh = true
	}
	s.pending += content
	for {
		i := strings.IndexByte(s.pending, '\n')
		if i < 0 {
			break
		}
		line := s.pending[:i]
		s.pending = s.pending[i+1:]
		if line == "" {
			if s.fresh && s.rows == 0 {
				// 段落前的空行（如 agent note 的 \n 前缀）：不作为内容上屏，
				// 分隔留给 needSep 在下一行真实内容前输出，避免孤立 ● 锚点
				continue
			}
			// 段落边界：上屏空行后定稿
			s.emitLine(line)
			s.finalizeParagraph()
			continue
		}
		s.emitLine(line)
	}
}

// finish 结束流式渲染（done 或前端切换到其他 token 类型）：
// 定稿剩余段落并复位；下次 append 从提交行重新开始（需要分隔换行）。
func (s *streamRenderer) finish() {
	if s.started {
		s.finalizeParagraph()
	}
	s.started = false
	s.needSep = true
}

// finalizeParagraph 擦除当前段落已上屏的原始行，渲染 markdown 替换上屏。
// 段落为纯空白时只擦除不替换（避免留下孤立的 ● 锚点）。
func (s *streamRenderer) finalizeParagraph() {
	if s.pending != "" {
		s.emitLine(s.pending)
		s.pending = ""
	}
	rows := s.rows
	rendered := s.renderParagraph()
	blank := strings.TrimSpace(s.text.String()) == ""
	s.rows, s.fresh = 0, true
	s.text.Reset()

	if rows == 0 {
		return
	}

	s.f.sb.WithLock(func() {
		if s.f.sb.Active() && !s.f.isHITL() {
			s.f.sb.Clear()
		}
		// 光标当前在栏行或最后内容行的下一行行首，上移 rows 行落到段落起始行
		fmt.Fprintf(s.errOut, "\r\033[%dA\033[J", rows)
		if !blank {
			fmt.Fprint(s.out, rendered)
			fmt.Fprint(s.errOut, rawNL)
		}
		if s.f.sb.Active() && !s.f.isHITL() {
			s.f.sb.Draw()
		}
	})
}

// emitLine 上屏一行原始文本。段落首行带分隔换行与 ● 锚点。
func (s *streamRenderer) emitLine(line string) {
	var b strings.Builder
	if s.needSep {
		b.WriteString(rawNL)
		s.needSep = false
	}
	if s.fresh {
		b.WriteString(markerText.glyph)
		s.fresh = false
	}
	if s.kind == plugin.KindThinking {
		b.WriteString(thinkingColor + line + ansiReset)
	} else {
		b.WriteString(line)
	}
	s.rows++
	s.text.WriteString(line + "\n")
	out := b.String()

	s.f.sb.WithLock(func() {
		if s.f.sb.Active() && !s.f.isHITL() {
			s.f.sb.Clear()
		}
		fmt.Fprint(s.out, out)
		fmt.Fprint(s.errOut, rawNL)
		if s.f.sb.Active() && !s.f.isHITL() {
			s.f.sb.Draw()
		}
	})
}

// renderParagraph 渲染当前段落的原始文本为带锚点的定稿块。
// thinking 段落走灰色弱化样式。
func (s *streamRenderer) renderParagraph() string {
	text := s.text.String()
	if s.kind == plugin.KindThinking {
		return formatThinking(text, s.f.termWidth)
	}
	return block(markerText, RenderMarkdown(strings.TrimSpace(text), s.f.termWidth))
}
