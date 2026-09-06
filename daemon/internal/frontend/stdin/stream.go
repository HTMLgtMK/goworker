package stdin

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/rivo/uniseg"

	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// streamRenderer 负责流式 token 的增量上屏与定稿替换。
//
// 策略（行级流式 + 整段定稿）：增量文本先以原始形式逐行上屏（不跑 markdown
// 渲染），流结束（done / kind 切换 / tool token）时擦除全部原始行，用 glamour
// 对整条消息一次性渲染替换。不做分段定稿：按空行拆段会把同属一个列表/代码块
// 的段落切成多个锚点块（孤立 ●、列表结构丢失），glamour 的边距也会在每次独立
// 渲染时不一致——正是排版间距混乱的来源。
//
// 粒度选行而不是字符：行边界与状态栏"独占一行"协议天然兼容，无需光标列寻址
// （折行/宽字符下列计算存在终端歧义）。
//
// 终端协议与 Write() 一致：状态栏活跃时每次上屏 = 清栏 → 写内容 → 换行 →
// 重绘栏，状态栏始终独占已写内容的下一行；由此擦除时的光标位置是确定的
// （栏行或最后内容行的下一行行首），上移 rows 行即回到消息起始行。
type streamRenderer struct {
	f *StdinFrontend

	// 输出出口，默认终端；测试注入 buffer 捕获
	out    io.Writer // 内容（stdout）
	errOut io.Writer // 换行/控制序列（stderr）

	kind    plugin.RenderKind // 正在流式渲染的 token 类型
	pending string            // 未凑齐整行的尾部增量（不上屏）
	text    strings.Builder   // 当前消息累积的原始文本（含已上屏行与 pending）
	rows    int               // 当前消息已上屏的终端物理行数（含折行）
	fresh   bool              // 当前消息尚未上屏任何内容
	needSep bool              // 下一次上屏前是否需要分隔换行（提交行/上一块 → 内容区）
	started bool              // 是否处于流式渲染中（两次 finish 之间）
}

func newStreamRenderer(f *StdinFrontend) *streamRenderer {
	return &streamRenderer{f: f, needSep: true, out: os.Stdout, errOut: os.Stderr}
}

func (s *streamRenderer) bind(f *StdinFrontend) { s.f = f }

// append 追加一段流式增量（text/thinking）。kind 切换时先定稿当前消息。
func (s *streamRenderer) append(kind plugin.RenderKind, content string) {
	if s.started && s.kind != kind {
		s.finish()
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
		if line == "" && s.fresh && s.rows == 0 {
			// 消息开头的空行（如 agent note 的 \n 前缀）：不作为内容上屏，
			// 分隔留给 needSep 在下一行真实内容前输出，避免孤立锚点
			continue
		}
		s.emitLine(line)
	}
}

// finish 定稿：擦除已上屏的原始行，整条消息一次 glamour 渲染替换上屏。
// 消息为纯空白时只擦除不替换（避免留下孤立的锚点）。
func (s *streamRenderer) finish() {
	if s.started {
		if s.pending != "" {
			s.emitLine(s.pending)
			s.pending = ""
		}
		rows := s.rows
		rendered, blank := s.renderMessage(), strings.TrimSpace(s.text.String()) == ""
		s.rows, s.fresh = 0, true
		s.text.Reset()

		if rows > 0 {
			s.f.sb.WithLock(func() {
				if s.f.sb.Active() && !s.f.isHITL() {
					s.f.sb.Clear()
				}
				// 光标当前在栏行或最后内容行的下一行行首，上移 rows 行落到消息起始行
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
	}
	s.started = false
	s.needSep = true
}

// emitLine 上屏一行原始文本。消息首行带分隔换行与锚点（thinking 用 ✻ 与正文 ● 区分）。
// rows 按"终端物理行"计数：超宽行会被终端折行成多个物理行，擦除的上移行数
// 必须覆盖折行，否则定稿替换后残留折行的上半截。
func (s *streamRenderer) emitLine(line string) {
	var b strings.Builder
	cols := 0
	if s.needSep {
		b.WriteString(rawNL)
		s.needSep = false
	}
	if s.fresh {
		glyph := markerText
		if s.kind == plugin.KindThinking {
			glyph = markerThinking
		}
		b.WriteString(glyph.glyph)
		cols += glyph.indent
		s.fresh = false
	}
	if s.kind == plugin.KindThinking {
		b.WriteString(thinkingColor + line + ansiReset)
	} else {
		b.WriteString(line)
	}
	s.rows += physicalRows(cols+uniseg.StringWidth(line), s.f.termWidth)
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

// physicalRows 换算显示宽度占用的终端物理行数（≥1，向上取整）。
// termWidth 未知（0）时按 80 兜底，与 Run() 的取宽兜底一致。
func physicalRows(cols, termWidth int) int {
	if termWidth <= 0 {
		termWidth = 80
	}
	if cols <= 0 {
		return 1
	}
	return (cols + termWidth - 1) / termWidth
}

// renderMessage 渲染当前消息的原始文本为带锚点的定稿块。
// thinking 消息走 ✻ 锚点 + 灰色弱化样式。
func (s *streamRenderer) renderMessage() string {
	text := s.text.String()
	if s.kind == plugin.KindThinking {
		return formatThinking(text, s.f.termWidth)
	}
	return block(markerText, RenderMarkdown(strings.TrimSpace(text), s.f.termWidth))
}
