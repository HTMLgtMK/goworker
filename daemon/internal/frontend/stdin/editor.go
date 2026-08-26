package stdin

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/x/term"
	"github.com/rivo/uniseg"
)

const maxHistory = 50

// LineEditor 是一个 raw mode 下的行编辑器，支持 ↑↓ 历史、←→ 光标、Delete。
//
// 字节解码（UTF-8 累积、CSI 解析、Esc 判定）由 KeyDecoder 完成，editor
// 只消费语义清晰的按键事件。它是责任链栈上的行消费者：
//   - 只关注编辑键（字符/Enter/Backspace/方向键），无活跃读行会话时放行
//   - 取消键（KeyEsc/KeyCtrlC）放行给栈底 keyWatcher，keyWatcher 通过
//     cancelCh 通知阻塞的 ReadLine 放弃输入
type LineEditor struct {
	keyCh    chan KeyEvent   // dispatch 投递的编辑按键
	cancelCh <-chan struct{} // keyWatcher 触发的取消信号
	active   atomic.Bool     // 是否有活跃的 ReadLine 会话
	state    *term.State
	Prompt   string   // 输入提示符；HITL 输入独占一行，直接用默认 "> "
	buf      []rune   // 以 rune 为单位跟踪输入（正确支持中文等 UTF-8）
	pos      int      // rune 索引
	history  []string // 历史记录，最新在末尾
	histIdx  int      // -1 = 新输入，0 到 len-1 = 历史中的索引

	termWidth     int
	renderedRows  int
	cursorLineIdx int
}

// NewLineEditor 创建并进入 raw mode。
// cancelCh 由 keyWatcher 共享，取消键触发时 ReadLine 醒来返回 canceled。
func NewLineEditor(cancelCh <-chan struct{}) (*LineEditor, error) {
	fd := os.Stdin.Fd()
	state, err := term.MakeRaw(fd)
	if err != nil {
		return nil, fmt.Errorf("make raw: %w", err)
	}
	width := 80
	if w, _, err := term.GetSize(fd); err == nil && w > 0 {
		width = w
	}
	return &LineEditor{
		keyCh:     make(chan KeyEvent, 64),
		cancelCh:  cancelCh,
		state:     state,
		Prompt:    "> ",
		termWidth: width,
		history:   make([]string, 0, maxHistory),
		histIdx:   -1,
	}, nil
}

// Close 恢复终端状态。
func (e *LineEditor) Close() error {
	return term.Restore(os.Stdin.Fd(), e.state)
}

// Consume 实现 Consumer：只关注编辑键，且仅在有活跃读行会话时消费。
// 取消键（KeyEsc/KeyCtrlC）或非编辑键一律放行给栈底 keyWatcher；
// 无活跃会话时的编辑键也放行（agent 运行期的误敲最终被 keyWatcher 丢弃）。
func (e *LineEditor) Consume(ev KeyEvent) bool {
	switch ev.Type {
	case KeyChar, KeyEnter, KeyBackspace, KeyDelete, KeyArrowUp, KeyArrowDown, KeyArrowLeft, KeyArrowRight:
		if e.active.Load() {
			e.keyCh <- ev
			return true
		}
	}
	return false
}

// ReadLine 读取一行输入。
//   - line: 输入的文本（不含换行符）
//   - canceled: 用户按 Esc/Ctrl+C 取消了输入（keyWatcher 经 cancelCh 通知）
//   - err: 读取错误
func (e *LineEditor) ReadLine() (line string, canceled bool, err error) {
	e.active.Store(true)
	defer e.active.Store(false)

	// 清掉上次残留的取消信号（如 agent 运行期用户按过 Esc）
	select {
	case <-e.cancelCh:
	default:
	}

	e.buf = e.buf[:0]
	e.pos = 0
	e.histIdx = -1
	e.drawPrompt()

	for {
		select {
		case ev := <-e.keyCh:
			switch ev.Type {
			case KeyEnter:
				e.finishLine()
				line = string(e.buf)
				e.addHistory(line)
				return line, false, nil

			case KeyBackspace:
				if e.pos > 0 {
					e.pos--
					e.buf = append(e.buf[:e.pos], e.buf[e.pos+1:]...)
					e.redrawInput()
				}

			case KeyDelete:
				if e.pos < len(e.buf) {
					e.buf = append(e.buf[:e.pos], e.buf[e.pos+1:]...)
					e.redrawInput()
				}

			case KeyArrowUp:
				e.historyPrev()
			case KeyArrowDown:
				e.historyNext()
			case KeyArrowLeft:
				if e.pos > 0 {
					e.pos--
					e.redrawInput()
				}
			case KeyArrowRight:
				if e.pos < len(e.buf) {
					e.pos++
					e.redrawInput()
				}

			case KeyChar:
				e.buf = append(e.buf[:e.pos], append([]rune{ev.Rune}, e.buf[e.pos:]...)...)
				e.pos++
				e.redrawInput()
			}

		case <-e.cancelCh:
			e.clearInput()
			return "", true, nil
		}
	}
}

// historyPrev 上一条历史。
func (e *LineEditor) historyPrev() {
	if len(e.history) == 0 {
		return
	}
	if e.histIdx == -1 {
		e.histIdx = len(e.history) - 1
	} else if e.histIdx > 0 {
		e.histIdx--
	} else {
		return
	}
	e.loadHistory()
}

// historyNext 下一条历史。
func (e *LineEditor) historyNext() {
	if e.histIdx == -1 {
		return
	}
	e.histIdx++
	if e.histIdx >= len(e.history) {
		e.histIdx = -1
		e.buf = e.buf[:0]
		e.pos = 0
		e.redrawInput()
		return
	}
	e.loadHistory()
}

func (e *LineEditor) loadHistory() {
	e.buf = []rune(e.history[e.histIdx])
	e.pos = len(e.buf)
	e.redrawInput()
}

func (e *LineEditor) addHistory(line string) {
	if line == "" {
		return
	}
	if len(e.history) > 0 && e.history[len(e.history)-1] == line {
		return
	}
	e.history = append(e.history, line)
	if len(e.history) > maxHistory {
		e.history = e.history[1:]
	}
}

// ---- 终端绘制 ----

func (e *LineEditor) drawPrompt() {
	fmt.Fprint(os.Stderr, e.Prompt)
	e.renderedRows = 1
	e.cursorLineIdx = 0
}

func (e *LineEditor) redrawInput() {
	if e.cursorLineIdx > 0 {
		fmt.Fprintf(os.Stderr, "\033[%dA", e.cursorLineIdx)
	}
	fmt.Fprint(os.Stderr, "\r")
	for row := 0; row < e.renderedRows; row++ {
		fmt.Fprint(os.Stderr, "\033[K")
		if row < e.renderedRows-1 {
			fmt.Fprint(os.Stderr, "\r\n")
		}
	}
	if e.renderedRows > 1 {
		fmt.Fprintf(os.Stderr, "\033[%dA\r", e.renderedRows-1)
	}

	display := string(e.buf)
	fmt.Fprintf(os.Stderr, "%s%s", e.Prompt, display)

	total := e.terminalPosition(len(e.buf))
	cursor := e.terminalPosition(e.pos)
	e.renderedRows = total.row + 1
	e.cursorLineIdx = cursor.row
	if up := total.row - cursor.row; up > 0 {
		fmt.Fprintf(os.Stderr, "\033[%dA", up)
	}
	fmt.Fprintf(os.Stderr, "\r\033[%dC", cursor.col)
}

type terminalPos struct {
	row int
	col int
}

func (e *LineEditor) terminalPosition(pos int) terminalPos {
	if pos > len(e.buf) {
		pos = len(e.buf)
	}
	text := e.Prompt + string(e.buf[:pos])
	width := e.width()
	position := terminalPos{}
	graphemes := uniseg.NewGraphemes(text)
	for graphemes.Next() {
		w := graphemes.Width()
		if w <= 0 {
			continue
		}
		if position.col > 0 && position.col+w > width {
			position.row++
			position.col = 0
		}
		position.col += w
		if position.col > width {
			position.col = width
		}
	}
	return position
}

func (e *LineEditor) width() int {
	if e.termWidth > 0 {
		return e.termWidth
	}
	return 80
}

func (e *LineEditor) clearInput() {
	e.buf = e.buf[:0]
	e.pos = 0
	e.redrawInput()
}

func (e *LineEditor) finishLine() {
	fmt.Fprint(os.Stderr, "\r\n")
}

// DrainInput 清空 keyCh 与 cancelCh 中积压的事件，超时 5ms 后退出。
// 用于 HITL 会话/agent 运行结束后清掉用户已敲但未消费的残留输入。
func (e *LineEditor) DrainInput() {
	for {
		select {
		case <-e.keyCh:
		case <-e.cancelCh:
		case <-time.After(5 * time.Millisecond):
			return
		}
	}
}
