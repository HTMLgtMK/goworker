package stdin

import (
	"fmt"
	"os"
	"strings"
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

	// autocomplete（specs/002）：ac 为纯逻辑状态机，acList/acIdx 是当前展示的
	// 候选区（nil = 无弹层），popupRows 是上一次渲染落在输入行下方的候选行数。
	ac        *completer
	acList    []candidate
	acIdx     int
	popupRows int
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

// SetCompleter 注入命令补全器（frontend 启动时从 engine.Commands() 构建）。
func (e *LineEditor) SetCompleter(c *completer) { e.ac = c }

// Consume 实现 Consumer：只关注编辑键，且仅在有活跃读行会话时消费。
// 取消键（KeyEsc/KeyCtrlC）或非编辑键一律放行给栈底 keyWatcher；
// 无活跃会话时的编辑键也放行（agent 运行期的误敲最终被 keyWatcher 丢弃）。
func (e *LineEditor) Consume(ev KeyEvent) bool {
	switch ev.Type {
	case KeyChar, KeyEnter, KeyBackspace, KeyDelete, KeyTab, KeyShiftTab,
		KeyArrowUp, KeyArrowDown, KeyArrowLeft, KeyArrowRight:
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
	e.resetPopup()
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

			case KeyTab, KeyShiftTab:
				e.handleTab(ev.Type == KeyTab)

			case KeyBackspace:
				if e.pos > 0 {
					e.dropPopup()
					e.pos--
					e.buf = append(e.buf[:e.pos], e.buf[e.pos+1:]...)
					e.redrawInput()
				}

			case KeyDelete:
				if e.pos < len(e.buf) {
					e.dropPopup()
					e.buf = append(e.buf[:e.pos], e.buf[e.pos+1:]...)
					e.redrawInput()
				}

			case KeyArrowUp:
				e.dropPopup()
				e.historyPrev()
			case KeyArrowDown:
				e.dropPopup()
				e.historyNext()
			case KeyArrowLeft:
				if e.pos > 0 {
					e.dropPopup()
					e.pos--
					e.redrawInput()
				}
			case KeyArrowRight:
				if e.pos < len(e.buf) {
					e.dropPopup()
					e.pos++
					e.redrawInput()
				}

			case KeyChar:
				e.dropPopup()
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

// ---- 补全（specs/002-command-autocomplete） ----

// handleTab 处理一次 Tab/Shift-Tab：仅当行以 / 开头且光标在首个词的行尾时补全。
// 无候选响铃；唯一候选直接补全；多候选进入 cycle 并在输入行下方展示候选区。
func (e *LineEditor) handleTab(forward bool) {
	if e.ac == nil || !e.tabEligible() {
		return
	}
	outcome, text, list, idx := e.ac.complete(string(e.buf), forward)
	switch outcome {
	case tabNoMatch:
		fmt.Fprint(os.Stderr, "\a") // 终端响铃，无副作用
	case tabUnique:
		e.buf = []rune(text)
		e.pos = len(e.buf)
		e.resetPopup()
		e.redrawInput()
	case tabCycling:
		e.buf = []rune(text)
		e.pos = len(e.buf)
		e.acList = list
		e.acIdx = idx
		e.redrawInput()
	}
}

// tabEligible 判定当前是否处于可补全语境：
// 行以 / 开头、光标在行尾（避免静默丢弃光标后的内容）、行内无空白（光标在首个词内）。
func (e *LineEditor) tabEligible() bool {
	if len(e.buf) == 0 || e.buf[0] != '/' || e.pos != len(e.buf) {
		return false
	}
	for _, r := range e.buf {
		if r == ' ' || r == '\t' {
			return false
		}
	}
	return true
}

// dropPopup 任意编辑键退出 cycle：清候选区状态并擦屏（若候选区正在展示）。
func (e *LineEditor) dropPopup() {
	if e.ac != nil {
		e.ac.reset()
	}
	if e.acList != nil {
		e.resetPopup()
		e.redrawInput()
	}
}

func (e *LineEditor) resetPopup() {
	e.acList = nil
	e.acIdx = 0
}

// maxPopupRows 候选区最多展示的行数，超出部分以计数提示收尾。
const maxPopupRows = 8

// popupLines 渲染候选区各行：当前 cycle 候选反白，描述灰色弱化，超宽截断。
func (e *LineEditor) popupLines() []string {
	if len(e.acList) == 0 {
		return nil
	}
	shown := e.acList
	hidden := 0
	if len(shown) > maxPopupRows {
		shown, hidden = shown[:maxPopupRows], len(shown)-maxPopupRows
	}
	width := e.width()
	lines := make([]string, 0, len(shown)+1)
	for i, c := range shown {
		name := c.completion
		if i == e.acIdx {
			name = "\033[7m" + name + "\033[0m" // 反白当前候选
		}
		// 描述预算：总宽 - 名字 - 分隔两列 - 1 列余量（防 word wrap 换行）
		budget := width - uniseg.StringWidth(c.completion) - 3
		desc := ""
		if c.description != "" && budget > 1 {
			desc = "  " + thinkingColor + truncateVisible(c.description, budget) + ansiReset
		}
		lines = append(lines, "  "+name+desc)
	}
	if hidden > 0 {
		lines = append(lines, "  "+thinkingColor+fmt.Sprintf("… (+%d more)", hidden)+ansiReset)
	}
	return lines
}

// truncateVisible 按显示宽度截断（grapheme 感知，中文不劈半），溢出以 … 收尾。
func truncateVisible(s string, budget int) string {
	if uniseg.StringWidth(s) <= budget {
		return s
	}
	var b strings.Builder
	w := 0
	g := uniseg.NewGraphemes(s)
	for g.Next() {
		gw := g.Width()
		if w+gw > budget-1 {
			break
		}
		b.WriteString(g.Str())
		w += gw
	}
	b.WriteString("…")
	return b.String()
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
	// 擦除范围含上次渲染落在输入行下方的候选区
	total := e.renderedRows + e.popupRows
	for row := 0; row < total; row++ {
		fmt.Fprint(os.Stderr, "\033[K")
		if row < total-1 {
			fmt.Fprint(os.Stderr, "\r\n")
		}
	}
	if total > 1 {
		fmt.Fprintf(os.Stderr, "\033[%dA\r", total-1)
	}

	display := string(e.buf)
	ghostStyled, ghostPlain := e.ghostHint()
	fmt.Fprintf(os.Stderr, "%s%s%s", e.Prompt, display, ghostStyled)

	// total 含 ghost 的换行（ghost 只在行尾出现，宽度计入行占位）
	tp := e.positionOf(e.Prompt + display + ghostPlain)
	cursor := e.terminalPosition(e.pos)
	e.renderedRows = tp.row + 1
	e.cursorLineIdx = cursor.row

	// 候选区渲染在输入最后一行下方
	e.popupRows = 0
	for _, line := range e.popupLines() {
		fmt.Fprintf(os.Stderr, "\r\n%s", line)
		e.popupRows++
	}

	// 从候选区末行回到输入光标处
	if up := e.popupRows + (tp.row - cursor.row); up > 0 {
		fmt.Fprintf(os.Stderr, "\033[%dA", up)
	}
	fmt.Fprintf(os.Stderr, "\r\033[%dC", cursor.col)
}

type terminalPos struct {
	row int
	col int
}

// ghostHint 行内灰色提示（fish 风格）：光标在行尾且正在输入首个 / 词时，
// 在光标后追加最佳匹配的剩余字符 + 描述。纯展示 —— Enter 只提交 buf，
// Tab 接受提示（走 handleTab 既有路径）。返回 (带色渲染, 纯文本) 供宽度计算。
func (e *LineEditor) ghostHint() (styled, plain string) {
	if e.ac == nil || len(e.buf) == 0 || e.buf[0] != '/' || e.pos != len(e.buf) {
		return "", ""
	}
	for _, r := range e.buf {
		if r == ' ' || r == '\t' {
			return "", ""
		}
	}
	input := string(e.buf)
	matches := e.ac.match(input)
	if len(matches) == 0 {
		return "", ""
	}
	best := matches[0]

	remainder := ""
	if len(best.completion) > len(input) {
		remainder = best.completion[len(input):]
	}
	// 描述预算：终端宽 - 提示符与已输入 - 剩余字符 - 分隔两列 - 1 列余量
	used := uniseg.StringWidth(e.Prompt+input) + uniseg.StringWidth(remainder)
	budget := e.width() - used - 3
	desc := ""
	if best.description != "" && budget > 1 {
		desc = "  " + truncateVisible(best.description, budget)
	}
	if remainder == "" && desc == "" {
		return "", ""
	}
	plain = remainder + desc
	styled = thinkingColor + remainder + desc + ansiReset
	return styled, plain
}

func (e *LineEditor) positionOf(text string) terminalPos {
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

// terminalPosition 返回 buf[:pos] 在终端上的行列位置（含提示符）。
func (e *LineEditor) terminalPosition(pos int) terminalPos {
	if pos > len(e.buf) {
		pos = len(e.buf)
	}
	return e.positionOf(e.Prompt + string(e.buf[:pos]))
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
	e.resetPopup()
	e.redrawInput()
}

func (e *LineEditor) finishLine() {
	// 候选区悬挂在输入行下方，先擦掉再提交，避免残留在屏幕上
	if e.acList != nil {
		e.resetPopup()
		e.redrawInput()
	}
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
