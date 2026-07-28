package stdin

import (
	"fmt"
	"os"
	"unicode/utf8"
	"time"

	"github.com/charmbracelet/x/term"
	"github.com/rivo/uniseg"
)

const (
	escByte       = 0x1b
	enterByte     = '\r'
	backspaceByte = 0x7f
	maxHistory    = 50
)

// escTimeout 区分单独 Esc 和 CSI 序列的前导字节
var escTimeout = 15 * time.Millisecond

// LineEditor 是一个 raw mode 下的行编辑器，支持 ↑↓ 历史、Esc 取消。
type LineEditor struct {
	fd      uintptr
	state   *term.State
	Prompt  string    // 输入提示符，"\" 开头，可临时置空避免 HITL 时画重复提示
	buf     []rune    // 以 rune 为单位跟踪输入（正确支持中文等 UTF-8）
	pos     int       // rune 索引
	history []string  // 历史记录，最新在末尾
	histIdx int       // -1 = 新输入，0 到 len-1 = 历史中的索引
	ubuf    []byte    // UTF-8 多字节累积缓冲
}

// NewLineEditor 创建并进入 raw mode。
func NewLineEditor() (*LineEditor, error) {
	fd := os.Stdin.Fd()
	state, err := term.MakeRaw(fd)
	if err != nil {
		return nil, fmt.Errorf("make raw: %w", err)
	}
	return &LineEditor{
		fd:      fd,
		state:   state,
		Prompt:  "> ",
		history: make([]string, 0, maxHistory),
		histIdx: -1,
	}, nil
}

// Close 恢复终端状态。
func (e *LineEditor) Close() error {
	return term.Restore(e.fd, e.state)
}

// ReadLine 读取一行输入。
//   - line: 输入的文本（不含换行符）
//   - canceled: 用户按 Esc 取消了输入
//   - err: 读取错误
func (e *LineEditor) ReadLine() (line string, canceled bool, err error) {
	e.buf = e.buf[:0]
	e.pos = 0
	e.histIdx = -1
	e.ubuf = e.ubuf[:0]
	e.drawPrompt()

	for {
		var b [1]byte
		n, err := os.Stdin.Read(b[:])
		if err != nil || n == 0 {
			return "", false, fmt.Errorf("read stdin: %w", err)
		}

		switch b[0] {
		case enterByte:
			e.finishLine()
			line = string(e.buf)
			e.addHistory(line)
			return line, false, nil

		case escByte:
			canceled, err := e.handleEscape()
			if err != nil {
				return "", false, err
			}
			if canceled {
				e.clearInput()
				return "", true, nil
			}

		case backspaceByte:
			if e.pos > 0 {
				e.pos--
				e.buf = append(e.buf[:e.pos], e.buf[e.pos+1:]...)
				e.redrawInput()
			}

		default:
			// 累积 UTF-8 字节，完整 rune 时插入
			if b[0] >= 0x20 {
				e.ubuf = append(e.ubuf, b[0])
				if utf8.FullRune(e.ubuf) {
					r, _ := utf8.DecodeRune(e.ubuf)
					e.ubuf = e.ubuf[:0]
					e.buf = append(e.buf[:e.pos], append([]rune{r}, e.buf[e.pos:]...)...)
					e.pos++
					e.redrawInput()
				}
			}
		}
	}
}

// handleEscape 处理 Escape 相关的输入序列。
func (e *LineEditor) handleEscape() (canceled bool, err error) {
	next, err := readByteTimeout(escTimeout)
	if err != nil {
		return false, err
	}

	if next != '[' {
		if len(e.buf) == 0 {
			return true, nil
		}
		e.clearInput()
		return true, nil
	}

	var dir [1]byte
	n, err := os.Stdin.Read(dir[:])
	if err != nil || n == 0 {
		return false, fmt.Errorf("read csi: %w", err)
	}

	peek, _ := readByteTimeout(escTimeout)

	switch dir[0] {
	case 'A':
		e.historyPrev()
	case 'B':
		e.historyNext()
	case 'C':
		if e.pos < len(e.buf) {
			e.pos++
			e.redrawInput()
		}
	case 'D':
		if e.pos > 0 {
			e.pos--
			e.redrawInput()
		}
	case '3':
		if peek == '~' && e.pos < len(e.buf) {
			e.buf = append(e.buf[:e.pos], e.buf[e.pos+1:]...)
			e.redrawInput()
		}
	}

	return false, nil
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
}

func (e *LineEditor) redrawInput() {
	display := string(e.buf)
	pw := uniseg.StringWidth(e.Prompt)
	fmt.Fprintf(os.Stderr, "\r%s%s\033[K", e.Prompt, display)
	if e.pos < len(e.buf) {
		prefix := string(e.buf[:e.pos])
		w := pw + uniseg.StringWidth(prefix)
		fmt.Fprintf(os.Stderr, "\r\033[%dC", w)
	}
}

func (e *LineEditor) clearInput() {
	e.buf = e.buf[:0]
	e.pos = 0
	e.ubuf = e.ubuf[:0]
	e.redrawInput()
}

func (e *LineEditor) finishLine() {
	fmt.Fprint(os.Stderr, "\r\n")
}

// DrainInput 在 agent 运行后调用，清空积压的 stdin 输入。
func (e *LineEditor) DrainInput() {
	for {
		b, err := readByteTimeout(5 * time.Millisecond)
		if err != nil || b == 0 {
			break
		}
	}
}

// ---- 包级函数 ----

// readByteTimeout 带超时从 stdin 读取一个字节。超时返回 0, nil。
func readByteTimeout(d time.Duration) (byte, error) {
	ch := make(chan byte, 1)
	errCh := make(chan error, 1)

	go func() {
		var b [1]byte
		n, err := os.Stdin.Read(b[:])
		if err != nil || n == 0 {
			errCh <- err
			return
		}
		ch <- b[0]
	}()

	select {
	case b := <-ch:
		return b, nil
	case err := <-errCh:
		return 0, err
	case <-time.After(d):
		return 0, nil
	}
}
