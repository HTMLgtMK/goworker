package stdin

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// testLineEditor 创建一个不进入 raw mode 的 LineEditor 用于测试。
// 返回 cancelCh 写端，供取消场景模拟 keyWatcher 触发。
func testLineEditor() (*LineEditor, chan struct{}) {
	cancelCh := make(chan struct{}, 1)
	return &LineEditor{
		keyCh:    make(chan KeyEvent, 256),
		cancelCh: cancelCh,
		Prompt:   "",
		history:  make([]string, 0, maxHistory),
		histIdx:  -1,
	}, cancelCh
}

// readLineAsync 在 goroutine 中运行 ReadLine，结果通过 channel 返回。
// 返回前等 ReadLine 进入活跃读行状态，避免测试主 goroutine 提前投递被 Consume 拒绝。
type readLineResult struct {
	line     string
	canceled bool
}

func readLineAsync(ed *LineEditor) chan readLineResult {
	ch := make(chan readLineResult, 1)
	go func() {
		line, canceled, _ := ed.ReadLine()
		ch <- readLineResult{line, canceled}
	}()
	for !ed.active.Load() {
		time.Sleep(time.Millisecond)
	}
	return ch
}

// ---- Consume 语义 ----

func TestConsume_InactiveDrops(t *testing.T) {
	// 无活跃 ReadLine 时，编辑键不被消费、不积压（agent 运行期的误敲直接丢弃）
	ed, _ := testLineEditor()
	if ed.Consume(char('a')) {
		t.Fatal("inactive editor should not consume edit keys")
	}
	if len(ed.keyCh) != 0 {
		t.Fatalf("keyCh has %d events after inactive consume, want 0", len(ed.keyCh))
	}
}

func TestConsume_KeyEscNeverConsumed(t *testing.T) {
	// 取消键无条件放行（归 keyWatcher），即便在读行状态
	ed, _ := testLineEditor()
	if ed.Consume(key(KeyEsc)) {
		t.Fatal("editor should never consume cancel keys")
	}
}

func TestConsume_ActiveConsumes(t *testing.T) {
	ed, _ := testLineEditor()
	rch := readLineAsync(ed)
	if !ed.Consume(char('a')) {
		t.Fatal("active editor should consume edit keys")
	}
	typeKey(ed, key(KeyEnter))
	select {
	case r := <-rch:
		if r.line != "a" {
			t.Fatalf("got %q, want %q", r.line, "a")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ReadLine")
	}
}

// ---- 正常输入 ----

func TestReadLine_NormalInput(t *testing.T) {
	ed, _ := testLineEditor()

	rch := readLineAsync(ed)
	typeString(ed, "hello world")
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "hello world" {
			t.Fatalf("got %q, want %q", r.line, "hello world")
		}
		if r.canceled {
			t.Fatal("unexpected canceled")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ReadLine")
	}
}

func TestReadLine_EmptyEnter(t *testing.T) {
	ed, _ := testLineEditor()

	rch := readLineAsync(ed)
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "" {
			t.Fatalf("got %q, want empty", r.line)
		}
		if r.canceled {
			t.Fatal("unexpected canceled")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ReadLine")
	}
}

// ---- 取消（keyWatcher 经 cancelCh 触发）----

func TestReadLine_CancelViaCancelCh(t *testing.T) {
	ed, cancelCh := testLineEditor()

	rch := readLineAsync(ed)
	cancelCh <- struct{}{} // 模拟 keyWatcher 收到 Esc

	select {
	case r := <-rch:
		if !r.canceled {
			t.Fatal("expected canceled")
		}
		if r.line != "" {
			t.Fatalf("got %q, want empty", r.line)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ReadLine")
	}
}

func TestReadLine_CancelWithInput(t *testing.T) {
	ed, cancelCh := testLineEditor()

	rch := readLineAsync(ed)
	typeString(ed, "partial")
	cancelCh <- struct{}{}

	select {
	case r := <-rch:
		if !r.canceled {
			t.Fatal("expected canceled")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ReadLine")
	}
}

// ---- 退格 ----

func TestReadLine_Backspace(t *testing.T) {
	ed, _ := testLineEditor()

	rch := readLineAsync(ed)
	typeString(ed, "abc")
	typeKey(ed, key(KeyBackspace)) // abc → ab
	typeString(ed, "x")
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "abx" {
			t.Fatalf("got %q, want %q", r.line, "abx")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// ---- 上下键历史 ----

func TestReadLine_HistoryUpDown(t *testing.T) {
	ed, _ := testLineEditor()

	// 先在历史里放两条记录
	ed.addHistory("first cmd")
	ed.addHistory("second cmd")

	rch := readLineAsync(ed)

	// ↑ 两次: second → first，↓ 一次: 回到 second
	typeKey(ed, key(KeyArrowUp))
	typeKey(ed, key(KeyArrowUp))
	typeKey(ed, key(KeyArrowDown))
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "second cmd" {
			t.Fatalf("got %q, want %q", r.line, "second cmd")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestReadLine_HistoryEmpty(t *testing.T) {
	ed, _ := testLineEditor()

	// 没历史时 ↑ 应该无效果
	rch := readLineAsync(ed)
	typeKey(ed, key(KeyArrowUp))
	typeString(ed, "new")
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "new" {
			t.Fatalf("got %q, want %q", r.line, "new")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// ---- Delete 与左右光标 ----

func TestReadLine_DeleteAndArrows(t *testing.T) {
	ed, _ := testLineEditor()

	rch := readLineAsync(ed)
	typeString(ed, "abc")
	// 光标回到 b 前，Delete 删掉 b
	typeKey(ed, key(KeyArrowLeft))
	typeKey(ed, key(KeyArrowLeft))
	typeKey(ed, key(KeyDelete))
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "ac" {
			t.Fatalf("got %q, want %q", r.line, "ac")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// ---- 中文输入 ----

func TestReadLine_Chinese(t *testing.T) {
	ed, _ := testLineEditor()

	rch := readLineAsync(ed)
	typeString(ed, "你好世界")
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "你好世界" {
			t.Fatalf("got %q, want %q", r.line, "你好世界")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// ---- 混合中文和退格 ----

func TestReadLine_ChineseBackspace(t *testing.T) {
	ed, _ := testLineEditor()

	rch := readLineAsync(ed)
	typeString(ed, "你好吗")
	typeKey(ed, key(KeyBackspace)) // 删除"吗"
	typeString(ed, "的")
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "你好的" {
			t.Fatalf("got %q, want %q", r.line, "你好的")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// ---- DrainInput ----

func TestDrainInput_Empty(t *testing.T) {
	ed, _ := testLineEditor()
	ed.DrainInput()
	// 不 panic 就通过
}

func TestDrainInput_ClearsEvents(t *testing.T) {
	ed, _ := testLineEditor()
	ed.keyCh <- char('a')
	ed.keyCh <- char('b')
	ed.keyCh <- key(KeyEnter)

	ed.DrainInput()

	if len(ed.keyCh) != 0 {
		t.Fatalf("keyCh has %d events after drain, want 0", len(ed.keyCh))
	}
}

// ---- 残留事件场景 ----

func TestInactiveStaleDropped(t *testing.T) {
	// agent 运行期（无活跃 ReadLine）用户乱敲：Consume 全被拒绝，不积压
	ed, _ := testLineEditor()

	typeString(ed, "stale")
	if len(ed.keyCh) != 0 {
		t.Fatalf("stale events should be dropped when inactive, keyCh=%d", len(ed.keyCh))
	}

	// 之后 ReadLine 等的是全新输入
	rch := readLineAsync(ed)
	typeString(ed, "fresh input")
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "fresh input" {
			t.Fatalf("got %q, want %q", r.line, "fresh input")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for fresh input")
	}
}

func TestDrainThenReadLine_MultipleEnter(t *testing.T) {
	// HITL 场景：会话内积压的 Enter 先被清掉，ReadLine 等新输入
	ed, _ := testLineEditor()

	for i := 0; i < 10; i++ {
		ed.keyCh <- key(KeyEnter)
	}
	ed.DrainInput()

	rch := readLineAsync(ed)
	typeString(ed, "a")
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch:
		if r.line != "a" {
			t.Fatalf("got %q, want %q", r.line, "a")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestRedrawInputClearsWrappedPreviousRender(t *testing.T) {
	ed, _ := testLineEditor()
	ed.Prompt = "> "
	ed.termWidth = 6

	output := captureStderr(t, func() {
		ed.buf = []rune("abcde")
		ed.pos = len(ed.buf)
		ed.redrawInput()

		ed.buf = []rune("a")
		ed.pos = len(ed.buf)
		ed.redrawInput()
	})

	if !strings.Contains(output, "\x1b[1A") {
		t.Fatalf("redraw did not move up to clear wrapped input: %q", output)
	}
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stderr: %v", err)
	}
	os.Stderr = w
	defer func() { os.Stderr = old }()

	fn()
	if err := w.Close(); err != nil {
		t.Fatalf("close stderr writer: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	return string(out)
}

func TestTerminalPositionWrapsWideGraphemeAtLineEdge(t *testing.T) {
	ed, _ := testLineEditor()
	ed.Prompt = "> "
	ed.termWidth = 4
	ed.buf = []rune("a你")

	got := ed.terminalPosition(len(ed.buf))
	want := terminalPos{row: 1, col: 2}
	if got != want {
		t.Fatalf("terminalPosition wide edge = %#v, want %#v", got, want)
	}
}

func TestTerminalPositionExactWidthStaysOnCurrentRow(t *testing.T) {
	ed, _ := testLineEditor()
	ed.Prompt = "> "
	ed.termWidth = 4
	ed.buf = []rune("ab")

	got := ed.terminalPosition(len(ed.buf))
	want := terminalPos{row: 0, col: 4}
	if got != want {
		t.Fatalf("terminalPosition exact width = %#v, want %#v", got, want)
	}
}

// ---- 取消后干净状态 ----

func TestReadLine_ReadLineCleanState(t *testing.T) {
	// 两次连续的 ReadLine，确保状态不被污染
	ed, cancelCh := testLineEditor()

	// 第一次：取消
	rch1 := readLineAsync(ed)
	cancelCh <- struct{}{}

	select {
	case r := <-rch1:
		if !r.canceled {
			t.Fatal("first ReadLine expected canceled")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// 第二次：正常输入
	rch2 := readLineAsync(ed)
	typeString(ed, "after esc")
	typeKey(ed, key(KeyEnter))

	select {
	case r := <-rch2:
		if r.line != "after esc" {
			t.Fatalf("got %q, want %q", r.line, "after esc")
		}
		if r.canceled {
			t.Fatal("second ReadLine unexpected canceled")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}
