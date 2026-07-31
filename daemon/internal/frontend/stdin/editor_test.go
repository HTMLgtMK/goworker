package stdin

import (
	"testing"
	"time"
)

// testLineEditor 创建一个不进入 raw mode 的 LineEditor 用于测试。
func testLineEditor(ch chan byte) *LineEditor {
	return &LineEditor{
		stdinCh: ch,
		Prompt:  "",
		history: make([]string, 0, maxHistory),
		histIdx: -1,
	}
}

// writeBytes 模拟键盘输入，将字符串的每个字节逐个写入 channel。
// 注意用 []byte 转换确保多字节 UTF-8 的每个字节都写入（不能用 for range string）。
func writeBytes(ch chan<- byte, s string) {
	for _, b := range []byte(s) {
		ch <- b
	}
}

// readLineAsync 在 goroutine 中运行 ReadLine，结果通过 channel 返回。
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
	return ch
}

// ---- 正常输入 ----

func TestReadLine_NormalInput(t *testing.T) {
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	rch := readLineAsync(ed)
	writeBytes(ch, "hello world")
	ch <- enterByte

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
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	rch := readLineAsync(ed)
	ch <- enterByte

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

// ---- Esc 取消 ----

func TestReadLine_EscCancel(t *testing.T) {
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	rch := readLineAsync(ed)
	ch <- escByte // 独立 Esc → 15ms 超时 → canceled

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

func TestReadLine_EscWithInputCancels(t *testing.T) {
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	rch := readLineAsync(ed)
	writeBytes(ch, "partial")
	ch <- escByte // 有输入时按 Esc → 取消

	select {
	case r := <-rch:
		if !r.canceled {
			t.Fatal("expected canceled")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ReadLine")
	}
}

// ---- Ctrl+C 取消 ----

func TestReadLine_CtrlC(t *testing.T) {
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	rch := readLineAsync(ed)
	ch <- ctrlCByte

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
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	rch := readLineAsync(ed)
	writeBytes(ch, "abc")
	ch <- backspaceByte // abc → ab
	writeBytes(ch, "x")
	ch <- enterByte

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
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	// 先在历史里放两条记录
	ed.addHistory("first cmd")
	ed.addHistory("second cmd")

	rch := readLineAsync(ed)

	// ↑ 两次: second → first（CSI 序列: ESC [ A）
	ch <- escByte
	ch <- '['
	ch <- 'A'
	ch <- escByte
	ch <- '['
	ch <- 'A'
	// ↓ 一次: 回到 second（ESC [ B）
	ch <- escByte
	ch <- '['
	ch <- 'B'
	// 回车
	ch <- enterByte

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
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	// 没历史时 ↑ 应该无效果
	rch := readLineAsync(ed)
	ch <- escByte
	ch <- '['
	ch <- 'A' // CSI Up — 历史为空，什么都不发生
	writeBytes(ch, "new")
	ch <- enterByte

	select {
	case r := <-rch:
		if r.line != "new" {
			t.Fatalf("got %q, want %q", r.line, "new")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// ---- 中文输入 ----

func TestReadLine_Chinese(t *testing.T) {
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	rch := readLineAsync(ed)
	writeBytes(ch, "你好世界")
	ch <- enterByte

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
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	rch := readLineAsync(ed)
	writeBytes(ch, "你好吗")
	ch <- backspaceByte // 删除"吗"
	writeBytes(ch, "的")
	ch <- enterByte

	select {
	case r := <-rch:
		if r.line != "你好的" {
			t.Fatalf("got %q, want %q", r.line, "你好的")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// ---- drainChannel ----

func TestDrainChannel_Empty(t *testing.T) {
	ch := make(chan byte, 64)
	drainChannel(ch)
	// 不 panic 就通过
}

func TestDrainChannel_ClearsBytes(t *testing.T) {
	ch := make(chan byte, 64)
	ch <- 'a'
	ch <- 'b'
	ch <- '\r'

	drainChannel(ch)

	if len(ch) != 0 {
		t.Fatalf("channel has %d bytes after drain, want 0", len(ch))
	}
}

// ---- drain + ReadLine 模拟 HITL 场景 ----

func TestDrainThenReadLine_StaleBytesCleared(t *testing.T) {
	// 模拟 HITL 前积压的字节（agent 运行期间用户乱敲的）
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	stale := "stale\r"
	writeBytes(ch, stale)

	// drain 清空
	drainChannel(ch)

	// ReadLine 应该等新输入，不消费残留
	rch := readLineAsync(ed)

	// 等一小段时间确保 ReadLine 阻塞了（如果它消费了残留，已经返回了）
	time.Sleep(50 * time.Millisecond)
	select {
	case <-rch:
		t.Fatal("ReadLine returned immediately after drain — stale bytes not cleared")
	default:
	}

	// 现在发正常输入
	writeBytes(ch, "fresh input")
	ch <- enterByte

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
	// HITL 场景：用户狂按 Enter，一堆 \r 积压
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	for i := 0; i < 10; i++ {
		ch <- '\r'
	}

	drainChannel(ch)

	rch := readLineAsync(ed)

	time.Sleep(50 * time.Millisecond)
	select {
	case <-rch:
		t.Fatal("ReadLine immediately returned after drain of multiple enters")
	default:
	}

	ch <- 'a'
	ch <- enterByte

	select {
	case r := <-rch:
		if r.line != "a" {
			t.Fatalf("got %q, want %q", r.line, "a")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// ---- Esc 在 drain + ReadLine 中正常生效 ----

func TestDrainThenReadLine_EscCancels(t *testing.T) {
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	writeBytes(ch, "stale data")
	drainChannel(ch)

	rch := readLineAsync(ed)
	time.Sleep(20 * time.Millisecond)

	ch <- escByte

	select {
	case r := <-rch:
		if !r.canceled {
			t.Fatal("expected canceled after Esc")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// ---- ReadLine 后 Esc 确认不遗留数据 ----

func TestReadLine_ReadLineCleanState(t *testing.T) {
	// 两次连续的 ReadLine，确保状态不被污染
	ch := make(chan byte, 64)
	ed := testLineEditor(ch)

	// 第一次：Esc 取消
	rch1 := readLineAsync(ed)
	ch <- escByte

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
	writeBytes(ch, "after esc")
	ch <- enterByte

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
