package stdin

import (
	"testing"
	"time"
)

// testDecoder 创建一个 KeyDecoder，解码结果投递到 channel。
func testDecoder() (*KeyDecoder, chan KeyEvent) {
	ch := make(chan KeyEvent, 64)
	d := NewKeyDecoder(func(ev KeyEvent) { ch <- ev })
	return d, ch
}

// feed 逐字节投递给解码器。
func feed(d *KeyDecoder, bs ...byte) {
	for _, b := range bs {
		d.In() <- b
	}
}

// recv 等待一个解码事件，超时失败。
func recv(t *testing.T, ch chan KeyEvent, timeout time.Duration) KeyEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatal("timeout waiting for key event")
		return KeyEvent{}
	}
}

func TestDecode_PlainText(t *testing.T) {
	d, ch := testDecoder()
	feed(d, 'h', 'e', 'l', 'l', 'o')

	got := recv(t, ch, time.Second)
	if got.Type != KeyChar || got.Rune != 'h' {
		t.Fatalf("got %+v, want KeyChar('h')", got)
	}
}

func TestDecode_Chinese(t *testing.T) {
	// "你" = E4 BD A0 三字节 → 1 个 KeyChar
	d, ch := testDecoder()
	feed(d, 0xe4, 0xbd, 0xa0)

	got := recv(t, ch, time.Second)
	if got.Type != KeyChar || got.Rune != '你' {
		t.Fatalf("got %+v, want KeyChar('你')", got)
	}
}

func TestDecode_SpecialKeys(t *testing.T) {
	d, ch := testDecoder()
	feed(d, enterByte, backspaceByte, ctrlCByte)

	if ev := recv(t, ch, time.Second); ev.Type != KeyEnter {
		t.Fatalf("want KeyEnter, got %+v", ev)
	}
	if ev := recv(t, ch, time.Second); ev.Type != KeyBackspace {
		t.Fatalf("want KeyBackspace, got %+v", ev)
	}
	if ev := recv(t, ch, time.Second); ev.Type != KeyCtrlC {
		t.Fatalf("want KeyCtrlC, got %+v", ev)
	}
}

func TestDecode_StandaloneEsc(t *testing.T) {
	// 独立 Esc：15ms 无 '[' → KeyEsc
	d, ch := testDecoder()
	feed(d, escByte)

	got := recv(t, ch, 500*time.Millisecond)
	if got.Type != KeyEsc {
		t.Fatalf("want KeyEsc, got %+v", got)
	}
}

func TestDecode_ArrowKeys(t *testing.T) {
	d, ch := testDecoder()
	// ESC [ A/B/C/D
	feed(d, escByte, '[', 'A')
	if ev := recv(t, ch, time.Second); ev.Type != KeyArrowUp {
		t.Fatalf("want KeyArrowUp, got %+v", ev)
	}

	feed(d, escByte, '[', 'D')
	if ev := recv(t, ch, time.Second); ev.Type != KeyArrowLeft {
		t.Fatalf("want KeyArrowLeft, got %+v", ev)
	}
}

func TestDecode_DeleteKey(t *testing.T) {
	// ESC [ 3 ~ → KeyDelete
	d, ch := testDecoder()
	feed(d, escByte, '[', '3', '~')

	got := recv(t, ch, time.Second)
	if got.Type != KeyDelete {
		t.Fatalf("want KeyDelete, got %+v", got)
	}
}

func TestDecode_UnknownCSI(t *testing.T) {
	// ESC [ Z（未识别序列）→ 忽略，不产生事件
	d, ch := testDecoder()
	feed(d, escByte, '[', 'Z')

	select {
	case ev := <-ch:
		t.Fatalf("unexpected event for unknown CSI: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDecode_EscThenChar(t *testing.T) {
	// ESC a：独立 Esc 后跟字符 → KeyEsc + KeyChar('a')
	d, ch := testDecoder()
	feed(d, escByte, 'a')

	if ev := recv(t, ch, 500*time.Millisecond); ev.Type != KeyEsc {
		t.Fatalf("want KeyEsc, got %+v", ev)
	}
	if ev := recv(t, ch, time.Second); ev.Type != KeyChar || ev.Rune != 'a' {
		t.Fatalf("want KeyChar('a'), got %+v", ev)
	}
}

func TestDecode_MixedInput(t *testing.T) {
	// 混合：中文 + Enter + 退格
	d, ch := testDecoder()
	feed(d, 0xe4, 0xbd, 0xa0, enterByte, backspaceByte)

	if ev := recv(t, ch, time.Second); ev.Type != KeyChar || ev.Rune != '你' {
		t.Fatalf("want KeyChar('你'), got %+v", ev)
	}
	if ev := recv(t, ch, time.Second); ev.Type != KeyEnter {
		t.Fatalf("want KeyEnter, got %+v", ev)
	}
	if ev := recv(t, ch, time.Second); ev.Type != KeyBackspace {
		t.Fatalf("want KeyBackspace, got %+v", ev)
	}
}
