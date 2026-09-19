package stdin

import (
	"time"
	"unicode/utf8"
)

// 原始字节常量（解码层使用）。
const (
	escByte       = 0x1b
	ctrlCByte     = 0x03
	enterByte     = '\r'
	backspaceByte = 0x7f
	tabByte       = 0x09
)

// KeyType 表示解码后的按键事件类型。
// 责任链消费的是 KeyEvent 而非原始字节——Esc 是取消键还是 CSI 前导的歧义
// 由 KeyDecoder 消化，consumer 只看自己按下了什么键。
type KeyType int

const (
	KeyChar       KeyType = iota // 可打印字符（含中文等，Rune 有效）
	KeyEnter                     // 回车，提交当前行
	KeyBackspace                 // 退格
	KeyDelete                    // Delete 键（CSI ESC [ 3 ~）
	KeyArrowUp                   // ↑（CSI ESC [ A）
	KeyArrowDown                 // ↓（CSI ESC [ B）
	KeyArrowLeft                 // ←（CSI ESC [ D）
	KeyArrowRight                // →（CSI ESC [ C）
	KeyEsc                       // 独立 Esc（取消）
	KeyCtrlC                     // Ctrl+C（取消）
	KeyTab                       // Tab（补全，0x09）
	KeyShiftTab                  // Shift+Tab（反向补全，CSI ESC [ Z）
)

// KeyEvent 是解码后的按键事件。
type KeyEvent struct {
	Type KeyType
	Rune rune // KeyChar 时有效
}

// escTimeout 区分单独 Esc 和 CSI 序列的前导字节。
// 终端发送 CSI 序列的三个字节是连续的，单独按 Esc 后不会有后续字节。
var escTimeout = 15 * time.Millisecond

// KeyDecoder 将 os.Stdin 的字节流解码为按键事件。
//
// 承担所有跨字节的状态判断，让责任链只处理语义清晰的按键：
//   - UTF-8 多字节字符累积（"你" = 3 字节 → 1 个 KeyChar）
//   - Esc 前瞻：15ms 内无 '[' 判定为独立 Esc，有 '[' 进入 CSI 状态
//   - CSI 序列解析：ESC [ A/B/C/D → 方向键，ESC [ 3 ~ → Delete
//
// 运行在独立 goroutine：dispatchStdin 投字节到 inCh，解码结果经 out 回调
// 路由给责任链。Esc 的 15ms 超时由内部 timer 处理，不阻塞字节消费。
type KeyDecoder struct {
	inCh chan byte      // dispatchStdin → 字节
	out  func(KeyEvent) // 解码结果 → 责任链
	esc  *time.Timer    // escPending 时计时的超时器

	escPending bool   // 已收到 Esc，等待判定 CSI 还是独立 Esc
	escDead    bool   // 已确认 CSI，等待方向字节（此时不再超时）
	esc3       bool   // 已收到 ESC [ 3，等待 '~' 完成 Delete
	ubuf       []byte // UTF-8 多字节累积缓冲
}

// NewKeyDecoder 创建解码器并启动处理 goroutine。
// out 在解码出按键事件时被调用，须可安全并发。
func NewKeyDecoder(out func(KeyEvent)) *KeyDecoder {
	d := &KeyDecoder{
		inCh: make(chan byte, 256),
		out:  out,
		esc:  time.NewTimer(time.Hour), // 初始禁用
		ubuf: make([]byte, 0, 4),
	}
	if !d.esc.Stop() {
		<-d.esc.C
	}
	go d.run()
	return d
}

// In 返回字节投递通道，供 dispatchStdin 写入。
func (d *KeyDecoder) In() chan<- byte { return d.inCh }

func (d *KeyDecoder) run() {
	for {
		select {
		case b := <-d.inCh:
			d.process(b)
		case <-d.esc.C:
			if d.escPending {
				d.escPending = false
				d.emit(KeyEvent{Type: KeyEsc}) // 15ms 无 '[' → 独立 Esc
			}
		}
	}
}

// process 处理单个字节，推进解码状态机。
func (d *KeyDecoder) process(b byte) {
	switch {
	case d.escPending:
		d.finishEsc(b)
	case d.escDead:
		d.finishCSI(b)
	case b == escByte:
		d.escPending = true
		d.esc.Reset(escTimeout)
	case b == ctrlCByte:
		d.emit(KeyEvent{Type: KeyCtrlC})
	case b == tabByte:
		d.emit(KeyEvent{Type: KeyTab})
	case b == enterByte:
		d.emit(KeyEvent{Type: KeyEnter})
	case b == backspaceByte:
		d.emit(KeyEvent{Type: KeyBackspace})
	case b >= 0x20:
		d.ubuf = append(d.ubuf, b)
		if utf8.FullRune(d.ubuf) {
			r, _ := utf8.DecodeRune(d.ubuf)
			d.ubuf = d.ubuf[:0]
			d.emit(KeyEvent{Type: KeyChar, Rune: r})
		}
	}
}

// finishEsc 处理 Esc 之后的字节：'[' 进入 CSI，否则按独立 Esc 处理。
func (d *KeyDecoder) finishEsc(b byte) {
	d.escPending = false
	if !d.esc.Stop() {
		// timer 已触发（残留值在 channel）：清掉，避免下次 Reset 立即误触发
		select {
		case <-d.esc.C:
		default:
		}
	}
	if b != '[' {
		// 独立 Esc 后跟非 '[' 字节（如 ESC a）：先输出 Esc，再重新处理当前字节
		d.emit(KeyEvent{Type: KeyEsc})
		d.process(b)
		return
	}
	d.escDead = true // 已确认 CSI，等方向字节（终端发 CSI 是原子的，不会超时）
}

// finishCSI 处理 CSI 序列的方向字节。
func (d *KeyDecoder) finishCSI(b byte) {
	if d.esc3 {
		d.esc3 = false
		d.escDead = false
		if b == '~' {
			d.emit(KeyEvent{Type: KeyDelete})
		}
		return
	}
	switch b {
	case 'A':
		d.escDead = false
		d.emit(KeyEvent{Type: KeyArrowUp})
	case 'B':
		d.escDead = false
		d.emit(KeyEvent{Type: KeyArrowDown})
	case 'C':
		d.escDead = false
		d.emit(KeyEvent{Type: KeyArrowRight})
	case 'D':
		d.escDead = false
		d.emit(KeyEvent{Type: KeyArrowLeft})
	case 'Z':
		d.escDead = false
		d.emit(KeyEvent{Type: KeyShiftTab})
	case '3':
		d.esc3 = true // 等待 '~' 完成 Delete（ESC [ 3 ~）
	default:
		// 未识别的 CSI 序列，忽略整段
		d.escDead = false
	}
}

func (d *KeyDecoder) emit(ev KeyEvent) { d.out(ev) }
