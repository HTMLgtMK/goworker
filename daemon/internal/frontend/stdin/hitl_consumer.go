package stdin

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/tinguo/goworker/daemon/internal/spec"
)

const (
	hitlStyle = "\x1b[1;36m" // 加粗青色
	hitlReset = "\x1b[0m"
)

// hitl 加粗青色：HITL 决策提示的统一样式，与普通 agent 输出区分。
func hitl(s string) string { return hitlStyle + s + hitlReset }

type hitlPhase int

const (
	phaseDecision    hitlPhase = iota // 读取 a/r/e/p 决策
	phaseNewCmd                       // 等待编辑后的新命令
	phaseInstruction                  // 等待用户指令
)

// HITLConsumer 是 HITL 会话的责任链消费者。
//
// 行编辑由本节点自持（不依赖 LineEditor）：决策行/新命令行/指令行都是
// 简单的单行输入，不需要历史与光标移动，因此不复用 editor 的完整能力。
// 编辑键（字符/Enter/Backspace）在有活跃读行会话时消费，其余事件
// （含取消键 KeyEsc/KeyCtrlC）一律放行给栈底 keyWatcher。
type HITLConsumer struct {
	keyCh    chan KeyEvent   // dispatch 投递的编辑按键
	cancelCh <-chan struct{} // keyWatcher 触发的取消信号
	active   atomic.Bool     // 是否有活跃的 readLine 会话
	writer   func(string)    // 提示输出（前端 Write）
	phase    hitlPhase
}

// NewHITLConsumer 创建 HITL 会话消费者。
// cancelCh 与 LineEditor 共享——取消键触发时阻塞的 readLine 一起醒来。
func NewHITLConsumer(cancelCh <-chan struct{}, writer func(string)) *HITLConsumer {
	return &HITLConsumer{
		keyCh:    make(chan KeyEvent, 64),
		cancelCh: cancelCh,
		writer:   writer,
	}
}

// Consume 实现 Consumer：只关注编辑键，且仅在有活跃 readLine 会话时消费。
// 取消键等其余事件放行，穿透到栈底 keyWatcher。
func (c *HITLConsumer) Consume(ev KeyEvent) bool {
	switch ev.Type {
	case KeyChar, KeyEnter, KeyBackspace:
		if c.active.Load() {
			c.keyCh <- ev
			return true
		}
	}
	return false
}

// Run 执行完整 HITL 会话并返回决策。
// 阻塞直到用户给出决策或取消输入。canceled=true 表示用户按 Esc/Ctrl+C 中止，
// 调用方（前端 decide）据此取消整个 agent 执行。
func (c *HITLConsumer) Run(req *spec.InterruptRequest) (decision spec.HITLDecision, canceled bool) {
	c.phase = phaseDecision
	c.writer("\n" + hitl("⚠ "+req.Command+" ("+req.RiskReason+")") + "\n")
	decisionPrompt := hitl("[a]pprove, [e]dit, [r]eject, res[p]ond [a]: ") + " "
	c.writer(decisionPrompt)

	line, canceled := c.readLine(decisionPrompt)
	if canceled {
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}, true
	}
	line = strings.TrimSpace(line)

	switch {
	case line == "" || line == "a" || line == "approve":
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionApprove}, false

	case line == "r" || line == "reject":
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}, false

	case line == "e" || line == "edit":
		c.phase = phaseNewCmd
		editPrompt := hitl("  New command: ")
		c.writer(editPrompt)
		edited, canceled := c.readLine(editPrompt)
		if canceled {
			return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}, true
		}
		return spec.HITLDecision{
			InterruptID: req.ID,
			Type:        spec.DecisionEdit,
			Command:     strings.TrimSpace(edited),
		}, false

	case line == "p" || line == "respond":
		c.phase = phaseInstruction
		respPrompt := hitl("  Your instruction: ")
		c.writer(respPrompt)
		msg, canceled := c.readLine(respPrompt)
		if canceled {
			return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}, true
		}
		return spec.HITLDecision{
			InterruptID: req.ID,
			Type:        spec.DecisionRespond,
			Message:     strings.TrimSpace(msg),
		}, false

	default:
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}, false
	}
}

// readLine 读取一行输入：字符累积、Backspace 删除、Enter 提交。
// prompt 含 ANSI 样式，用于回显定位；输入始终追加到末尾，光标无需移动。
func (c *HITLConsumer) readLine(prompt string) (line string, canceled bool) {
	c.active.Store(true)
	defer c.active.Store(false)

	// 清掉残留取消信号
	select {
	case <-c.cancelCh:
	default:
	}

	var buf []rune
	for {
		select {
		case ev := <-c.keyCh:
			switch ev.Type {
			case KeyEnter:
				fmt.Fprint(os.Stderr, "\r\n")
				return string(buf), false

			case KeyBackspace:
				if len(buf) > 0 {
					buf = buf[:len(buf)-1]
					c.redraw(prompt, buf)
				}

			case KeyChar:
				buf = append(buf, ev.Rune)
				c.redraw(prompt, buf)
			}

		case <-c.cancelCh:
			return "", true
		}
	}
}

// redraw 重画当前输入行：回到行首 → prompt + 已输入内容 → 清行尾。
func (c *HITLConsumer) redraw(prompt string, buf []rune) {
	fmt.Fprintf(os.Stderr, "\r%s%s\033[K", prompt, string(buf))
}

// DrainInput 清空 keyCh 中积压的按键事件。
// 用于 HITL 会话结束后清掉用户已敲但未消费的残留输入。
func (c *HITLConsumer) DrainInput() {
	for {
		select {
		case <-c.keyCh:
		default:
			return
		}
	}
}
