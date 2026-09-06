package stdin

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/rivo/uniseg"

	term "github.com/charmbracelet/x/term"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// Width 返回当前终端列数（atomic，SIGWINCH 与渲染并发访问安全）。
func (f *StdinFrontend) Width() int { return int(f.termWidth.Load()) }

// rawNL 在 raw mode 下 \n 不会自动回车到行首，需要补 \r
const (
	rawNL         = "\r\n"
	thinkingColor = "\033[38;5;244m"
	ansiReset     = "\033[0m"
)

// StdinFrontend 是一个基于标准输入/输出的用户界面。
//
// 字节从 os.Stdin 由 dispatchStdin goroutine 读取，经 KeyDecoder 解码为按键事件，
// 再按 consumer stack 责任链路由：栈顶消费者优先，第一个消费的节点终止。
// keyWatcher 常驻栈底，取消键穿透所有行消费者后由它统一处理。
type StdinFrontend struct {
	engine *core.Engine
	editor *LineEditor
	// 终端列数，用于 markdown 渲染的 word wrap 和 HR 宽度。
	// SIGWINCH goroutine 写、token 消费 goroutine 读，必须 atomic。
	termWidth atomic.Int32

	stack        *ConsumerStack
	decoder      *KeyDecoder
	keyWatcher   *keyWatcher   // 常驻栈底，处理取消键
	hitlConsumer *HITLConsumer // HITL 会话消费者（decide 时压栈）
	cancelCh     chan struct{} // keyWatcher → 阻塞的 ReadLine 的取消信号

	agentCtx    context.Context
	agentCancel context.CancelFunc

	cancelledByUser atomic.Bool // 用户按了 Esc/Ctrl+C

	sb     *statusbar.Bar
	stream *streamRenderer // 流式 token 增量渲染器
}

func NewStdinFrontend(engine *core.Engine, config *config.StdinConfig) *StdinFrontend {
	if config.Theme != "" {
		SetTheme(config.Theme)
	}

	sb := statusbar.New()
	sb.Use(NewProgressAddon(), NewIterationAddon(), NewUsageAddon())

	f := &StdinFrontend{
		engine:   engine,
		sb:       sb,
		stack:    NewConsumerStack(),
		stream:   newStreamRenderer(nil),
		cancelCh: make(chan struct{}, 1),
	}
	f.stream.bind(f)
	return f
}

// dispatchStdin 是唯一的 os.Stdin 字节读取者，投递给 KeyDecoder 解码。
func (f *StdinFrontend) dispatchStdin() {
	var b [1]byte
	for {
		n, err := os.Stdin.Read(b[:])
		if err != nil || n == 0 {
			return
		}
		f.decoder.In() <- b[0]
	}
}

// isHITL 返回当前是否处于 HITL 会话中（栈顶为 HITL 消费者）。
// HITL 期间的输出独占一行，不重绘状态栏。
func (f *StdinFrontend) isHITL() bool {
	return f.stack.Top() == f.hitlConsumer
}

// Write 实现 plugin.Context 的 Writer 回调。
// raw mode 下 \n 不回车，手动补 \r。先清掉已有的 \r 避免双倍。
// 如果有 status bar 在底部，先清再写最后重绘，确保新内容在状态栏上方。
func (f *StdinFrontend) Write(s string) {
	content := strings.ReplaceAll(strings.ReplaceAll(s, "\r", ""), "\n", rawNL)
	f.sb.WithLock(func() {
		if f.sb.Active() && !f.isHITL() {
			f.sb.Clear()
		}
		fmt.Print(content)
		if f.sb.Active() && !f.isHITL() {
			fmt.Fprint(os.Stderr, "\r\n")
			f.sb.Draw()
		}
	})
}

// handleToken 处理 WriteToken 回调：text/thinking 走流式增量渲染，
// tool 类 token 先定稿未完成的流式段落再按整块渲染。done 定稿全部。
func (f *StdinFrontend) handleToken(kind plugin.RenderKind, content string, done bool) {
	switch kind {
	case plugin.KindText, plugin.KindThinking:
		if content != "" {
			f.stream.append(kind, content)
		}
	case plugin.KindToolCall:
		f.stream.finish()
		f.writeToolCall(content)
	case plugin.KindToolResult:
		f.stream.finish()
		f.writeToolResult(content)
	}
	if done {
		f.stream.finish()
	}
}

// thinkingHeader 是 thinking 块的头行前缀，内容首行紧跟其后同行开始。
const thinkingHeader = "✻ Thinking"

// formatThinking 把 thinking 文本渲染成灰色弱化的定稿块（流式消息定稿复用）。
// 形态：内容首行紧跟 "✻ Thinking" 头行（空两格），续行悬挂对齐到锚点列，整体灰色。
func formatThinking(content string, width int) string {
	content = strings.TrimSpace(content)
	if content == "" {
		return ""
	}
	prefixCols := markerThinking.indent + uniseg.StringWidth(thinkingHeader) + 2 // "✻ Thinking  "
	// 先剥掉 glamour 的正文主题色再灰化，否则深色前景覆盖灰色，thinking 看起来和正文同色
	rendered := stripANSI(RenderMarkdown(content, renderWidth(width, prefixCols)))
	lines := normalizeRendered(rendered)
	if len(lines) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(thinkingHeader + "  " + lines[0])
	pad := strings.Repeat(" ", markerThinking.indent)
	for _, l := range lines[1:] {
		b.WriteString("\n")
		if l != "" {
			// 空行不垫缩进，避免行尾幽灵空白
			b.WriteString(pad)
		}
		b.WriteString(l)
	}
	formatted := b.String()
	formatted = strings.ReplaceAll(formatted, ansiReset, ansiReset+thinkingColor)
	return thinkingColor + formatted + ansiReset
}

// writeToolCall 渲染工具调用 token。
// agent 层只发 name(args) 纯数据，marker（●）和空行分隔归前端。
func (f *StdinFrontend) writeToolCall(content string) {
	if content == "" {
		return
	}
	f.Write("\n" + block(markerText, content))
}

// writeToolResult 渲染工具结果 token。
// agent 层只输出纯数据，frontend 负责加 ⎿ 前缀、灰色和缩进。
func (f *StdinFrontend) writeToolResult(content string) {
	content = strings.TrimLeft(content, "\n")
	content = strings.TrimRight(content, "\n") // 去尾随换行，避免 block() 垫出幽灵空白行
	if content == "" {
		return
	}
	f.Write("\n\033[38;5;244m" + block(markerTool, content) + "\033[0m")
}

// decide 执行一次 HITL 决策会话，实现 plugin.Context 的 Decide 契约。
// 将 HITLConsumer 压栈（栈顶，HITL 期间独占输入），会话结束弹出。
// 用户按 Esc/Ctrl+C 取消时，一并取消整个 agent 执行。
func (f *StdinFrontend) decide(req *hitl.InterruptRequest) hitl.Decision {
	// 先定稿未完成的流式段落：HITL 提示会移动光标，悬挂的原始行会让
	// 之后的擦除偏移错位（擦掉 HITL 输出或残留半截内容）
	f.stream.finish()

	f.stack.Push(f.hitlConsumer)
	defer func() {
		f.stack.Pop()
		// 清掉 HITL 会话内已敲未消费的残留，避免污染下次输入
		f.editor.DrainInput()
		f.hitlConsumer.DrainInput()
	}()

	// 清掉状态栏残留行并换行，给 HITL 提示独占一行，避免和状态栏抢位置
	f.sb.WithLock(func() {
		if f.sb.Active() {
			f.sb.Clear()
		}
		fmt.Fprint(os.Stderr, rawNL)
	})

	decision, canceled := f.hitlConsumer.Run(req)
	if canceled {
		f.cancelledByUser.Store(true)
		if f.agentCancel != nil {
			f.agentCancel()
		}
	}
	return decision
}

// Run 启动主事件循环。
func (f *StdinFrontend) Run() error {
	editor, err := NewLineEditor(f.cancelCh)
	if err != nil {
		return fmt.Errorf("line editor: %w", err)
	}
	f.editor = editor
	defer editor.Close()

	// 命令补全候选：Commands() 在运行期不变（全部 Init 时注册），启动时缓存一次
	editor.SetCompleter(newCompleter(f.engine.Commands()))

	f.hitlConsumer = NewHITLConsumer(f.cancelCh, f.Write)
	f.keyWatcher = &keyWatcher{cancelCh: f.cancelCh}
	f.decoder = NewKeyDecoder(func(ev KeyEvent) { f.stack.dispatch(ev) })

	f.stack.Push(f.keyWatcher) // keyWatcher 常驻栈底，取消键统一归它
	f.stack.Push(editor)       // 主输入行消费者

	// 获取终端宽度，失败则用 80 列作为兜底
	f.termWidth.Store(80)
	if w, _, err := term.GetSize(os.Stdin.Fd()); err == nil && w > 0 {
		f.termWidth.Store(int32(w))
	}

	// SIGWINCH 实时跟踪宽度：markdown wrap、流式擦除行数都依赖它，
	// 快照式的启动取值会在 resize 后全部错位
	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	defer signal.Stop(winch)
	go func() {
		for range winch {
			if w, _, err := term.GetSize(os.Stdin.Fd()); err == nil && w > 0 {
				f.termWidth.Store(int32(w))
			}
		}
	}()

	go f.dispatchStdin()

	// 常驻状态栏刷新循环：active 由 ProgressAddon 的事件驱动（begin 激活/end 停用），
	// 非活跃时空转。ctx 与前端同生命周期。
	sbCtx, sbCancel := context.WithCancel(context.Background())
	defer sbCancel()
	go f.sb.Run(sbCtx, func() bool { return f.stack.Top() == f.hitlConsumer })

	for {
		line, canceled, err := editor.ReadLine()
		if err != nil {
			return err
		}

		// Esc 取消当前输入
		if canceled {
			fmt.Print("\r\n") // 换行，准备下一次输入
			continue
		}

		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			continue
		case line == "/quit" || line == "/exit" || line == "/q":
			fmt.Print("bye\r\n")
			return nil
		}

		ctx := plugin.NewContext(context.Background(), f.Write, f.decide, nil)

		// 注入 WriteToken — 所有渲染逻辑收敛至此
		ctx.WriteToken = f.handleToken

		// 所有命令（含斜杠命令）统一走取消监听 + 状态栏事件驱动：
		// 长耗时命令（/compact /new 的固化与压缩）期间 Esc 可取消，
		// 状态栏 begin/end 由 runWithCancel 统一发布，命令内部发阶段事件
		f.runWithCancel(ctx, line)
	}
}

// keyWatcher 常驻责任链栈底，统一处理取消键（Esc/Ctrl+C）。
// 取消时通知当前阻塞的 ReadLine 放弃输入；若 agent 正在运行则一并取消 agent。
// 不关注编辑键，一律放行（编辑键已被栈上行消费者吃掉，不会传到它这层）。
type keyWatcher struct {
	cancelCh    chan<- struct{}        // 通知 ReadLine/HITL 放弃当前输入
	cancelAgent atomic.Pointer[func()] // 取消 agent 的回调（nil 表示无 agent 在运行）
}

// attach 挂载 agent 取消回调（runWithCancel 在 main goroutine 调用）。
func (k *keyWatcher) attach(fn func()) {
	k.cancelAgent.Store(&fn)
}

// detach 卸载取消回调（agent 结束后调用）。
func (k *keyWatcher) detach() {
	k.cancelAgent.Store(nil)
}

func (k *keyWatcher) Consume(ev KeyEvent) bool {
	if ev.Type != KeyEsc && ev.Type != KeyCtrlC {
		return false
	}
	// 通知当前阻塞的 ReadLine 放弃输入（非阻塞；无人接收则残留，下次读行前清理）
	select {
	case k.cancelCh <- struct{}{}:
	default:
	}
	// atomic.Pointer 保证与 attach/detach 并发安全（Consume 在 decoder goroutine）
	if ca := k.cancelAgent.Load(); ca != nil {
		// 立即写反馈，不等 agent goroutine 退出（stderr raw mode 要 \r\n）
		fmt.Fprint(os.Stderr, "\r\n  ⏹ cancelling...\r\n")
		(*ca)()
	}
	return true
}

// runWithCancel 启动命令：挂载取消回调到常驻 keyWatcher，结束后卸载。
// 状态栏生命周期事件驱动：命令前发 PhaseBegin、结束后（含取消）发 PhaseEnd，
// 阶段文本由命令内部经 ctx.Publish 发布，ProgressAddon 订阅并驱动 Bar。
func (f *StdinFrontend) runWithCancel(ctx *plugin.Context, line string) {
	f.cancelledByUser.Store(false)
	f.agentCtx, f.agentCancel = context.WithCancel(context.Background())

	// 注入事件总线：命令（Session.Compact / handleNew 等）经此发阶段事件
	ctx.Publish = f.sb.Publish
	ctx.Publish(runtimeconfig.EventPhase, runtimeconfig.PhaseEvent{Kind: runtimeconfig.PhaseBegin})
	defer func() {
		ctx.Publish(runtimeconfig.EventPhase, runtimeconfig.PhaseEvent{Kind: runtimeconfig.PhaseEnd})
	}()

	// 包装取消回调：用户取消时置 cancelledByUser，避免 Eval 返回的取消错误被当异常打印
	f.keyWatcher.attach(func() {
		f.cancelledByUser.Store(true)
		f.agentCancel()
	})
	defer func() {
		f.keyWatcher.detach()
		f.agentCancel()
		f.agentCtx = nil
		f.agentCancel = nil
		f.editor.DrainInput()
	}()

	// 注入可取消的 context（handleAgent 会 WithTimeout 派生，取消沿链传播）
	ctx.Ctx = f.agentCtx

	err := f.engine.Eval(ctx, line)

	if !f.cancelledByUser.Load() && err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\r\n", err)
	}
}
