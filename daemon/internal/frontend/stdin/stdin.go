package stdin

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	term "github.com/charmbracelet/x/term"

	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

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
	engine    *core.Engine
	editor    *LineEditor
	termWidth int // 终端列数，用于 markdown 渲染的 word wrap 和 HR 宽度

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

// formatThinking 把 thinking 文本渲染成灰色弱化的定稿块（流式段落定稿复用）。
func formatThinking(content string, width int) string {
	content = strings.TrimSpace(content)
	if content == "" {
		return ""
	}
	rendered := RenderMarkdown(content, width)
	formatted := block(markerText, "Thinking\n"+rendered)
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

	f.hitlConsumer = NewHITLConsumer(f.cancelCh, f.Write)
	f.keyWatcher = &keyWatcher{cancelCh: f.cancelCh}
	f.decoder = NewKeyDecoder(func(ev KeyEvent) { f.stack.dispatch(ev) })

	f.stack.Push(f.keyWatcher) // keyWatcher 常驻栈底，取消键统一归它
	f.stack.Push(editor)       // 主输入行消费者

	// 获取终端宽度，失败则用 80 列作为兜底
	f.termWidth = 80
	if w, _, err := term.GetSize(os.Stdin.Fd()); err == nil && w > 0 {
		f.termWidth = w
	}

	go f.dispatchStdin()

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

		// 判断是否需要启用取消监听（agent 交互）
		isAgent := !strings.HasPrefix(line, "/") || strings.HasPrefix(line, "/agent")
		if isAgent {
			f.runWithCancel(ctx, line)
		} else if err := f.engine.Eval(ctx, line); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\r\n", err)
		}
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

// runWithCancel 启动 agent：挂载取消回调到常驻 keyWatcher，agent 结束后卸载。
func (f *StdinFrontend) runWithCancel(ctx *plugin.Context, line string) {
	f.cancelledByUser.Store(false)
	f.agentCtx, f.agentCancel = context.WithCancel(context.Background())

	// 注入事件总线，agent plugin 可通过 Publish 广播事件给 status bar addon
	ctx.Publish = f.sb.Publish
	f.sb.Start()

	// 包装取消回调：用户取消时置 cancelledByUser，避免 Eval 返回的取消错误被当异常打印
	f.keyWatcher.attach(func() {
		f.cancelledByUser.Store(true)
		f.agentCancel()
	})
	defer func() {
		f.keyWatcher.detach()
		f.sb.Stop()
		f.agentCancel()
		f.agentCtx = nil
		f.agentCancel = nil
		f.editor.DrainInput()
	}()

	// 注入可取消的 context（handleAgent 会 WithTimeout 派生，取消沿链传播）
	ctx.Ctx = f.agentCtx

	// 状态栏刷新；HITL 激活（栈顶为 HITL consumer）时暂停
	go f.sb.Run(f.agentCtx, func() bool { return f.stack.Top() == f.hitlConsumer })

	err := f.engine.Eval(ctx, line)

	if !f.cancelledByUser.Load() && err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\r\n", err)
	}
}
