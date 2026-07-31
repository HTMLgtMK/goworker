package stdin

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	term "github.com/charmbracelet/x/term"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// rawNL 在 raw mode 下 \n 不会自动回车到行首，需要补 \r
const rawNL = "\r\n"

// StdinFrontend 是一个基于标准输入/输出的用户界面。
//
// 字节从 os.Stdin 由 dispatchStdin goroutine 读取，投递到 stdinCh channel，
// 各消费者（LineEditor、key watcher）从 channel 取字节，不存在 fd 竞争。
type StdinFrontend struct {
	engine    *core.Engine
	editor    *LineEditor
	termWidth int // 终端列数，用于 markdown 渲染的 word wrap 和 HR 宽度

	stdinCh chan byte // dispatcher → consumers

	agentCtx    context.Context
	agentCancel context.CancelFunc

	hitlActive      atomic.Bool // HITL 激活时 key watcher 休眠
	cancelledByUser atomic.Bool // 用户按了 Esc/Ctrl+C

	sb *statusbar.Bar
}

func NewStdinFrontend(engine *core.Engine, config *config.StdinConfig) *StdinFrontend {
	if config.Theme != "" {
		SetTheme(config.Theme)
	}

	sb := statusbar.New()
	sb.Use(NewProgressAddon(), NewIterationAddon())

	return &StdinFrontend{
		engine: engine,
		sb:     sb,
	}
}

// dispatchStdin 是唯一的 os.Stdin 字节读取者，持续将字节投递到 stdinCh。
func (f *StdinFrontend) dispatchStdin() {
	var b [1]byte
	for {
		n, err := os.Stdin.Read(b[:])
		if err != nil || n == 0 {
			return
		}
		f.stdinCh <- b[0]
	}
}

// Write 实现 spec.Context 的 Writer 回调。
// raw mode 下 \n 不回车，手动补 \r。先清掉已有的 \r 避免双倍。
// 如果有 status bar 在底部，先清再写最后重绘，确保新内容在状态栏上方。
func (f *StdinFrontend) Write(s string) {
	content := strings.ReplaceAll(strings.ReplaceAll(s, "\r", ""), "\n", rawNL)
	f.sb.WithLock(func() {
		if f.sb.Active() && !f.hitlActive.Load() {
			f.sb.Clear()
		}
		fmt.Print(content)
		if f.sb.Active() && !f.hitlActive.Load() {
			fmt.Fprint(os.Stderr, "\r\n")
			f.sb.Draw()
		}
	})
}

// writeText 渲染文本类 token（markdown 输出）。
// 统一走 block() 编组：首行 ● 前缀，延续行对齐到内容列。
func (f *StdinFrontend) writeText(content string) {
	rendered := RenderMarkdown(strings.TrimSpace(content), f.termWidth)
	f.Write("\n" + block(markerText, rendered))
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

// readLine 供 HITL 确认使用。
func (f *StdinFrontend) readLine() (string, error) {
	f.hitlActive.Store(true)
	defer f.hitlActive.Store(false)

	// 清残留字节（agent 运行期间积压的 Enter/乱敲）
	drainChannel(f.stdinCh)

	// 清掉状态栏残留行并换行，给输入独占一行，避免和状态栏抢位置
	f.sb.WithLock(func() {
		if f.sb.Active() {
			f.sb.Clear()
		}
		fmt.Fprint(os.Stderr, rawNL)
	})

	line, canceled, err := f.editor.ReadLine()
	if err != nil {
		return "", err
	}
	if canceled {
		// 用户在 HITL 时按 Esc/Ctrl+C → 取消整个 agent 执行
		f.cancelledByUser.Store(true)
		if f.agentCancel != nil {
			f.agentCancel()
		}
		return "", fmt.Errorf("cancelled")
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// Run 启动主事件循环。
func (f *StdinFrontend) Run() error {
	f.stdinCh = make(chan byte, 64)
	go f.dispatchStdin()

	editor, err := NewLineEditor(f.stdinCh)
	if err != nil {
		return fmt.Errorf("line editor: %w", err)
	}
	f.editor = editor
	defer editor.Close()

	// 获取终端宽度，失败则用 80 列作为兜底
	f.termWidth = 80
	if w, _, err := term.GetSize(os.Stdin.Fd()); err == nil && w > 0 {
		f.termWidth = w
	}

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

		ctx := spec.NewContext(context.Background(), f.Write, f.readLine, nil)

		// 注入 WriteToken — 所有渲染逻辑收敛至此
		ctx.WriteToken = func(kind spec.RenderKind, content string) {
			switch kind {
			case spec.KindText:
				f.writeText(content)
			case spec.KindToolCall:
				f.writeToolCall(content)
			case spec.KindToolResult:
				f.writeToolResult(content)
			}
		}

		// 判断是否需要启用取消监听（agent 交互）
		isAgent := !strings.HasPrefix(line, "/") || strings.HasPrefix(line, "/agent")
		if isAgent {
			f.runWithCancel(ctx, line)
		} else if err := f.engine.Eval(ctx, line); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\r\n", err)
		}
	}
}

// runWithCancel 启动 agent 并监听 Esc/Ctrl+C 取消。
func (f *StdinFrontend) runWithCancel(ctx *spec.Context, line string) {
	f.cancelledByUser.Store(false)
	f.agentCtx, f.agentCancel = context.WithCancel(context.Background())

	// 注入事件总线，agent plugin 可通过 Publish 广播事件给 status bar addon
	ctx.Publish = f.sb.Publish
	f.sb.Start()

	defer func() {
		f.sb.Stop()

		f.agentCancel()
		f.agentCtx = nil
		f.agentCancel = nil
		f.editor.DrainInput()
	}()

	// 注入可取消的 context（handleAgent 会 WithTimeout 派生，取消沿链传播）
	ctx.Ctx = f.agentCtx

	// 启动按键监听 goroutine + 状态栏
	go f.runKeyWatcher(f.agentCtx)
	go f.sb.Run(f.agentCtx, func() bool { return f.hitlActive.Load() })

	err := f.engine.Eval(ctx, line)

	if !f.cancelledByUser.Load() && err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\r\n", err)
	}
}

// runKeyWatcher 在 agent 执行期间（非 HITL 阶段）监听 Esc/Ctrl+C。
// 用 ticker + 非阻塞消费：HITL 激活或 ctx 取消时立刻让出 stdinCh，
// 绝不阻塞在 channel 上，否则会和 LineEditor 抢字节——把 HITL 的第一个输入吃掉。
func (f *StdinFrontend) runKeyWatcher(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if f.hitlActive.Load() {
				continue
			}
			if f.drainKeys(ctx) {
				return
			}
		}
	}
}

// drainKeys 非阻塞消费当前积压的输入字节，监听 Esc/Ctrl+C。
// 命中取消键返回 true；HITL 激活、ctx 取消或缓冲清空时返回 false。
func (f *StdinFrontend) drainKeys(ctx context.Context) bool {
	for {
		if f.hitlActive.Load() {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case b := <-f.stdinCh:
			if b == escByte || b == ctrlCByte {
				f.cancelledByUser.Store(true)
				if f.agentCancel != nil { // defer 可能已置 nil，避免 nil 调用 panic
					f.agentCancel()
				}
				// 立即写反馈，不等 agent goroutine 退出（stderr raw mode 要 \r\n）
				fmt.Fprint(os.Stderr, "\r\n  ⏹ cancelling...\r\n")
				return true
			}
		default:
			return false
		}
	}
}

// drainChannel 非阻塞清空 channel。
func drainChannel(ch <-chan byte) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
