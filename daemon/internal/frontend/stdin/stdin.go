package stdin

import (
	"context"
	"fmt"
	"os"
	"strings"

	term "github.com/charmbracelet/x/term"

	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// rawNL 在 raw mode 下 \n 不会自动回车到行首，需要补 \r
const rawNL = "\r\n"

// StdinFrontend 是一个基于标准输入/输出的用户界面，使用 raw mode 行编辑器。
type StdinFrontend struct {
	engine    *core.Engine
	editor    *LineEditor
	termWidth int // 终端列数，用于 markdown 渲染的 word wrap 和 HR 宽度
}

func NewStdinFrontend(engine *core.Engine, theme string) *StdinFrontend {
	if theme != "" {
		SetTheme(theme)
	}
	return &StdinFrontend{engine: engine}
}

// Write 实现 spec.Context 的 Writer 回调。
// raw mode 下 \n 不回车，手动补 \r。先清掉已有的 \r 避免双倍。
func (f *StdinFrontend) Write(s string) {
	s = strings.ReplaceAll(s, "\r", "")
	fmt.Print(strings.ReplaceAll(s, "\n", rawNL))
}

// writeText 渲染文本类 token（markdown 输出）。
// 每行统一缩进 2 格（首行用 ● 前缀，后续行用空格），避免多行表格/列表对齐错乱。
func (f *StdinFrontend) writeText(content string) {
	rendered := RenderMarkdown(strings.TrimSpace(content), f.termWidth)
	// 首行 ● + 后续行 2 空格缩进，保持多行内容对齐
	rendered = "● " + strings.ReplaceAll(rendered, "\n", "\n  ")
	f.Write("\n" + rendered)
}

// writeToolCall 渲染工具调用 token。
func (f *StdinFrontend) writeToolCall(content string) {
	if len(content) > 0 && content[0] == '\n' {
		f.Write("\n● " + strings.TrimPrefix(content[1:], "● "))
	} else {
		f.Write(content)
	}
}

// writeToolResult 渲染工具结果 token。
// agent 层只输出纯数据，frontend 负责加 ⎿ 前缀、灰色和缩进。
func (f *StdinFrontend) writeToolResult(content string) {
	content = strings.TrimLeft(content, "\n")
	if content == "" {
		return
	}
	indented := strings.ReplaceAll(content, "\n", "\n   ")
	f.Write("\n\033[38;5;244m  ⎿  " + indented + "\033[0m")
}

// readLine 供 HITL 确认使用。
func (f *StdinFrontend) readLine() (string, error) {
	oldPrompt := f.editor.Prompt
	f.editor.Prompt = ""
	defer func() { f.editor.Prompt = oldPrompt }()
	line, _, err := f.editor.ReadLine()
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// Run 启动主事件循环。
func (f *StdinFrontend) Run() error {
	editor, err := NewLineEditor()
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

// runWithCancel 启动 agent 并监听 Esc 取消。
func (f *StdinFrontend) runWithCancel(ctx *spec.Context, line string) {
	// 暂不实现 Esc 取消 agent 执行，避免 stdin 竞争
	// Esc 在输入时可用（editor.ReadLine），agent 运行期间按 Esc 会在后续 DrainInput 被清掉
	err := f.engine.Eval(ctx, line)
	f.editor.DrainInput()

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\r\n", err)
	}
}
