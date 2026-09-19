package engine

import (
	"fmt"
	"os"
	"strings"

	term "github.com/charmbracelet/x/term"
	"github.com/muesli/termenv"

	"github.com/tinguo/goworker/daemon/internal/core/model"
	"github.com/tinguo/goworker/daemon/internal/version"
)

// RegisterBuiltinCommands 注册引擎内置命令（/help, /config 等）。
func (e *Engine) RegisterBuiltinCommands() {
	e.RegisterCommand(model.Command{
		Name:        "/help",
		Description: "显示帮助",
		Handler: func(ctx *model.Context) error {
			ctx.Writer("Available commands:\n")
			for _, cmd := range e.Commands() {
				ctx.Writer(fmt.Sprintf("  %s  — %s\n", cmd.Name, cmd.Description))
			}
			return nil
		},
	})

	e.RegisterCommand(model.Command{
		Name:        "/diagnose",
		Aliases:     []string{"/diag"},
		Description: "输出终端/主题/LLM/运行时诊断信息（排版问题排查用）",
		Handler:     e.handleDiagnose,
	})

	e.RegisterCommand(model.Command{
		Name:        "/version",
		Aliases:     []string{"/ver"},
		Description: "显示版本号/commit/构建信息",
		Handler: func(ctx *model.Context) error {
			ctx.Writer(version.Get().String() + "\n")
			return nil
		},
	})

	e.RegisterCommand(model.Command{
		Name:        "/config",
		Aliases:     []string{"/cfg"},
		Description: "查看/修改全局配置",
		Handler: func(ctx *model.Context) error {
			args := ctx.Args

			if len(args) == 0 {
				ctx.Writer(e.Config().Display())
				return nil
			}

			switch args[0] {
			case "help":
				ctx.Writer("用法:\n")
				ctx.Writer("  /config             — 查看完整配置\n")
				ctx.Writer("  /config set <k>=<v> — 修改配置项\n")
				ctx.Writer("  可用 key: llm.endpoint, llm.model, llm.api_key,\n")
				ctx.Writer("           sandbox.mode, sandbox.allowed_work_dir\n")

			case "set":
				if len(args) < 2 {
					ctx.Writer("用法: /config set <key>=<value>\n")
					return nil
				}
				kv := strings.SplitN(args[1], "=", 2)
				if len(kv) != 2 {
					ctx.Writer("格式错误，示例: /config set llm.endpoint=http://localhost:8000/v1\n")
					return nil
				}
				key, val := kv[0], kv[1]

				cfg := e.Config()
				if err := cfg.SetField(key, val); err != nil {
					ctx.Writer(fmt.Sprintf("✘ %v\n", err))
					return nil
				}
				if err := e.SaveConfig(cfg); err != nil {
					ctx.Writer(fmt.Sprintf("✘ 保存失败: %v\n", err))
					return nil
				}
				ctx.Writer(fmt.Sprintf("✔ %s 已更新\n", key))

			default:
				ctx.Writer("未知子命令，可用: help, set\n")
			}
			return nil
		},
	})
}

// handleDiagnose 输出排版与运行环境诊断信息。
// 排版类 bug（折行错位、锚点错位）几乎都源于"渲染宽度 ≠ 真实终端宽度"、
// 颜色能力误判、主题 margin 叠加——这里把这些一次打全。
func (e *Engine) handleDiagnose(ctx *model.Context) error {
	w := func(format string, args ...any) {
		ctx.Writer(fmt.Sprintf(format, args...))
	}

	w("== 终端 ==\n")
	files := []struct {
		name string
		fd   uintptr
	}{
		{"stdin", os.Stdin.Fd()},
		{"stdout", os.Stdout.Fd()},
		{"stderr", os.Stderr.Fd()},
	}
	for _, f := range files {
		cols, rows := 0, 0
		if cw, ch, err := term.GetSize(f.fd); err == nil {
			cols, rows = cw, ch
		}
		w("  %-6s tty=%-5v %dx%d（列x行）\n", f.name, term.IsTerminal(f.fd), cols, rows)
	}
	w("  TERM=%s COLORTERM=%s\n", os.Getenv("TERM"), os.Getenv("COLORTERM"))
	w("  termenv profile=%s\n", termenv.ColorProfile().String())
	w("  注：渲染宽度取自 stdin 侧；若与窗口实际宽度不符（resize 未刷新），折行必然错位\n")

	w("== 前端/主题 ==\n")
	w("  theme=%s\n", e.Config().Frontend.Stdin.Theme)

	w("== LLM ==\n")
	llm := e.Config().LLM
	name, p, err := llm.ResolveDefault()
	if err != nil {
		w("  default provider: 解析失败 — %v\n", err)
	} else {
		key := ""
		if p.APIKey != "" {
			key = fmt.Sprintf("***(%d chars)", len(p.APIKey))
		}
		w("  default=%s type=%s model=%s\n", name, p.Type, p.Model)
		w("  endpoint=%s\n", p.Endpoint)
		w("  context_window=%d thinking(mode=%s effort=%s show=%v)\n",
			p.ContextWindow, p.Thinking.RequestMode, p.Thinking.Effort, llm.Thinking.Show)
		w("  api_key=%s\n", key)
		w("  streaming: openai 类型自动启用 ChatStream（含 SSE 增量/usage），无需配置\n")
	}
	w("  providers: ")
	for n := range llm.Providers {
		w("%s ", n)
	}
	w("\n")

	w("== 运行时 ==\n")
	w("  %s\n", version.Get().String())
	return nil
}
