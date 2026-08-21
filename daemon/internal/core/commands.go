package core

import (
	"fmt"
	"strings"

	spec "github.com/tinguo/goworker/ai-runtime/plugin"
)

// RegisterBuiltinCommands 注册引擎内置命令（/help, /config 等）。
func (e *Engine) RegisterBuiltinCommands() {
	e.RegisterCommand(spec.Command{
		Name:        "/help",
		Description: "显示帮助",
		Handler: func(ctx *spec.Context) error {
			ctx.Writer("Available commands:\n")
			for _, cmd := range e.Commands() {
				ctx.Writer(fmt.Sprintf("  %s  — %s\n", cmd.Name, cmd.Description))
			}
			return nil
		},
	})

	e.RegisterCommand(spec.Command{
		Name:        "/config",
		Aliases:     []string{"/cfg"},
		Description: "查看/修改全局配置",
		Handler: func(ctx *spec.Context) error {
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
