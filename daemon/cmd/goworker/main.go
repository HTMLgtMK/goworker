package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/frontend/stdin"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// ---- 示例插件 ----

type PluginA struct{}

func (p *PluginA) Name() string { return "PluginA" }

func (p *PluginA) Init(h *spec.Hub) error {
	return h.RegisterCommand(spec.Command{
		Name:        "/a",
		Aliases:     []string{"/A"},
		Description: "A 命令",
		Handler: func(ctx *spec.Context) error {
			ctx.Writer("Executing A\n")
			return nil
		},
	})
}

func (p *PluginA) Start() error { log.Printf("PluginA started"); return nil }
func (p *PluginA) Stop() error  { log.Printf("PluginA stopped"); return nil }

type PluginB struct{}

func (p *PluginB) Name() string { return "PluginB" }

func (p *PluginB) Init(h *spec.Hub) error {
	return h.RegisterCommand(spec.Command{
		Name:        "/b",
		Aliases:     []string{"/B"},
		Description: "B 命令",
		Handler: func(ctx *spec.Context) error {
			ctx.Writer("Executing B\n")
			return nil
		},
	})
}

func (p *PluginB) Start() error { log.Printf("PluginB started"); return nil }
func (p *PluginB) Stop() error  { log.Printf("PluginB stopped"); return nil }

// ---- 入口 ----

func main() {
	// 加载全局配置
	cfgPath := config.DefaultPath()
	cfg := config.Load(cfgPath)
	log.Printf("[main] config loaded from %s", cfgPath)

	engine := core.NewEngine(cfg)
	defer engine.StopAll()

	// 注册拦截器
	engine.Use(core.LoggingInterceptor())

	// 注册插件
	if err := engine.Register(&PluginA{}); err != nil {
		log.Printf("register PluginA: %v", err)
		return
	}
	if err := engine.Register(&PluginB{}); err != nil {
		log.Printf("register PluginB: %v", err)
		return
	}
	if err := engine.Register(&agent.AgentPlugin{}); err != nil {
		log.Printf("register AgentPlugin: %v", err)
		return
	}

	// 内置命令
	engine.RegisterCommand(spec.Command{
		Name:        "/help",
		Description: "显示帮助",
		Handler: func(ctx *spec.Context) error {
			ctx.Writer("Available commands:\n")
			for _, cmd := range engine.Commands() {
				ctx.Writer(fmt.Sprintf("  %s  — %s\n", cmd.Name, cmd.Description))
			}
			return nil
		},
	})

	engine.RegisterCommand(spec.Command{
		Name:        "/config",
		Aliases:     []string{"/cfg"},
		Description: "查看/修改全局配置",
		Handler: func(ctx *spec.Context) error {
			args := ctx.Args

			if len(args) == 0 {
				ctx.Writer(engine.Config().Display())
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

				cfg := engine.Config()
				if err := cfg.SetField(key, val); err != nil {
					ctx.Writer(fmt.Sprintf("✘ %v\n", err))
					return nil
				}
				if err := engine.SaveConfig(cfg); err != nil {
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

	// 启动插件
	if err := engine.StartAll(); err != nil {
		log.Printf("start plugins: %v", err)
		return
	}

	// 发送启动事件
	engine.Notify(spec.Event{Type: spec.EventPluginStarted, Payload: "system"})

	// 启动前端（goroutine，不阻塞）
	frontend := stdin.NewStdinFrontend(engine)
	errCh := make(chan error, 1)
	go func() {
		errCh <- frontend.Run()
	}()

	// 等待信号或前端退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("frontend error: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("received signal %v, shutting down...", sig)
	}
}
