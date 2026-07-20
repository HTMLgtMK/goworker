package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

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
	engine := core.NewEngine()

	// 注册中间件
	engine.Use(core.LoggingMiddleware())

	// 注册插件
	if err := engine.Register(&PluginA{}); err != nil {
		log.Fatalf("register: %v", err)
	}
	if err := engine.Register(&PluginB{}); err != nil {
		log.Fatalf("register: %v", err)
	}
	if err := engine.Register(&agent.AgentPlugin{}); err != nil {
		log.Fatalf("register: %v", err)
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

	// 启动插件
	if err := engine.StartAll(); err != nil {
		log.Fatalf("start: %v", err)
	}

	// 发送启动事件
	engine.Notify(spec.Event{Type: spec.EventPluginStarted, Payload: "system"})

	// 启动前端
	frontend := stdin.NewStdinFrontend(engine)
	if err := frontend.Run(); err != nil {
		log.Fatalf("frontend: %v", err)
		// 停止插件
		engine.StopAll()
	}

	log.Println("await for shutdown signal...")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("shutting down...")
}
