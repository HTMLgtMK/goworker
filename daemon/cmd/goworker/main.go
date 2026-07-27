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
