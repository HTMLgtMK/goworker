package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/frontend/stdin"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

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
	if err := engine.Register(&agent.AgentPlugin{}); err != nil {
		log.Printf("register AgentPlugin: %v", err)
		return
	}

	// 内置命令
	engine.RegisterBuiltinCommands()

	// 启动插件
	if err := engine.StartAll(); err != nil {
		log.Printf("start plugins: %v", err)
		return
	}

	// 发送启动事件
	engine.Notify(spec.Event{Type: spec.EventPluginStarted, Payload: "system"})

	// 启动前端（goroutine，不阻塞）
	frontend := stdin.NewStdinFrontend(engine, &cfg.Frontend.Stdin)
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
