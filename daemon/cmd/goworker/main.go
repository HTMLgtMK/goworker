package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/frontend/stdin"
	"github.com/tinguo/goworker/daemon/internal/logger"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// ---- 入口 ----

func main() {
	// 加载全局配置
	cfgPath := config.DefaultPath()
	cfg := config.Load(cfgPath)

	// 初始化日志器（等级过滤 + 可选文件轮转/清理）
	log, err := logger.Setup(cfg.Log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Close()
	log.Info("config loaded", "path", cfgPath)
	if cfg.Log.File != "" {
		log.Info("log file", "path", cfg.Log.File)
	} else {
		log.Info("log to stderr only")
	}

	engine := core.NewEngine(cfg, log)
	defer engine.StopAll()

	// 注册拦截器
	engine.Use(core.LoggingInterceptor(log))

	// 注册插件
	if err := engine.Register(&agent.AgentPlugin{}); err != nil {
		log.Error("register AgentPlugin failed", "error", err)
		return
	}

	// 内置命令
	engine.RegisterBuiltinCommands()

	// 启动插件
	if err := engine.StartAll(); err != nil {
		log.Error("start plugins failed", "error", err)
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
			log.Error("frontend error", "error", err)
		}
	case sig := <-sigCh:
		log.Info("received signal, shutting down", "signal", sig.String())
	}
}
