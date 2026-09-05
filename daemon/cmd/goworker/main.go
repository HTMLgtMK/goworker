package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/agent"
	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/dispatcher"
	"github.com/tinguo/goworker/daemon/internal/frontend/stdin"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// ---- 入口 ----

// runACPWorker 以 stdio ACP Agent 模式服务 ZCode 会话：配置解析、HTTP 客户端、
// provider 装配与 agent 插件共用同一套（ProviderFactory），stdin EOF 即退出。
func runACPWorker() {
	cfgPath := config.DefaultPath()
	cfg := config.Load(cfgPath)
	if cfg == nil {
		os.Exit(1)
	}
	log, err := logger.Setup(cfg.Log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Close()
	slog.SetDefault(log.Logger)

	server := agent.ServeACPWorker(agent.Stdio(), agent.ACPWorkerDeps{
		Config:   cfg.ToRuntime(),
		AuditDir: filepath.Join(config.DefaultDir(), "audit"),
	})
	// 阻塞到对端关闭输入（dispatcher spawn 的子进程随任务结束被回收）
	<-server.Done()
	_ = server.Close()
}

func main() {
	// goworker acp：ZCode 以 ACP worker 身份跑在 stdio 上（被 dispatcher/编辑器驱动）
	if len(os.Args) > 1 && os.Args[1] == "acp" {
		runACPWorker()
		return
	}

	// 加载全局配置
	cfgPath := config.DefaultPath()
	cfg := config.Load(cfgPath)

	// 初始化日志器（等级过滤 + 可选文件轮转/清理）
	log, err := logger.Setup(cfg.Log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	// 插件/provider 用全局 slog 打日志（agent 插件没有自己的 logger 句柄），
	// 默认它只进 stderr、忽略 config 的 log.level/log.file —— 接上配置的 handler 后，
	// log.level: debug 才能看到 llm.chat request/response 这类调试日志。
	slog.SetDefault(log.Logger)
	defer log.Close()
	log.Info("config loaded", "path", cfgPath)
	if cfg.Log.File != "" {
		log.Info("log file", "path", cfg.Log.File)
	} else {
		log.Info("log to stderr only")
	}

	// ai-runtime 聚合配置 + 目录路径注入（daemon 是路径中枢，ai-runtime 不反向依赖 DefaultDir）
	runtimeCfg := cfg.ToRuntime()
	paths := runtimeconfig.Paths{
		ConfigDir:     config.DefaultDir(),
		SkillsUser:    filepath.Join(config.DefaultDir(), "skills"),
		SkillsProject: filepath.Join(".goworker", "skills"),
		AuditDir:      filepath.Join(config.DefaultDir(), "audit"),
		DispatchDir:   filepath.Join(config.DefaultDir(), "dispatch"),
	}

	engine := core.NewEngine(cfg, runtimeCfg, log)
	defer engine.StopAll()

	// 注册拦截器
	engine.Use(core.LoggingInterceptor(log))

	// 注册插件（ai-runtime 的 agent 插件：配置与路径构造函数注入）
	if err := engine.Register(agent.NewPlugin(runtimeCfg, paths)); err != nil {
		log.Error("register AgentPlugin failed", "error", err)
		return
	}

	// dispatcher 插件：dispatch.enabled=true 才注册，默认不影响现有功能
	if runtimeCfg.Dispatch.Enabled {
		if err := engine.Register(dispatcher.NewPlugin(runtimeCfg, paths)); err != nil {
			log.Error("register DispatcherPlugin failed", "error", err)
			return
		}
	}

	// 内置命令
	engine.RegisterBuiltinCommands()

	// 启动插件
	if err := engine.StartAll(); err != nil {
		log.Error("start plugins failed", "error", err)
		return
	}

	// 发送启动事件
	engine.Notify(plugin.Event{Type: plugin.EventPluginStarted, Payload: "system"})

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
