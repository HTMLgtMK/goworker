package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/app"
	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/frontend/stdin"
	"github.com/tinguo/goworker/daemon/internal/plugin"
	"github.com/tinguo/goworker/daemon/internal/version"
)

// ---- 入口 ----

func main() {
	// 走 flag 包而不是手搓 os.Args：`-version` / `--version` 都能认。
	// 默认 ExitOnError 正是想要的行为——未知参数直接 usage + exit 2，
	// CLI 就该这么硬气，没必要宽容。
	showVersion := flag.Bool("version", false, "显示版本信息后退出")
	flag.Parse()
	if *showVersion {
		fmt.Println(version.Get().String())
		return
	}

	// 加载全局配置
	cfgPath := config.DefaultPath()
	cfg := config.Load(cfgPath)
	if cfg == nil {
		// Load 遇到无效配置会返回 nil（具体原因它已经记日志了）。
		// 不挡一下的话下一行 cfg.Log 就是空指针解引用——配置写错一个字段
		// 换来一个段错误，排查成本高得离谱。
		fmt.Fprintf(os.Stderr, "配置无效: %s\n详见上方日志；修好后重试，或删掉该文件用默认配置。\n", cfgPath)
		os.Exit(1)
	}

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

	// ai-runtime 聚合配置（各插件的目录路径注入见 daemon/internal/app 装配层）
	runtimeCfg := cfg.ToRuntime()

	engine := core.NewEngine(cfg, runtimeCfg, log)
	defer engine.StopAll()

	// 注册拦截器
	engine.Use(core.LoggingInterceptor(log))

	// 注册插件：按构建标签装配（CMake GOWORKER_NO_PLUGINS → goworker_no_<name> off-tag），
	// 装配层见 daemon/internal/app
	if err := app.RegisterPlugins(engine, runtimeCfg, log); err != nil {
		log.Error("register plugins failed", "error", err)
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
