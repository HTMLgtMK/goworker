package main

import (
	"flag"
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
	"github.com/tinguo/goworker/daemon/internal/frontend/vscode"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// ---- 入口 ----

// version 由构建注入（cmake/go build 的 ldflags -X main.version=...），源码直跑为 dev。
var version = "dev"

func printVersion() {
	fmt.Printf("goworker %s\n", version)
}

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

	// 沙箱下发：dispatcher 侧可经 worker 的 env 配置收紧无人值守 worker 的命令门禁，
	// 如 env: ["GOWORKER_SANDBOX_MODE=strict"]。取值与 sandbox 模式一致，非法值拒绝启动。
	runtimeCfg := cfg.ToRuntime()
	if mode := os.Getenv("GOWORKER_SANDBOX_MODE"); mode != "" {
		switch mode {
		case "normal", "strict", "readonly", "off":
			runtimeCfg.Sandbox.Mode = mode
		default:
			fmt.Fprintf(os.Stderr, "invalid GOWORKER_SANDBOX_MODE %q (normal|strict|readonly|off)\n", mode)
			os.Exit(1)
		}
		slog.Info("acp worker: sandbox mode overridden", "mode", mode)
	}

	server := agent.ServeACPWorker(agent.Stdio(), agent.ACPWorkerDeps{
		Config:   runtimeCfg,
		AuditDir: filepath.Join(config.DefaultDir(), "audit"),
	})
	// 阻塞到对端关闭输入（dispatcher spawn 的子进程随任务结束被回收）
	<-server.Done()
	_ = server.Close()
}

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

	// 版本查询：goworker [-v|--version|version]
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "-v", "--version", "version":
			printVersion()
			return
		}
	}

	// goworker acp：ZCode 以 ACP worker 身份跑在 stdio 上（被 dispatcher/编辑器驱动）
	if len(os.Args) > 1 && os.Args[1] == "acp" {
		runACPWorker()
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

	// ai-runtime 聚合配置 + 目录路径注入（daemon 是路径中枢，ai-runtime 不反向依赖 DefaultDir）
	runtimeCfg := cfg.ToRuntime()
	paths := runtimeconfig.Paths{
		ConfigDir:     config.DefaultDir(),
		SkillsUser:    filepath.Join(config.DefaultDir(), "skills"),
		SkillsProject: filepath.Join(".goworker", "skills"),
		AuditDir:      filepath.Join(config.DefaultDir(), "audit"),
		DispatchDir:   filepath.Join(config.DefaultDir(), "dispatch"),
	}

	// vscode ACP 前端 socket：未显式配置时按 DefaultDir 派生默认路径。
	// 与 dispatcher 的 <DispatchDir>/acp.sock 完全分离：那是无人值守 worker 的
	// 任务提交入口，这里只服务外部编辑器驱动主会话。
	if runtimeCfg.Frontend.Vscode.Socket == "" {
		runtimeCfg.Frontend.Vscode.Socket = filepath.Join(config.DefaultDir(), "frontend", "vscode.sock")
	}

	engine := core.NewEngine(cfg, runtimeCfg, log)
	defer engine.StopAll()

	// 注册拦截器
	engine.Use(core.LoggingInterceptor(log))

	// 注册插件（ai-runtime 的 agent 插件：配置与路径构造函数注入）。
	// 实例保留引用：vscode 前端的会话清单/load 判定直接落在它上。
	agentPlugin := agent.NewPlugin(runtimeCfg, paths)
	if err := engine.Register(agentPlugin); err != nil {
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

	// vscode ACP 前端：vscode.enabled=true 时经本地 Unix socket 服务外部编辑器（ACP 协议）。
	// 启动失败与 dispatcher 插件的 listener 启动失败一致：记录错误后终止启动，不静默降级。
	if runtimeCfg.Frontend.Vscode.Enabled {
		vscodeFrontend := vscode.New(runtimeCfg.Frontend.Vscode.Socket, func(ctx *plugin.Context, input string) error {
			return engine.Eval(ctx, input)
		}, engine.Commands, agentPlugin)
		if err := vscodeFrontend.Start(); err != nil {
			log.Error("start vscode frontend failed", "error", err)
			return
		}
		// defer 注册在 engine.StopAll 之后（LIFO）：退出时先停 frontend 再停 engine
		defer vscodeFrontend.Stop()
		log.Info("vscode frontend listening", "socket", runtimeCfg.Frontend.Vscode.Socket)
	}

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
