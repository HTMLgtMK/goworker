package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/app"
	appconfig "github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/cli/stdin"
	"github.com/tinguo/goworker/daemon/internal/cli/vscode"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	"github.com/tinguo/goworker/daemon/internal/core/service"
	"github.com/tinguo/goworker/daemon/internal/version"
)

// cmd/goworker 是终端 CLI 壳：装配交给 internal/app，本文件只做
// -version / acp 子命令分派、vscode+stdin 前端启动与 OS 信号处理。
// Android 壳在 mobile/（gobind 入口），共享同一套 app.New 装配。

// runACPWorker 以 stdio ACP Agent 模式服务 ZCode 会话：配置解析、HTTP 客户端、
// provider 装配与 agent 插件共用同一套（ProviderFactory），stdin EOF 即退出。
func runACPWorker() {
	cfgPath := appconfig.DefaultPath()
	cfg := appconfig.Load(cfgPath)
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

	server := service.ServeACPWorker(service.Stdio(), service.ACPWorkerDeps{
		Config:   runtimeCfg,
		AuditDir: filepath.Join(appconfig.DefaultDir(), "audit"),
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

	// goworker acp：ZCode 以 ACP worker 身份跑在 stdio 上（被 dispatcher/编辑器驱动）
	if len(os.Args) > 1 && os.Args[1] == "acp" {
		runACPWorker()
		return
	}

	appl, err := app.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	defer appl.Stop()

	// vscode ACP 前端：vscode.enabled=true 时经本地 Unix socket 服务外部编辑器（ACP 协议）。
	// agent 插件被裁剪（最小壳）时跳过启动：会话清单/load 没有承载者，不静默降级成半残前端。
	if appl.Runtime.Frontend.Vscode.Enabled {
		if appl.Agent == nil {
			appl.Log.Warn("vscode frontend disabled: agent plugin not compiled in (GOWORKER_NO_PLUGINS)")
		} else {
			// socket 未显式配置时按 DefaultDir 派生默认路径。与 dispatcher 的
			// <DispatchDir>/acp.sock 完全分离：那是无人值守 worker 的任务提交入口，
			// 这里只服务外部编辑器驱动主会话。
			if appl.Runtime.Frontend.Vscode.Socket == "" {
				appl.Runtime.Frontend.Vscode.Socket = filepath.Join(appconfig.DefaultDir(), "frontend", "vscode.sock")
			}
			vscodeFrontend := vscode.New(appl.Runtime.Frontend.Vscode.Socket, func(ctx *model.Context, input string) error {
				return appl.Engine.Eval(ctx, input)
			}, appl.Engine.Commands, appl.Agent)
			if err := vscodeFrontend.Start(); err != nil {
				appl.Log.Error("start vscode frontend failed", "error", err)
				return
			}
			// defer 注册在 appl.Stop 之后（LIFO）：退出时先停 frontend 再停 engine+日志
			defer vscodeFrontend.Stop()
			appl.Log.Info("vscode frontend listening", "socket", appl.Runtime.Frontend.Vscode.Socket)
		}
	}

	// 启动前端（goroutine，不阻塞）
	frontend := stdin.NewStdinFrontend(appl.Engine, &appl.Config.Frontend.Stdin)
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
			appl.Log.Error("frontend error", "error", err)
		}
	case sig := <-sigCh:
		appl.Log.Info("received signal, shutting down", "signal", sig.String())
	}
}
