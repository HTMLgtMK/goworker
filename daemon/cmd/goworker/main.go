package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/tinguo/goworker/daemon/internal/app"
	appconfig "github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/cli/stdin"
	"github.com/tinguo/goworker/daemon/internal/cli/vscode"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	"github.com/tinguo/goworker/daemon/internal/version"
)

// cmd/goworker 是终端 CLI 壳：装配交给 internal/app，本文件只做子命令分派
// （-version / acp）、前端运行与 OS 信号处理。Android 壳在 mobile/（gobind 入口），
// 共享同一套 app 装配。

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

	// goworker acp：ZCode 以 ACP worker 身份跑在 stdio 上（被 dispatcher/编辑器驱动）。
	// 与 REPL 壳不同路：worker 不装 Engine/插件，见 app.RunACPWorker 的注释。
	if len(os.Args) > 1 && os.Args[1] == "acp" {
		if err := app.RunACPWorker(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		return
	}

	appl, err := app.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	defer appl.Stop()

	if err := runFrontends(appl); err != nil {
		appl.Log.Error("frontend error", "error", err)
	}
}

// runFrontends 启动全部前端并阻塞到退出条件（前端结束或收到 SIGINT/SIGTERM）。
// 退出顺序由 defer LIFO 保证：先停 vscode 前端，再由调用方的 appl.Stop 停 engine+日志。
func runFrontends(appl *app.Application) error {
	// vscode ACP 前端：vscode.enabled=true 时经本地 Unix socket 服务外部编辑器（ACP 协议）。
	// 启动失败与 dispatcher 插件的 listener 启动失败一致：返回错误终止启动，不静默降级。
	if appl.Runtime.Frontend.Vscode.Enabled {
		stopVSCode, err := startVSCodeFrontend(appl)
		if err != nil {
			return err
		}
		if stopVSCode != nil { // 最小壳（agent 插件被裁剪）时为 nil，没有需要停的东西
			defer stopVSCode()
		}
	}

	// stdin 前端（goroutine，不阻塞）
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
		return err
	case sig := <-sigCh:
		appl.Log.Info("received signal, shutting down", "signal", sig.String())
		return nil
	}
}

// startVSCodeFrontend 启动 vscode ACP 前端，返回它的 Stop 函数。
// agent 插件被裁剪（最小壳）时返回 nil stop：会话清单/load 没有承载者，
// 不静默降级成半残前端。
func startVSCodeFrontend(appl *app.Application) (func(), error) {
	if appl.Agent == nil {
		appl.Log.Warn("vscode frontend disabled: agent plugin not compiled in (GOWORKER_NO_PLUGINS)")
		return nil, nil
	}

	// socket 未显式配置时按 DefaultDir 派生默认路径。与 dispatcher 的
	// <DispatchDir>/acp.sock 完全分离：那是无人值守 worker 的任务提交入口，
	// 这里只服务外部编辑器驱动主会话。
	if appl.Runtime.Frontend.Vscode.Socket == "" {
		appl.Runtime.Frontend.Vscode.Socket = filepath.Join(appconfig.DefaultDir(), "frontend", "vscode.sock")
	}
	frontend := vscode.New(appl.Runtime.Frontend.Vscode.Socket, func(ctx *model.Context, input string) error {
		return appl.Engine.Eval(ctx, input)
	}, appl.Engine.Commands, appl.Agent)
	if err := frontend.Start(); err != nil {
		return nil, fmt.Errorf("start vscode frontend: %w", err)
	}
	appl.Log.Info("vscode frontend listening", "socket", appl.Runtime.Frontend.Vscode.Socket)
	// Stop 带 error，但关停路径上无从处理也不再影响结果 —— 与原 defer 丢弃语义一致
	return func() { _ = frontend.Stop() }, nil
}
