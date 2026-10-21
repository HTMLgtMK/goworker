package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tinguo/goworker/daemon/internal/app"
	"github.com/tinguo/goworker/daemon/internal/cli/stdin"
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

// runFrontends 启动 stdin 前端并阻塞到退出条件（前端结束或收到 SIGINT/SIGTERM）。
// ACP 入口不在这里：外部编辑器经 app.New 统一装配的 ACP service 接入
// （frontend/acp.sock），随 appl.Stop 的 services 生命周期一起关停。
func runFrontends(appl *app.Application) error {
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
