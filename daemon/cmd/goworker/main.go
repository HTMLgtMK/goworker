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

// cmd/goworker 是终端 CLI 壳：装配交给 internal/app，本文件只做
// -version 参数、stdin 前端启动与 OS 信号处理。
// Android 壳在 mobile/（gobind 入口），共享同一套 app.New 装配。
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

	appl, err := app.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	defer appl.Stop()

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
