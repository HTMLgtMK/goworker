package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tinguo/goworker/daemon/internal/app"
	"github.com/tinguo/goworker/daemon/internal/cli/stdin"
	"github.com/tinguo/goworker/daemon/internal/version"
)

// cmd/goworker 是终端 CLI 壳：装配交给 internal/app，本文件只做子命令分派
// （acp / status / web / service）、运行模式选择（默认 --daemon service 形态 /
// --tui 交互式）与 OS 信号处理。Android 壳在 mobile/（gobind 入口），共享同一套
// app 装配。

func main() {
	// 子命令必须先于 flag.Parse 截获：flag 包遇到首个位置参数就会停下，
	// `goworker status --json` 这类"子命令 + 自己的 flag"到不了全局解析。
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "acp":
			// goworker acp：ZCode 以 ACP worker 身份跑在 stdio 上（被 dispatcher/编辑器驱动）。
			// 与 CLI 壳不同路：worker 不装 Engine/插件，见 app.RunACPWorker 的注释。
			if err := app.RunACPWorker(); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
			return
		case "status":
			runStatus(os.Args[2:])
			return
		case "web":
			runWeb(os.Args[2:])
			return
		case "service":
			runService(os.Args[2:])
			return
		}
	}

	// 走 flag 包而不是手搓 os.Args：`-version` / `--version` 都能认。
	// 默认 ExitOnError 正是想要的行为——未知参数直接 usage + exit 2，
	// CLI 就该这么硬气，没必要宽容。
	showVersion := flag.Bool("version", false, "print version information and exit")
	daemonMode := flag.Bool("daemon", false, "run as a service (default): background services only, no interactive UI")
	tuiMode := flag.Bool("tui", false, "run the interactive terminal REPL (previous default)")
	flag.Parse()
	if *showVersion {
		fmt.Println(version.Get().String())
		return
	}
	if *daemonMode && *tuiMode {
		fmt.Fprintln(os.Stderr, "--daemon and --tui are mutually exclusive")
		os.Exit(2)
	}

	mode := "daemon"
	if *tuiMode {
		mode = "tui"
	}

	appl, err := app.New(app.WithMode(mode))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	defer appl.Stop()

	if mode == "tui" {
		if err := runTUI(appl); err != nil {
			appl.Log.Error("frontend error", "error", err)
		}
		return
	}
	runDaemon(appl)
}

// runTUI 启动 stdin 交互前端并阻塞到退出条件（前端结束或收到 SIGINT/SIGTERM）。
// ACP 入口不在这里：外部编辑器经 app.New 统一装配的 ACP service 接入
// （frontend/acp.sock），随 appl.Stop 的 services 生命周期一起关停。
func runTUI(appl *app.Application) error {
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

// logLine 带时间戳输出一行。服务化运行时 stdout 落进 launchd 日志文件，同一
// 文件里会累积多次启动/重拉的输出，没有时间戳就分不清哪行是谁写的。
func logLine(format string, a ...any) {
	fmt.Printf("[%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), fmt.Sprintf(format, a...))
}

// runDaemon 以 service 形态运行：stdout 打一段 banner（版本 + 端点 + 路径）后
// 保持安静，等 SIGINT/SIGTERM 收尾。进程内服务（acp/dispatcher/status socket）
// 已随 app.New 起好，这里只剩等信号；详细运行日志按配置走 stderr 与日志文件。
func runDaemon(appl *app.Application) {
	info := appl.StatusInfo()
	logLine("%s", info.Version)
	if info.ACPSocket != "" {
		logLine("acp      %s", info.ACPSocket)
	}
	logLine("dispatch %s", info.DispatchSocket)
	logLine("status   %s (query with: goworker status)", info.StatusSocket)
	logLine("config   %s", info.ConfigPath)
	if info.LogFile != "" {
		logLine("log      %s", info.LogFile)
	}
	logLine("running, press Ctrl-C to stop")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	appl.Log.Info("received signal, shutting down", "signal", sig.String())
}
