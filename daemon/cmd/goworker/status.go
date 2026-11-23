package main

// goworker status：拨号 <ConfigDir>/status.sock 读取 daemon 的一次性 JSON 快照。
// daemon 侧实现在 daemon/internal/app/status.go，两端共用 StatusInfo 结构。

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/tinguo/goworker/daemon/internal/app"
)

// status 查询超时：快照极小，拨不通/读不完都说明 daemon 有问题，别陪着挂。
const statusTimeout = 2 * time.Second

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "print the raw JSON snapshot from the daemon")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	raw, err := queryStatus()
	if err != nil {
		hintNotRunning(err)
		os.Exit(1)
	}

	if *asJSON {
		os.Stdout.Write(raw)
		return
	}
	printStatus(raw)
}

// queryStatus 拨号当前实例的 status socket 并读完快照。
func queryStatus() ([]byte, error) {
	return queryStatusAt(app.StatusSocketPath())
}

// queryStatusAt 按 socket 路径读快照；daemon 写完一行即关闭连接。
// service 子命令用它查询 LaunchAgent 烘焙出来的（可能不同于当前 shell env 的）
// 配置目录。
func queryStatusAt(socketPath string) ([]byte, error) {
	conn, err := net.DialTimeout("unix", socketPath, statusTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(statusTimeout))
	return io.ReadAll(conn)
}

// hintNotRunning 把拨号失败翻译成"没在跑"的人话。残留 socket（daemon 已死但
// 文件还在）拨号报 ECONNREFUSED —— 与 acp listener 的残留探测判定一致。
func hintNotRunning(err error) {
	switch {
	case errors.Is(err, os.ErrNotExist):
		fmt.Println("goworker daemon is not running")
	case errors.Is(err, syscall.ECONNREFUSED):
		fmt.Println("goworker daemon is not running (stale status socket left behind; it will be cleaned up on next start)")
	default:
		fmt.Fprintf(os.Stderr, "status query failed: %v\n", err)
		return
	}
	fmt.Println("start it with: goworker (daemon form) or goworker --tui (interactive REPL)")
}

// printStatus 渲染人话快照；各端点用 100ms 拨号探活（与 daemon 侧残留探测同手法）。
func printStatus(raw []byte) {
	var info app.StatusInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		// 契约对不上时退回原样输出，别把可用的信息藏起来
		os.Stdout.Write(raw)
		return
	}

	fmt.Println("goworker daemon is running")
	fmt.Printf("  version  %s\n", info.Version)
	fmt.Printf("  pid      %d (up %s, started at %s)\n", info.PID, formatUptime(info.UptimeSeconds), info.StartedAt)
	fmt.Printf("  mode     %s\n", modeLabel(info.Mode))
	fmt.Printf("  config   %s\n", info.ConfigPath)
	if info.LogFile != "" {
		fmt.Printf("  log      %s\n", info.LogFile)
	}
	if info.ACPSocket != "" {
		fmt.Printf("  acp      %s  %s\n", info.ACPSocket, probeLabel(info.ACPSocket))
	}
	if info.DispatchSocket != "" {
		fmt.Printf("  dispatch %s  %s\n", info.DispatchSocket, probeLabel(info.DispatchSocket))
	}
	fmt.Printf("  status   %s\n", info.StatusSocket)
}

func probeLabel(path string) string {
	if probeSocket(path) {
		return "listening"
	}
	return "not listening"
}

// probeSocket 对 Unix socket 做 100ms 拨号探活；探活只判端点在不在，不发协议。
func probeSocket(path string) bool {
	conn, err := net.DialTimeout("unix", path, 100*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func modeLabel(mode string) string {
	switch mode {
	case "daemon":
		return "daemon (service form)"
	case "tui":
		return "tui (interactive REPL)"
	default:
		return mode
	}
}

func formatUptime(seconds int64) string {
	d := time.Duration(seconds) * time.Second
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	switch {
	case days > 0:
		return fmt.Sprintf("%dd%dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh%dm", hours, minutes)
	case minutes > 0:
		return fmt.Sprintf("%dm", minutes)
	default:
		return d.Round(time.Second).String()
	}
}
