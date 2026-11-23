package main

// goworker service：把 daemon（和可选的 web 网关）注册成 macOS launchd 服务。
// LaunchAgent 按用户安装于 ~/Library/LaunchAgents，登录自启（RunAtLoad）、
// 崩溃自动拉起（KeepAlive），日志落 ~/Library/Logs/goworker。
//
// 两个 agent 的 ProgramArguments 都指向本二进制（--daemon / web），入口解析、
// env 组装、子进程托管的逻辑只有一份。launchd 环境不继承 shell env，install 时
// 把当前 PATH 与 GOWORKER_* 环境变量烘焙进 plist。
//
// 仅 darwin；其他平台明确报不支持，不做半吊子抽象。

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	appconfig "github.com/tinguo/goworker/daemon/internal/app/config"
)

const (
	serviceDaemonLabel = "com.goworker.daemon"
	serviceWebLabel    = "com.goworker.web"

	// bootstrap/kickstart 是异步的：不等 agent 真正就绪就报"完成"，紧跟着的
	// service status 会在 spawn 窗口期显示未启动，看起来像坏了。给足就绪窗口。
	serviceReadyTimeout = 10 * time.Second
)

func runService(args []string) {
	if runtime.GOOS != "darwin" {
		fmt.Fprintln(os.Stderr, "service command supports macOS (launchd) only")
		os.Exit(1)
	}

	// 先摘 action 再 Parse：flag 包遇到首个位置参数就停，`install --web` 里的
	// --web 会被当成位置参数吞掉，必须把子命令摘出来单独解析剩下的 flag。
	action := ""
	if len(args) > 0 {
		action, args = args[0], args[1:]
	}

	fs := flag.NewFlagSet("service", flag.ExitOnError)
	withWeb := fs.Bool("web", false, "install: also register the web gateway LaunchAgent")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	switch action {
	case "install":
		serviceInstall(*withWeb)
	case "uninstall":
		serviceUninstall()
	case "start", "stop", "restart":
		target := "daemon"
		if fs.Arg(0) != "" {
			target = fs.Arg(0)
		}
		serviceLifecycle(action, target)
	case "status":
		serviceStatus()
	default:
		fmt.Fprintln(os.Stderr, "usage: goworker service install [--web] | uninstall | start|stop|restart [daemon|web|all] | status")
		os.Exit(2)
	}
}

func serviceInstall(withWeb bool) {
	exe, err := stableExecutable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	// web 前置检查提前：避免 daemon 已常驻、web 却安装失败。
	var web *webSettings
	if withWeb {
		settings, err := precheckWebInstall()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to register the web service: %v\n", err)
			os.Exit(1)
		}
		web = &settings
	}

	if err := installDaemonAgent(exe); err != nil {
		fmt.Fprintf(os.Stderr, "failed to register the daemon service: %v\n", err)
		os.Exit(1)
	}
	if web != nil {
		if err := installWebAgent(exe, *web); err != nil {
			fmt.Fprintf(os.Stderr, "failed to register the web service: %v\n", err)
			fmt.Fprintln(os.Stderr, "note: the daemon service was already registered and is running; remove it with `goworker service uninstall` if you want a clean slate")
			os.Exit(1)
		}
	} else {
		fmt.Println("hint: run `goworker service install --web` to keep the web gateway running as a service too")
	}
}

// precheckWebInstall 做 web 侧确定性检查；旧 agent 占的端口会在 bootout 后释放。
func precheckWebInstall() (webSettings, error) {
	settings, err := resolveWebSettings(webOverrides{})
	if err != nil {
		return webSettings{}, fmt.Errorf("resolve gateway entry: %w", err)
	}
	if isBootstrapped(serviceWebLabel) {
		return settings, nil
	}
	inUse, err := tcpInUse(settings.Host, settings.Port)
	if err != nil {
		return webSettings{}, fmt.Errorf("port precheck failed for %s:%s: %w", settings.Host, settings.Port, err)
	}
	if inUse {
		return webSettings{}, fmt.Errorf("port %s:%s is already in use; stop the process holding it first (possibly a gateway outside launchd)",
			settings.Host, settings.Port)
	}
	return settings, nil
}

// stableExecutable 取本二进制的稳定路径。服务注册的是路径不是进程，go run 的
// 临时产物在构建缓存清理后就成了死链，必须拒绝。
func stableExecutable() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	if strings.Contains(exe, "go-build") {
		return "", fmt.Errorf("refusing to register a service from a temporary `go run` binary (%s); build to a fixed path first (go build / cmake)", exe)
	}
	return exe, nil
}

// installDaemonAgent 写 daemon plist 并 bootstrap。已托管 = 重装（bootout 走
// 优雅停止后拉新）；在跑但不受 launchd 托管 = 拒绝（直接 bootstrap 会撞
// socket already in use，KeepAlive 下变成崩溃循环）。
func installDaemonAgent(exe string) error {
	if isBootstrapped(serviceDaemonLabel) {
		if err := bootoutAndWait(serviceDaemonLabel); err != nil {
			return fmt.Errorf("uninstall previous daemon agent: %w", err)
		}
	} else if _, err := queryStatus(); err == nil {
		return fmt.Errorf("a daemon is already running outside of launchd; stop it first (see PID via `goworker status`), then install")
	}

	plistPath, err := writeAgentPlist(serviceDaemonLabel, []string{exe, "--daemon"}, "daemon", nil)
	if err != nil {
		return err
	}
	if err := bootstrapWithRetry(serviceDaemonLabel); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	fmt.Println("registered daemon LaunchAgent")
	fmt.Printf("  plist      %s\n", plistPath)
	if dir := plistEnv(serviceDaemonLabel)["GOWORKER_CONFIG_DIR"]; dir != "" {
		fmt.Printf("  config dir (baked): %s\n", dir)
	}
	fmt.Printf("  logs       %s\n", filepath.Join(mustLaunchLogsDir(), "daemon.out.log"))
	reportAgentReadiness("install", serviceDaemonLabel)
	return nil
}

func installWebAgent(exe string, settings webSettings) error {
	if isBootstrapped(serviceWebLabel) {
		if err := bootoutAndWait(serviceWebLabel); err != nil {
			return fmt.Errorf("uninstall previous web agent: %w", err)
		}
	}
	inUse, err := tcpInUse(settings.Host, settings.Port)
	if err != nil {
		return fmt.Errorf("port precheck failed for %s:%s: %w", settings.Host, settings.Port, err)
	}
	if inUse {
		return fmt.Errorf("port %s:%s is already in use; stop the process holding it first (possibly a gateway outside launchd)",
			settings.Host, settings.Port)
	}

	// 烘焙 entry：运行时不再重新 glob 探测，install 打印的就是实际入口。
	plistPath, err := writeAgentPlist(serviceWebLabel, []string{exe, "web"}, "web",
		map[string]string{"GOWORKER_WEB_ENTRY": settings.Entry})
	if err != nil {
		return err
	}
	if err := bootstrapWithRetry(serviceWebLabel); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	fmt.Println("registered web LaunchAgent")
	fmt.Printf("  plist      %s\n", plistPath)
	fmt.Printf("  entry      %s\n", settings.Entry)
	fmt.Printf("  logs       %s\n", filepath.Join(mustLaunchLogsDir(), "web.out.log"))
	reportAgentReadiness("install", serviceWebLabel)
	return nil
}

func serviceUninstall() {
	for _, label := range []string{serviceDaemonLabel, serviceWebLabel} {
		if err := bootoutAndWait(label); err != nil {
			fmt.Fprintf(os.Stderr, "failed to uninstall %s: %v\n", label, err)
			os.Exit(1)
		}
		plistPath, err := agentPlistPath(label)
		if err == nil {
			_ = os.Remove(plistPath)
		}
		fmt.Printf("uninstalled %s\n", label)
	}
}

func serviceLifecycle(action, target string) {
	labels := []string{serviceDaemonLabel}
	switch target {
	case "web":
		labels = []string{serviceWebLabel}
	case "all":
		labels = []string{serviceDaemonLabel, serviceWebLabel}
	case "daemon":
	default:
		fmt.Fprintf(os.Stderr, "unknown target %q (daemon|web|all)\n", target)
		os.Exit(2)
	}

	for _, label := range labels {
		if err := lifecycleOne(action, label); err != nil {
			fmt.Fprintf(os.Stderr, "%s %s failed: %v\n", action, label, err)
			os.Exit(1)
		}
		if action == "stop" {
			fmt.Printf("%s %s: done\n", action, label)
			continue
		}
		reportAgentReadiness(action, label)
	}
}

// lifecycleOne 的 stop 用 bootout（KeepAlive 服务只有卸载才真正停得下来），
// start/restart 在未加载时退化为 bootstrap。
func lifecycleOne(action, label string) error {
	domain := launchDomain()
	bootstrapped := isBootstrapped(label)
	switch action {
	case "start":
		if bootstrapped {
			_, err := launchctl("kickstart", domain+"/"+label)
			return err
		}
		return bootstrapAgent(label)
	case "stop":
		if !bootstrapped {
			return nil
		}
		_, err := launchctl("bootout", domain+"/"+label)
		return err
	case "restart":
		if bootstrapped {
			_, err := launchctl("kickstart", "-k", domain+"/"+label)
			return err
		}
		return bootstrapAgent(label)
	}
	return fmt.Errorf("unknown action %q", action)
}

func bootstrapAgent(label string) error {
	plistPath, err := agentPlistPath(label)
	if err != nil {
		return err
	}
	if _, err := launchctl("bootstrap", launchDomain(), plistPath); err != nil {
		// 拆装竞态偶发 EIO，退避重试兜底
		if err2 := bootstrapWithRetry(label); err2 != nil {
			return fmt.Errorf("bootstrap: %w (retry also failed: %v)", err, err2)
		}
	}
	return nil
}

// reportAgentReadiness 在 start/restart/install 后等 agent 真正就绪再报结果。
func reportAgentReadiness(action, label string) {
	switch label {
	case serviceDaemonLabel:
		raw, err := waitDaemonReady(label, serviceReadyTimeout)
		if err != nil {
			fmt.Printf("%s %s: launched, but not ready within %s — check %s\n",
				action, label, serviceReadyTimeout, agentErrLog("daemon"))
			return
		}
		if pid := snapshotPID(raw); pid > 0 {
			fmt.Printf("%s %s: running (pid %d)\n", action, label, pid)
			return
		}
		fmt.Printf("%s %s: running\n", action, label)
	case serviceWebLabel:
		url, ok := agentWebHealthURL(label)
		if ok && waitWebReady(url, serviceReadyTimeout) {
			fmt.Printf("%s %s: listening on %s\n", action, label, url)
			fmt.Printf("open in browser: %s\n", url)
			return
		}
		fmt.Printf("%s %s: launched, but not ready within %s — check %s\n",
			action, label, serviceReadyTimeout, agentErrLog("web"))
	}
}

func serviceStatus() {
	fmt.Printf("LaunchAgents (launchd %s)\n", launchDomain())
	for _, label := range []string{serviceDaemonLabel, serviceWebLabel} {
		installed := "not installed"
		if plistPath, err := agentPlistPath(label); err == nil {
			if _, err := os.Stat(plistPath); err == nil {
				installed = "installed"
			}
		}
		state, pid := agentState(label)
		line := fmt.Sprintf("  %-20s %s", label, installed)
		if state != "" {
			line += "  state=" + state
		}
		if pid != "" {
			line += "  pid=" + pid
		}
		fmt.Println(line)
	}
	fmt.Println()

	// daemon 快照按 agent 烘焙的 config dir 查：launchd 环境的 GOWORKER_CONFIG_DIR
	// 可能与当前 shell 不同，按当前 env 查会误报"未运行"。未烘焙时在 agent 环境视角
	// 下取默认目录（当前 shell 即使设了该变量也与 agent 无关）。
	var dir string
	withAgentEnv(serviceDaemonLabel, func() {
		dir = appconfig.DefaultDir()
	})
	raw, err := queryStatusAt(filepath.Join(dir, "status.sock"))
	if err != nil {
		hintNotRunning(err)
		if isBootstrapped(serviceDaemonLabel) {
			fmt.Printf("the daemon agent is bootstrapped but not answering — check %s\n", agentErrLog("daemon"))
		}
		return
	}
	fmt.Printf("daemon snapshot (config dir: %s)\n", dir)
	printStatus(raw)

	if isBootstrapped(serviceWebLabel) {
		if url, ok := agentWebHealthURL(serviceWebLabel); ok {
			if waitWebReady(url, 2*time.Second) {
				fmt.Printf("web gateway listening on %s\n", url)
			} else {
				fmt.Printf("web gateway not answering at %s — check %s\n", url, agentErrLog("web"))
			}
		}
	}
}

// ---- launchd 底层 ----

// bootoutAndWait：bootout 是异步的，旧 job 没拆完就 bootstrap 同名 label 会报
// "5: Input/output error"，必须轮询到 print 失败（job 真正消失）再继续。
func bootoutAndWait(label string) error {
	domain := launchDomain()
	if !isBootstrapped(label) {
		return nil
	}
	if _, err := launchctl("bootout", domain+"/"+label); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !isBootstrapped(label) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out uninstalling %s (previous job has not fully exited)", label)
}

// bootstrapWithRetry：同名 label 拆装竞态偶发 EIO，退避重试兜底。
func bootstrapWithRetry(label string) error {
	plistPath, err := agentPlistPath(label)
	if err != nil {
		return err
	}
	domain := launchDomain()
	var lastErr error
	for i := 0; i < 3; i++ {
		_, err := launchctl("bootstrap", domain, plistPath)
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	return lastErr
}

func launchDomain() string {
	return fmt.Sprintf("gui/%d", os.Getuid())
}

func agentPlistPath(label string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", label+".plist"), nil
}

func mustLaunchLogsDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp"
	}
	dir := filepath.Join(home, "Library", "Logs", "goworker")
	_ = os.MkdirAll(dir, 0o755)
	return dir
}

func agentErrLog(name string) string {
	return filepath.Join(mustLaunchLogsDir(), name+".err.log")
}

// bakedEnv 是 install 时刻要烘焙进 plist 的环境变量：launchd 不继承 shell env，
// PATH 与 GOWORKER_* 不带过去的话 daemon 会找不到配置目录、node 会找不到二进制。
func bakedEnv() map[string]string {
	path := os.Getenv("PATH")
	if path == "" {
		path = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
	}
	env := map[string]string{"PATH": path}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GOWORKER_") {
			continue
		}
		if i := strings.IndexByte(kv, '='); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}
	return env
}

func writeAgentPlist(label string, programArgs []string, logName string, extraEnv map[string]string) (string, error) {
	plistPath, err := agentPlistPath(label)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil {
		return "", err
	}

	env := bakedEnv()
	for k, v := range extraEnv {
		env[k] = v
	}

	logsDir := mustLaunchLogsDir()
	plist := renderPlist(label, programArgs,
		filepath.Join(logsDir, logName+".out.log"),
		filepath.Join(logsDir, logName+".err.log"),
		env,
	)
	if err := os.WriteFile(plistPath, []byte(plist), 0o644); err != nil {
		return "", fmt.Errorf("write plist: %w", err)
	}
	return plistPath, nil
}

func renderPlist(label string, programArgs []string, stdoutPath, stderrPath string, env map[string]string) string {
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	b.WriteString("<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n")
	b.WriteString("<plist version=\"1.0\">\n<dict>\n")
	b.WriteString("  <key>Label</key>\n  <string>" + xmlEscape(label) + "</string>\n")
	b.WriteString("  <key>ProgramArguments</key>\n  <array>\n")
	for _, arg := range programArgs {
		b.WriteString("    <string>" + xmlEscape(arg) + "</string>\n")
	}
	b.WriteString("  </array>\n")
	b.WriteString("  <key>RunAtLoad</key>\n  <true/>\n")
	b.WriteString("  <key>KeepAlive</key>\n  <true/>\n")
	b.WriteString("  <key>ThrottleInterval</key>\n  <integer>10</integer>\n")
	b.WriteString("  <key>StandardOutPath</key>\n  <string>" + xmlEscape(stdoutPath) + "</string>\n")
	b.WriteString("  <key>StandardErrorPath</key>\n  <string>" + xmlEscape(stderrPath) + "</string>\n")
	if len(env) > 0 {
		b.WriteString("  <key>EnvironmentVariables</key>\n  <dict>\n")
		keys := make([]string, 0, len(env))
		for k := range env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.WriteString("    <key>" + xmlEscape(k) + "</key>\n")
			b.WriteString("    <string>" + xmlEscape(env[k]) + "</string>\n")
		}
		b.WriteString("  </dict>\n")
	}
	b.WriteString("</dict>\n</plist>\n")
	return b.String()
}

func xmlEscape(s string) string {
	return strings.NewReplacer(
		"&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;",
	).Replace(s)
}

func launchctl(args ...string) (string, error) {
	cmd := exec.Command("launchctl", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("launchctl %s: %w (%s)",
			strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func isBootstrapped(label string) bool {
	_, err := launchctl("print", launchDomain()+"/"+label)
	return err == nil
}

// agentState 从 `launchctl print` 输出抓 state/pid；未加载返回空值。
func agentState(label string) (string, string) {
	out, err := launchctl("print", launchDomain()+"/"+label)
	if err != nil {
		return "", ""
	}
	var state, pid string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "state ="):
			state = strings.TrimSpace(strings.TrimPrefix(line, "state ="))
		case strings.HasPrefix(line, "pid ="):
			pid = strings.TrimSpace(strings.TrimPrefix(line, "pid ="))
		}
	}
	return state, pid
}

// plistEnv 读 plist 里烘焙的 GOWORKER_* 环境变量。plist 由本命令自己渲染，
// 结构固定，正则提取即可，不值得引 XML 解析器。
var plistEnvKeyRe = regexp.MustCompile(`<key>(GOWORKER_[A-Z_]+)</key>\s*<string>([^<]*)</string>`)

func plistEnv(label string) map[string]string {
	plistPath, err := agentPlistPath(label)
	if err != nil {
		return nil
	}
	data, err := os.ReadFile(plistPath)
	if err != nil {
		return nil
	}
	env := map[string]string{}
	for _, m := range plistEnvKeyRe.FindAllStringSubmatch(string(data), -1) {
		env[m[1]] = m[2]
	}
	return env
}

// withAgentEnv 在 LaunchAgent 的环境视角下执行 fn：plist 烘焙的 GOWORKER_*
// 覆盖当前 shell env，未烘焙的相关变量清空 —— 解析结果与 agent 运行时一致。
func withAgentEnv(label string, fn func()) {
	agentEnv := plistEnv(label)
	keys := []string{
		"GOWORKER_CONFIG_DIR", "GOWORKER_WEB_PORT", "GOWORKER_WEB_HOST",
		"GOWORKER_WEB_WORKSPACE", "GOWORKER_WEB_ENTRY",
	}
	type saved struct {
		key, value string
		existed    bool
	}
	var restores []saved
	for _, k := range keys {
		old, had := os.LookupEnv(k)
		restores = append(restores, saved{k, old, had})
		if v, ok := agentEnv[k]; ok {
			_ = os.Setenv(k, v)
		} else {
			_ = os.Unsetenv(k)
		}
	}
	fn()
	for i := len(restores) - 1; i >= 0; i-- {
		r := restores[i]
		if r.existed {
			_ = os.Setenv(r.key, r.value)
		} else {
			_ = os.Unsetenv(r.key)
		}
	}
}

// agentWebHealthURL 以 agent 的环境视角解析网关地址，供就绪等待与 status 探活。
func agentWebHealthURL(label string) (string, bool) {
	var url string
	var ok bool
	withAgentEnv(label, func() {
		settings, err := resolveWebSettings(webOverrides{})
		if err != nil {
			return
		}
		url = "http://" + net.JoinHostPort(settings.Host, settings.Port)
		ok = true
	})
	return url, ok
}

// waitDaemonReady 等 agent 拉起的 daemon 真正应答 status socket（在 agent 的
// 配置目录视角下轮询）。
func waitDaemonReady(label string, timeout time.Duration) ([]byte, error) {
	var socket string
	withAgentEnv(label, func() {
		socket = filepath.Join(appconfig.DefaultDir(), "status.sock")
	})
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if raw, err := queryStatusAt(socket); err == nil {
			return raw, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("daemon not ready within %s", timeout)
}

func snapshotPID(raw []byte) int {
	var s struct {
		PID int `json:"pid"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0
	}
	return s.PID
}
