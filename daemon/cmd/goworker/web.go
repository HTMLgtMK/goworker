package main

// goworker web：托管启动 Node web 网关（扩展仓 apps/web-host 的 gateway）。
// 入口/端口/工作区按 flag > GOWORKER_WEB_* 环境变量 > frontend.web 配置 > 自动探测
// 的优先级解析；spawn 独立进程组、透传输出、轮询 /api/health，随信号收停整组，
// 不留孤儿 node。service install --web 的 LaunchAgent 也是跑到这里（ProgramArguments
// = [goworker, web]），解析逻辑只有这一份。

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	appconfig "github.com/tinguo/goworker/daemon/internal/app/config"
)

// webOverrides 是命令行显式覆盖值（空串 = 未指定，落到下一档优先级）。
type webOverrides struct {
	Entry     string
	Port      string
	Host      string
	Workspace string
}

type webSettings struct {
	Entry       string // 绝对路径（~ 已展开）
	Port        string
	Host        string
	Workspace   string // 空 = 不注入 GOWORKER_WEB_WORKSPACE（网关取自身 cwd）
	EntrySource string // flag / env / config / auto，仅用于启动提示
}

func runWeb(args []string) {
	fs := flag.NewFlagSet("web", flag.ExitOnError)
	entry := fs.String("entry", "", "gateway entry path (gateway.js / gateway.ts)")
	port := fs.String("port", "", "listen port (default 5177)")
	host := fs.String("host", "", "listen address (default 127.0.0.1)")
	workspace := fs.String("workspace", "", "workspace root injected as GOWORKER_WEB_WORKSPACE")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	settings, err := resolveWebSettings(webOverrides{
		Entry: *entry, Port: *port, Host: *host, Workspace: *workspace,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to resolve web gateway entry: %v\n", err)
		os.Exit(1)
	}

	startWebGateway(settings)
}

// resolveWebSettings 合并五处取值来源。entry 缺省时在 ~/Work/*/apps/web-host 下
// 自动探测（dist 产物优先，src 入口兜底给开发用）。
func resolveWebSettings(ov webOverrides) (webSettings, error) {
	cfg, err := loadWebConfig()
	if err != nil {
		return webSettings{}, err
	}

	entry, entrySource := ov.Entry, "flag"
	if entry == "" {
		if entry = os.Getenv("GOWORKER_WEB_ENTRY"); entry != "" {
			entrySource = "env"
		}
	}
	if entry == "" && cfg != nil && cfg.Frontend.Web.Entry != "" {
		entry, entrySource = cfg.Frontend.Web.Entry, "config"
	}
	if entry == "" {
		entry = autoDetectWebEntry()
		if entry != "" {
			entrySource = "auto-detected"
		}
	}
	entry = expandTilde(entry)
	if _, err := os.Stat(entry); err != nil {
		return webSettings{}, fmt.Errorf("entry does not exist: %s (set frontend.web.entry in config.yaml or pass --entry)", entry)
	}

	port := firstNonEmpty(ov.Port, os.Getenv("GOWORKER_WEB_PORT"), cfgPort(cfg), "5177")
	host := firstNonEmpty(ov.Host, os.Getenv("GOWORKER_WEB_HOST"), cfgHost(cfg), "127.0.0.1")
	workspace := firstNonEmpty(ov.Workspace, os.Getenv("GOWORKER_WEB_WORKSPACE"), cfgWorkspace(cfg), deriveWebWorkspace(entry))
	workspace = expandTilde(workspace)

	if err := validatePort(port); err != nil {
		return webSettings{}, err
	}
	if workspace == "" {
		// launchd 托管时 cwd 是 "/"，workspace 空会让网关拿 "/" 当 WORKSPACE_ROOT。
		// 入口不在 apps/web-host 布局且无显式声明时派生不出 workspace，fail-fast。
		return webSettings{}, fmt.Errorf("cannot derive workspace from entry %q — pass --workspace or set frontend.web.workspace", entry)
	}

	return webSettings{
		Entry:       entry,
		Port:        port,
		Host:        host,
		Workspace:   workspace,
		EntrySource: entrySource,
	}, nil
}

// loadWebConfig 读用户配置供 web 解析兜底；配置无效时直接报错（与 daemon 同标准）。
func loadWebConfig() (*appconfig.Config, error) {
	cfg := appconfig.Load(appconfig.DefaultPath())
	if cfg == nil {
		// Load 已把具体原因记到 stderr 日志
		return nil, errors.New("config.yaml is invalid — fix it before starting the web gateway")
	}
	return cfg, nil
}

func cfgPort(cfg *appconfig.Config) string {
	if cfg == nil || cfg.Frontend.Web.Port <= 0 {
		return ""
	}
	return fmt.Sprintf("%d", cfg.Frontend.Web.Port)
}

func cfgHost(cfg *appconfig.Config) string {
	if cfg == nil {
		return ""
	}
	return cfg.Frontend.Web.Host
}

func cfgWorkspace(cfg *appconfig.Config) string {
	if cfg == nil {
		return ""
	}
	return cfg.Frontend.Web.Workspace
}

// autoDetectWebEntry 在 ~/Work 下按 apps/web-host 布局探测网关入口。
func autoDetectWebEntry() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	// dist 产物优先（生产形态），src 入口兜底（开发形态，npx tsx 跑）
	for _, rel := range []string{
		"Work/*/apps/web-host/dist/gateway.js",
		"Work/*/apps/web-host/src/gateway.ts",
	} {
		hits, _ := filepath.Glob(filepath.Join(home, rel))
		if len(hits) > 0 {
			return hits[0]
		}
	}
	return ""
}

// deriveWebWorkspace 从 apps/web-host 布局的 entry 派生仓库根作 workspace
// （…/apps/web-host/{dist,src}/gateway(.js|.ts) → 去掉 apps/ 之后的部分）。
// 网关的 WORKSPACE_ROOT = env ?? process.cwd()，托管启动时 cwd 不可信，必须注入。
func deriveWebWorkspace(entry string) string {
	idx := strings.LastIndex(entry, filepath.Join("apps", "web-host", ""))
	if idx < 0 {
		return ""
	}
	return filepath.Clean(entry[:idx])
}

// startWebGateway 拉起网关子进程并托管到退出：透传输出、探活、信号收停进程组。
func startWebGateway(settings webSettings) {
	addr := net.JoinHostPort(settings.Host, settings.Port)
	baseURL := fmt.Sprintf("http://%s", addr)

	// daemon 未跑只提示不阻塞：网关自带 socket 重连
	if _, err := queryStatus(); err != nil {
		fmt.Fprintln(os.Stderr, "warning: goworker daemon is not running; starting the gateway anyway — it will retry the sockets on its own")
	}
	// 端口预检：网关撞 EADDRINUSE 会直接崩，提前给出可读原因
	inUse, err := tcpInUse(settings.Host, settings.Port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "port precheck failed for %s: %v\n", addr, err)
		os.Exit(1)
	}
	if inUse {
		fmt.Fprintf(os.Stderr, "port %s is already in use — another gateway instance may be running\n", addr)
		fmt.Fprintf(os.Stderr, "find it with: lsof -nP -iTCP:%s -sTCP:LISTEN; or pick another port via --port / GOWORKER_WEB_PORT\n", settings.Port)
		os.Exit(1)
	}

	cmd, err := buildGatewayCommand(settings)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = webChildEnv(settings)
	// 独立进程组：收停时对整组发信号，网关自己的子进程不留孤儿
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// LaunchAgent 下 cwd 是 "/"：用 workspace 根当子进程工作目录，npx tsx / node
	// 才能正确解析 node_modules 与相对资源（resolveWebSettings 已保证 workspace 非空）。
	cmd.Dir = settings.Workspace

	logLine("web gateway entry: %s (source: %s)", settings.Entry, settings.EntrySource)
	if settings.Workspace != "" {
		logLine("workspace: %s", settings.Workspace)
	}
	logLine("starting gateway, waiting for %s/api/health ...", baseURL)

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start gateway: %v\n", err)
		os.Exit(1)
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	// 探活只影响提示，不影响托管生命周期
	go func() {
		if waitWebReady(baseURL, 15*time.Second) {
			logLine("web gateway listening on %s", baseURL)
			logLine("open in browser: %s", baseURL)
		} else {
			logLine("warning: %s/api/health not ready within 15s (SPA not built? run: npm run build --workspace @goworker/web)", baseURL)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-waitCh:
		// 网关自己退出（崩溃/正常结束）时，守着 Setpgid 建的进程组把残留成员
		// 一并收掉：node/npx 拉起的子进程不会因为主进程消失而自动退出，留着
		// 就是占端口的孤儿（launchd 只看得到主进程）。
		stopWebProcessGroup(cmd.Process.Pid)
		exitCodeFromWait(err)
	case <-sigCh:
		stopWebProcessGroup(cmd.Process.Pid)
		select {
		case <-waitCh:
		case <-time.After(5 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-waitCh
		}
	}
}

// buildGatewayCommand 按入口扩展名选运行器：.ts 走 npx tsx（开发形态），
// 其余按 esbuild 单文件 bundle 用 node 直跑。
func buildGatewayCommand(settings webSettings) (*exec.Cmd, error) {
	switch filepath.Ext(settings.Entry) {
	case ".ts":
		npx, err := lookTool("npx")
		if err != nil {
			return nil, err
		}
		return exec.Command(npx, "tsx", settings.Entry), nil
	default:
		node, err := lookTool("node")
		if err != nil {
			return nil, err
		}
		return exec.Command(node, settings.Entry), nil
	}
}

// lookTool 定位外部运行器；launchd 环境的 PATH 极简，补常见 Homebrew 位置。
func lookTool(name string) (string, error) {
	if p, err := exec.LookPath(name); err == nil {
		return p, nil
	}
	for _, dir := range []string{"/opt/homebrew/bin", "/usr/local/bin"} {
		candidate := filepath.Join(dir, name)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("%s not found (neither in PATH nor at common install locations)", name)
}

// webChildEnv 组装子进程环境：以 resolveWebSettings 解出的 settings 为权威（flag >
// env > config > default），用 appendEnvOverride 覆盖宿主 env 里的同名 key——否则
// 宿主 env 残留的 GOWORKER_WEB_PORT 会让 flag 设的值失效、Go 与 Node 各听各的。
func webChildEnv(settings webSettings) []string {
	env := appendEnvOverride(os.Environ(), "GOWORKER_CONFIG_DIR", appconfig.DefaultDir())
	env = appendEnvOverride(env, "GOWORKER_WEB_PORT", settings.Port)
	env = appendEnvOverride(env, "GOWORKER_WEB_HOST", settings.Host)
	if settings.Workspace != "" {
		env = appendEnvOverride(env, "GOWORKER_WEB_WORKSPACE", settings.Workspace)
	}
	return env
}

// appendEnvOverride 追加一个「强制覆盖」的环境变量：先清掉宿主 env 里的同名项，
// 再追加我们自己的值，避免向子进程传重复 key（node 靠最后一个覆盖才不炸，属于
// 依实现而定的巧合，不该依赖）。
func appendEnvOverride(env []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		if !strings.HasPrefix(kv, prefix) {
			out = append(out, kv)
		}
	}
	return append(out, prefix+value)
}

func stopWebProcessGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
}

func exitCodeFromWait(err error) {
	if err == nil {
		return
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code := exitErr.ExitCode()
		if code < 0 { // 被信号杀掉
			fmt.Fprintf(os.Stderr, "gateway terminated by signal %d\n", -code)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "gateway exited with code %d\n", code)
		os.Exit(code)
	}
	fmt.Fprintf(os.Stderr, "failed to wait for gateway: %v\n", err)
	os.Exit(1)
}

func waitWebReady(baseURL string, timeout time.Duration) bool {
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/api/health")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func tcpInUse(host, port string) (bool, error) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 300*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return true, nil
	}
	// 只有「连接被拒绝」才判定空闲；其余错误（非法地址/超时/权限）要么探测本身
	// 出了问题，要么意味着真有服务在——原样上抛，别把非法端口当成「端口空闲」放行。
	if errors.Is(err, syscall.ECONNREFUSED) {
		return false, nil
	}
	return false, err
}

// validatePort 校验合并后的端口值，非法直接报错（别让 node 用 ERR_SOCKET_BAD_PORT
// 崩在 launchd 日志里）。
func validatePort(port string) error {
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid port %q (want 1..65535)", port)
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// expandTilde 把路径开头的 ~ 展开为用户主目录（entry/workspace 常这么写）。
func expandTilde(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(path, "~"))
		}
	}
	return path
}
