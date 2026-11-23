package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// makeFakeEntry 造一个真实存在的入口文件（resolveWebSettings 会 stat 校验）。
func makeFakeEntry(t *testing.T, parts ...string) string {
	t.Helper()
	entry := filepath.Join(parts...)
	if err := os.MkdirAll(filepath.Dir(entry), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry, []byte("// fake gateway"), 0o644); err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestResolveWebSettingsPrecedence(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("GOWORKER_CONFIG_DIR", filepath.Join(home, "cfg"))

	flagEntry := makeFakeEntry(t, home, "flag", "gateway.js")
	configEntry := makeFakeEntry(t, home, "from-config", "gateway.js")

	// 配置档兜底（写一份最小 config.yaml）
	configDir := filepath.Join(home, "cfg")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfgYAML := "frontend:\n  web:\n    entry: " + configEntry + "\n    port: 6001\n    host: 127.0.0.1\n    workspace: /from/config\n"
	if err := os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte(cfgYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	// flag 最高优先
	s, err := resolveWebSettings(webOverrides{Entry: flagEntry, Port: "6002", Host: "0.0.0.0", Workspace: "/flag/ws"})
	if err != nil {
		t.Fatalf("resolve with flags: %v", err)
	}
	if s.Entry != flagEntry || s.Port != "6002" || s.Host != "0.0.0.0" || s.Workspace != "/flag/ws" {
		t.Errorf("flag 优先级失效: %+v", s)
	}

	// env 次之（entry 用 config 档，port/host/workspace 用 env 档）
	t.Setenv("GOWORKER_WEB_PORT", "6003")
	t.Setenv("GOWORKER_WEB_HOST", "127.0.0.2")
	t.Setenv("GOWORKER_WEB_WORKSPACE", "/env/ws")
	s, err = resolveWebSettings(webOverrides{})
	if err != nil {
		t.Fatalf("resolve with env: %v", err)
	}
	if s.Entry != configEntry || s.Port != "6003" || s.Host != "127.0.0.2" || s.Workspace != "/env/ws" {
		t.Errorf("env 优先级失效: %+v", s)
	}

	// env 清空后回落 config
	os.Unsetenv("GOWORKER_WEB_PORT")
	os.Unsetenv("GOWORKER_WEB_HOST")
	os.Unsetenv("GOWORKER_WEB_WORKSPACE")
	s, err = resolveWebSettings(webOverrides{})
	if err != nil {
		t.Fatalf("resolve with config: %v", err)
	}
	if s.Port != "6001" || s.Host != "127.0.0.1" || s.Workspace != "/from/config" {
		t.Errorf("config 兜底失效: %+v", s)
	}
	if s.EntrySource != "config" {
		t.Errorf("EntrySource = %q, want config", s.EntrySource)
	}
}

func TestResolveWebSettingsAutoDetect(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("GOWORKER_CONFIG_DIR", filepath.Join(home, "cfg"))
	// GOWORKER_CONFIG_DIR 下没有 config.yaml → Load 走 Default()，entry 自动探测

	entry := makeFakeEntry(t, home, "Work", "goworker-vscode-extension", "apps", "web-host", "dist", "gateway.js")

	s, err := resolveWebSettings(webOverrides{})
	if err != nil {
		t.Fatalf("auto detect: %v", err)
	}
	if s.Entry != entry {
		t.Errorf("Entry = %q, want %q", s.Entry, entry)
	}
	if s.EntrySource != "auto-detected" {
		t.Errorf("EntrySource = %q, want auto-detected", s.EntrySource)
	}
	// workspace 从 apps/web-host 布局派生仓库根
	if want := filepath.Join(home, "Work", "goworker-vscode-extension"); s.Workspace != want {
		t.Errorf("Workspace = %q, want %q", s.Workspace, want)
	}
	// 默认端口/主机
	if s.Port != "5177" || s.Host != "127.0.0.1" {
		t.Errorf("默认端口/主机失效: %+v", s)
	}
}

func TestResolveWebSettingsMissingEntry(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("GOWORKER_CONFIG_DIR", filepath.Join(t.TempDir(), "cfg"))

	if _, err := resolveWebSettings(webOverrides{Entry: "/no/such/gateway.js"}); err == nil {
		t.Error("入口不存在时应报错")
	}
}

// TestWebChildEnvOverridesHostEnv 回归：settings 是 flag > env 解析后的权威值，
// 宿主 env 里残留的 GOWORKER_WEB_* 必须被覆盖——否则 `--port 6002` 在父进程生效、
// 子进程仍旧听 GOWORKER_WEB_PORT 的值，两边各说各话。
func TestWebChildEnvOverridesHostEnv(t *testing.T) {
	t.Setenv("GOWORKER_WEB_PORT", "9999")
	t.Setenv("GOWORKER_WEB_HOST", "10.0.0.1")
	t.Setenv("GOWORKER_WEB_WORKSPACE", "/stale/ws")

	env := webChildEnv(webSettings{Port: "6002", Host: "0.0.0.0", Workspace: "/flag/ws"})

	got := map[string][]string{}
	for _, kv := range env {
		if i := strings.IndexByte(kv, '='); i > 0 {
			got[kv[:i]] = append(got[kv[:i]], kv[i+1:])
		}
	}
	for key, want := range map[string]string{
		"GOWORKER_WEB_PORT":      "6002",
		"GOWORKER_WEB_HOST":      "0.0.0.0",
		"GOWORKER_WEB_WORKSPACE": "/flag/ws",
	} {
		if vals := got[key]; len(vals) != 1 || vals[0] != want {
			t.Errorf("%s = %v, want exactly [%q] (宿主残留必须被覆盖且不重复)", key, vals, want)
		}
	}
}

func TestRenderPlist(t *testing.T) {
	plist := renderPlist("com.goworker.daemon",
		[]string{"/usr/local/bin/goworker", "--daemon"},
		"/Users/x/Library/Logs/goworker/daemon.out.log",
		"/Users/x/Library/Logs/goworker/daemon.err.log",
		map[string]string{"PATH": "/usr/bin:/bin", "GOWORKER_CONFIG_DIR": "/tmp&x"},
	)

	for _, want := range []string{
		"<string>com.goworker.daemon</string>",
		"<string>/usr/local/bin/goworker</string>",
		"<string>--daemon</string>",
		"<key>RunAtLoad</key>\n  <true/>",
		"<key>KeepAlive</key>\n  <true/>",
		"<key>GOWORKER_CONFIG_DIR</key>\n    <string>/tmp&amp;x</string>", // XML 转义
		"<key>PATH</key>",
	} {
		if !strings.Contains(plist, want) {
			t.Errorf("plist 缺少 %q\n---\n%s", want, plist)
		}
	}
	if !strings.HasPrefix(plist, "<?xml") {
		t.Error("plist 缺少 XML 声明")
	}
}
