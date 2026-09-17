package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tinguo/goworker/ai-sandbox"
)

// TestSessionCWDLifecycle 验证会话工作目录的构造注入与运行时更新（SetCWD 锁内
// 写入，cwd() 快照读）。
func TestSessionCWDLifecycle(t *testing.T) {
	s := NewSession(SessionDeps{CWD: "/workspace/a"})
	if got := s.cwd(); got != "/workspace/a" {
		t.Errorf("initial cwd = %q, want /workspace/a（SessionDeps.CWD 注入）", got)
	}
	s.SetCWD("/workspace/b")
	if got := s.cwd(); got != "/workspace/b" {
		t.Errorf("cwd after SetCWD = %q, want /workspace/b", got)
	}
	s.SetCWD("") // 未声明 = 回退现行为
	if got := s.cwd(); got != "" {
		t.Errorf("cwd after clear = %q, want empty", got)
	}
}

// TestSessionCWDConcurrentUpdates 验证 SetCWD 与 cwd() 并发读写 -race 安全。
func TestSessionCWDConcurrentUpdates(t *testing.T) {
	s := NewSession(SessionDeps{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			s.SetCWD(filepath.Join("/ws", string(rune('a'+i))))
		}(i)
		go func() {
			defer wg.Done()
			_ = s.cwd()
		}()
	}
	wg.Wait()
}

// TestDefaultToolsBashRunsInConfiguredWorkDir 验证 bash 工具 cmd.Dir 取自
// cfg.AllowedWorkDir —— Session.Run 把会话 cwd 注入该字段，即「会话 cwd 优先，
// 为空回退现行为」的回退分支保持原语义。
func TestDefaultToolsBashRunsInConfiguredWorkDir(t *testing.T) {
	dir := t.TempDir()
	tools := DefaultTools(&sandbox.Config{AllowedWorkDir: dir})
	var bash func(context.Context, map[string]any) (string, error)
	for _, tl := range tools {
		if tl.Name == "bash" {
			bash = tl.Execute
		}
	}
	if bash == nil {
		t.Fatal("bash tool not found in DefaultTools")
	}
	out, err := bash(context.Background(), map[string]any{"command": "pwd -P"})
	if err != nil {
		t.Fatalf("bash pwd: %v", err)
	}
	// macOS 临时目录经符号链接（/var/... → /private/var/...），pwd -P 打印物理路径
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("evalsymlinks: %v", err)
	}
	if got := strings.TrimSpace(out); got != resolved {
		t.Errorf("bash pwd = %q, want %q", got, resolved)
	}
}

// TestResolveToolPath 验证 read/write 相对路径锚点：相对路径拼到工具执行目录，
// 绝对路径与无配置（nil/空）场景保持原样（进程 cwd 语义）。
func TestResolveToolPath(t *testing.T) {
	cfg := &sandbox.Config{AllowedWorkDir: "/workspace"}
	if got := resolveToolPath(cfg, "a/b.go"); got != filepath.Join("/workspace", "a/b.go") {
		t.Errorf("resolveToolPath(relative) = %q, want anchored under /workspace", got)
	}
	if got := resolveToolPath(cfg, "/abs/a.go"); got != "/abs/a.go" {
		t.Errorf("resolveToolPath(abs) = %q, want unchanged", got)
	}
	if got := resolveToolPath(nil, "a.go"); got != "a.go" {
		t.Errorf("resolveToolPath(nil cfg) = %q, want unchanged", got)
	}
	if got := resolveToolPath(&sandbox.Config{}, "a.go"); got != "a.go" {
		t.Errorf("resolveToolPath(empty workdir) = %q, want unchanged", got)
	}
}

// TestDefaultToolsReadAnchorsRelativePath 验证 read_file 的相对路径在配置了
// 工具执行目录时锚定到该目录（会话 cwd 注入后即会话工作目录）。
func TestDefaultToolsReadAnchorsRelativePath(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("anchored"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	tools := DefaultTools(&sandbox.Config{AllowedWorkDir: dir})
	var read func(context.Context, map[string]any) (string, error)
	for _, tl := range tools {
		if tl.Name == "read_file" {
			read = tl.Execute
		}
	}
	out, err := read(context.Background(), map[string]any{"path": "notes.txt"})
	if err != nil {
		t.Fatalf("read_file relative: %v", err)
	}
	if !strings.Contains(out, "anchored") {
		t.Errorf("read_file output = %q, want anchored content", out)
	}
}
