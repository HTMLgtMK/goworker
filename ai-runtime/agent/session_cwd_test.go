package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-sandbox"
)

// TestSessionRunRevalidatesExistingCWDAgainstRunSnapshot ensures a CWD accepted under
// an earlier policy cannot be reused by a run whose immutable snapshot has a tighter boundary.
func TestSessionRunRevalidatesExistingCWDAgainstRunSnapshot(t *testing.T) {
	oldCWD := t.TempDir()
	currentBoundary := t.TempDir()
	current := &runtimeconfig.Config{Sandbox: sandbox.SandboxConfig{AllowedWorkDir: currentBoundary}}
	var collected, providers int
	sentinel := errors.New("cwd outside current sandbox")
	s := NewSession(SessionDeps{
		Config:         &runtimeconfig.Config{},
		ConfigSnapshot: func() *runtimeconfig.Config { return current },
		CWD:            oldCWD,
		ValidateCWD: func(cwd string, cfg *runtimeconfig.Config) error {
			if cfg.Sandbox.AllowedWorkDir != currentBoundary {
				t.Errorf("validator boundary = %q, want snapshot boundary %q", cfg.Sandbox.AllowedWorkDir, currentBoundary)
			}
			if cwd != oldCWD {
				t.Errorf("validator cwd = %q, want existing cwd %q", cwd, oldCWD)
			}
			return sentinel
		},
		CollectTools: func(*sandbox.Config, ToolCollectionOptions) []core.Tool {
			collected++
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			providers++
			return nil, nil
		},
	})

	var output strings.Builder
	result, err := s.Run(context.Background(), RunRequest{Input: "run"}, RunCallbacks{
		Write: func(text string) { output.WriteString(text) },
	})
	if err != nil {
		t.Fatalf("Run() error = %v, want nil; validation failure belongs in RunResult", err)
	}
	if result.State != RunFailed || !errors.Is(result.Err, sentinel) || result.Persisted {
		t.Fatalf("Run() = %+v, want failed, unpersisted result with validation error", result)
	}
	if !strings.Contains(output.String(), sentinel.Error()) {
		t.Errorf("Run output = %q, want validation error", output.String())
	}
	if collected != 0 || providers != 0 {
		t.Fatalf("tools/providers invoked = %d/%d, want neither before CWD validation", collected, providers)
	}
}

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
