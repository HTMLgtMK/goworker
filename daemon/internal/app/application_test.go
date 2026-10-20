package app

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
)

// TestNew_InvalidConfig 校验装配失败路径：返回 error（不 panic、不 os.Exit），
// 且失败分支不产出半成品 Application。
func TestNew_InvalidConfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GOWORKER_CONFIG_DIR", dir)
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("llm: [broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	appl, err := New()
	if err == nil {
		appl.Stop() // 别把可能存在的资源留在测试进程里
		t.Fatal("New with invalid config: want error, got nil")
	}
	if appl != nil {
		t.Fatal("New with invalid config: want nil Application")
	}
}

// TestNew_DefaultConfig 校验成功路径：空配置目录走 Default()，
// 装配出完整 Application 且 Stop 幂等退出。
func TestNew_DefaultConfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GOWORKER_CONFIG_DIR", dir)
	appl, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer appl.Stop()
	if appl.Engine == nil {
		t.Fatal("Engine is nil")
	}
	if appl.Config == nil {
		t.Fatal("Config is nil")
	}
	if appl.Runtime == nil {
		t.Fatal("Runtime is nil")
	}
	if got := appl.Engine.HasPlugin("agent"); got != wantAgentPlugin() {
		t.Fatalf("agent plugin registered = %v, want %v", got, wantAgentPlugin())
	}
	source, sourceErr := appl.SessionSource()
	if wantAgentPlugin() {
		if sourceErr != nil || source == nil {
			t.Fatalf("SessionSource() = (%#v, %v), want capability", source, sourceErr)
		}
	} else if sourceErr == nil || source != nil {
		t.Fatalf("SessionSource() = (%#v, %v), want unavailable", source, sourceErr)
	}
	if appl.ACP == nil {
		t.Fatal("ACP service is nil")
	}
	socket := filepath.Join(dir, "frontend", "acp.sock")
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatalf("stat acp socket: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("acp socket mode = %04o, want 0600", info.Mode().Perm())
	}
}

func TestNew_WithACPConnectionUsesConnectionTransport(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GOWORKER_CONFIG_DIR", dir)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	appl, err := New(WithACPConnection(serverConn))
	if err != nil {
		serverConn.Close()
		t.Fatalf("New with ACP connection: %v", err)
	}
	defer appl.Stop()

	if appl.ACP == nil {
		t.Fatal("ACP service is nil")
	}
	socket := filepath.Join(dir, "frontend", "acp.sock")
	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("default ACP socket stat error = %v, want not exist", err)
	}
}
