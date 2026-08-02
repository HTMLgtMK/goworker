package mcp

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// buildFakeServer 编译 testdata 里的假 MCP server，返回可执行路径。
func buildFakeServer(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "fakeserver")
	cmd := exec.Command("go", "build", "-o", out, "./testdata/fakeserver")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build fakeserver: %v", err)
	}
	return out
}

func TestStdioClient_FullLifecycle(t *testing.T) {
	serverPath := buildFakeServer(t)
	ctx := context.Background()

	c, err := NewStdioClient(serverPath, nil)
	if err != nil {
		t.Fatalf("NewStdioClient: %v", err)
	}
	defer c.Close()

	// initialize 握手：协商版本 + 拿 server 身份
	initRes, err := c.Initialize(ctx, &InitializeRequest{
		ProtocolVersion: LatestProtocolVersion,
		ClientInfo:      Implementation{Name: "goworker-test", Version: "0.0.1"},
	})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if initRes.ProtocolVersion != LatestProtocolVersion {
		t.Errorf("protocolVersion = %q, want %q", initRes.ProtocolVersion, LatestProtocolVersion)
	}
	if initRes.ServerInfo.Name != "fakeserver" {
		t.Errorf("serverInfo = %+v", initRes.ServerInfo)
	}

	// 列工具
	toolsRes, err := c.ListTools(ctx, &ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(toolsRes.Tools) != 1 || toolsRes.Tools[0].Name != "echo" {
		t.Fatalf("tools = %+v", toolsRes.Tools)
	}

	// 调工具
	callRes, err := c.CallTool(ctx, &CallToolRequest{Name: "echo", Arguments: map[string]any{"text": "hello"}})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if len(callRes.Content) != 1 || !strings.Contains(callRes.Content[0].Text, "hello") {
		t.Errorf("content = %+v", callRes.Content)
	}
}

func TestStdioClient_ProcessExit(t *testing.T) {
	// server 进程秒退（bash -c "exit 1"）：调用必须失败而非挂死
	c, err := NewStdioClient("bash", []string{"-c", "exit 1"})
	if err != nil {
		t.Fatalf("NewStdioClient: %v", err)
	}
	defer c.Close()

	_, err = c.ListTools(context.Background(), &ListToolsRequest{})
	if err == nil {
		t.Fatal("want error when server process already exited")
	}
}
