package aiacp

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

// TestServerStoresClientCapabilities 验证 initialize 协商的 clientCapabilities
// 在 server 侧可读：未 initialize 时 Has 一律 false，initialize 后按声明可见。
func TestServerStoresClientCapabilities(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, &lifecycleHandler{})
	t.Cleanup(func() { _ = server.Close() })
	client := NewClient("test-agent", clientEnd, nil)
	t.Cleanup(func() { _ = client.Close() })

	if server.ClientCapabilities().Has("x-device") {
		t.Fatal("capabilities must be empty before initialize")
	}

	var caps protocol.ClientCapabilities
	if err := json.Unmarshal([]byte(`{"x-device": {"tools": true}}`), &caps); err != nil {
		t.Fatalf("decode clientCapabilities: %v", err)
	}
	if _, err := client.InitializeWithRequest(context.Background(), protocol.InitializeRequest{
		ProtocolVersion:    protocol.Version,
		ClientCapabilities: caps,
	}); err != nil {
		t.Fatalf("InitializeWithRequest: %v", err)
	}
	if !server.ClientCapabilities().Has("x-device") {
		t.Fatal("server did not store clientCapabilities.x-device from initialize")
	}
}
