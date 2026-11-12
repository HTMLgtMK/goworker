package aiacp

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

// TestInitializeReadyRunsAfterResponseWrite checks that the ready hook issues a reverse call.
// Frame ordering is checked independently by TestInitializeResponseFramePrecedesReverseRequest.
func TestInitializeReadyRunsAfterResponseWrite(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	handler := &initializeReadyHandler{ready: make(chan struct{})}
	server := ServeConn(serverEnd, handler)
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient("test-agent", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	called := make(chan struct{})
	client.Handle("x-device/tools", func(context.Context, json.RawMessage) (any, error) {
		select {
		case <-handler.ready:
		default:
			t.Error("reverse call arrived before the ready hook")
		}
		close(called)
		return map[string]any{"tools": []any{}}, nil
	})

	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("initialize ready hook did not issue reverse call")
	}
}

type initializeReadyHandler struct {
	ready chan struct{}
}

func (*initializeReadyHandler) Prompt(context.Context, string, []protocol.ContentBlock, Reporter) (string, error) {
	return protocol.StopEndTurn, nil
}

func (h *initializeReadyHandler) InitializeReady(ctx context.Context, reporter Reporter) {
	close(h.ready)
	caller, ok := reporter.(interface {
		Call(context.Context, string, any, any) error
	})
	if !ok {
		return
	}
	var response map[string]any
	_ = caller.Call(ctx, "x-device/tools", struct{}{}, &response)
}

func TestServerStoresClientCapabilities(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, &lifecycleHandler{})
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient("test-agent", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
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
