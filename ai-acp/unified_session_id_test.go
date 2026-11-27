package aiacp

import (
	"context"
	"net"
	"testing"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

func TestSessionNewUsesManagedSessionID(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	handler := &lifecycleHandler{}
	server := ServeConn(serverEnd, handler)
	t.Cleanup(func() { _ = server.Close() })

	client, err := NewClient("test-agent", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	response, err := client.NewSessionWithResponse(ctx, protocol.NewSessionRequest{Cwd: "/repo"})
	if err != nil {
		t.Fatalf("NewSessionWithResponse: %v", err)
	}
	if response.SessionID != "managed-1" {
		t.Fatalf("session ID = %q, want application-managed session ID %q", response.SessionID, "managed-1")
	}
	created, _, _, _ := handler.sessionValues()
	if response.SessionID != created {
		t.Fatalf("session ID = %q, creator returned %q", response.SessionID, created)
	}
}
