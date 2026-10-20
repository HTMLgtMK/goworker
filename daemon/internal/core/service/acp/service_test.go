package acp

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
)

type recordingHandler struct {
	prompted chan struct{}
	once     sync.Once
}

func (h *recordingHandler) Prompt(context.Context, string, []protocol.ContentBlock, aiacp.Reporter) (string, error) {
	h.once.Do(func() { close(h.prompted) })
	return protocol.StopEndTurn, nil
}

type blockingHandler struct {
	started    chan struct{}
	cancelled  chan struct{}
	startOnce  sync.Once
	cancelOnce sync.Once
}

func (h *blockingHandler) Prompt(ctx context.Context, _ string, _ []protocol.ContentBlock, _ aiacp.Reporter) (string, error) {
	h.startOnce.Do(func() { close(h.started) })
	<-ctx.Done()
	h.cancelOnce.Do(func() { close(h.cancelled) })
	return "", ctx.Err()
}

func TestServiceStartStopIsIdempotent(t *testing.T) {
	dir := shortTempDir(t)
	sockPath := filepath.Join(dir, "nested", "acp.sock")
	service := NewService(&recordingHandler{prompted: make(chan struct{})})

	if err := service.Start(sockPath); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := service.Start(sockPath); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if err := service.Start(filepath.Join(dir, "other.sock")); err == nil {
		t.Fatal("Start with a different path succeeded")
	}

	assertMode(t, filepath.Dir(sockPath), 0o700)
	assertMode(t, sockPath, 0o600)
	if err := service.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := service.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	select {
	case <-service.Done():
	default:
		t.Fatal("Done is not closed after Stop")
	}
	if _, err := os.Stat(sockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket after Stop: %v, want not exist", err)
	}
}

func TestServiceAcceptsUnixACPClient(t *testing.T) {
	sockPath := filepath.Join(shortTempDir(t), "acp.sock")
	handler := &recordingHandler{prompted: make(chan struct{})}
	service := NewService(handler)
	if err := service.Start(sockPath); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = service.Stop() }()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	client := aiacp.NewClient("test", conn, nil)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sessionID, err := client.NewSession(ctx, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := client.Prompt(ctx, sessionID, []protocol.ContentBlock{{Type: "text", Text: "hello"}}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	select {
	case <-handler.prompted:
	case <-time.After(time.Second):
		t.Fatal("session handler was not called")
	}
}

func TestPeerDoneAfterTransportClose(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	handler := &recordingHandler{prompted: make(chan struct{})}
	peer, err := NewPeer(serverConn, handler)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	peer.Start()
	client := aiacp.NewClient("test", clientConn, nil)
	if err := client.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}

	select {
	case <-peer.Done():
	case <-time.After(time.Second):
		t.Fatal("peer Done did not close after transport close")
	}
}

func TestServiceStopCancelsActiveHandlerAndReleasesSocket(t *testing.T) {
	sockPath := filepath.Join(shortTempDir(t), "acp.sock")
	handler := &blockingHandler{
		started:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
	service := NewService(handler)
	if err := service.Start(sockPath); err != nil {
		t.Fatalf("Start: %v", err)
	}

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	client := aiacp.NewClient("test", conn, nil)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sessionID, err := client.NewSession(ctx, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	promptDone := make(chan error, 1)
	go func() {
		_, err := client.Prompt(context.Background(), sessionID, []protocol.ContentBlock{{Type: "text", Text: "wait"}})
		promptDone <- err
	}()
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	if err := service.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-handler.cancelled:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel active handler")
	}
	select {
	case err := <-promptDone:
		if err == nil {
			t.Fatal("Prompt succeeded after service Stop")
		}
	case <-time.After(time.Second):
		t.Fatal("Prompt did not finish after service Stop")
	}
	if _, err := os.Stat(sockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket after Stop: %v, want not exist", err)
	}
}

func TestNewUnixListenerRefusesNonSocketAndRemovesStaleSocket(t *testing.T) {
	dir := shortTempDir(t)
	nonSocket := filepath.Join(dir, "not-a-socket")
	if err := os.WriteFile(nonSocket, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewUnixListener(nonSocket); err == nil {
		t.Fatal("NewUnixListener accepted a non-socket path")
	}
	if _, err := os.Stat(nonSocket); err != nil {
		t.Fatalf("non-socket path was removed: %v", err)
	}

	stalePath := filepath.Join(dir, "stale.sock")
	listener, err := net.Listen("unix", stalePath)
	if err != nil {
		t.Fatalf("create stale socket: %v", err)
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		t.Fatal("net.Listen returned a non-Unix listener")
	}
	unixListener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatalf("close stale listener: %v", err)
	}
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("stale socket disappeared before probe: %v", err)
	}

	fresh, err := NewUnixListener(stalePath)
	if err != nil {
		t.Fatalf("NewUnixListener stale path: %v", err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatalf("close fresh listener: %v", err)
	}
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "acp-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode %s = %04o, want %04o", path, got, want)
	}
}
