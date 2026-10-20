package acp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
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

type fakeListener struct {
	closed    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	doneOnce  sync.Once
}

func newFakeListener() *fakeListener {
	return &fakeListener{
		closed: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (l *fakeListener) Serve(_ func(io.ReadWriteCloser)) error {
	<-l.closed
	l.doneOnce.Do(func() { close(l.done) })
	return nil
}

func (l *fakeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *fakeListener) Done() <-chan struct{} { return l.done }

func unixListenerFactory(sockPath string) ListenerFactory {
	return func() (Listener, error) {
		return NewUnixListener(sockPath)
	}
}

func TestServiceUsesListenerFactory(t *testing.T) {
	listener := newFakeListener()
	calls := 0
	service := NewService(func() aiacp.SessionHandler {
		return &recordingHandler{prompted: make(chan struct{})}
	}, func() (Listener, error) {
		calls++
		return listener, nil
	})

	if err := service.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if calls != 1 {
		t.Fatalf("listener factory called %d times, want 1", calls)
	}
	if err := service.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if calls != 1 {
		t.Fatalf("listener factory called %d times after second Start, want 1", calls)
	}
	if err := service.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-listener.Done():
	default:
		t.Fatal("listener Done is not closed after service Stop")
	}
}

func TestServiceListenerFactoryErrorLeavesServiceStopped(t *testing.T) {
	wantErr := errors.New("listener unavailable")
	service := NewService(func() aiacp.SessionHandler {
		return &recordingHandler{prompted: make(chan struct{})}
	}, func() (Listener, error) {
		return nil, wantErr
	})

	err := service.Start()
	if !errors.Is(err, wantErr) {
		t.Fatalf("Start error = %v, want wrapped %v", err, wantErr)
	}
	if !strings.Contains(err.Error(), "create listener") {
		t.Fatalf("Start error = %q, want listener context", err)
	}
	if service.started {
		t.Fatal("service marked started after listener factory failure")
	}
	select {
	case <-service.Done():
		t.Fatal("Done closed after failed Start")
	default:
	}
	if err := service.Stop(); err != nil {
		t.Fatalf("Stop after failed Start: %v", err)
	}
}

func TestServiceStartStopIsIdempotent(t *testing.T) {
	dir := shortTempDir(t)
	sockPath := filepath.Join(dir, "nested", "acp.sock")
	service := NewService(func() aiacp.SessionHandler {
		return &recordingHandler{prompted: make(chan struct{})}
	}, unixListenerFactory(sockPath))

	if err := service.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := service.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
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
	handlers := newRecordingHandlerFactory(t)
	service := NewService(handlers.factory, unixListenerFactory(sockPath))
	if err := service.Start(); err != nil {
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
	case <-handlers.only().prompted:
	case <-time.After(time.Second):
		t.Fatal("session handler was not called")
	}
}

// TestServiceHandlerFactoryIsPerConnection 验证每条连接都调用 factory 并拿到
// 独立 handler 实例：多 frontend 同时连一个 socket 时 session 状态不串线。
func TestServiceHandlerFactoryIsPerConnection(t *testing.T) {
	sockPath := filepath.Join(shortTempDir(t), "acp.sock")
	handlers := newRecordingHandlerFactory(t)
	service := NewService(handlers.factory, unixListenerFactory(sockPath))
	if err := service.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = service.Stop() }()

	const clients = 3
	for i := 0; i < clients; i++ {
		go func() {
			conn, err := net.Dial("unix", sockPath)
			if err != nil {
				t.Errorf("Dial: %v", err)
				return
			}
			defer conn.Close()
			client := aiacp.NewClient("test", conn, nil)
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := client.Initialize(ctx); err != nil {
				t.Errorf("Initialize: %v", err)
				return
			}
			sessionID, err := client.NewSession(ctx, "")
			if err != nil {
				t.Errorf("NewSession: %v", err)
				return
			}
			if _, err := client.Prompt(ctx, sessionID, []protocol.ContentBlock{{Type: "text", Text: "hi"}}); err != nil {
				t.Errorf("Prompt: %v", err)
				return
			}
		}()
	}

	// factory 每条连接调用一次；每条连接的 handler 都应完成一次 prompt。
	seen := make(map[*recordingHandler]struct{}, clients)
	deadline := time.After(3 * time.Second)
	for len(seen) < clients {
		select {
		case h := <-handlers.produced:
			seen[h] = struct{}{}
		case <-deadline:
			t.Fatalf("only %d/%d connections produced a handler", len(seen), clients)
		}
	}
	if got := handlers.count(); got != clients {
		t.Fatalf("factory called %d times, want %d", got, clients)
	}
	for h := range seen {
		select {
		case <-h.prompted:
		case <-time.After(time.Second):
			t.Fatal("one connection's handler was never prompted")
		}
	}
}

// recordingHandlers 记录 factory 创建的每个 handler，供测试断言隔离语义。
type recordingHandlers struct {
	mu       sync.Mutex
	created  []*recordingHandler
	factory  HandlerFactory
	produced chan *recordingHandler
}

func newRecordingHandlerFactory(t *testing.T) *recordingHandlers {
	t.Helper()
	rh := &recordingHandlers{produced: make(chan *recordingHandler, 16)}
	rh.factory = func() aiacp.SessionHandler {
		h := &recordingHandler{prompted: make(chan struct{})}
		rh.mu.Lock()
		rh.created = append(rh.created, h)
		rh.mu.Unlock()
		rh.produced <- h
		return h
	}
	return rh
}

func (r *recordingHandlers) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.created)
}

// only 断言 factory 恰好创建过一个 handler 并返回它。
func (r *recordingHandlers) only() *recordingHandler {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.created) != 1 {
		panic(fmt.Sprintf("factory created %d handlers, want 1", len(r.created)))
	}
	return r.created[0]
}

func TestServiceClientDisconnectReleasesPeer(t *testing.T) {
	sockPath := filepath.Join(shortTempDir(t), "acp.sock")
	service := NewService(func() aiacp.SessionHandler {
		return &recordingHandler{prompted: make(chan struct{})}
	}, unixListenerFactory(sockPath))
	if err := service.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	client := aiacp.NewClient("test", conn, nil)
	if err := client.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- service.Stop() }()
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after client disconnect")
	}
	select {
	case <-service.Done():
	default:
		t.Fatal("Done is not closed after Stop")
	}
}

func TestServiceStopCancelsActiveHandlerAndReleasesSocket(t *testing.T) {
	sockPath := filepath.Join(shortTempDir(t), "acp.sock")
	handler := &blockingHandler{
		started:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
	service := NewService(func() aiacp.SessionHandler {
		return handler
	}, unixListenerFactory(sockPath))
	if err := service.Start(); err != nil {
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
