package acp

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type listenerTestConn struct {
	mu         sync.Mutex
	closeCalls int
	closed     chan struct{}
	closeOnce  sync.Once
}

func newListenerTestConn() *listenerTestConn {
	return &listenerTestConn{closed: make(chan struct{})}
}

func (c *listenerTestConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}

func (c *listenerTestConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func (c *listenerTestConn) Close() error {
	c.mu.Lock()
	c.closeCalls++
	c.mu.Unlock()
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *listenerTestConn) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCalls
}

func TestNewConnectionListenerRejectsNilConnection(t *testing.T) {
	listener, err := NewConnectionListener(nil)
	if err == nil {
		t.Fatal("NewConnectionListener(nil) succeeded")
	}
	if listener != nil {
		t.Fatal("NewConnectionListener(nil) returned a listener")
	}
}

func TestConnectionListenerRoundTrip(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	listener, err := NewConnectionListener(serverConn)
	if err != nil {
		t.Fatalf("NewConnectionListener: %v", err)
	}

	callbackStarted := make(chan struct{})
	callbackErr := make(chan error, 1)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- listener.Serve(func(conn io.ReadWriteCloser) {
			close(callbackStarted)
			request := make([]byte, 4)
			if _, err := io.ReadFull(conn, request); err != nil {
				callbackErr <- err
				return
			}
			if string(request) != "ping" {
				callbackErr <- errors.New("unexpected request")
				return
			}
			_, err := conn.Write([]byte("pong"))
			callbackErr <- err
		})
	}()

	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("connection callback did not start")
	}
	if _, err := clientConn.Write([]byte("ping")); err != nil {
		t.Fatalf("client Write: %v", err)
	}
	response := make([]byte, 4)
	if _, err := io.ReadFull(clientConn, response); err != nil {
		t.Fatalf("client Read: %v", err)
	}
	if string(response) != "pong" {
		t.Fatalf("response = %q, want pong", response)
	}
	if err := <-callbackErr; err != nil {
		t.Fatalf("callback: %v", err)
	}
	if err := <-serveResult; err != nil {
		t.Fatalf("Serve: %v", err)
	}
	select {
	case <-listener.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not close after callback returned")
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConnectionListenerCloseBeforeServe(t *testing.T) {
	conn := newListenerTestConn()
	listener, err := NewConnectionListener(conn)
	if err != nil {
		t.Fatalf("NewConnectionListener: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := conn.closeCount(); got != 1 {
		t.Fatalf("connection Close calls = %d, want 1", got)
	}
	select {
	case <-listener.Done():
	default:
		t.Fatal("Done is not closed after Close-before-Serve")
	}

	called := false
	if err := listener.Serve(func(io.ReadWriteCloser) { called = true }); err != nil {
		t.Fatalf("Serve after Close: %v", err)
	}
	if called {
		t.Fatal("callback ran after Close-before-Serve")
	}
}

func TestConnectionListenerCloseWaitsForCallback(t *testing.T) {
	conn := newListenerTestConn()
	listener, err := NewConnectionListener(conn)
	if err != nil {
		t.Fatalf("NewConnectionListener: %v", err)
	}

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- listener.Serve(func(io.ReadWriteCloser) {
			close(callbackStarted)
			<-releaseCallback
		})
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("connection callback did not start")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- listener.Close() }()
	if err := <-closeResult; err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-listener.Done():
		t.Fatal("Done closed before callback returned")
	default:
	}

	close(releaseCallback)
	if err := <-serveResult; err != nil {
		t.Fatalf("Serve: %v", err)
	}
	select {
	case <-listener.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not close after callback returned")
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := conn.closeCount(); got != 1 {
		t.Fatalf("connection Close calls = %d, want 1", got)
	}
}

func TestUnixListenerCloseBeforeServe(t *testing.T) {
	path := filepath.Join(listenerTempDir(t), "nested", "acp.sock")
	listener, err := NewUnixListener(path)
	if err != nil {
		t.Fatalf("NewUnixListener: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	select {
	case <-listener.Done():
	default:
		t.Fatal("Done is not closed after Close-before-Serve")
	}
	if err := listener.Serve(func(io.ReadWriteCloser) {
		t.Fatal("callback ran after Close-before-Serve")
	}); err != nil {
		t.Fatalf("Serve after Close: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket after Close: %v, want not exist", err)
	}
}

func TestUnixListenerCloseWaitsForCallback(t *testing.T) {
	path := filepath.Join(listenerTempDir(t), "acp.sock")
	listener, err := NewUnixListener(path)
	if err != nil {
		t.Fatalf("NewUnixListener: %v", err)
	}

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- listener.Serve(func(io.ReadWriteCloser) {
			close(callbackStarted)
			<-releaseCallback
		})
	}()

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		conn.Close()
		t.Fatal("Unix callback did not start")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- listener.Close() }()
	if err := <-closeResult; err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-listener.Done():
		t.Fatal("Done closed before callback returned")
	default:
	}

	close(releaseCallback)
	if err := <-serveResult; err != nil {
		t.Fatalf("Serve: %v", err)
	}
	select {
	case <-listener.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not close after callback returned")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("connection Close: %v", err)
	}
}

func TestUnixListenerDoesNotRemoveReplacementSocket(t *testing.T) {
	path := filepath.Join(listenerTempDir(t), "acp.sock")
	listener, err := NewUnixListener(path)
	if err != nil {
		t.Fatalf("NewUnixListener: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove original path: %v", err)
	}

	replacement, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("replacement Listen: %v", err)
	}
	replacementUnix, ok := replacement.(*net.UnixListener)
	if !ok {
		replacement.Close()
		t.Fatal("replacement listener is not Unix listener")
	}
	replacementUnix.SetUnlinkOnClose(false)
	defer func() {
		_ = replacement.Close()
		_ = os.Remove(path)
	}()

	if err := listener.Close(); err != nil {
		t.Fatalf("original Close: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("replacement socket after original Close: %v", err)
	}
}

func listenerTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "acp-listener-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
