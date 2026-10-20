package acp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const staleSocketProbeTimeout = 100 * time.Millisecond

// Listener owns the Unix socket used by an ACP service. It removes only the
// socket it created, so a failed or replaced path cannot be clobbered on close.
type Listener struct {
	mu        sync.Mutex
	listener  net.Listener
	path      string
	ownedPath os.FileInfo
	closed    bool
	serveOnce bool
	done      chan struct{}
	doneOnce  sync.Once
}

// NewUnixListener creates a private Unix-domain listener at sockPath.
//
// A stale socket is removed after checking that no server is accepting it.
// Non-socket paths are never removed. The socket itself is chmoded to 0600;
// a newly-created parent directory is 0700.
func NewUnixListener(sockPath string) (*Listener, error) {
	if sockPath == "" {
		return nil, errors.New("acp: socket path is required")
	}

	if err := ensureSocketDir(filepath.Dir(sockPath)); err != nil {
		return nil, fmt.Errorf("acp: prepare socket directory: %w", err)
	}
	if err := removeStaleSocket(sockPath); err != nil {
		return nil, err
	}

	raw, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("acp: listen on %s: %w", sockPath, err)
	}
	unixListener, ok := raw.(*net.UnixListener)
	if !ok {
		_ = raw.Close()
		return nil, fmt.Errorf("acp: unexpected Unix listener type %T", raw)
	}
	// Let Listener own cleanup. The standard library otherwise unlinks the
	// path during Close before we can verify that it is still ours.
	unixListener.SetUnlinkOnClose(false)

	ownedPath, err := os.Lstat(sockPath)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("acp: inspect socket %s: %w", sockPath, err)
	}
	if err := os.Chmod(sockPath, 0o600); err != nil {
		_ = raw.Close()
		_ = removeSocket(sockPath, ownedPath)
		return nil, fmt.Errorf("acp: secure socket %s: %w", sockPath, err)
	}
	currentPath, err := os.Lstat(sockPath)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("acp: inspect secured socket %s: %w", sockPath, err)
	}
	if !os.SameFile(ownedPath, currentPath) {
		_ = raw.Close()
		return nil, fmt.Errorf("acp: socket path changed while starting")
	}

	return &Listener{
		listener:  raw,
		path:      sockPath,
		ownedPath: ownedPath,
		done:      make(chan struct{}),
	}, nil
}

// Serve accepts connections until Close is called. The callback is run in a
// goroutine for each connection, and Serve waits for those callbacks before it
// returns so its caller has a complete lifecycle barrier.
func (l *Listener) Serve(onAccept func(conn io.ReadWriteCloser)) error {
	if onAccept == nil {
		return errors.New("acp: nil accept callback")
	}

	l.mu.Lock()
	if l.serveOnce {
		l.mu.Unlock()
		return errors.New("acp: listener Serve called more than once")
	}
	l.serveOnce = true
	closed := l.closed
	listener := l.listener
	l.mu.Unlock()
	defer l.doneOnce.Do(func() { close(l.done) })

	if closed {
		return nil
	}

	var callbacks sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			callbacks.Wait()
			if l.isClosed() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("acp: accept connection: %w", err)
		}

		callbacks.Add(1)
		go func() {
			defer callbacks.Done()
			onAccept(conn)
		}()
	}
}

// Close stops accepting and removes this listener's socket. It is idempotent.
func (l *Listener) Close() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	listener := l.listener
	path := l.path
	ownedPath := l.ownedPath
	l.mu.Unlock()

	var closeErr error
	if listener != nil {
		closeErr = listener.Close()
	}
	removeErr := removeSocket(path, ownedPath)
	l.doneOnce.Do(func() { close(l.done) })
	return errors.Join(closeErr, removeErr)
}

// Done is closed when the listener has been closed or Serve has returned.
func (l *Listener) Done() <-chan struct{} {
	if l == nil {
		return nil
	}
	return l.done
}

func (l *Listener) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

func ensureSocketDir(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s is not a directory", dir)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// MkdirAll honors umask, but an unusual umask can still make this less
	// obvious when inspecting the path. Explicitly secure the leaf we made.
	return os.Chmod(dir, 0o700)
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acp: inspect socket path: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("acp: refuse to remove non-socket path %s", path)
	}

	conn, probeErr := net.DialTimeout("unix", path, staleSocketProbeTimeout)
	if probeErr == nil {
		_ = conn.Close()
		return fmt.Errorf("acp: socket already in use: %s", path)
	}
	if !isStaleSocketError(probeErr) {
		return fmt.Errorf("acp: probe existing socket %s: %w", path, probeErr)
	}

	current, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acp: recheck socket path: %w", err)
	}
	if current.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("acp: refuse to remove non-socket path %s", path)
	}
	if !os.SameFile(info, current) {
		return fmt.Errorf("acp: socket path changed while probing: %s", path)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("acp: remove stale socket %s: %w", path, err)
	}
	return nil
}

func isStaleSocketError(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ENOENT)
}

func removeSocket(path string, ownedPath os.FileInfo) error {
	if path == "" {
		return nil
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("acp: inspect socket for removal: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || (ownedPath != nil && !os.SameFile(info, ownedPath)) {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("acp: remove socket: %w", err)
	}
	return nil
}
