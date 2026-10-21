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

// Listener owns one ACP transport. Serve invokes onAccept for each connection,
// Close stops the transport, and Done signals that its transport lifecycle has
// completed.
type Listener interface {
	Serve(func(conn io.ReadWriteCloser)) error
	Close() error
	Done() <-chan struct{}
}

// ListenerFactory constructs an ACP transport listener.
type ListenerFactory func() (Listener, error)

// AbsListener contains the shared lifecycle state for all ACP transports.
// Concrete listeners compose it and provide transport-specific behavior through
// the injected functions.
type AbsListener struct {
	acceptFn  func() (io.ReadWriteCloser, error)
	closeFn   func() error
	cleanupFn func() error

	mu        sync.Mutex
	closed    bool
	serveOnce bool
	done      chan struct{}
	doneOnce  sync.Once
}

// UnixListener owns the Unix socket transport and composes AbsListener for its
// lifecycle behavior.
type UnixListener struct {
	*AbsListener
}

// ConnectionListener adapts one already-established connection and composes
// AbsListener for its lifecycle behavior.
type ConnectionListener struct {
	*AbsListener
}

func newAbsListener(
	acceptFn func() (io.ReadWriteCloser, error),
	closeFn func() error,
	cleanupFn func() error,
) (*AbsListener, error) {
	if acceptFn == nil {
		return nil, errors.New("acp: accept function is required")
	}
	if closeFn == nil {
		return nil, errors.New("acp: close function is required")
	}
	return &AbsListener{
		acceptFn:  acceptFn,
		closeFn:   closeFn,
		cleanupFn: cleanupFn,
		done:      make(chan struct{}),
	}, nil
}

// NewUnixListener creates a private Unix-domain listener at sockPath.
//
// A stale socket is removed after checking that no server is accepting it.
// Non-socket paths are never removed. The socket itself is chmoded to 0600;
// a newly-created parent directory is 0700.
func NewUnixListener(sockPath string) (*UnixListener, error) {
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
	unixSocket, ok := raw.(*net.UnixListener)
	if !ok {
		_ = raw.Close()
		return nil, fmt.Errorf("acp: unexpected Unix listener type %T", raw)
	}
	// Let the shared listener own cleanup. The standard library otherwise unlinks
	// the path during Close before ownership can be verified.
	unixSocket.SetUnlinkOnClose(false)

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

	base, err := newAbsListener(
		func() (io.ReadWriteCloser, error) {
			conn, err := raw.Accept()
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
		raw.Close,
		func() error { return removeSocket(sockPath, ownedPath) },
	)
	if err != nil {
		_ = raw.Close()
		_ = removeSocket(sockPath, ownedPath)
		return nil, err
	}
	return &UnixListener{AbsListener: base}, nil
}

// NewConnectionListener creates a listener for one already-established,
// full-duplex connection. Serve invokes its callback exactly once and waits for
// that callback to finish.
func NewConnectionListener(conn io.ReadWriteCloser) (*ConnectionListener, error) {
	if conn == nil {
		return nil, errors.New("acp: connection is required")
	}

	accepted := false
	base, err := newAbsListener(
		func() (io.ReadWriteCloser, error) {
			if accepted {
				return nil, io.EOF
			}
			accepted = true
			return conn, nil
		},
		conn.Close,
		nil,
	)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &ConnectionListener{AbsListener: base}, nil
}

// Serve accepts connections until Close is called. The callback is run in a
// goroutine for each connection, and Serve waits for those callbacks before it
// returns so its caller has a complete lifecycle barrier.
func (l *AbsListener) Serve(onAccept func(conn io.ReadWriteCloser)) error {
	if l == nil {
		return errors.New("acp: nil listener")
	}
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
	l.mu.Unlock()
	defer l.doneOnce.Do(func() { close(l.done) })

	if closed {
		return nil
	}

	var callbacks sync.WaitGroup
	for {
		conn, err := l.acceptFn()
		if err != nil {
			callbacks.Wait()
			if l.isClosed() || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("acp: accept connection: %w", err)
		}
		if conn == nil {
			callbacks.Wait()
			return errors.New("acp: accept function returned nil connection")
		}

		callbacks.Add(1)
		go func() {
			defer callbacks.Done()
			onAccept(conn)
		}()
	}
}

// Close stops the transport and runs its cleanup function. It is idempotent.
func (l *AbsListener) Close() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	closeFn := l.closeFn
	cleanupFn := l.cleanupFn
	serveStarted := l.serveOnce
	l.mu.Unlock()

	closeErr := closeFn()
	var cleanupErr error
	if cleanupFn != nil {
		cleanupErr = cleanupFn()
	}
	if !serveStarted {
		l.doneOnce.Do(func() { close(l.done) })
	}
	return errors.Join(closeErr, cleanupErr)
}

// Done closes when Serve has returned and all callbacks have completed. If the
// listener is closed before Serve starts, it closes because there is no Serve to
// wait for.
func (l *AbsListener) Done() <-chan struct{} {
	if l == nil {
		return nil
	}
	return l.done
}

func (l *AbsListener) isClosed() bool {
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
