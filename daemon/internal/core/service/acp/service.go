package acp

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	aiacp "github.com/tinguo/goworker/ai-acp"
)

// Service is a reusable ACP daemon adapter. It owns its Unix listener and all
// connected peers; callers own only the injected protocol-level handler.
type Service struct {
	handler aiacp.SessionHandler

	mu       sync.Mutex
	listener *Listener
	sockPath string
	peers    map[*aiacp.Server]struct{}

	started  bool
	stopping bool
	done     chan struct{}
	doneOnce sync.Once
	acceptWG sync.WaitGroup
}

// NewService constructs a stopped service. The handler must implement only the
// ai-acp SessionHandler contract; daemon task/session routing remains an
// application concern.
func NewService(handler aiacp.SessionHandler) *Service {
	return &Service{
		handler: handler,
		peers:   make(map[*aiacp.Server]struct{}),
		done:    make(chan struct{}),
	}
}

// Start creates the Unix listener and begins accepting ACP peers. A successful
// Start is idempotent for the same socket path. A failed Start leaves the
// service stopped so the caller can correct the path and retry.
func (s *Service) Start(sockPath string) error {
	if s == nil {
		return errors.New("acp: nil service")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		if s.sockPath == sockPath {
			return nil
		}
		return fmt.Errorf("acp: service already started on %s", s.sockPath)
	}
	if s.stopping {
		return errors.New("acp: service has been stopped")
	}
	if s.handler == nil {
		return errors.New("acp: session handler is required")
	}

	listener, err := NewUnixListener(sockPath)
	if err != nil {
		return err
	}
	s.listener = listener
	s.sockPath = sockPath
	s.started = true
	s.acceptWG.Add(1)
	go s.acceptLoop(listener)
	return nil
}

// Start is also available as a convenience for one-shot composition roots.
// The returned service remains the owner of the socket and must be stopped.
func Start(sockPath string, handler aiacp.SessionHandler) (*Service, error) {
	service := NewService(handler)
	if err := service.Start(sockPath); err != nil {
		return nil, err
	}
	return service, nil
}

// Stop closes the listener, cancels every active peer, and waits for all
// accept/peer goroutines to leave. Concurrent and repeated calls are safe.
func (s *Service) Stop() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	if s.stopping {
		done := s.done
		s.mu.Unlock()
		<-done
		return nil
	}
	s.stopping = true
	listener := s.listener
	peers := make([]*aiacp.Server, 0, len(s.peers))
	for peer := range s.peers {
		peers = append(peers, peer)
	}
	s.mu.Unlock()

	var stopErr error
	if listener != nil {
		stopErr = listener.Close()
	}
	for _, peer := range peers {
		stopErr = errors.Join(stopErr, peer.Close())
	}
	if listener != nil {
		s.acceptWG.Wait()
	}
	s.closeDone()
	return stopErr
}

// Done closes once the service has stopped. A service that has not been
// started remains live until Stop is called, which makes ownership explicit.
func (s *Service) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *Service) acceptLoop(listener *Listener) {
	defer s.acceptWG.Done()
	err := listener.Serve(s.accept)
	if err != nil {
		slog.Error("acp listener failed", "error", err)
	}

	s.mu.Lock()
	stopping := s.stopping
	if !stopping {
		s.stopping = true
	}
	peers := make([]*aiacp.Server, 0, len(s.peers))
	if !stopping {
		for peer := range s.peers {
			peers = append(peers, peer)
		}
	}
	s.mu.Unlock()

	if !stopping {
		_ = listener.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
		s.closeDone()
	}
}

func (s *Service) accept(conn io.ReadWriteCloser) {
	server, err := aiacp.NewServer(conn, s.handler)
	if err != nil {
		_ = conn.Close()
		slog.Error("failed to create ACP server", "error", err)
		return
	}
	if !s.addPeer(server) {
		_ = server.Close()
		return
	}
	defer s.removePeer(server)
	defer server.Close()

	server.Start()
	<-server.Done()
}

func (s *Service) addPeer(server *aiacp.Server) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return false
	}
	s.peers[server] = struct{}{}
	return true
}

func (s *Service) removePeer(server *aiacp.Server) {
	s.mu.Lock()
	delete(s.peers, server)
	s.mu.Unlock()
}

func (s *Service) closeDone() {
	s.doneOnce.Do(func() { close(s.done) })
}
