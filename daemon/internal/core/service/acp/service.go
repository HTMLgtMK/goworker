package acp

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	aiacp "github.com/tinguo/goworker/ai-acp"
)

// HandlerFactory creates the protocol-level handler for one accepted
// connection. Each connection gets its own handler instance so per-connection
// session state never leaks across peers.
type HandlerFactory func() aiacp.SessionHandler

// Service is a reusable ACP daemon adapter. It owns the listener returned by
// listenerFactory and all connected peers; callers supply factories for both the
// transport and the per-connection protocol handler. Daemon task/session routing
// remains an application concern.
type Service struct {
	handlerFactory  HandlerFactory
	listenerFactory ListenerFactory

	mu       sync.Mutex
	listener Listener
	peers    map[*aiacp.Server]struct{}

	started  bool
	stopping bool
	done     chan struct{}
	doneOnce sync.Once
	acceptWG sync.WaitGroup
}

// Name identifies the ACP service to the application service registry.
func (s *Service) Name() string { return "acp" }

// NewService constructs a stopped service. The factories must implement only
// the transport and ai-acp SessionHandler contracts; daemon task/session routing
// remains an application concern.
func NewService(handlerFactory HandlerFactory, listenerFactory ListenerFactory) *Service {
	return &Service{
		handlerFactory:  handlerFactory,
		listenerFactory: listenerFactory,
		peers:           make(map[*aiacp.Server]struct{}),
		done:            make(chan struct{}),
	}
}

// Start creates the listener once and begins accepting ACP peers. A successful
// Start is idempotent. A failed listener factory leaves the service stopped so
// the caller can correct the factory and retry only with a new service.
func (s *Service) Start() error {
	if s == nil {
		return errors.New("acp: nil service")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}
	if s.stopping {
		return errors.New("acp: service has been stopped")
	}
	if s.handlerFactory == nil {
		return errors.New("acp: handler factory is required")
	}
	if s.listenerFactory == nil {
		return errors.New("acp: listener factory is required")
	}

	listener, err := s.listenerFactory()
	if err != nil {
		return fmt.Errorf("acp: create listener: %w", err)
	}
	if listener == nil {
		return errors.New("acp: listener factory returned nil listener")
	}

	s.listener = listener
	s.started = true
	s.acceptWG.Add(1)
	go s.acceptLoop(listener)
	return nil
}

// Start is also available as a convenience for one-shot composition roots.
// The returned service owns the listener and must be stopped.
func Start(handlerFactory HandlerFactory, listenerFactory ListenerFactory) (*Service, error) {
	service := NewService(handlerFactory, listenerFactory)
	if err := service.Start(); err != nil {
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

func (s *Service) acceptLoop(listener Listener) {
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
	// factory 在连接粒度调用：每条连接一份 handler，session 状态不跨连接串线。
	handler := s.handlerFactory()
	if handler == nil {
		_ = conn.Close()
		slog.Error("acp: handler factory returned nil")
		return
	}
	server, err := aiacp.NewServer(conn, handler)
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
