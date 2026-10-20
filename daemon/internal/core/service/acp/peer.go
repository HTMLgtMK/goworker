package acp

import (
	"io"
	"sync"

	aiacp "github.com/tinguo/goworker/ai-acp"
)

// Peer owns one ACP server and the transport attached to it. The daemon core
// sees only ai-acp's protocol-facing SessionHandler; task routing stays outside
// this adapter.
type Peer struct {
	server *aiacp.Server

	startOnce sync.Once
}

// NewPeer constructs an ACP server for conn. The server is not started until
// Start is called, which lets the service register the peer before it can race
// with Stop.
func NewPeer(conn io.ReadWriteCloser, handler aiacp.SessionHandler) (*Peer, error) {
	server, err := aiacp.NewServer(conn, handler)
	if err != nil {
		return nil, err
	}
	return &Peer{server: server}, nil
}

// Start begins serving the peer. It is safe to call more than once.
func (p *Peer) Start() {
	if p == nil || p.server == nil {
		return
	}
	p.startOnce.Do(p.server.Start)
}

// Close cancels active prompts and closes the peer transport.
func (p *Peer) Close() error {
	if p == nil || p.server == nil {
		return nil
	}
	return p.server.Close()
}

// Done closes after the transport and all active prompt handlers stop.
func (p *Peer) Done() <-chan struct{} {
	if p == nil || p.server == nil {
		return nil
	}
	return p.server.Done()
}

// Server exposes the underlying ai-acp server to adapters that need to issue
// protocol-level reverse requests. It does not expose a runtime agent.Session.
func (p *Peer) Server() *aiacp.Server {
	if p == nil {
		return nil
	}
	return p.server
}
