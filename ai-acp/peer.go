package aiacp

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

// Peer owns one ACP transport and the protocol connection running over it.
// It is deliberately unaware of sessions: Server and Client add those semantics
// on top of this lifecycle primitive.
type Peer struct {
	conn *protocol.Conn
	done chan struct{}

	startOnce sync.Once
	doneOnce  sync.Once
}

// NewPeer constructs a peer. The transport must be non-nil and is owned by the
// returned peer after this call.
func NewPeer(rwc io.ReadWriteCloser) (*Peer, error) {
	return newPeer(rwc, nil)
}

func newPeer(rwc io.ReadWriteCloser, closeFn func() error) (*Peer, error) {
	if rwc == nil {
		return nil, errors.New("acp: nil transport")
	}
	if closeFn != nil {
		rwc = &closingTransport{ReadWriteCloser: rwc, closeFn: closeFn}
	}
	return &Peer{
		conn: protocol.NewConn(rwc),
		done: make(chan struct{}),
	}, nil
}

type closingTransport struct {
	io.ReadWriteCloser
	closeFn   func() error
	closeOnce sync.Once
	closeErr  error
}

func (t *closingTransport) Close() error {
	t.closeOnce.Do(func() { t.closeErr = t.closeFn() })
	return t.closeErr
}

// Start begins serving the connection in the background. It is safe to call
// Start more than once; only the first call starts the read loop.
func (p *Peer) Start() {
	p.startOnce.Do(func() {
		go func() {
			_ = p.Serve()
		}()
	})
}

// Serve runs the protocol read loop until the transport closes. Callers that
// need a synchronous lifecycle can invoke it directly instead of Start.
func (p *Peer) Serve() error {
	err := p.conn.Serve()
	p.doneOnce.Do(func() { close(p.done) })
	return err
}

// Handle registers a handler for an incoming JSON-RPC request.
func (p *Peer) Handle(method string, handler protocol.Handler) {
	p.conn.Handle(method, handler)
}

// HandleNotification registers a handler for an incoming JSON-RPC notification.
func (p *Peer) HandleNotification(method string, handler protocol.Notifier) {
	p.conn.HandleNotification(method, handler)
}

// Call sends an ACP JSON-RPC request and waits for its response.
func (p *Peer) Call(ctx context.Context, method string, params, result any) error {
	return p.conn.Call(ctx, method, params, result)
}

// Notify sends an ACP JSON-RPC notification.
func (p *Peer) Notify(method string, params any) error {
	return p.conn.Notify(method, params)
}

// Close closes the protocol connection and its underlying transport.
func (p *Peer) Close() error {
	err := p.conn.Close()
	p.doneOnce.Do(func() { close(p.done) })
	return err
}

// Done is closed when the peer stops serving.
func (p *Peer) Done() <-chan struct{} { return p.done }

// ServeErr reports the transport/protocol error that ended serving, if any.
func (p *Peer) ServeErr() error { return p.conn.ServeErr() }
