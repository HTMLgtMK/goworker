package dispatch

import (
	"context"
	"errors"
	"io"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
)

// ErrPermissionUnsupported distinguishes an agent that cannot answer a
// permission request from a client that explicitly denied it.
var ErrPermissionUnsupported = aiacp.ErrPermissionUnsupported

// TaskHandler is the dispatch-domain adapter. ACP itself only sees the
// prompt/session interfaces in ai-acp; task routing remains here.
type TaskHandler interface {
	Run(ctx context.Context, sessionID, prompt string, rep Reporter) (string, error)
}

// Reporter is the small ACP event surface exposed to task handlers.
type Reporter = aiacp.Reporter

// SessionAware receives the cwd declared by session/new.
type SessionAware interface {
	SetSession(sessionID, cwd string)
}

// SessionServerAware additionally receives the dispatch wrapper so task code
// can issue application-defined reverse calls over the ACP connection.
type SessionServerAware interface {
	SetSessionServer(sessionID, cwd string, server *Server)
}

// SessionLoader is the dispatch adapter for session/load.
type SessionLoader interface {
	LoadSession(sessionID, cwd string, rep Reporter) error
}

// SessionLister supplies the dispatch session list.
type SessionLister interface {
	ListSessions() []protocol.SessionInfo
}

// SessionModesProvider supplies optional ACP session modes.
type SessionModesProvider interface {
	SessionModes() *protocol.SessionModeState
}

// Server is a compatibility adapter for task-oriented dispatch handlers. The
// actual ACP lifecycle and JSON-RPC implementation live in ai-acp.Server.
type Server struct {
	*aiacp.Server
}

type taskAdapter struct {
	handler TaskHandler
	server  *Server
}

func (a *taskAdapter) Prompt(ctx context.Context, sessionID string, prompt []protocol.ContentBlock, _ aiacp.Reporter) (string, error) {
	var text string
	for i, block := range prompt {
		if block.Type != "text" {
			continue
		}
		if i > 0 {
			text += "\n"
		}
		text += block.Text
	}
	return a.handler.Run(ctx, sessionID, text, a.server)
}

func (a *taskAdapter) CreateSession(_ context.Context, sessionID string, req protocol.NewSessionRequest, _ aiacp.Reporter) error {
	if aware, ok := a.handler.(SessionServerAware); ok {
		aware.SetSessionServer(sessionID, req.Cwd, a.server)
		return nil
	}
	if aware, ok := a.handler.(SessionAware); ok {
		aware.SetSession(sessionID, req.Cwd)
	}
	return nil
}

func (a *taskAdapter) LoadSession(_ context.Context, req protocol.LoadSessionRequest, _ aiacp.Reporter) error {
	loader, ok := a.handler.(SessionLoader)
	if !ok {
		return &protocol.RPCError{Code: -32000, Message: "dispatch: session load not supported"}
	}
	return loader.LoadSession(req.SessionID, req.Cwd, a.server)
}

type sessionListAdapter struct{ handler TaskHandler }

func (a *sessionListAdapter) ListSessions(_ context.Context, _ protocol.ListSessionsRequest) ([]protocol.SessionInfo, error) {
	lister, ok := a.handler.(SessionLister)
	if !ok {
		return nil, &protocol.RPCError{Code: -32000, Message: "dispatch: session list not supported"}
	}
	return lister.ListSessions(), nil
}

func (a *taskAdapter) SessionModes() *protocol.SessionModeState {
	if provider, ok := a.handler.(SessionModesProvider); ok {
		return provider.SessionModes()
	}
	return nil
}

// NewServer constructs a dispatch adapter without starting its read loop.
// ServeConn is the normal convenience API.
func NewServer(rwc io.ReadWriteCloser, handler TaskHandler) (*Server, error) {
	if handler == nil {
		return nil, errors.New("dispatch: nil task handler")
	}
	adapter := &taskAdapter{handler: handler}
	options := []aiacp.ServerOption{
		aiacp.WithSessionCreator(adapter),
	}
	if _, ok := handler.(SessionLoader); ok {
		options = append(options, aiacp.WithSessionLoader(adapter))
	} else {
		options = append(options, aiacp.WithSessionLoader(nil))
	}
	if _, ok := handler.(SessionLister); ok {
		options = append(options, aiacp.WithSessionLister(&sessionListAdapter{handler: handler}))
	} else {
		options = append(options, aiacp.WithSessionLister(nil))
	}
	if _, ok := handler.(SessionModesProvider); ok {
		options = append(options, aiacp.WithSessionModesProvider(adapter))
	} else {
		options = append(options, aiacp.WithSessionModesProvider(nil))
	}
	core, err := aiacp.NewServer(rwc, adapter, options...)
	if err != nil {
		return nil, err
	}
	server := &Server{Server: core}
	adapter.server = server
	return server, nil
}

// ServeConn starts an ACP server on an existing transport.
func ServeConn(rwc io.ReadWriteCloser, handler TaskHandler) *Server {
	server, err := NewServer(rwc, handler)
	if err != nil {
		panic(err)
	}
	server.Start()
	return server
}

// Update and the remaining reporter methods are promoted from aiacp.Server.
// Keep this compile-time assertion close to the adapter so API drift is caught.
var _ aiacp.SessionHandler = (*taskAdapter)(nil)
