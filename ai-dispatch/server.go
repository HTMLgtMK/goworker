package dispatch

import (
	"context"
	"errors"
	"fmt"
	"io"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	aisession "github.com/tinguo/goworker/ai-session"
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

// SessionServerAware receives the dispatch wrapper as well as session context
// so task code can issue reverse calls over the ACP connection. When a handler
// implements both awareness interfaces, this one takes precedence to avoid
// initializing the same worker twice.
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

type SessionCreator = aiacp.SessionCreator

// ServerOption customizes dispatch lifecycle composition.
type ServerOption func(*serverConfig)

type serverConfig struct {
	sessionCreator    SessionCreator
	sessionCreatorSet bool
}

// WithSessionCreator injects the application's shared session creator. The
// adapter uses its returned canonical ID to initialize SessionAware or
// SessionServerAware handlers exactly once. Without it, the standalone adapter
// generates one protocol-local ID before initializing the handler.
func WithSessionCreator(creator SessionCreator) ServerOption {
	return func(config *serverConfig) {
		config.sessionCreator = creator
		config.sessionCreatorSet = true
	}
}

// Server is a compatibility adapter for task-oriented dispatch handlers. The
// actual ACP lifecycle and JSON-RPC implementation live in ai-acp.Server.
type Server struct {
	*aiacp.Server
}

type taskAdapter struct {
	handler               TaskHandler
	applicationCreator    SessionCreator
	applicationCreatorSet bool
	server                *Server
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

func (a *taskAdapter) CreateSession(ctx context.Context, req protocol.NewSessionRequest, reporter aiacp.Reporter) (aisession.ID, error) {
	var sessionID aisession.ID
	if a.applicationCreatorSet {
		if a.applicationCreator == nil {
			return "", errors.New("dispatch: injected session creator is nil")
		}
		id, err := a.applicationCreator.CreateSession(ctx, req, reporter)
		if err != nil {
			return "", err
		}
		sessionID = id
	} else {
		id, err := aisession.NewID()
		if err != nil {
			return "", err
		}
		sessionID = id
	}
	if err := sessionID.Validate(); err != nil {
		return "", fmt.Errorf("dispatch: session creator returned invalid ID: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if aware, ok := a.handler.(SessionServerAware); ok {
		aware.SetSessionServer(sessionID.String(), req.Cwd, a.server)
		return sessionID, nil
	}
	if aware, ok := a.handler.(SessionAware); ok {
		aware.SetSession(sessionID.String(), req.Cwd)
	}
	return sessionID, nil
}

func (a *taskAdapter) LoadSession(_ context.Context, sessionID aisession.ID, req protocol.LoadSessionRequest, _ aiacp.Reporter) error {
	loader, ok := a.handler.(SessionLoader)
	if !ok {
		return &protocol.RPCError{Code: -32000, Message: "dispatch: session load not supported"}
	}
	return loader.LoadSession(sessionID.String(), req.Cwd, a.server)
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
// In standalone use it allocates protocol-local IDs; embedded applications
// should pass WithSessionCreator with their shared canonical identity owner.
func NewServer(rwc io.ReadWriteCloser, handler TaskHandler, options ...ServerOption) (*Server, error) {
	if handler == nil {
		return nil, errors.New("dispatch: nil task handler")
	}
	adapter := &taskAdapter{handler: handler}
	config := serverConfig{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.sessionCreatorSet {
		adapter.applicationCreator = config.sessionCreator
		adapter.applicationCreatorSet = true
	}
	acpOptions := []aiacp.ServerOption{
		aiacp.WithSessionCreator(adapter),
	}
	if _, ok := handler.(SessionLoader); ok {
		acpOptions = append(acpOptions, aiacp.WithSessionLoader(adapter))
	} else {
		acpOptions = append(acpOptions, aiacp.WithSessionLoader(nil))
	}
	if _, ok := handler.(SessionLister); ok {
		acpOptions = append(acpOptions, aiacp.WithSessionLister(&sessionListAdapter{handler: handler}))
	} else {
		acpOptions = append(acpOptions, aiacp.WithSessionLister(nil))
	}
	if _, ok := handler.(SessionModesProvider); ok {
		acpOptions = append(acpOptions, aiacp.WithSessionModesProvider(adapter))
	} else {
		acpOptions = append(acpOptions, aiacp.WithSessionModesProvider(nil))
	}
	core, err := aiacp.NewServer(rwc, adapter, acpOptions...)
	if err != nil {
		return nil, err
	}
	server := &Server{Server: core}
	adapter.server = server
	return server, nil
}

// ServeConn starts an ACP server on an existing transport.
func ServeConn(rwc io.ReadWriteCloser, handler TaskHandler, options ...ServerOption) *Server {
	server, err := NewServer(rwc, handler, options...)
	if err != nil {
		panic(err)
	}
	server.Start()
	return server
}

// Update and the remaining reporter methods are promoted from aiacp.Server.
// Keep this compile-time assertion close to the adapter so API drift is caught.
var _ aiacp.SessionHandler = (*taskAdapter)(nil)
