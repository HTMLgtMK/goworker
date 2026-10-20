package aiacp

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

// Reporter is the transport-independent way a session handler emits ACP
// events or asks the client for a permission decision.
type Reporter interface {
	Update(sessionID string, body protocol.SessionUpdateBody)
	MessageChunk(sessionID, text string)
	RequestPermission(ctx context.Context, sessionID string, req protocol.PermissionRequest) (string, error)
}

// SessionHandler handles session/prompt. It contains no task or daemon
// concepts; applications decide what a prompt means.
type SessionHandler interface {
	Prompt(ctx context.Context, sessionID string, prompt []protocol.ContentBlock, reporter Reporter) (string, error)
}

// SessionCreator is optional. The server allocates the ACP session ID and
// gives it to the adapter so it can attach application state to that ID.
type SessionCreator interface {
	CreateSession(ctx context.Context, sessionID string, req protocol.NewSessionRequest, reporter Reporter) error
}

// SessionLoader is optional and is advertised during initialize when present.
type SessionLoader interface {
	LoadSession(ctx context.Context, req protocol.LoadSessionRequest, reporter Reporter) error
}

// SessionLister is optional. A missing implementation makes session/list
// return a protocol error instead of inventing persistence semantics.
type SessionLister interface {
	ListSessions(ctx context.Context, req protocol.ListSessionsRequest) ([]protocol.SessionInfo, error)
}

// SessionModesProvider is optional and supplies the modes included in
// session/new and session/load responses.
type SessionModesProvider interface {
	SessionModes() *protocol.SessionModeState
}

type serverConfig struct {
	creator    SessionCreator
	loader     SessionLoader
	lister     SessionLister
	modes      SessionModesProvider
	creatorSet bool
	loaderSet  bool
	listerSet  bool
	modesSet   bool
}

// ServerOption customizes optional session lifecycle adapters.
type ServerOption func(*serverConfig)

// WithSessionCreator registers the session/new adapter.
func WithSessionCreator(creator SessionCreator) ServerOption {
	return func(config *serverConfig) {
		config.creatorSet = true
		config.creator = creator
	}
}

// WithSessionLoader registers the session/load adapter. Passing nil explicitly
// disables handler auto-discovery, which is useful for compatibility adapters.
func WithSessionLoader(loader SessionLoader) ServerOption {
	return func(config *serverConfig) {
		config.loaderSet = true
		config.loader = loader
	}
}

// WithSessionLister registers the session/list adapter.
func WithSessionLister(lister SessionLister) ServerOption {
	return func(config *serverConfig) {
		config.listerSet = true
		config.lister = lister
	}
}

// WithSessionModesProvider registers the optional session modes provider.
func WithSessionModesProvider(modes SessionModesProvider) ServerOption {
	return func(config *serverConfig) {
		config.modesSet = true
		config.modes = modes
	}
}

// Server is the ACP Agent role. The application-facing handler only receives
// protocol session data; task routing stays outside this package.
type Server struct {
	*Peer
	handler SessionHandler
	config  serverConfig

	mu      sync.Mutex
	closed  bool
	cancels map[string]context.CancelFunc
	prompts sync.WaitGroup

	startOnce sync.Once
	doneOnce  sync.Once
	done      chan struct{}
	started   bool
}

// NewServer constructs a server but does not start its read loop. Use Start
// after all application adapters have been installed, or use ServeConn.
func NewServer(rwc io.ReadWriteCloser, handler SessionHandler, options ...ServerOption) (*Server, error) {
	if handler == nil {
		return nil, errors.New("acp: nil session handler")
	}
	peer, err := NewPeer(rwc)
	if err != nil {
		return nil, err
	}
	config := serverConfig{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if !config.loaderSet {
		if loader, ok := handler.(SessionLoader); ok {
			config.loader = loader
		}
	}
	if !config.listerSet {
		if lister, ok := handler.(SessionLister); ok {
			config.lister = lister
		}
	}
	if !config.creatorSet {
		if creator, ok := handler.(SessionCreator); ok {
			config.creator = creator
		}
	}
	if !config.modesSet {
		if modes, ok := handler.(SessionModesProvider); ok {
			config.modes = modes
		}
	}

	s := &Server{
		Peer:    peer,
		handler: handler,
		config:  config,
		cancels: make(map[string]context.CancelFunc),
		done:    make(chan struct{}),
	}
	peer.Handle(protocol.MethodInitialize, s.handleInitialize)
	peer.Handle(protocol.MethodSessionList, s.handleSessionList)
	peer.Handle(protocol.MethodSessionNew, s.handleSessionNew)
	peer.Handle(protocol.MethodSessionLoad, s.handleSessionLoad)
	peer.Handle(protocol.MethodSessionPrompt, s.handlePrompt)
	peer.HandleNotification(protocol.MethodSessionCancel, s.handleCancel)
	return s, nil
}

// Start begins serving and closes Done only after the connection and all
// in-flight prompt handlers have stopped.
func (s *Server) Start() {
	s.startOnce.Do(func() {
		s.mu.Lock()
		s.started = true
		s.mu.Unlock()
		s.Peer.Start()
		go func() {
			<-s.Peer.Done()
			s.cancelAll()
			s.prompts.Wait()
			s.doneOnce.Do(func() { close(s.done) })
		}()
	})
}

// Done signals that the transport and every active prompt have stopped.
func (s *Server) Done() <-chan struct{} { return s.done }

// ServeConn constructs and starts a server on an existing transport.
func ServeConn(rwc io.ReadWriteCloser, handler SessionHandler, options ...ServerOption) *Server {
	server, err := NewServer(rwc, handler, options...)
	if err != nil {
		panic(err)
	}
	server.Start()
	return server
}

// Close stops the transport and cancels active prompts.
func (s *Server) Close() error {
	s.cancelAll()
	err := s.Peer.Close()
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		s.doneOnce.Do(func() { close(s.done) })
	}
	return err
}

func (s *Server) handleInitialize(_ context.Context, params json.RawMessage) (any, error) {
	var req protocol.InitializeRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("acp: decode initialize: %w", err)
	}
	caps := protocol.AgentCapabilities{}
	if s.config.loader != nil {
		var err error
		caps, err = caps.With(protocol.LoadSessionCapability{})
		if err != nil {
			return nil, fmt.Errorf("acp: advertise loadSession: %w", err)
		}
	}
	return protocol.InitializeResponse{
		ProtocolVersion:   protocol.Version,
		AgentCapabilities: caps,
	}, nil
}

func (s *Server) handleSessionNew(ctx context.Context, params json.RawMessage) (any, error) {
	var req protocol.NewSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("acp: decode session/new: %w", err)
	}
	sessionID := newSessionID()
	if s.config.creator != nil {
		if err := s.config.creator.CreateSession(ctx, sessionID, req, s); err != nil {
			return nil, err
		}
	}
	response := protocol.NewSessionResponse{SessionID: sessionID}
	if s.config.modes != nil {
		response.Modes = s.config.modes.SessionModes()
	}
	return response, nil
}

func (s *Server) handleSessionList(ctx context.Context, params json.RawMessage) (any, error) {
	if s.config.lister == nil {
		return nil, &protocol.RPCError{Code: -32000, Message: "acp: session list not supported"}
	}
	var req protocol.ListSessionsRequest
	if len(params) != 0 && string(params) != "null" {
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, fmt.Errorf("acp: decode session/list: %w", err)
		}
	}
	sessions, err := s.config.lister.ListSessions(ctx, req)
	if err != nil {
		return nil, err
	}
	if sessions == nil {
		sessions = []protocol.SessionInfo{}
	}
	return protocol.ListSessionsResponse{Sessions: sessions}, nil
}

func (s *Server) handleSessionLoad(ctx context.Context, params json.RawMessage) (any, error) {
	var req protocol.LoadSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("acp: decode session/load: %w", err)
	}
	if req.SessionID == "" {
		return nil, errors.New("acp: session/load requires sessionId")
	}
	if s.config.loader == nil {
		return nil, &protocol.RPCError{Code: -32000, Message: "acp: session load not supported"}
	}
	if err := s.config.loader.LoadSession(ctx, req, s); err != nil {
		return nil, err
	}
	response := protocol.LoadSessionResponse{}
	if s.config.modes != nil {
		response.Modes = s.config.modes.SessionModes()
	}
	return response, nil
}

func (s *Server) handlePrompt(ctx context.Context, params json.RawMessage) (any, error) {
	var req protocol.PromptRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("acp: decode session/prompt: %w", err)
	}
	if req.SessionID == "" || len(req.Prompt) == 0 {
		return nil, errors.New("acp: session/prompt requires sessionId and prompt")
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		return nil, protocol.ErrClosed
	}
	if _, active := s.cancels[req.SessionID]; active {
		s.mu.Unlock()
		cancel()
		return nil, &protocol.RPCError{Code: -32000, Message: fmt.Sprintf("acp: session %q already has an active prompt", req.SessionID)}
	}
	s.cancels[req.SessionID] = cancel
	s.prompts.Add(1)
	s.mu.Unlock()
	defer s.prompts.Done()
	defer func() {
		s.mu.Lock()
		delete(s.cancels, req.SessionID)
		s.mu.Unlock()
		cancel()
	}()

	stopReason, err := s.handler.Prompt(runCtx, req.SessionID, req.Prompt, s)
	if err != nil {
		return nil, err
	}
	if stopReason == "" {
		stopReason = protocol.StopEndTurn
	}
	return protocol.PromptResponse{StopReason: stopReason}, nil
}

func (s *Server) cancelAll() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	cancels := make([]context.CancelFunc, 0, len(s.cancels))
	for sessionID, cancel := range s.cancels {
		delete(s.cancels, sessionID)
		cancels = append(cancels, cancel)
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (s *Server) handleCancel(params json.RawMessage) {
	var req protocol.CancelNotification
	if err := json.Unmarshal(params, &req); err != nil || req.SessionID == "" {
		return
	}
	s.mu.Lock()
	cancel := s.cancels[req.SessionID]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Update sends a session/update notification to the client. Raw update bodies
// are forwarded without dropping extension fields.
func (s *Server) Update(sessionID string, body protocol.SessionUpdateBody) {
	if body.Raw != nil {
		_ = s.Notify(protocol.MethodSessionUpdate, struct {
			SessionID string          `json:"sessionId"`
			Update    json.RawMessage `json:"update"`
		}{SessionID: sessionID, Update: body.Raw})
		return
	}
	_ = s.Notify(protocol.MethodSessionUpdate, protocol.SessionUpdate{
		SessionID: sessionID,
		Update:    body,
	})
}

// MessageChunk is a convenience for the common agent message update.
func (s *Server) MessageChunk(sessionID, text string) {
	s.Update(sessionID, protocol.SessionUpdateBody{
		SessionUpdate: protocol.UpdateAgentMessageChunk,
		Content:       &protocol.ContentBlock{Type: "text", Text: text},
	})
}

// RequestPermission asks the client for a decision and returns its selected
// option ID. A cancelled or unknown outcome is treated as a denial.
func (s *Server) RequestPermission(ctx context.Context, sessionID string, req protocol.PermissionRequest) (string, error) {
	req.SessionID = sessionID
	var response protocol.PermissionResponse
	if err := s.Call(ctx, protocol.MethodSessionRequestPermission, req, &response); err != nil {
		var rpcErr *protocol.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == protocol.CodeMethodNotFound {
			return "", ErrPermissionUnsupported
		}
		return "", fmt.Errorf("acp: request permission: %w", err)
	}
	if response.Outcome.Outcome != "selected" {
		return "", nil
	}
	return response.Outcome.OptionID, nil
}

// Call issues an application-defined reverse request over the same ACP peer.
func (s *Server) Call(ctx context.Context, method string, params, result any) error {
	return s.Peer.Call(ctx, method, params, result)
}

var ErrPermissionUnsupported = errors.New("acp: peer does not support session/request_permission")

func newSessionID() string {
	return fmt.Sprintf("sess_%x", randomBytes(8))
}

func randomBytes(size int) []byte {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		panic("acp: generate session id: " + err.Error())
	}
	return value
}
