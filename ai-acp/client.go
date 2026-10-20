package aiacp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

// ClientCallbacks contains the application callbacks for agent-originated ACP
// messages. A nil permission callback is an explicit deny-by-default policy.
type ClientCallbacks struct {
	OnUpdate     func(protocol.SessionUpdate)
	OnPermission func(context.Context, protocol.PermissionRequest) (string, error)
}

// Client is the ACP Client role. It drives sessions on one Agent connection
// while accepting updates and permission requests from that Agent.
type Client struct {
	*Peer
	name string

	cbMu         sync.RWMutex
	onUpdate     func(protocol.SessionUpdate)
	onPermission func(context.Context, protocol.PermissionRequest) (string, error)
}

// NewClient constructs and starts a client on an existing transport. closeFn
// runs once after the peer transport is closed and can reap an owning process.
func NewClient(name string, rwc io.ReadWriteCloser, closeFn func() error) *Client {
	peer, err := newPeer(rwc, closeFn)
	if err != nil {
		panic(err)
	}
	client := &Client{Peer: peer, name: name}
	client.HandleNotification(protocol.MethodSessionUpdate, client.handleUpdate)
	client.Handle(protocol.MethodSessionRequestPermission, client.handlePermission)
	client.Start()
	return client
}

// SetCallbacks replaces the update and permission callbacks atomically.
func (c *Client) SetCallbacks(callbacks ClientCallbacks) {
	c.cbMu.Lock()
	c.onUpdate = callbacks.OnUpdate
	c.onPermission = callbacks.OnPermission
	c.cbMu.Unlock()
}

// Initialize performs the ACP handshake with the protocol version used by
// this module.
func (c *Client) Initialize(ctx context.Context) (protocol.InitializeResponse, error) {
	return c.InitializeWithRequest(ctx, protocol.InitializeRequest{ProtocolVersion: protocol.Version})
}

// InitializeWithRequest performs the ACP handshake with explicit capabilities.
func (c *Client) InitializeWithRequest(ctx context.Context, request protocol.InitializeRequest) (protocol.InitializeResponse, error) {
	var response protocol.InitializeResponse
	if request.ProtocolVersion == 0 {
		request.ProtocolVersion = protocol.Version
	}
	if err := c.Call(ctx, protocol.MethodInitialize, request, &response); err != nil {
		return protocol.InitializeResponse{}, err
	}
	return response, nil
}

// NewSession creates a session in cwd and returns the server-assigned ID.
func (c *Client) NewSession(ctx context.Context, cwd string) (string, error) {
	return c.NewSessionWithRequest(ctx, protocol.NewSessionRequest{
		Cwd:        cwd,
		McpServers: []map[string]any{},
	})
}

// NewSessionWithRequest creates a session using an explicit ACP request.
func (c *Client) NewSessionWithRequest(ctx context.Context, request protocol.NewSessionRequest) (string, error) {
	if request.McpServers == nil {
		request.McpServers = []map[string]any{}
	}
	var response protocol.NewSessionResponse
	if err := c.Call(ctx, protocol.MethodSessionNew, request, &response); err != nil {
		return "", err
	}
	if response.SessionID == "" {
		return "", fmt.Errorf("acp: agent %q returned empty sessionId", c.name)
	}
	return response.SessionID, nil
}

// ListSessions returns the sessions visible to the Agent.
func (c *Client) ListSessions(ctx context.Context, request protocol.ListSessionsRequest) (protocol.ListSessionsResponse, error) {
	var response protocol.ListSessionsResponse
	if err := c.Call(ctx, protocol.MethodSessionList, request, &response); err != nil {
		return protocol.ListSessionsResponse{}, err
	}
	if response.Sessions == nil {
		response.Sessions = []protocol.SessionInfo{}
	}
	return response, nil
}

// SessionLoad resumes a session and returns the requested ID as confirmation.
func (c *Client) SessionLoad(ctx context.Context, sessionID, cwd string) (string, error) {
	return c.LoadSession(ctx, protocol.LoadSessionRequest{
		SessionID:  sessionID,
		Cwd:        cwd,
		McpServers: []map[string]any{},
	})
}

// LoadSession resumes a session using an explicit ACP request.
func (c *Client) LoadSession(ctx context.Context, request protocol.LoadSessionRequest) (string, error) {
	if request.SessionID == "" {
		return "", fmt.Errorf("acp: session/load requires sessionId")
	}
	if request.McpServers == nil {
		request.McpServers = []map[string]any{}
	}
	var response protocol.LoadSessionResponse
	if err := c.Call(ctx, protocol.MethodSessionLoad, request, &response); err != nil {
		return "", err
	}
	return request.SessionID, nil
}

// Prompt submits one prompt turn and waits for its stop reason.
func (c *Client) Prompt(ctx context.Context, sessionID string, prompt []protocol.ContentBlock) (string, error) {
	var response protocol.PromptResponse
	request := protocol.PromptRequest{SessionID: sessionID, Prompt: prompt}
	if err := c.Call(ctx, protocol.MethodSessionPrompt, request, &response); err != nil {
		return "", err
	}
	return response.StopReason, nil
}

// PromptRequest submits an explicit prompt request and returns the full
// protocol response.
func (c *Client) PromptRequest(ctx context.Context, request protocol.PromptRequest) (protocol.PromptResponse, error) {
	var response protocol.PromptResponse
	if err := c.Call(ctx, protocol.MethodSessionPrompt, request, &response); err != nil {
		return protocol.PromptResponse{}, err
	}
	return response, nil
}

// Cancel requests cancellation of an active prompt. It is a notification and
// therefore has no server response to wait for.
func (c *Client) Cancel(sessionID string) error {
	return c.Notify(protocol.MethodSessionCancel, protocol.CancelNotification{SessionID: sessionID})
}

// Name identifies the connected Agent in diagnostics.
func (c *Client) Name() string { return c.name }

func (c *Client) handleUpdate(params json.RawMessage) {
	var update protocol.SessionUpdate
	if err := json.Unmarshal(params, &update); err != nil {
		slog.Warn("acp: decode session/update", "client", c.name, "err", err)
		return
	}
	c.cbMu.RLock()
	onUpdate := c.onUpdate
	c.cbMu.RUnlock()
	if onUpdate != nil {
		onUpdate(update)
	}
}

func (c *Client) handlePermission(ctx context.Context, params json.RawMessage) (any, error) {
	var request protocol.PermissionRequest
	if err := json.Unmarshal(params, &request); err != nil {
		return nil, fmt.Errorf("acp: decode request_permission: %w", err)
	}
	c.cbMu.RLock()
	onPermission := c.onPermission
	c.cbMu.RUnlock()
	if onPermission == nil {
		return protocol.CancelledPermissionOutcome(), nil
	}
	optionID, err := onPermission(ctx, request)
	if err != nil {
		slog.Warn("acp: permission callback failed, deny", "client", c.name, "tool", request.ToolCall.Title, "err", err)
		return protocol.CancelledPermissionOutcome(), nil
	}
	return protocol.SelectedPermissionOutcome(optionID), nil
}
