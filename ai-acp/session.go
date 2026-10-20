package aiacp

import (
	"context"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

// Session is a handle for one ACP session on a Client connection.
//
// The session ID is assigned by the Agent and remains bound to this handle for
// its lifetime. Session methods delegate transport and lifecycle behavior to
// the owning Client.
type Session struct {
	client *Client
	id     string
}

func newSession(client *Client, id string) *Session {
	return &Session{client: client, id: id}
}

// ID returns the stable ACP session ID represented by the handle.
func (s *Session) ID() string { return s.id }

// Prompt submits one prompt turn and waits for its stop reason.
func (s *Session) Prompt(ctx context.Context, prompt []protocol.ContentBlock) (string, error) {
	return s.client.Prompt(ctx, s.id, prompt)
}

// PromptText submits a single text prompt turn and waits for its stop reason.
func (s *Session) PromptText(ctx context.Context, text string) (string, error) {
	return s.Prompt(ctx, []protocol.ContentBlock{protocol.TextBlock(text)})
}

// Cancel requests cancellation of an active prompt. It follows Client.Cancel's
// notification semantics and does not wait for the prompt to finish.
func (s *Session) Cancel() error { return s.client.Cancel(s.id) }

// NewSessionHandle creates a session and returns a handle bound to its ID.
func (c *Client) NewSessionHandle(ctx context.Context, cwd string) (*Session, error) {
	id, err := c.NewSession(ctx, cwd)
	if err != nil {
		return nil, err
	}
	return newSession(c, id), nil
}

// LoadSessionHandle resumes a session and returns a handle bound to its ID.
func (c *Client) LoadSessionHandle(ctx context.Context, request protocol.LoadSessionRequest) (*Session, error) {
	id, err := c.LoadSession(ctx, request)
	if err != nil {
		return nil, err
	}
	return newSession(c, id), nil
}
