package aiacp

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

const (
	maxSessionNewBufferedEvents = 256
	maxSessionNewBufferedBytes  = 1 << 20
)

type bufferedSessionUpdate struct {
	sessionID string
	body      protocol.SessionUpdateBody
	bytes     int
}

type sessionCreationReporterState uint8

const (
	sessionCreationPending sessionCreationReporterState = iota
	sessionCreationActive
	sessionCreationDiscarded
)

// sessionCreationReporter withholds creation-time notifications until the
// session/new response is on the wire. It becomes the ordinary connection
// reporter after publication.
type sessionCreationReporter struct {
	mu sync.Mutex

	ctx         context.Context
	target      Reporter
	state       sessionCreationReporterState
	pending     []bufferedSessionUpdate
	pendingSize int
	stopContext func() bool
	limitLogged bool
}

func newSessionCreationReporter(ctx context.Context, target Reporter) *sessionCreationReporter {
	reporter := &sessionCreationReporter{ctx: ctx, target: target}
	reporter.stopContext = context.AfterFunc(ctx, reporter.discard)
	return reporter
}

func (r *sessionCreationReporter) Update(sessionID string, body protocol.SessionUpdateBody) {
	body = cloneSessionUpdateBody(body)
	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.state {
	case sessionCreationActive:
		r.target.Update(sessionID, body)
	case sessionCreationDiscarded:
		return
	default:
		if r.ctx.Err() != nil {
			r.discardLocked()
			return
		}
		encoded, err := json.Marshal(protocol.SessionUpdate{SessionID: sessionID, Update: body})
		if err != nil {
			r.logDropped("cannot encode session/new update", "err", err)
			return
		}
		if len(r.pending) >= maxSessionNewBufferedEvents || r.pendingSize+len(encoded) > maxSessionNewBufferedBytes {
			r.logDropped("session/new update buffer limit exceeded", "events", len(r.pending), "bytes", r.pendingSize)
			return
		}
		r.pending = append(r.pending, bufferedSessionUpdate{sessionID: sessionID, body: body, bytes: len(encoded)})
		r.pendingSize += len(encoded)
	}
}

func (r *sessionCreationReporter) MessageChunk(sessionID, text string) {
	r.Update(sessionID, protocol.SessionUpdateBody{
		SessionUpdate: protocol.UpdateAgentMessageChunk,
		Content:       &protocol.ContentBlock{Type: "text", Text: text},
	})
}

func (r *sessionCreationReporter) ClientCapabilities() protocol.ClientCapabilities {
	provider, ok := r.target.(ClientCapabilitiesProvider)
	if !ok {
		return protocol.ClientCapabilities{}
	}
	return provider.ClientCapabilities()
}

// Call remains available while session/new is pending so the creator can issue
// reverse requests. The state check under mu is the acceptance boundary: a call
// accepted before discard may finish afterward (its ctx controls cancellation),
// while calls arriving after discard are rejected without reaching the peer.
func (r *sessionCreationReporter) Call(ctx context.Context, method string, params, result any) error {
	r.mu.Lock()
	if r.state == sessionCreationDiscarded {
		r.mu.Unlock()
		return protocol.ErrClosed
	}
	target := r.target
	r.mu.Unlock()

	caller, ok := target.(ReverseCaller)
	if !ok {
		return ErrReverseCallUnsupported
	}
	return caller.Call(ctx, method, params, result)
}

func (r *sessionCreationReporter) RequestPermission(ctx context.Context, sessionID string, req protocol.PermissionRequest) (string, error) {
	r.mu.Lock()
	if r.state == sessionCreationPending {
		r.mu.Unlock()
		return "", ErrSessionNotPublished
	}
	if r.state == sessionCreationDiscarded {
		r.mu.Unlock()
		return "", protocol.ErrClosed
	}
	target := r.target
	r.mu.Unlock()
	return target.RequestPermission(ctx, sessionID, req)
}

func (r *sessionCreationReporter) AfterResponseWritten(context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != sessionCreationPending {
		return
	}
	r.state = sessionCreationActive
	if r.stopContext != nil {
		r.stopContext()
		r.stopContext = nil
	}
	for _, update := range r.pending {
		r.target.Update(update.sessionID, update.body)
	}
	r.pending = nil
	r.pendingSize = 0
}

func (r *sessionCreationReporter) AfterResponseWriteFailed(context.Context) {
	r.discard()
}

func (r *sessionCreationReporter) discard() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.discardLocked()
}

func (r *sessionCreationReporter) discardLocked() {
	if r.state != sessionCreationPending {
		return
	}
	r.state = sessionCreationDiscarded
	r.pending = nil
	r.pendingSize = 0
	if r.stopContext != nil {
		r.stopContext()
		r.stopContext = nil
	}
}

func (r *sessionCreationReporter) logDropped(message string, args ...any) {
	if r.limitLogged {
		return
	}
	r.limitLogged = true
	slog.Warn("acp: dropping session/new notification", append([]any{"reason", message}, args...)...)
}

func cloneSessionUpdateBody(body protocol.SessionUpdateBody) protocol.SessionUpdateBody {
	body.Raw = append(json.RawMessage(nil), body.Raw...)
	if body.Content != nil {
		content := *body.Content
		body.Content = &content
	}
	return body
}
