package service

import (
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-runtime/session"
)

const sessionEventBuffer = 32

var errSessionEventHubClosed = errors.New("agent: session event hub closed")

func (p *AgentPlugin) sessionEventPublisher(sessionID string) func(string, any) {
	return func(eventType string, data any) {
		payload, err := json.Marshal(data)
		if err != nil {
			slog.Warn("agent: marshal session event failed", "session", sessionID, "type", eventType, "err", err)
			return
		}
		err = p.sessionEvents.publish(session.SessionEvent{
			SessionID: sessionID, Type: eventType, Time: time.Now(), Payload: payload,
		})
		if err != nil && !errors.Is(err, errSessionEventHubClosed) {
			slog.Warn("agent: publish session event failed", "session", sessionID, "type", eventType, "err", err)
		}
	}
}

type sessionEventListener struct {
	events   chan session.SessionEvent
	done     chan struct{}
	shutdown chan struct{}
}

type sessionEventHub struct {
	mu     sync.Mutex
	subs   map[*sessionEventListener]string
	closed bool
}

func (h *sessionEventHub) subscribe(sessionID string) (*sessionEventListener, func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, nil, errSessionEventHubClosed
	}
	if h.subs == nil {
		h.subs = make(map[*sessionEventListener]string)
	}
	sub := &sessionEventListener{events: make(chan session.SessionEvent, sessionEventBuffer), done: make(chan struct{}), shutdown: make(chan struct{})}
	h.subs[sub] = sessionID
	return sub, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.removeLocked(sub)
	}, nil
}

func (h *sessionEventHub) publish(event session.SessionEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errSessionEventHubClosed
	}
	for sub, sessionID := range h.subs {
		if sessionID != event.SessionID {
			continue
		}
		select {
		case sub.events <- event:
		default:
			h.removeLocked(sub)
			slog.Warn("agent: session event hub disconnected slow subscriber", "session", sessionID, "type", event.Type)
		}
	}
	return nil
}

func (h *sessionEventHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for sub := range h.subs {
		delete(h.subs, sub)
		close(sub.shutdown)
		close(sub.events)
	}
}

func (h *sessionEventHub) removeLocked(sub *sessionEventListener) {
	if _, ok := h.subs[sub]; ok {
		delete(h.subs, sub)
		close(sub.done)
		close(sub.events)
	}
}
