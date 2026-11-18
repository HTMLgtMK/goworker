package service

import (
	"context"
	"sync"

	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
)

// sessionEventSubscription merges live session notifications with ordered run replay.
type sessionEventSubscription struct {
	events chan session.SessionEvent
	cancel context.CancelFunc
	once   sync.Once
	err    error
	run    agentcontrol.EventSubscription
	stop   func()
}

func newSessionEventSubscription(run agentcontrol.EventSubscription, live *sessionEventListener, stop func()) *sessionEventSubscription {
	ctx, cancel := context.WithCancel(context.Background())
	s := &sessionEventSubscription{
		events: make(chan session.SessionEvent, sessionEventBuffer),
		cancel: cancel, run: run, stop: stop,
	}
	go s.pump(ctx, live)
	return s
}

func (s *sessionEventSubscription) Events() <-chan session.SessionEvent { return s.events }

func (s *sessionEventSubscription) Close() error {
	s.once.Do(func() {
		s.cancel()
		s.stop()
		s.err = s.run.Close()
	})
	return s.err
}

func (s *sessionEventSubscription) pump(ctx context.Context, live *sessionEventListener) {
	defer close(s.events)
	defer s.Close()
	run := s.run.Events()
	sessionEvents := live.events
	for {
		var ev session.SessionEvent
		var ok bool
		select {
		case ev, ok = <-run:
			if !ok {
				return
			}
		case ev, ok = <-sessionEvents:
			if !ok {
				sessionEvents = nil
				continue
			}
		case <-live.done:
			return
		case <-ctx.Done():
			return
		}
		select {
		case s.events <- ev:
			continue
		default:
		}
		select {
		case s.events <- ev:
		case <-live.done:
			return
		case <-live.shutdown:
			return
		case <-ctx.Done():
			return
		}
	}
}
