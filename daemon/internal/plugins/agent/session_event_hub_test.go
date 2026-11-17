package service

import (
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-runtime/session"
)

func TestSessionEventHubRoutesAndUnsubscribes(t *testing.T) {
	var hub sessionEventHub
	first, cancelFirst, err := hub.subscribe("first")
	if err != nil {
		t.Fatal(err)
	}
	defer cancelFirst()
	second, cancelSecond, err := hub.subscribe("second")
	if err != nil {
		t.Fatal(err)
	}
	defer cancelSecond()

	event := session.SessionEvent{SessionID: "first", Type: session.EventSessionTitleUpdated}
	if err := hub.publish(event); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-first.events:
		if got.SessionID != event.SessionID || got.Type != event.Type {
			t.Fatalf("got event %+v, want %+v", got, event)
		}
	case <-time.After(time.Second):
		t.Fatal("event not delivered")
	}
	select {
	case got := <-second.events:
		t.Fatalf("event sent to wrong session: %+v", got)
	default:
	}
	cancelFirst()
	if _, ok := <-first.events; ok {
		t.Fatal("unsubscribed channel remains open")
	}
}

func TestSessionEventHubClose(t *testing.T) {
	var hub sessionEventHub
	sub, cancel, err := hub.subscribe("session")
	if err != nil {
		t.Fatal(err)
	}
	hub.close()
	hub.close()
	cancel()
	select {
	case <-sub.shutdown:
	default:
		t.Fatal("closing the hub did not wake the subscriber")
	}
	if _, _, err := hub.subscribe("session"); err == nil {
		t.Fatal("closed hub accepted a subscriber")
	}
	if err := hub.publish(session.SessionEvent{SessionID: "session"}); err == nil {
		t.Fatal("closed hub accepted an event")
	}
}

func TestSessionEventHubShutdownDrainsRunEvents(t *testing.T) {
	var hub sessionEventHub
	live, stop, err := hub.subscribe("session")
	if err != nil {
		t.Fatal(err)
	}
	run := &testEventSubscription{events: make(chan session.SessionEvent, 1), closed: make(chan struct{})}
	sub := newSessionEventSubscription(run, live, stop)
	defer sub.Close()
	hub.close()
	run.events <- session.SessionEvent{Type: session.EventRunFinished}
	close(run.events)
	select {
	case ev, ok := <-sub.Events():
		if !ok || ev.Type != session.EventRunFinished {
			t.Fatalf("got %+v, open=%t", ev, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("run event not drained")
	}
}

func TestSessionEventHubShutdownDoesNotBlockOnSlowSubscriber(t *testing.T) {
	var hub sessionEventHub
	live, stop, err := hub.subscribe("session")
	if err != nil {
		t.Fatal(err)
	}
	run := &testEventSubscription{events: make(chan session.SessionEvent), closed: make(chan struct{})}
	sub := newSessionEventSubscription(run, live, stop)
	defer sub.Close()
	for i := 0; i <= sessionEventBuffer; i++ {
		select {
		case run.events <- session.SessionEvent{Seq: uint64(i + 1)}:
		case <-time.After(time.Second):
			t.Fatal("could not fill subscription")
		}
	}
	hub.close()
	select {
	case <-run.closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown left blocked subscriber")
	}
}

func TestSessionEventHubDisconnectsBlockedMergedSubscriber(t *testing.T) {
	var hub sessionEventHub
	live, stop, err := hub.subscribe("session")
	if err != nil {
		t.Fatal(err)
	}
	run := &testEventSubscription{events: make(chan session.SessionEvent), closed: make(chan struct{})}
	sub := newSessionEventSubscription(run, live, stop)
	defer sub.Close()
	for i := 0; i <= sessionEventBuffer; i++ {
		select {
		case run.events <- session.SessionEvent{SessionID: "session"}:
		case <-time.After(time.Second):
			t.Fatal("could not fill merged subscription")
		}
	}
	for i := 0; i <= sessionEventBuffer; i++ {
		if err := hub.publish(session.SessionEvent{SessionID: "session"}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-run.closed:
	case <-time.After(time.Second):
		t.Fatal("blocked merged subscriber was not disconnected")
	}
}

type testEventSubscription struct {
	events chan session.SessionEvent
	closed chan struct{}
	once   sync.Once
}

func (s *testEventSubscription) Events() <-chan session.SessionEvent { return s.events }
func (s *testEventSubscription) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func TestSessionEventHubDisconnectsSlowSubscribers(t *testing.T) {
	var hub sessionEventHub
	ch, cancel, err := hub.subscribe("session")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	for i := 0; i <= sessionEventBuffer; i++ {
		if err := hub.publish(session.SessionEvent{SessionID: "session"}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < sessionEventBuffer; i++ {
		<-ch.events
	}
	select {
	case _, ok := <-ch.events:
		if ok {
			t.Fatal("slow subscriber still open")
		}
	case <-time.After(time.Second):
		t.Fatal("slow subscriber was not disconnected")
	}
	if err := hub.publish(session.SessionEvent{SessionID: "session"}); err != nil {
		t.Fatal(err)
	}
}
