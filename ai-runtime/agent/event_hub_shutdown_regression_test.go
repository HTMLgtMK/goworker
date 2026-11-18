package agent

import (
	"context"
	"testing"

	"github.com/tinguo/goworker/ai-runtime/session"
)

func TestEventHubShutdownDrainsAvailableReplay(t *testing.T) {
	for i := 0; i < 50; i++ {
		hub := mustNewEventHub(t, "session", nil)
		if err := hub.Close(); err != nil {
			t.Fatal(err)
		}
		out := make(chan session.SessionEvent, 1)
		live := make(chan session.SessionEvent)
		close(live)
		sub := &hubSub{live: live, dead: make(chan struct{})}
		hub.pumpEvents(context.Background(), 0, out, sub, []session.SessionEvent{{Seq: 1, Type: session.EventRunFinished}})
		if ev, ok := <-out; !ok || ev.Type != session.EventRunFinished {
			t.Fatalf("iteration %d: dropped event %+v", i, ev)
		}
	}
}
