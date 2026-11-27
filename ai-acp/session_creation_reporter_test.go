package aiacp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

type updateRecorder struct {
	mu           sync.Mutex
	updates      []string
	firstEntered chan struct{}
	releaseFirst chan struct{}
}

func (r *updateRecorder) Update(_ string, body protocol.SessionUpdateBody) {
	if body.Title == "first" && r.firstEntered != nil {
		close(r.firstEntered)
		<-r.releaseFirst
	}
	r.mu.Lock()
	r.updates = append(r.updates, body.Title)
	r.mu.Unlock()
}

func (*updateRecorder) MessageChunk(string, string) {}
func (*updateRecorder) RequestPermission(context.Context, string, protocol.PermissionRequest) (string, error) {
	return "", nil
}

func (r *updateRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.updates...)
}

func (r *sessionCreationReporter) snapshot() (sessionCreationReporterState, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, len(r.pending)
}

func TestSessionCreationReporterPreservesConcurrentOrder(t *testing.T) {
	recorder := &updateRecorder{firstEntered: make(chan struct{}), releaseFirst: make(chan struct{})}
	reporter := newSessionCreationReporter(context.Background(), recorder)
	reporter.Update("canonical", protocol.SessionUpdateBody{Title: "first"})

	flushed := make(chan struct{})
	go func() {
		reporter.AfterResponseWritten(context.Background())
		close(flushed)
	}()
	select {
	case <-recorder.firstEntered:
	case <-time.After(time.Second):
		t.Fatal("response hook did not flush queued notification")
	}

	updated := make(chan struct{})
	go func() {
		reporter.Update("canonical", protocol.SessionUpdateBody{Title: "second"})
		close(updated)
	}()
	select {
	case <-updated:
		t.Fatal("concurrent update overtook buffered notification")
	case <-time.After(10 * time.Millisecond):
	}
	close(recorder.releaseFirst)
	select {
	case <-updated:
	case <-time.After(time.Second):
		t.Fatal("concurrent update stayed blocked after flush")
	}
	<-flushed

	got := recorder.snapshot()
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("update order = %v, want [first second]", got)
	}
}

func TestSessionCreationReporterDiscardsOnFailedResponse(t *testing.T) {
	recorder := &updateRecorder{}
	reporter := newSessionCreationReporter(context.Background(), recorder)
	reporter.Update("canonical", protocol.SessionUpdateBody{Title: "not-published"})
	reporter.AfterResponseWriteFailed(context.Background())
	reporter.Update("canonical", protocol.SessionUpdateBody{Title: "late"})

	if got := recorder.snapshot(); len(got) != 0 {
		t.Fatalf("updates after failed response = %v, want none", got)
	}
	state, pending := reporter.snapshot()
	if pending != 0 || state != sessionCreationDiscarded {
		t.Fatalf("reporter state = %d with %d pending updates, want discarded and empty", state, pending)
	}
}

func TestSessionCreationReporterDiscardsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reporter := newSessionCreationReporter(ctx, &updateRecorder{})
	reporter.Update("canonical", protocol.SessionUpdateBody{Title: "not-published"})
	cancel()

	deadline := time.Now().Add(time.Second)
	state, pending := reporter.snapshot()
	for state != sessionCreationDiscarded && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		state, pending = reporter.snapshot()
	}
	if state != sessionCreationDiscarded || pending != 0 {
		t.Fatalf("cancel left reporter state %d with %d pending updates", state, pending)
	}
}

func TestSessionCreationReporterRejectsPermissionBeforePublication(t *testing.T) {
	reporter := newSessionCreationReporter(context.Background(), &updateRecorder{})
	if _, err := reporter.RequestPermission(context.Background(), "canonical", protocol.PermissionRequest{}); !errors.Is(err, ErrSessionNotPublished) {
		t.Fatalf("RequestPermission error = %v, want %v", err, ErrSessionNotPublished)
	}
}

type reverseCallTarget struct {
	mu        sync.Mutex
	calls     int
	enterOnce sync.Once
	entered   chan struct{}
	release   chan struct{}
}

func (*reverseCallTarget) Update(string, protocol.SessionUpdateBody) {}
func (*reverseCallTarget) MessageChunk(string, string)               {}
func (*reverseCallTarget) RequestPermission(context.Context, string, protocol.PermissionRequest) (string, error) {
	return "", nil
}

func (r *reverseCallTarget) Call(context.Context, string, any, any) error {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	if r.entered != nil {
		r.enterOnce.Do(func() { close(r.entered) })
		<-r.release
	}
	return nil
}

func (r *reverseCallTarget) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func TestSessionCreationReporterRejectsReverseCallAfterDiscard(t *testing.T) {
	target := &reverseCallTarget{}
	reporter := newSessionCreationReporter(context.Background(), target)
	reporter.AfterResponseWriteFailed(context.Background())

	err := reporter.Call(context.Background(), "x-test/reverse", nil, nil)
	if !errors.Is(err, protocol.ErrClosed) {
		t.Fatalf("Call after discard error = %v, want %v", err, protocol.ErrClosed)
	}
	if target.callCount() != 0 {
		t.Fatalf("underlying reverse RPC calls = %d, want 0", target.callCount())
	}
}

func TestSessionCreationReporterCallCommittedBeforeDiscardMayFinish(t *testing.T) {
	target := &reverseCallTarget{entered: make(chan struct{}), release: make(chan struct{})}
	reporter := newSessionCreationReporter(context.Background(), target)
	result := make(chan error, 1)
	go func() {
		result <- reporter.Call(context.Background(), "x-test/reverse", nil, nil)
	}()

	select {
	case <-target.entered:
	case <-time.After(time.Second):
		t.Fatal("reverse RPC did not reach the underlying caller")
	}
	reporter.AfterResponseWriteFailed(context.Background())
	close(target.release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("reverse RPC committed before discard failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("committed reverse RPC did not finish")
	}

	if err := reporter.Call(context.Background(), "x-test/later", nil, nil); !errors.Is(err, protocol.ErrClosed) {
		t.Fatalf("later reverse RPC error = %v, want %v", err, protocol.ErrClosed)
	}
	if target.callCount() != 1 {
		t.Fatalf("underlying reverse RPC calls = %d, want 1", target.callCount())
	}
}
