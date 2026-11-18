package agent

import (
	"sync/atomic"
	"testing"
)

type countingCloseJournal struct {
	memEventJournal
	closes atomic.Int32
}

func (j *countingCloseJournal) Close() error { j.closes.Add(1); return nil }

type blockingCloseJournal struct {
	countingCloseJournal
	entered chan struct{}
	release chan struct{}
}

func (j *blockingCloseJournal) SeqRange() (uint64, uint64, bool, error) {
	close(j.entered)
	<-j.release
	return 0, 0, false, nil
}

func TestCloseEventsWaitsForHubConstruction(t *testing.T) {
	j := &blockingCloseJournal{entered: make(chan struct{}), release: make(chan struct{})}
	rt := NewSessionRuntime("session", NewSession(SessionDeps{RunEvents: j}))
	constructed := make(chan error, 1)
	go func() { _, err := rt.EnsureHub(); constructed <- err }()
	<-j.entered
	closed := make(chan struct{})
	go func() { rt.CloseEvents(); close(closed) }()
	close(j.release)
	if err := <-constructed; err != nil {
		t.Fatal(err)
	}
	<-closed
	if got := j.closes.Load(); got != 1 {
		t.Fatalf("closes = %d", got)
	}
	if _, err := rt.EnsureHub(); err == nil {
		t.Fatal("hub reopened after close")
	}
}

func TestCloseEventsRejectsLateHubCreation(t *testing.T) {
	j := &countingCloseJournal{}
	rt := NewSessionRuntime("session", NewSession(SessionDeps{RunEvents: j}))
	rt.CloseEvents()
	if _, err := rt.EnsureHub(); err == nil {
		t.Fatal("created hub after event journal closed")
	}
	if got := j.closes.Load(); got != 1 {
		t.Fatalf("closes = %d", got)
	}
}

func TestCloseEventsClosesJournalOnce(t *testing.T) {
	for _, withHub := range []bool{false, true} {
		j := &countingCloseJournal{}
		rt := NewSessionRuntime("session", NewSession(SessionDeps{RunEvents: j}))
		if withHub {
			if _, err := rt.EnsureHub(); err != nil {
				t.Fatal(err)
			}
		}
		rt.CloseEvents()
		rt.CloseEvents()
		if got := j.closes.Load(); got != 1 {
			t.Errorf("withHub=%t closes=%d, want 1", withHub, got)
		}
	}
}
