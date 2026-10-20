package service

import (
	"errors"
	"strings"
	"testing"
)

type registryTestService struct {
	name     string
	events   *[]string
	startErr error
	stopErr  error
	starts   int
	stops    int
}

func (s *registryTestService) Name() string { return s.name }

func (s *registryTestService) Start() error {
	s.starts++
	*s.events = append(*s.events, "start:"+s.name)
	return s.startErr
}

func (s *registryTestService) Stop() error {
	s.stops++
	*s.events = append(*s.events, "stop:"+s.name)
	return s.stopErr
}

func TestRegistryStartsInRegistrationOrderAndStopsInReverse(t *testing.T) {
	var events []string
	first := &registryTestService{name: "first", events: &events}
	second := &registryTestService{name: "second", events: &events}
	registry := NewRegistry()
	if err := registry.Register(first); err != nil {
		t.Fatalf("Register first: %v", err)
	}
	if err := registry.Register(second); err != nil {
		t.Fatalf("Register second: %v", err)
	}
	if got, ok := registry.Get("first"); !ok || got != first {
		t.Fatalf("Get first = (%v, %v), want (%p, true)", got, ok, first)
	}

	if err := registry.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := registry.Start(); err != nil {
		t.Fatalf("idempotent Start: %v", err)
	}
	if err := registry.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := registry.Stop(); err != nil {
		t.Fatalf("idempotent Stop: %v", err)
	}

	want := []string{"start:first", "start:second", "stop:second", "stop:first"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if first.starts != 1 || second.starts != 1 || first.stops != 1 || second.stops != 1 {
		t.Fatalf("service calls = first(%d/%d), second(%d/%d), want one start/stop each", first.starts, first.stops, second.starts, second.stops)
	}
}

func TestRegistryRejectsDuplicateAndIllegalRegistrations(t *testing.T) {
	var events []string
	registry := NewRegistry()
	service := &registryTestService{name: "same", events: &events}
	if err := registry.Register(nil); err == nil {
		t.Fatal("Register nil: want error")
	}
	if err := registry.Register(&registryTestService{events: &events}); err == nil {
		t.Fatal("Register unnamed service: want error")
	}
	if err := registry.Register(service); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := registry.Register(&registryTestService{name: "same", events: &events}); err == nil {
		t.Fatal("duplicate Register: want error")
	}
	if err := registry.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := registry.Register(&registryTestService{name: "late", events: &events}); err == nil {
		t.Fatal("Register after Start: want error")
	}
	if err := registry.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := registry.Register(&registryTestService{name: "later", events: &events}); err == nil {
		t.Fatal("Register after Stop: want error")
	}
}

func TestRegistryRollsBackStartedServicesOnStartFailure(t *testing.T) {
	var events []string
	wantStartErr := errors.New("second unavailable")
	first := &registryTestService{name: "first", events: &events}
	second := &registryTestService{name: "second", events: &events, startErr: wantStartErr}
	third := &registryTestService{name: "third", events: &events}
	registry := NewRegistry()
	for _, service := range []*registryTestService{first, second, third} {
		if err := registry.Register(service); err != nil {
			t.Fatalf("Register %s: %v", service.name, err)
		}
	}

	err := registry.Start()
	if !errors.Is(err, wantStartErr) {
		t.Fatalf("Start error = %v, want %v", err, wantStartErr)
	}
	if !strings.Contains(err.Error(), "second") {
		t.Fatalf("Start error = %q, want service name", err)
	}
	want := []string{"start:first", "start:second", "stop:first"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("rollback events = %v, want %v", events, want)
	}
	if third.starts != 0 || third.stops != 0 || first.stops != 1 {
		t.Fatalf("service calls after rollback = first(%d/%d), second(%d/%d), third(%d/%d)", first.starts, first.stops, second.starts, second.stops, third.starts, third.stops)
	}

	if secondErr := registry.Start(); !errors.Is(secondErr, wantStartErr) {
		t.Fatalf("second Start error = %v, want original error", secondErr)
	}
	if len(events) != len(want) {
		t.Fatalf("second Start invoked services, events = %v", events)
	}
	if err := registry.Stop(); err != nil {
		t.Fatalf("Stop after rollback: %v", err)
	}
}

func TestRegistryAggregatesStopErrorsInReverseOrder(t *testing.T) {
	var events []string
	firstErr := errors.New("first stop failed")
	secondErr := errors.New("second stop failed")
	registry := NewRegistry()
	first := &registryTestService{name: "first", events: &events, stopErr: firstErr}
	second := &registryTestService{name: "second", events: &events, stopErr: secondErr}
	if err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(second); err != nil {
		t.Fatal(err)
	}
	if err := registry.Start(); err != nil {
		t.Fatal(err)
	}

	err := registry.Stop()
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("Stop error = %v, want both service errors", err)
	}
	want := []string{"start:first", "start:second", "stop:second", "stop:first"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if err := registry.Stop(); !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("second Stop error = %v, want original errors", err)
	}
	if first.stops != 1 || second.stops != 1 {
		t.Fatalf("second Stop invoked services: first=%d second=%d", first.stops, second.stops)
	}
}

func TestRegistryStopBeforeStartPreventsLaterStart(t *testing.T) {
	var events []string
	service := &registryTestService{name: "service", events: &events}
	registry := NewRegistry()
	if err := registry.Register(service); err != nil {
		t.Fatal(err)
	}
	if err := registry.Stop(); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
	if err := registry.Stop(); err != nil {
		t.Fatalf("second Stop before Start: %v", err)
	}
	if err := registry.Start(); err == nil {
		t.Fatal("Start after Stop: want error")
	}
	if len(events) != 0 {
		t.Fatalf("events = %v, want no service calls", events)
	}
}
