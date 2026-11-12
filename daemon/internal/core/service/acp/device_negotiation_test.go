package acp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
)

type lateProbeReporter struct {
	capsReporter
	started chan struct{}
	release chan struct{}
}

func (r *lateProbeReporter) Call(_ context.Context, _ string, _ any, result any) error {
	close(r.started)
	<-r.release
	return json.Unmarshal([]byte(`{"tools":[{"name":"vibrate","schema":"{\"type\":\"object\"}"}]}`), result)
}

func TestDeviceNegotiationInFlightTimeoutRejectsLateProbe(t *testing.T) {
	h := &FrontendHandler{toolsReady: make(chan struct{})}
	reporter := &lateProbeReporter{
		capsReporter: capsReporter{caps: decodeCaps(t, `{"x-device":{}}`)},
		started:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	done := make(chan struct{})
	go func() {
		h.InitializeReady(context.Background(), reporter)
		close(done)
	}()
	<-reporter.started
	ctx, cancel := context.WithTimeout(context.Background(), 2*acpworker.ProbeTimeout)
	defer cancel()
	if !h.waitDeviceTools(ctx) {
		close(reporter.release)
		<-done
		t.Fatal("timeout should degrade without closing the connection")
	}
	close(reporter.release)
	<-done
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.deviceCaller != nil || len(h.deviceDescriptors) != 0 {
		t.Fatal("late in-flight probe changed the timed-out negotiation result")
	}
}

func TestDeviceNegotiationTimeoutRejectsLateProbe(t *testing.T) {
	h := &FrontendHandler{toolsReady: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 2*acpworker.ProbeTimeout)
	defer cancel()
	if !h.waitDeviceTools(ctx) {
		t.Fatal("timeout should degrade without closing the connection")
	}

	reporter := &capsReporter{caps: decodeCaps(t, `{"x-device":{}}`)}
	h.InitializeReady(context.Background(), reporter)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.deviceCaller != nil || len(h.deviceDescriptors) != 0 {
		t.Fatal("late probe changed the timed-out negotiation result")
	}
}

func TestDeviceNegotiationCanceledWaitDoesNotSettle(t *testing.T) {
	h := &FrontendHandler{toolsReady: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if h.waitDeviceTools(ctx) {
		t.Fatal("canceled request should not proceed")
	}

	reporter := &capsReporter{caps: decodeCaps(t, `{"x-device":{}}`)}
	h.InitializeReady(context.Background(), reporter)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.deviceCaller == nil || len(h.deviceDescriptors) != 1 {
		t.Fatal("a canceled waiter must not cancel connection negotiation")
	}
}

func TestDeviceNegotiationReadyBeforeTimeout(t *testing.T) {
	h := &FrontendHandler{toolsReady: make(chan struct{})}
	reporter := &capsReporter{caps: decodeCaps(t, `{"x-device":{}}`)}
	h.InitializeReady(context.Background(), reporter)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !h.waitDeviceTools(ctx) {
		t.Fatal("completed negotiation should let the request proceed")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.deviceCaller == nil || len(h.deviceDescriptors) != 1 {
		t.Fatal("successful negotiation did not retain tools")
	}
}
