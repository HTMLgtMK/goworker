package middlewares

import (
	"testing"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
)

func TestIterationMiddleware_FiresOnBeforeModel(t *testing.T) {
	fired := 0
	m := NewIterationMiddleware(func() { fired++ })

	m.OnBeforeModel(&core.BeforeModelEvent{})

	if fired != 1 {
		t.Errorf("publish called %d times, want 1", fired)
	}
}

func TestIterationMiddleware_NilPublishSkips(t *testing.T) {
	m := NewIterationMiddleware(nil)
	if r := m.OnBeforeModel(&core.BeforeModelEvent{}); r != nil {
		t.Errorf("unexpected response: %+v", r)
	}
}
