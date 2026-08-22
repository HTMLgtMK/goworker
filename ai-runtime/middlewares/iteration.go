package middlewares

import (
	"github.com/tinguo/goworker/ai-core/core"
)

type IterationMiddleware struct {
	publish func()
}

func NewIterationMiddleware(publish func()) *IterationMiddleware {
	return &IterationMiddleware{publish: publish}
}

func (m *IterationMiddleware) Name() string { return "iteration" }

func (m *IterationMiddleware) OnBeforeModel(*core.BeforeModelEvent) *core.MiddlewareResponse {
	if m.publish != nil {
		m.publish()
	}
	return nil
}
