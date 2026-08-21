package middlewares

import (
	"github.com/tinguo/goworker/ai-core/core"
)

// NewIterationMiddleware 创建迭代事件发布器。publish 为 nil 时静默跳过。
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
