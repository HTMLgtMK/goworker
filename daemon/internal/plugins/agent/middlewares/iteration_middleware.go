package middlewares

import (
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
)

// IterationMiddleware 每轮 ReAct 迭代发布一次迭代事件（status bar 计数用）。
// 与 UsageMiddleware 同构：发布能力由构造函数注入 —— middleware 事件携带的 Ctx 是
// 纯 stdlib context，够不到 frontend 的 Publish 句柄，发布必须由插件层在构造时塞入。
//
// 挂 OnBeforeModel：每轮迭代顶部 fire，语义与原 Agent.OnIteration 回调一致
// （同一迭代位置，只是从 Agent 字段挪进 middleware 链）。
type IterationMiddleware struct {
	publish func() // 可选：每轮迭代回调，nil 则跳过
}

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
