package middlewares

import (
	"github.com/tinguo/goworker/ai-core/core"
)

// NewUsageMiddleware 创建一个 token 用量观察者，记录到外部注入的 tracker。
// publish 为 nil 时不发布事件。
// iter 每 Run 归零：Usage.Iteration 语义是"本次 /agent 内的第几轮"，跨 query 累计的
// 调用序号由 /usage 按 slice index 显示，不由本组件承担。
func NewUsageMiddleware(tracker *core.UsageTracker, publish func(core.Usage)) *UsageMiddleware {
	return &UsageMiddleware{tracker: tracker, publish: publish}
}

func (m *UsageMiddleware) Name() string { return "usage" }

func (m *UsageMiddleware) OnAfterModel(ev *core.AfterModelEvent) *core.MiddlewareResponse {
	// 失败的调用不进账——统计只记真实发生的模型调用
	if ev.Err != nil {
		return nil
	}
	// 模型没返回 usage 时用发送前 messages 粗估（与压缩预检同一套估算）
	var estimate int
	if ev.Usage == nil {
		estimate = core.EstimateTokens(ev.History)
	}
	m.tracker.Record(m.iter, estimate, ev.Usage)
	m.iter++
	if m.publish != nil {
		m.publish(m.tracker.Snapshot())
	}
	return nil
}
