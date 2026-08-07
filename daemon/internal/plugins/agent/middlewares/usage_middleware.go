package middlewares

import (
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
)

// UsageMiddleware 通过观察 AfterModel 事件累计每次 Chat 调用的 token 用量。
// Agent 核心不感知统计存在——横切关注点交给观察者，而非硬编码进 ReAct 循环。
//
// tracker 由外部注入（plugin 持数据，/usage 命令读取），本组件只负责写入——
// 数据与行为分离，middleware 是纯观察者。注册（构造）时刻即统计开始。
//
// 触发时机保证：AfterModel 事件在 messages 追加本轮回复之前分发，
// 所以 ev.History 正是发送给模型的完整 prompt，估算有依据。
type UsageMiddleware struct {
	tracker *core.UsageTracker
	iter    int
	publish func(core.Usage) // 可选：每次记录后回调累计快照（status bar 用），nil 则跳过
}

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
