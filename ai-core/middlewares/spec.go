// Package middlewares 提供 agent ReAct 循环的引擎级中间件。
//
// 只依赖 ai-core/core + ai-core/spec + ai-core/config；
// 依赖 sandbox 的 HITL 中间件（安全策略）在 ai-runtime/middlewares。
package middlewares

import (
	"context"

	"github.com/tinguo/goworker/ai-core/config"
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-core/spec"
)

// MemoryBlockPrefix 是注入记忆块的 system 消息前缀。plugin 把 messages 写回
// STM（conversation）时用它过滤 —— 记忆块只服务本次 run 的注入，若漏进 STM
// 会被下次 run 重发、甚至被检查点当对话内容固化（自指污染）。
const MemoryBlockPrefix = "[记忆]"

// ---- 记忆注入 ----

// MemoryTask 是检索到的相关任务条目（ai-memory 具体类型的本地投影）。
type MemoryTask struct {
	ID        string
	Title     string
	Summary   string
	NextSteps []string
}

// MemoryFact 是检索到的长期事实条目（ai-memory 具体类型的本地投影）。
type MemoryFact struct {
	Topic   string
	Content string
}

// MemoryResult 是记忆检索的单条结果：至多一个 Task/Fact 非 nil。
type MemoryResult struct {
	Task  *MemoryTask
	Fact  *MemoryFact
	Score float64
}

// MemoryClient 是 middleware 对记忆组件的最小依赖面：每次 query 统一检索。
// 保持接口小 —— ai-runtime 用 adapter 包装 ai-memory 的 *Client 满足它。
type MemoryClient interface {
	Search(ctx context.Context, query string, taskTopK, factTopK int) ([]MemoryResult, error)
}

// MemoryMiddleware 每次用户 query 都从记忆组件检索相关条目（open task + LTM 事实）
// 作为一条 system 消息注入 —— 给 agent "这次查询相关的前情提要"。
//
// 每次 /agent 构造一个新实例，injected 保证本次 run 内只注入一次（ReAct 每迭代
// fire OnBeforeModel，重复检索注入既费 token 又污染历史）。
//
// 固化（写入）不在 middleware —— 由 plugin 层的检查点触发（/compact、进程退出、
// /new、/task checkpoint），见 Checkpointer。
type MemoryMiddleware struct {
	client   MemoryClient
	cfg      config.MemoryConfig
	window   int  // 模型上下文窗口，0 = 未知，注入预算用兜底值
	injected bool // 本次 /agent run 只注入一次
}

// ---- HITL 决策通道 ----

// ChannelDecisionProvider 包装 channel 实现 core.DecisionProvider。
// 插件侧的 promptForDecision 将用户决策写入 channel，这里收割。
type ChannelDecisionProvider struct {
	decisions <-chan spec.HITLDecision
}

// ---- 压缩 ----

// CompressionMiddleware 在 BeforeModel 预检请求大小：估算用量达到
// compress_at × context_window 时，用 Compressor 压缩历史并替换即将发送的消息。
//
// 阈值必须放在发请求前，不能等 AfterModel 记账 —— 跟踪到 100% 时下一次
// 请求已经溢出（400），连压缩调用本身都没有发送空间。估算不含工具 schema，
// 天然带余量，0.8 是合理默认。
type CompressionMiddleware struct {
	compressor *core.Compressor
	window     int     // 模型上下文窗口，0 = 未配置，不压缩
	compressAt float64 // 0-1 触发阈值，0 = 关闭
	done       bool    // 压缩后仍超阈值 → 滚动压缩无解，本轮不再重复压（防每轮烧 summarize）
}

// ---- 迭代计数 ----

// IterationMiddleware 每轮 ReAct 迭代发布一次迭代事件（status bar 计数用）。
// 与 UsageMiddleware 同构：发布能力由构造函数注入 —— middleware 事件携带的 Ctx 是
// 纯 stdlib context，够不到 frontend 的 Publish 句柄，发布必须由插件层在构造时塞入。
//
// 挂 OnBeforeModel：每轮迭代顶部 fire，语义与原 Agent.OnIteration 回调一致
// （同一迭代位置，只是从 Agent 字段挪进 middleware 链）。
type IterationMiddleware struct {
	publish func() // 可选：每轮迭代回调，nil 则跳过
}

// ---- 用量观察 ----

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
