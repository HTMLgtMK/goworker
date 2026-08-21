// Package middlewares 提供 runtime 级 agent middleware：安全策略、记忆注入、压缩、用量与迭代事件。
package middlewares

import (
	"context"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-sandbox"
)

// HITLMiddleware 通过 DecisionProvider 对接沙箱检查，拦截风险工具调用。
// 覆盖两类工具：
//   - bash：走 sandbox 正则规则（deny 直接拒绝 / risky 需要确认）
//   - mcp_*：外部进程，sandbox 约束不到 —— strict/readonly 直接拒绝，normal 走 HITL 确认
type HITLMiddleware struct {
	sandboxCfg       sandbox.Config
	decisionProvider hitl.DecisionProvider
	audit            *sandbox.AuditLogger // nil = 不审计
}

type HITLOption func(*HITLMiddleware)

const MemoryBlockPrefix = "[记忆]"

type MemoryTask struct {
	ID        string
	Title     string
	Summary   string
	NextSteps []string
}

type MemoryFact struct {
	Topic   string
	Content string
}

type MemoryResult struct {
	Task  *MemoryTask
	Fact  *MemoryFact
	Score float64
}

type MemoryClient interface {
	Search(ctx context.Context, query string, taskTopK, factTopK int) ([]MemoryResult, error)
}

type MemoryMiddleware struct {
	client   MemoryClient
	cfg      runtimeconfig.MemoryConfig
	window   int
	injected bool
}

type CompressionMiddleware struct {
	compressor *core.Compressor
	window     int
	compressAt float64
	done       bool
}

type IterationMiddleware struct {
	publish func()
}

type UsageMiddleware struct {
	tracker *core.UsageTracker
	iter    int
	publish func(core.Usage)
}
