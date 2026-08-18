// Package middlewares 提供依赖 ai-sandbox 的安全策略中间件。
//
// 引擎级中间件(usage/iteration/compression/memory)在 ai-core/middlewares；
// 本包只放依赖 sandbox 的 HITL 中间件（命令安全决策，属装配层）。
package middlewares

import (
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-sandbox"
)

// HITLMiddleware 通过 DecisionProvider 对接沙箱检查，拦截风险工具调用。
// 覆盖两类工具：
//   - bash：走 sandbox 正则规则（deny 直接拒绝 / risky 需要确认）
//   - mcp_*：外部进程，sandbox 约束不到 —— strict/readonly 直接拒绝，normal 走 HITL 确认
type HITLMiddleware struct {
	sandboxCfg       sandbox.Config
	decisionProvider core.DecisionProvider
	audit            *sandbox.AuditLogger // nil = 不审计
}

// HITLOption 中间件构造选项（变参，保持旧两参调用零改动）。
type HITLOption func(*HITLMiddleware)
