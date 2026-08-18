// Package agent 提供 ReAct 引擎本体：Agent 循环 + middleware 事件分发。
//
// 只依赖抽象(core.Provider/core.Tool/core.Middleware)；工具装配(DefaultTools)、
// 记忆、沙箱策略由上层 ai-runtime 注入。
package agent

import "github.com/tinguo/goworker/ai-core/core"

const defaultMaxIterations = int(^uint(0) >> 1) // math.MaxInt

// Agent 是一个可使用工具的 ReAct Agent。
type Agent struct {
	provider      core.Provider
	tools         []core.Tool
	toolMap       map[string]core.Tool
	middlewares   []core.Middleware
	maxIterations int
	systemPrompt  string
}

// Option 可配置 Agent 的可选行为。
type Option func(*Agent)
