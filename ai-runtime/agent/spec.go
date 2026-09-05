// Package agent 是 ai-runtime 的 agent SDK：把 ai-core 引擎 + 工具装配 + 记忆 +
// 沙箱策略 + 会话持久化组装成可复用的 Session，宿主（daemon）自行做 plugin 封装。
//
// 类型定义集中在本文件；实现分布在 session.go/checkpoint.go/memory_adapter.go 等。
package agent

import (
	"sync"

	"github.com/tinguo/goworker/ai-core/core"
	memory "github.com/tinguo/goworker/ai-memory"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/session"
	sandbox "github.com/tinguo/goworker/ai-sandbox"
)

type RunRequest struct {
	Input string
}

type RenderKind string

const (
	KindText       RenderKind = "text"
	KindThinking   RenderKind = "thinking"
	KindToolCall   RenderKind = "tool_call"
	KindToolResult RenderKind = "tool_result"
)

type RunCallbacks struct {
	Write      func(string)
	WriteToken func(kind RenderKind, content string)
	Decide     func(*hitl.InterruptRequest) hitl.Decision
	Publish    func(event string, data any)
}

// Session 是一段会话：收敛会话状态（conversation/usage）与核心操作（Run/Compact/…）。
// NewSession 创建会话（Init / /new 各一次）；/new 用新实例替换旧实例，旧会话状态随对象回收。
type Session struct {
	mu           sync.Mutex
	conversation []core.Message
	usage        *core.UsageTracker
	audit        *sandbox.AuditLogger // 命令决策审计：配置开启 + 首次 Run 惰性打开，会话生命周期复用

	deps SessionDeps
}

// SessionDeps 是会话构造输入包：插件级资源 + 本会话参数，Init 组装后每次 NewSession 复用。
// 其余字段跨会话不变。可变资源（instructions）的装载在 plugin 层，这里只收最终值。
type SessionDeps struct {
	Config       *runtimeconfig.Config                                  // 运行配置（含 Sandbox 段）
	AuditDir     string                                                 // sandbox 审计落盘目录
	Memory       *memory.Client                                         // nil = 禁用
	CollectTools func(cfg *sandbox.Config) []core.Tool                  // 方法值捕获 p，按需收集工具
	NewProvider  func(cfg *runtimeconfig.Config) (core.Provider, error) // 按当前 default_provider 构造协议适配器
	Instruction  *memory.InstructionSet                                 // 会话边界刷新（Init / /new 经 startSession 重载）
	Store        *session.Store                                         // nil = 持久化禁用
}
