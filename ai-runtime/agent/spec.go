// Package agent 是 ai-runtime 的 agent 插件：把 ai-core 引擎 + 工具装配 + 记忆 +
// 沙箱策略 + 会话持久化组装成 spec.Plugin，宿主一行 NewPlugin(cfg, paths) 接入。
//
// 类型定义集中在本文件；实现分布在 plugin.go/session.go/commands.go/tools.go 等。
package agent

import (
	"sync"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-core/spec"
	"github.com/tinguo/goworker/ai-memory"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/mcp"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-runtime/skills"
	"github.com/tinguo/goworker/ai-sandbox"
)

// AgentPlugin 是 /agent 插件入口，只负责三件事：持有会话状态、注册命令、管理生命周期。
// 配置与路径由 NewPlugin 构造函数注入（spec.Hub.Config 已 any 化，插件不再从 hub 读配置）。
type AgentPlugin struct {
	cfg   *runtimeconfig.Config
	paths runtimeconfig.Paths
	hub   *spec.Hub
	// deps 是 Session 的资源依赖，startSession 每次会话边界全量重建（含指令快照），
	// /new 经同一路径刷新后以新 deps 创建新会话。
	deps SessionDeps
	// session 是当前会话。NewSession 创建（Init 一次 + /new 一次）；/new 用新实例替换，
	// 旧会话状态（conversation/usage）随对象回收。
	session    *Session
	store      *session.Store        // 会话持久化 store，nil = 禁用
	skills     []skills.Skill        // Init 时加载的 skill 清单，注册为 skill_* 工具
	mcpClients map[string]mcp.Client // server name → 连接，Stop 时统一关闭
	mcpTools   []core.Tool           // 从已连接 server 拉取的工具（静态，collectTools 复用）
	memory     *memory.Client        // 记忆组件（MTM+LTM），Init 打开 / Stop 关闭；nil = 禁用
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
	Config       *runtimeconfig.Config                         // 运行配置（含 Sandbox 段）
	AuditDir     string                                        // sandbox 审计落盘目录
	Memory       *memory.Client                                // nil = 禁用
	CollectTools func(cfg *sandbox.Config) []core.Tool         // 方法值捕获 p，按需收集工具
	NewProvider  func(cfg *runtimeconfig.Config) core.Provider // 默认 NewOpenAIProvider，测试注入 fake
	Instruction  *memory.InstructionSet                        // 会话边界刷新（Init / /new 经 startSession 重载）
	Store        *session.Store                                // nil = 持久化禁用
}
