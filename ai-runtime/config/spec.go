// Package config 定义 ai-runtime 的运行配置聚合。
//
// LLM/Memory 是 runtime 策略配置；Sandbox/Session/MCP 是装配配置。
// YAML 解析与路径中枢由 daemon 负责，本包只做类型与默认值。
package config

import (
	"github.com/tinguo/goworker/ai-core/core"
	sandbox "github.com/tinguo/goworker/ai-sandbox"
)

// ThinkingRequestMode 指定 OpenAI-compatible 请求启用推理的方言。
type ThinkingRequestMode string

const (
	ThinkingRequestAuto   ThinkingRequestMode = "auto"
	ThinkingRequestEnable ThinkingRequestMode = "enable_thinking"
	ThinkingRequestEffort ThinkingRequestMode = "reasoning_effort"
)

// ThinkingEffort 是 reasoning_effort 方言的推理强度。
type ThinkingEffort string

const (
	ThinkingEffortLow    ThinkingEffort = "low"
	ThinkingEffortMedium ThinkingEffort = "medium"
	ThinkingEffortHigh   ThinkingEffort = "high"
)

type ThinkingConfig struct {
	Show bool `yaml:"show"`
}

// ProviderThinkingConfig 是 OpenAI-compatible 请求侧的推理方言配置。
type ProviderThinkingConfig struct {
	RequestMode ThinkingRequestMode `yaml:"request_mode"`
	Effort      ThinkingEffort      `yaml:"effort"`
}

// ProviderType 标识 Provider 使用的协议适配器。
type ProviderType string

const (
	ProviderTypeOpenAI    ProviderType = "openai"
	ProviderTypeAnthropic ProviderType = "anthropic"
)

// AnthropicAuthType 明确 Anthropic Messages 的认证头方言。
type AnthropicAuthType string

const (
	AnthropicAuthBearer AnthropicAuthType = "bearer"
	AnthropicAuthAPIKey AnthropicAuthType = "x-api-key"
)

// ProviderConfig 是一个命名 Provider 的连接与协议配置。
type ProviderConfig struct {
	Type          ProviderType           `yaml:"type"`
	Endpoint      string                 `yaml:"endpoint"`
	Model         string                 `yaml:"model"`
	APIKey        string                 `yaml:"api_key"`
	ContextWindow int                    `yaml:"context_window"`
	Thinking      ProviderThinkingConfig `yaml:"thinking,omitempty"`
	AuthType      AnthropicAuthType      `yaml:"auth_type,omitempty"`
	MaxTokens     int                    `yaml:"max_tokens,omitempty"`
}

// LLMConfig 保存 Provider 注册表与全局 Agent/UI 策略。
type LLMConfig struct {
	DefaultProvider string                    `yaml:"default_provider"`
	Providers       map[string]ProviderConfig `yaml:"providers"`
	CompressAt      float64                   `yaml:"compress_at"`
	CompactKeep     int                       `yaml:"compact_keep"`
	MaxIterations   int                       `yaml:"max_iterations"`
	Thinking        ThinkingConfig            `yaml:"thinking"`
}

// MemoryConfig 是 agent 记忆模块（MTM 任务档案 + LTM 事实条目）的配置。
// 声明式指令层（USER.md + AGENTS.md）与记忆组件同开关：Enabled=false 时两者都关。
type MemoryConfig struct {
	Dir               string  `yaml:"dir"`                 // 存储目录，默认 <DefaultDir>/memory
	Enabled           bool    `yaml:"enabled"`             // false = 整个记忆模块关闭
	TaskKeep          int     `yaml:"task_keep"`           // 保留任务档案数，0 = 不裁剪
	TaskInjectN       int     `yaml:"task_inject_n"`       // 会话边界时注入最近 N 个未完成任务
	LtmInjectTopK     int     `yaml:"ltm_inject_top_k"`    // 会话边界时注入相关事实条数
	LtmExtract        bool    `yaml:"ltm_extract"`         // 检查点固化时是否 LLM 抽取 LTM
	InjectBudgetRatio float64 `yaml:"inject_budget_ratio"` // 注入块占 context 窗口的比例上限（0-1）
	UserMaxChars      int     `yaml:"user_max_chars"`      // USER.md 画像容量上限（rune），超限 profile 工具报错
	AgentsMaxChars    int     `yaml:"agents_max_chars"`    // AGENTS.md（全局+项目合并）注入上限，超出截断
}

// SessionConfig 是会话持久化模块的配置。
type SessionConfig struct {
	Dir     string `yaml:"dir"`     // 会话 jsonl 存储目录
	Enabled bool   `yaml:"enabled"` // false = 会话持久化关闭，走纯内存逻辑
}

// MCPConfig 是 MCP server 连接配置。
type MCPConfig struct {
	Servers []MCPServer `yaml:"servers"` // 空 = 不连接任何 server
}

// MCPServer 描述一个 stdio MCP server 连接。
type MCPServer struct {
	Name    string   `yaml:"name"`           // 唯一标识，同时作工具名前缀
	Command string   `yaml:"command"`        // 可执行文件路径或命令名
	Args    []string `yaml:"args,omitempty"` // 传给进程的参数
}

// Config 是 ai-runtime 的运行配置聚合。
type Config struct {
	LLM     LLMConfig             `yaml:"llm"`
	Memory  MemoryConfig          `yaml:"memory"`
	Sandbox sandbox.SandboxConfig `yaml:"sandbox"`
	Session SessionConfig         `yaml:"session"`
	MCP     MCPConfig             `yaml:"mcp"`
}

// Paths 是宿主注入的目录路径，避免 ai-runtime 反向依赖 daemon 的 DefaultDir。
type Paths struct {
	ConfigDir     string // 配置目录（LoadInstructions 用）
	SkillsUser    string // 用户级 skills 目录
	SkillsProject string // 项目级 skills 目录
	AuditDir      string // sandbox 审计落盘目录
}

// 状态栏事件契约。statusbar 是前端 UI，不进 SDK；前端 addon 订阅这些事件名与载荷。
const (
	EventUsage     = "usage"
	EventIteration = "iteration"
)

// UsageEvent 是 EventUsage 的载荷：usage 快照 + 上下文窗口。
type UsageEvent struct {
	Usage         core.Usage
	ContextWindow int
}
