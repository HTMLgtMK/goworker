// Package config 定义 ai-runtime 的运行配置聚合。
//
// LLM/Memory 是 runtime 策略配置；Sandbox/Session/MCP 是装配配置。
// YAML 解析与路径中枢由 daemon 负责，本包只做类型与默认值。
package config

import (
	"fmt"
	"strings"

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
	LLM      LLMConfig             `yaml:"llm"`
	Memory   MemoryConfig          `yaml:"memory"`
	Sandbox  sandbox.SandboxConfig `yaml:"sandbox"`
	Session  SessionConfig         `yaml:"session"`
	MCP      MCPConfig             `yaml:"mcp"`
	Dispatch DispatchConfig        `yaml:"dispatch"`
	Frontend FrontendConfig        `yaml:"frontend"`
}

// FrontendConfig 是前端装配配置（当前仅 vscode ACP 入口）。
// 与 DispatchConfig 完全分离：dispatch 的 <DispatchDir>/acp.sock 是无人值守 worker
// 的任务提交入口；这里的 socket 是外部编辑器驱动 daemon 主会话的前端入口，
// 路径与生命周期互不相干。
type FrontendConfig struct {
	Vscode VscodeFrontendConfig `yaml:"vscode"`
}

// VscodeFrontendConfig 是 VS Code ACP 前端入口配置。
type VscodeFrontendConfig struct {
	// Enabled=false 时宿主不装配 socket 前端（默认 true，由 daemon 侧 Defaults 兜底）。
	Enabled bool `yaml:"enabled"`
	// Socket 是完整的 Unix socket 文件路径，仅 net.Listen("unix", path) 使用，
	// 不复用 DispatchDir/acp.sock；空 = 由宿主按其目录约定派生默认路径。
	Socket string `yaml:"socket,omitempty"`
}

// DispatchConfig 是 commit dispatcher 的配置。
// Enabled=false 时 dispatcher 插件不注册，其余字段不生效。
type DispatchConfig struct {
	Enabled       bool           `yaml:"enabled"`
	Workers       []WorkerConfig `yaml:"workers"`
	DefaultWorker string         `yaml:"default_worker,omitempty"` // 空 = 第一个 worker
	MaxParallel   int            `yaml:"max_parallel,omitempty"`   // 0 = 1
	Routes        []RouteConfig  `yaml:"routes,omitempty"`         // 关键词路由，优先于 default_worker
}

// WorkerConfig 描述一个 ACP worker 子进程（claude-agent-acp / codex-acp / goworker acp）。
type WorkerConfig struct {
	Name    string   `yaml:"name"`
	Command string   `yaml:"command"`
	Args    []string `yaml:"args,omitempty"`
	// OnPermission 无人值守时 worker 权限请求的应答策略：
	//   - "deny"（默认，拒绝并记审计）
	//   - "allow"（自动选择首个 allow 类 option）
	//   - "ask"（转给订阅了该任务的 ACP client 由用户裁决；无订阅者时保守拒绝）
	OnPermission string `yaml:"on_permission,omitempty"`
}

// RouteConfig 是关键词路由：prompt 命中任一关键词（大小写不敏感的包含匹配）
// 即派发给指定 worker；多条 route 按声明顺序，首条命中生效。
type RouteConfig struct {
	Keywords []string `yaml:"keywords"`
	Worker   string   `yaml:"worker"`
}

// Validate 校验 dispatch 配置；未启用时仅校验已填写的部分。
func (c DispatchConfig) Validate() error {
	seen := make(map[string]bool, len(c.Workers))
	for i, w := range c.Workers {
		if w.Name == "" {
			return fmt.Errorf("dispatch.workers[%d]: name is required", i)
		}
		if seen[w.Name] {
			return fmt.Errorf("dispatch.workers[%d]: duplicate name %q", i, w.Name)
		}
		if w.Command == "" {
			return fmt.Errorf("dispatch.workers[%d] (%s): command is required", i, w.Name)
		}
		seen[w.Name] = true
	}
	if c.DefaultWorker != "" && !seen[c.DefaultWorker] {
		return fmt.Errorf("dispatch.default_worker %q is not in workers", c.DefaultWorker)
	}
	if c.MaxParallel < 0 {
		return fmt.Errorf("dispatch.max_parallel must be >= 0")
	}
	for _, w := range c.Workers {
		switch w.OnPermission {
		case "", "deny", "allow", "ask":
		default:
			return fmt.Errorf("dispatch.workers[%s]: invalid on_permission %q (deny|allow|ask)", w.Name, w.OnPermission)
		}
	}
	for i, r := range c.Routes {
		if len(r.Keywords) == 0 {
			return fmt.Errorf("dispatch.routes[%d]: keywords is required", i)
		}
		if !seen[r.Worker] {
			return fmt.Errorf("dispatch.routes[%d]: worker %q is not in workers", i, r.Worker)
		}
	}
	if c.Enabled {
		if len(c.Workers) == 0 {
			return fmt.Errorf("dispatch.enabled requires at least one worker")
		}
		if c.DefaultWorker == "" {
			return fmt.Errorf("dispatch.default_worker is required when enabled")
		}
	}
	return nil
}

// ResolveDefaultWorker 返回默认 worker 名：显式配置 > 第一个 worker。
func (c DispatchConfig) ResolveDefaultWorker() (string, error) {
	if c.DefaultWorker != "" {
		return c.DefaultWorker, nil
	}
	if len(c.Workers) > 0 {
		return c.Workers[0].Name, nil
	}
	return "", fmt.Errorf("dispatch: no workers configured")
}

// MatchWorker 关键词路由：返回首个命中的 worker 名；未命中返回 false。
func (c DispatchConfig) MatchWorker(prompt string) (string, bool) {
	lower := strings.ToLower(prompt)
	for _, r := range c.Routes {
		for _, kw := range r.Keywords {
			if kw != "" && strings.Contains(lower, strings.ToLower(kw)) {
				return r.Worker, true
			}
		}
	}
	return "", false
}

// Worker 返回指定名称的 worker 配置。
func (c DispatchConfig) Worker(name string) (WorkerConfig, bool) {
	for _, w := range c.Workers {
		if w.Name == name {
			return w, true
		}
	}
	return WorkerConfig{}, false
}

// Paths 是宿主注入的目录路径，避免 ai-runtime 反向依赖 daemon 的 DefaultDir。
type Paths struct {
	ConfigDir     string // 配置目录（LoadInstructions 用）
	SkillsUser    string // 用户级 skills 目录
	SkillsProject string // 项目级 skills 目录
	AuditDir      string // sandbox 审计落盘目录
	DispatchDir   string // dispatcher 任务持久化目录
}

// 状态栏事件契约。statusbar 是前端 UI，不进 SDK；前端 addon 订阅这些事件名与载荷。
const (
	EventUsage     = "usage"
	EventIteration = "iteration"
	EventPhase     = "phase"
)

// UsageEvent 是 EventUsage 的载荷：usage 快照 + 上下文窗口。
type UsageEvent struct {
	Usage         core.Usage
	ContextWindow int
}

// PhaseKind 标记 PhaseEvent 的语义。
type PhaseKind int

const (
	PhaseBegin PhaseKind = iota // 命令开始执行：addon 据此激活状态栏
	PhaseStage                  // 阶段切换：Label 为阶段名（如"固化记忆中"）
	PhaseEnd                    // 命令执行结束：addon 据此定格并落行
)

// PhaseEvent 是 EventPhase 的载荷：长耗时命令（/compact /new /agent）的阶段进度。
// 命令框架负责 begin/end，命令内部在切换阶段时发 stage。
type PhaseEvent struct {
	Kind  PhaseKind
	Label string
}
