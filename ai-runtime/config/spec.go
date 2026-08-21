// Package config 定义 ai-runtime 的运行配置聚合。
//
// LLM/Memory 是 runtime 策略配置；Sandbox/Session/MCP 是装配配置。
// YAML 解析与路径中枢由 daemon 负责，本包只做类型与默认值。
package config

import (
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-sandbox"
)

// LLMConfig 是 LLM 提供商连接配置。
type LLMConfig struct {
	Endpoint      string  `yaml:"endpoint"`
	Model         string  `yaml:"model"`
	APIKey        string  `yaml:"api_key"`
	ContextWindow int     `yaml:"context_window"` // 模型上下文窗口（token），0 = 未知
	CompressAt    float64 `yaml:"compress_at"`    // 历史压缩触发阈值（0-1）：估算用量达窗口该比例时自动压缩，0 = 关闭
	CompactKeep   int     `yaml:"compact_keep"`   // 滚动压缩保留的最近消息条数（原文不压，只压更早的）
	MaxIterations int     `yaml:"max_iterations"` // ReAct 循环最大迭代数（模型往返次数），0 = 默认 15
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
