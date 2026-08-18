// Package config 定义 ai-runtime 的运行配置聚合。
//
// 引擎段(LLM/Memory)来自 ai-core/config；装配段(Sandbox/Session/MCP)由本包聚合。
// YAML 解析与路径中枢由 daemon 负责，本包只做类型与默认值。
package config

import (
	coreconfig "github.com/tinguo/goworker/ai-core/config"
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-sandbox"
)

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
	LLM     coreconfig.LLMConfig    `yaml:"llm"`
	Memory  coreconfig.MemoryConfig `yaml:"memory"`
	Sandbox sandbox.SandboxConfig   `yaml:"sandbox"`
	Session SessionConfig           `yaml:"session"`
	MCP     MCPConfig               `yaml:"mcp"`
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
