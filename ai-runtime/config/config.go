package config

import (
	coreconfig "github.com/tinguo/goworker/ai-core/config"
	"github.com/tinguo/goworker/ai-sandbox"
)

// Default 返回 ai-runtime 的默认配置。Memory.Dir/Session.Dir 留空，由宿主按配置目录派生。
func Default() *Config {
	return &Config{
		LLM:    coreconfig.DefaultLLM(),
		Memory: coreconfig.DefaultMemory(),
		Sandbox: sandbox.SandboxConfig{
			Mode: "normal",
		},
		Session: SessionConfig{Enabled: true},
	}
}
