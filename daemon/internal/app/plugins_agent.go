//go:build !goworker_no_agent

package app

import (
	"path/filepath"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/service"
)

// registerAgent 装配 ai-runtime 的 agent 插件：配置与路径构造函数注入
// （daemon 是路径中枢，ai-runtime 不反向依赖 DefaultDir）。
// 实例回填 a.Agent：vscode 前端的会话清单/load 判定直接落在它上。
func registerAgent(a *Application) error {
	paths := runtimeconfig.Paths{
		ConfigDir:     config.DefaultDir(),
		SkillsUser:    filepath.Join(config.DefaultDir(), "skills"),
		SkillsProject: filepath.Join(".goworker", "skills"),
		AuditDir:      filepath.Join(config.DefaultDir(), "audit"),
		DispatchDir:   filepath.Join(config.DefaultDir(), "dispatch"),
	}
	a.Agent = service.NewPlugin(a.Runtime, paths)
	return a.Engine.Register(a.Agent)
}
