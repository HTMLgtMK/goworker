//go:build !goworker_no_agent

package app

import (
	"path/filepath"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/core/service"
	"github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/engine"
)

// registerAgent 装配 ai-runtime 的 agent 插件：配置与路径构造函数注入
// （daemon 是路径中枢，ai-runtime 不反向依赖 DefaultDir）。
func registerAgent(engine *engine.Engine, runtimeCfg *runtimeconfig.Config, log *logger.Logger) error {
	paths := runtimeconfig.Paths{
		ConfigDir:     config.DefaultDir(),
		SkillsUser:    filepath.Join(config.DefaultDir(), "skills"),
		SkillsProject: filepath.Join(".goworker", "skills"),
		AuditDir:      filepath.Join(config.DefaultDir(), "audit"),
	}
	return engine.Register(service.NewPlugin(runtimeCfg, paths))
}
