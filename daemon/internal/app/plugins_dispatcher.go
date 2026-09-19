//go:build !goworker_no_dispatcher

package app

import (
	"path/filepath"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/dispatcher"
)

// registerDispatcher 装配多 agent 调度插件。运行期仍由 dispatch.enabled 把门：
// 构建期剔除（goworker_no_dispatcher）与运行期关闭是两层独立开关——前者裁二进制，
// 后者省资源，默认两层都关着，不影响既有功能。
func registerDispatcher(a *Application) error {
	if !a.Runtime.Dispatch.Enabled {
		return nil
	}
	paths := runtimeconfig.Paths{
		ConfigDir:     config.DefaultDir(),
		SkillsUser:    filepath.Join(config.DefaultDir(), "skills"),
		SkillsProject: filepath.Join(".goworker", "skills"),
		AuditDir:      filepath.Join(config.DefaultDir(), "audit"),
		DispatchDir:   filepath.Join(config.DefaultDir(), "dispatch"),
	}
	return a.Engine.Register(dispatcher.NewPlugin(a.Runtime, paths))
}
