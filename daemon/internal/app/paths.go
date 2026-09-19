package app

import (
	"path/filepath"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/app/config"
)

// defaultPaths 返回本实例的目录布局。
// daemon 是路径中枢，ai-runtime 不反向依赖 DefaultDir，靠构造函数注入 ——
// agent/dispatcher 插件与 ACP worker 都从这里拿，路径字面量不落第二处。
func defaultPaths() runtimeconfig.Paths {
	return runtimeconfig.Paths{
		ConfigDir:     config.DefaultDir(),
		SkillsUser:    filepath.Join(config.DefaultDir(), "skills"),
		SkillsProject: filepath.Join(".goworker", "skills"),
		AuditDir:      filepath.Join(config.DefaultDir(), "audit"),
		DispatchDir:   filepath.Join(config.DefaultDir(), "dispatch"),
	}
}
