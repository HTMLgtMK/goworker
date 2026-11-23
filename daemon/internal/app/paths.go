package app

import (
	"path/filepath"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/app/config"
)

// FrontendSocketPath 返回 daemon 主会话 ACP socket 路径（配置留空时的派生默认值）。
// registerACPService 与 StatusInfo 都从这里取，字面量不落第二处。
func FrontendSocketPath() string {
	return filepath.Join(config.DefaultDir(), "frontend", "acp.sock")
}

// DispatchSocketPath 返回 dispatcher 任务入口 socket 路径。
// dispatcher 插件经注入的 paths.DispatchDir join 同名字面量取值（见
// plugins/dispatcher/plugin.go），这里经 DefaultDir 派生 —— 同一配置目录下等价。
func DispatchSocketPath() string {
	return filepath.Join(config.DefaultDir(), "dispatch", "acp.sock")
}

// StatusSocketPath 返回 status 查询 socket 路径（`goworker status` 的拨号目标）。
func StatusSocketPath() string {
	return filepath.Join(config.DefaultDir(), "status.sock")
}

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
