//go:build !goworker_no_agent

package app

import (
	"github.com/tinguo/goworker/daemon/internal/core/service"
)

// registerAgent 装配 ai-runtime 的 agent 插件：配置与路径构造函数注入
// （daemon 是路径中枢，ai-runtime 不反向依赖 DefaultDir）。
// 实例回填 a.Agent：vscode 前端的会话清单/load 判定直接落在它上。
func registerAgent(a *Application) error {
	a.Agent = service.NewPlugin(a.Runtime, defaultPaths())
	return a.Engine.Register(a.Agent)
}
