// Package app 是 daemon 的插件装配层（composition root）：决定哪些内置插件
// 被编进二进制、以什么依赖注册进 engine。cmd/goworker 只负责入口与生命周期，
// 不感知具体插件。
//
// 每个可选插件对应一对构建标签文件：plugins_agent.go（//go:build !goworker_no_<name>）
// 与 plugins_agent_off.go（//go:build goworker_no_<name>）。off-tag 缺省不生效，
// 所以裸 go build（CI/release/README 流程）始终全量装配；CMake 的 GOWORKER_NO_PLUGINS
// 参数负责把要剔除的插件翻译成对应 off-tag 传给 go build。
// 新增插件：加一对文件 + 在 RegisterPlugins 里追加一段装配调用。
package app

import (
	"fmt"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/core"
)

// RegisterPlugins 按序装配编译进二进制的插件。
func RegisterPlugins(engine *core.Engine, runtimeCfg *runtimeconfig.Config, log *logger.Logger) error {
	if err := registerAgent(engine, runtimeCfg, log); err != nil {
		return fmt.Errorf("agent: %w", err)
	}
	return nil
}
