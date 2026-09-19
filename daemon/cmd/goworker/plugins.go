package main

import (
	"fmt"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/core"
)

// registerPlugins 按序装配编译进二进制的插件。
//
// 每个可选插件对应一对构建标签文件：plugin_<name>.go（//go:build !goworker_no_<name>）
// 与 plugin_<name>_off.go（//go:build goworker_no_<name>）。off-tag 缺省不生效，
// 所以裸 go build（CI/release/README 流程）始终全量装配；CMake 的 GOWORKER_PLUGINS
// 参数负责把「未选中」的插件翻译成对应 off-tag 传给 go build。
// 新增插件：加一对文件 + 在此追加一段装配调用。
func registerPlugins(engine *core.Engine, runtimeCfg *runtimeconfig.Config, log *logger.Logger) error {
	if err := registerAgent(engine, runtimeCfg, log); err != nil {
		return fmt.Errorf("agent: %w", err)
	}
	return nil
}
