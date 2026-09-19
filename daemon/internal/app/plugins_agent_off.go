//go:build goworker_no_agent

package app

import (
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/core"
)

// registerAgent 空实现：-tags goworker_no_agent 在编译期剔除 agent 插件，
// daemon 退化为最小壳（引擎 + 内置命令 + stdin 前端），chat/工具等能力不可用。
func registerAgent(_ *core.Engine, _ *runtimeconfig.Config, _ *logger.Logger) error {
	return nil
}
