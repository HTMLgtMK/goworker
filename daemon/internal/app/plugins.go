// 插件装配链（composition root）：决定哪些内置插件被编进二进制、以什么依赖
// 注册进 engine。壳层（cmd/goworker、mobile）经 app.New 间接使用，不感知具体插件。
//
// 每个可选插件对应一对构建标签文件：plugins_<name>.go（//go:build !goworker_no_<name>）
// 与 plugins_<name>_off.go（//go:build goworker_no_<name>）。off-tag 缺省不生效，
// 所以裸 go build（CI/release/README 流程）始终全量装配；CMake 的 GOWORKER_NO_PLUGINS
// 参数负责把要剔除的插件翻译成对应 off-tag 传给 go build。
// 新增插件：加一对文件 + 在 RegisterPlugins 里追加一段装配调用。
package app

import (
	"context"
	"fmt"

	_ "github.com/tinguo/goworker/daemon/internal/builtin"
	"github.com/tinguo/goworker/daemon/internal/core/service"
	"github.com/tinguo/goworker/daemon/internal/pluginregistry"
)

// RegisterPlugins constructs and registers every compiled-in plugin in dependency order.
func RegisterPlugins(a *Application) error {
	descriptors, err := pluginregistry.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshot plugin registry: %w", err)
	}

	deps := pluginregistry.Dependencies{
		Runtime: a.Runtime,
		Paths:   defaultPaths(),
	}
	for _, descriptor := range descriptors {
		plugin, err := descriptor.New(context.Background(), deps)
		if err != nil {
			return fmt.Errorf("construct plugin %q: %w", descriptor.Name, err)
		}
		if plugin == nil {
			continue
		}
		if plugin.Name() != descriptor.Name {
			return fmt.Errorf("plugin factory %q returned %q", descriptor.Name, plugin.Name())
		}
		if err := a.Engine.Register(plugin); err != nil {
			return fmt.Errorf("register plugin %q: %w", descriptor.Name, err)
		}
		if descriptor.Name == "agent" {
			agent, ok := plugin.(*service.AgentPlugin)
			if !ok {
				return fmt.Errorf("plugin %q does not implement AgentPlugin", descriptor.Name)
			}
			a.Agent = agent
		}
	}
	return nil
}
