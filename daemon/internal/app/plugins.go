// 插件装配链（composition root）：决定哪些内置插件被编进二进制、以什么依赖
// 注册进 engine。壳层（cmd/goworker、mobile）经 app.New 间接使用，不感知具体插件。
//
// 每个可选插件对应一对构建标签文件：plugins_<name>.go（//go:build !goworker_no_<name>）
// 与 plugins_<name>_off.go（//go:build goworker_no_<name>）。off-tag 缺省不生效，
// 所以裸 go build（CI/release/README 流程）始终全量装配；CMake 的 GOWORKER_NO_PLUGINS
// 参数负责把要剔除的插件翻译成对应 off-tag 传给 go build。
// 新增插件：加一对文件 + 在 RegisterPlugins 里追加一段装配调用。
package app

import "fmt"

// RegisterPlugins 按序装配编译进二进制的插件。
// a.Agent 在 registerAgent 里回填：vscode 前端要拿它做会话清单/load（最小壳时为 nil）。
func RegisterPlugins(a *Application) error {
	if err := registerAgent(a); err != nil {
		return fmt.Errorf("agent: %w", err)
	}
	if err := registerDispatcher(a); err != nil {
		return fmt.Errorf("dispatcher: %w", err)
	}
	return nil
}
