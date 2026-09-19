//go:build goworker_no_dispatcher

package app

// registerDispatcher 空实现：-tags goworker_no_dispatcher 编译期剔除调度插件，
// 二进制不含多 agent 派发能力（/dispatch /workers 命令族随之消失）。
func registerDispatcher(_ *Application) error {
	return nil
}
