// Package mobile 是 Android（gobind）入口壳：进程内嵌入 Agent-only ACP worker。
//
// Host 通过 daemon/internal/core/acp 服务 Agent Session，builtin side-effect import
// 确保 Agent Factory 被静态编入 AAR；gomobile 构建默认使用 goworker_no_dispatcher，
// 不会把 Dispatcher 插件带进移动端产物。Android 侧通过 ACP 协议提供文件、权限、
// MCP/Tool 与生命周期适配，不直接暴露 Go 的 context、channel 或函数类型。
package mobile
