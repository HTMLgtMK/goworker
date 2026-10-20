// Package mobile 是 Android（gobind）入口壳：进程内嵌入 app.Application 后端。
//
// Host 经 daemon/internal/app 的 Application 服务 ACP（完整 Engine + 插件 +
// services 装配，含内置命令与会话持久化），ACP 经 WithACPConnection 走进程内
// 管道。builtin side-effect import 确保 Agent Factory 与内置命令被静态编入 AAR；
// gomobile 构建默认使用 goworker_no_dispatcher，不会把 Dispatcher 插件带进
// 移动端产物。Android 侧通过 ACP 协议提供文件、权限、MCP/Tool 与生命周期适配，
// 不直接暴露 Go 的 context、channel 或函数类型。
package mobile
