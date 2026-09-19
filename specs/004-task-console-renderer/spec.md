# Task Console Renderer

## Context

GOWORKER 的 stdin frontend 只能看摘要，无法把一个已运行任务的 ACP 现场完整还原到编辑器里。Task Console 让 VS Code extension 经 Unix socket 查询任务 catalog，并把 Task Detail 作为独立 observer session 挂到已有 `--attach <task_id>` 上。

## Goals

- Activity Bar 中提供 Tasks、Workers、Runtime 入口。
- Task Detail 在 editor WebviewPanel 渲染完整 thought、message、tool 与未知 ACP update。
- catalog 与 trace 分离：`--console/*` 提供 metadata，`--attach` 提供 durable replay + live tail。
- Task Detail 在没有 reply/resume API 前明确只读。

## Non-goals

- 不迁移 daemon 到 extension host。
- 不暴露 HTTP、WebSocket 或 TCP listener。
- 不实现 permission/HITL、approve/reject 或 task resume。
- 不将人类文本命令输出作为 UI 协议。

## Validation

- Go：`go test ./daemon/internal/dispatcher ./ai-dispatch/task` 与 race detector。
- Extension：安装依赖后运行 `npm run typecheck`、`npm test`、`npm run package`。
- 手动：启动 dispatcher，打开 GOWORKER sidebar，进入 task，确认历史回放、live tail、关闭 panel 后任务仍持续执行。
