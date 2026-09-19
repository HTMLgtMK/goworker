# Quickstart

## 前置条件

- 本机已启动 GOWORKER dispatcher，并创建 Unix socket。
- Node.js 22+ 与 npm 可用。
- VS Code 版本满足 extension 的 `engines.vscode` 约束。

默认 socket：

```text
~/.config/goworker/dispatch/acp.sock
```

如果 dispatcher 使用其他本地 Unix socket，在 VS Code 设置中配置 `goworker.socketPath`。该设置只接受文件系统 socket 路径，不接受 `tcp://`、`http://` 或其他 URL。

## 构建并启动 Extension Development Host

```bash
cd extensions/vscode-goworker
npm install
npm run package
```

在 VS Code 打开 `extensions/vscode-goworker`，按 `F5` 启动 Extension Development Host。打开 Activity Bar 的 **GOWORKER**，执行 **GOWORKER: Refresh Tasks**，再点击任意 task。

## 验收路径

1. Tasks 视图显示 dispatcher catalog 返回的任务。
2. 打开 task 后，header 显示 catalog metadata。
3. trace 先重放已有 ACP update；运行中任务继续 live tail。
4. 上滚后，新 update 不强制滚动；点击“新事件”按钮返回底部。
5. 关闭详情 panel 后，原 task 仍继续执行；重新打开同一 task 可重新回放。

Task Detail 目前是只读观察。reply、resume、permission/HITL 还没有 dispatcher capability，UI 不应假装可以发送。
