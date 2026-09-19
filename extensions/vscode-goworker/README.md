# vscode-goworker

GOWORKER 的 VS Code 扩展：Agent Chat（ACP over Unix socket）、Chats 会话树
（session/list + load 回放）、任务面板（经 dispatcher socket 提交/审批）。

## 构建依赖：@htmlgtmk/acp-ui 本地联动（有意为之）

`package.json` 里 `@htmlgtmk/acp-ui` 是 `file:../../../vscode-acp-ui/dist-sdk`
的本地链接，指向仓库外的姊妹项目（与 `@agentclientprotocol/sdk` 一并由它提供）。

**这是决策不是疏漏**（2026-09-19）：SDK 处于快速演进期，保持本地联动省去发版
同步；代价是新 clone 需要 `../vscode-acp-ui` 先构建（`npm ci` 会失败），CI 不受
影响（ci.yml 只构建 go.work 的 Go 模块）。SDK 稳定后再评估 vendor 进仓库或改
registry 安装。

## Socket 路径解析

设置 `goworker.agentSocketPath` / `goworker.socketPath` 覆盖 → 否则依次
`GOWORKER_CONFIG_DIR`（与 daemon 对齐；注意 GUI 启动的 VS Code 可能不继承）
→ `~/.config/goworker/<frontend|dispatch>/...`。
