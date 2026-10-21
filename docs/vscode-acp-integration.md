# VS Code 扩展接入 daemon ACP（对接文档）

面向 `goworker-vscode-extension` 仓库的维护者。daemon 侧已删除 VS Code 专用
前端（原 `daemon/internal/cli/vscode`，监听 `frontend/vscode.sock`），外部
编辑器统一经 app-service 的 ACP socket 接入。本文是扩展侧需要跟进的全部改动。

## Socket 端点

```text
daemon 唯一的编辑器入口：~/.config/goworker/frontend/acp.sock
                          （GOWORKER_CONFIG_DIR 可重定位，扩展侧沿用现有解析逻辑）
已下线：~/.config/goworker/frontend/vscode.sock（不再监听，连接显性失败）
```

## 扩展侧改动（唯一必改点）

`src/extension.ts` 的 `agentSocketPath()`：

```text
fallback: frontend/vscode.sock → frontend/acp.sock
```

用户显式配置了 `goworker.agentSocketPath` 指向旧 `vscode.sock` 的，需要自行
更新或清掉走 fallback——连接失败是显性的，不会静默降级。

## daemon 侧配置变化

- `config.yaml` 的 `frontend.vscode` 段已成死字段：daemon 静默忽略，不报错
  （非严格 yaml 解析）。留着无害，删掉更干净。
- 控制 ACP socket 的配置段是 `frontend.acp`：`enabled`（默认 true）、
  `socket`（完整路径，空 = 按 `~/.config/goworker/frontend/acp.sock` 派生）。

## 协议契约

与 Android 客户端共享同一套语义，完整文档见
`daemon/mobile/PROTOCOL.md`（能力协商、方法面、session/update 流、HITL
request_permission、线程模型）。对 VS Code 的差异点：

- **不需要声明 `clientCapabilities`**：VS Code 是纯标准 ACP 客户端，不实现
  `x-device/*` 设备能力扩展，不发 `clientCapabilities` 即可——daemon 侧
  capability gate 会跳过探测，行为与改造前一致（无 `sys_*` 设备工具）。
- **建议保存 `agentCapabilities`**：initialize 响应含 `loadSession` 等能力
  声明，UI 应按能力降级，不要对能力面做硬编码假设。
- 未来若要给 agent 提供编辑器能力（如 Zed 式 `fs/read_file`）：走
  `x-device` 同款 client-ward 扩展模式（声明 capability key + 实现
  `x-<name>/tools`、`x-<name>/call`），daemon 侧零改动。

## 验证

```bash
# daemon 侧
goworker            # 启动后确认日志输出 acp service listening, socket=.../acp.sock

# 扩展侧
# GOWORKER 面板连接、session/list、session/load（重连重放）、
# slash 补全（available_commands_update）、HITL 授权对话框各过一遍
```
