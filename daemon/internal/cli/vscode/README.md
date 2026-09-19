# cli/vscode — VS Code ACP 前端

daemon 主进程内的 VS Code ACP 服务端：在 Unix socket 上服务 ACP 协议（REV_1 子集），
是 vscode-goworker 扩展（Agent Chat 面板 + Chats 会话树）的后端。

与 REPL 共享同一个 Engine 和 agent 插件 —— 扩展里的一次提问和终端里的 `/agent`
落在同一个会话状态上。三条 ACP 链路中它只负责这一条：

| 链路 | 传输 | 服务端位置 |
|---|---|---|
| **本包** | Unix socket `frontend/vscode.sock` | 主 daemon 进程（CLI 壳启动） |
| ACP worker | stdio（`goworker acp`） | dispatcher spawn 的独立进程（`app.RunACPWorker`） |
| 任务提交入口 | Unix socket `<DispatchDir>/acp.sock` | dispatcher 插件 ingress |

## 启用

配置默认开启（`frontend.vscode.enabled: true`），CLI 壳在 `app.New()` 之后自动
`Start()`，启动失败会终止启动而非静默降级。相关配置：

```yaml
frontend:
  vscode:
    enabled: true            # false = 不启动 vscode ACP 前端
    socket: ""               # 留空 = 按 <DefaultDir>/frontend/vscode.sock 派生
```

## 协议面

协议服务复用 `ai-dispatch` 的 ACP server（`dispatch.ServeConn`），本包的
`ingress` 实现 TaskHandler。支持的方法：

- `initialize` —— 能力协商
- `session/list` —— 会话清单（当前会话置顶 + 归档，Chats 树每次刷新走一条
  轻连接，列完即断）
- `session/new` / `session/prompt` —— 新会话与提问；streamed update（工具
  生命周期/思考/正文 token）经 `tokenToUpdate` 映射，与 stdio worker 共用同一份
  映射，保证两条 ACP 入口渲染一致
- `session/load` —— checkpoint 锚定的历史回放（扩展侧重连重放，失败回落新会话）
- `session/cancel` —— 中断当前回合
- HITL：沙箱中间件的 `hitl.InterruptRequest` 经 `FrontendContext.Decide` 译成
  `session/request_permission`（allow_once / reject_once，once 语义）

## 安全

socket 权限 `0600`，仅本机同用户可达；启动时清理陈旧 socket 文件并做同文件
（SameFile）校验，防止符号链接竞态；`Stop` 只删除自己拥有的 socket。

## 与扩展对接

扩展（`extensions/vscode-goworker`）用 `SocketAcpAgentTransport` 连接：

- socket 路径解析：VS Code 设置 `goworker.agentSocketPath` 覆盖 → 回落
  `<config 目录>/frontend/vscode.sock`，与 daemon 侧默认派生对称，两端都尊重
  `GOWORKER_CONFIG_DIR`（多实例时环境变量一致即自动对上）。
- 扩展命令：`goworker.openAgentChat`（聊天面板）、`goworker.newChatSession`、
  `goworker.openChatSession`（走 session/load 重放）、`goworker.reconnect` 等；
  `goworker.autoOpenChat` 控制工作区打开时自动弹面板。
- 最小壳构建（`GOWORKER_NO_PLUGINS=agent`）时 agent 插件不存在，CLI 壳会跳过
  前端启动并告警 —— 会话清单/load 没有承载者。

## 代码地图

- `frontend.go` —— `Frontend` 生命周期（listen 安全加固 / accept loop / Stop）、
  `SessionSource` 接口（由 `core/service` 的 agent 插件实现）
- `ingress`（frontend.go 内）—— ACP TaskHandler：`SetSession`（cwd 落地）、
  `ListSessions`、`LoadSession`、`Run`
- `command_output.go` —— 命令输出包 fenced code block 增量下发
- 会话 load 方案讨论见 `docs/session-load-discussion.md`，整体架构见
  `docs/architecture.md`
