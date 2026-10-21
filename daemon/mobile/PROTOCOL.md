# goworker Android 前端协议（ACP over 进程内管道）

Android app 是一个 **ACP Client**，goworker 是 **ACP Agent**。与 VS Code 扩展、
ZCode dispatcher 共享同一套协议语义（`ai-dispatch/protocol`），区别仅在传输层：
不 spawn 子进程，worker 经 gobind 嵌入 app 进程，本包（`daemon/mobile`）只桥接
字节流。

```
┌─────────────────────────── Android App ───────────────────────────┐
│  Kotlin ACP Client (JSON-RPC 编解码 / session 生命周期 / HITL UI)   │
│        ▲ Write(chunk)                │ OnData(chunk) ▼            │
│  ┌─────┴──────────── daemon/mobile (gobind bind 面) ────────────┐  │
│  │      io.Pipe 桥 → app.Application(WithACPConnection)         │  │
│  │      （Engine + 插件 + services）→ ACP service                │  │
│  │      → ConnectionListener（进程内管道，无 Unix socket）        │  │
│  └──────────────────────────┬───────────────────────────────────┘  │
│                             ▼                                      │
│        app-service 后端（ai-runtime Session：LLM/工具/沙箱/HITL）    │
└───────────────────────────────────────────────────────────────────┘
```

## 帧格式

换行分隔的 JSON-RPC 2.0（与 `ai-dispatch/protocol.Conn` 一致，每条消息一行）。
`Host.Write` 负责出方向定界：**一次 Write 一条完整消息**，追加 `\n`；
入方向 `OnData` 是原始字节切片，**可能含半条消息**，客户端必须按行累积组帧。

## 方法面（client → agent）

| 方法 | 说明 |
|---|---|
| `initialize` | 能力协商，首条必发；params 带 `clientCapabilities`（见下节），响应含 `agentCapabilities`（loadSession 等） |
| `session/new` | 建会话，params `{cwd, mcpServers}`；响应 `{sessionId, modes?}` |
| `session/load` | 回放历史会话（checkpoint 锚定，update 先于响应到达）；仅当 agentCapabilities 声明 `loadSession` 时可用 |
| `session/list` | 会话清单（当前 + 归档，`isCurrent` 显式区分） |
| `session/prompt` | 提交一轮对话，阻塞到回合结束返回 stop reason |
| `session/cancel` | **通知**（无响应）：取消进行中的 prompt —— Android 停止按钮发这个 |
| `session/set_mode` | 未注册（method-not-found 诚实失败） |

**单活动会话**：daemon 同一时刻只有一个活动会话。连接 B 发 `session/new`（或对
当前会话 `session/load`）会把连接 A 的会话顶成 superseded，此后 A 再发
`session/prompt` 返回 -32000 错误（消息含 "no longer active"）——prompt 不会静默
串到 B 的工作区里执行。客户端收到该错误后应重新 `session/new` 或 `session/load`
当前会话。

## 能力协商（initialize）

完整报文示例（客户端视角，逐字段可抄）：

```json
→ {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
     "protocolVersion":1,
     "clientCapabilities":{"x-device":{"tools":true}}
   }}
← {"jsonrpc":"2.0","id":1,"result":{
     "protocolVersion":1,
     "agentCapabilities":{"loadSession":true}
   }}
```

**clientCapabilities（client 声明）**

- key 存在性即门控，value 当前不解析 —— `{"x-device":{"tools":true}}` 里的
  `tools` 字段是给未来留的形状，写了不亏。
- 声明 `x-device` = 本客户端实现 `x-device/tools` / `x-device/call`
  （设备能力注册表见「系统能力工具」一节）。
- **不声明的后果（app-service 路径）**：agent 不发探测请求，设备工具静默为零
  —— 不报错、不降级提示，LLM 只是看不到任何 `sys_*` 工具。这是能力协商的
  预期行为（VS Code 等标准客户端就是这条路），但实现了 DeviceTools 的客户端
  忘了声明就是纯丢功能。
- 旧 AAR（ServeACPWorker 路径）不检查此字段：声明了无害、被忽略。

**agentCapabilities（agent 声明）**

- 客户端**必须保存** initialize 响应并据此降级，不要对能力面做硬编码假设：
  - `loadSession` 缺失 → 藏掉/禁用「恢复历史会话」入口，别发 `session/load`
    吃 -32000。
  - 两条 Go 路径的能力面不同（ServeACPWorker 无 load/list；app-service 全有），
    客户端同一份代码要能对着任意一份 AAR 跑。（当前 mobile Host 已切 app-service
    路径；ServeACPWorker 仅剩 stdio worker 形态在用 —— ZCode dispatcher spawn
    的无人值守进程。）

**客户端迁移清单（旧 AAR → capability 门控版 AAR）**

1. initialize params 补 `clientCapabilities: {"x-device": {"tools": true}}`
   —— 唯一必改点。
2. 保存 initialize 响应的 `agentCapabilities`，UI 按能力降级。
3. 其余不动：`x-device/tools` / `x-device/call` 的应答逻辑、-32601 兜底、
   权限对话框语义全部照旧。
4. 新 AAR（app-service 路径）要求客户端声明 `clientCapabilities`（见上节），
   未声明时设备工具静默为零；Host 侧已强制启用 ACP service —— 显式注入连接时
   `frontend.acp.enabled` 开关无效，装配层无视该开关注册 service。

## agent → client

| 方法/通知 | 说明 |
|---|---|
| `session/update`（通知） | 流式增量：`agent_message_chunk` / `agent_thought_chunk`（thinking）/ `tool_call` / `tool_call_update`（状态 pending→completed） |
| `session/request_permission`（请求） | HITL 唯一出口：params 含 `sessionId` / `toolCall{toolCallId,title}` / `options[{optionId,...}]`；客户端弹对话框，返回选中的 optionId；超时/取消一律按拒绝语义 |

## 线程模型

- `OnData` / `OnClose` 来自后台 goroutine —— Kotlin 碰 UI 必须切主线程
- `session/prompt` 是阻塞请求：Kotlin 用独立读循环 + 挂起请求表（id → pending），
  不要在主线程同步等响应
- `session/request_permission` 阻塞 worker 侧直到应答：Kotlin 收到后必须
  **最终**回一个 response（超时由 worker 侧 `ExpiresAt` 兜底判拒），
  否则该轮工具调用永远挂起
- Go 侧全部导出方法有 recover 壳；Kotlin 回调里抛异常同样不会崩 worker

## 生命周期

```
NewHost(configDir, callbacks)   只存 configDir 与回调，不校验配置
                               （GOWORKER_CONFIG_DIR 注入；GOWORKER_SANDBOX_MODE
                                可收紧沙箱门禁，Start 时经 app.New 校验）
  → Start()                     io.Pipe 桥 + app.New 同步装配（配置/日志 →
                                Engine → 插件 → services.Start）
  → initialize → session/new → (session/prompt ⇄ update/permission)* → …
  → Close()                     幂等；之后 OnClose 恰好回调一次
```

配置文件落在 `configDir`（`config.yaml`），LLM endpoint 等可经 ACP 无关路径
预置（app 首启引导生成），或后续经 agent 的命令通道设置。**会话持久化现在真的
生效**：旧 worker 路径 Store=nil 纯内存，重启进程即丢；app-service 路径经
session store 落 JSONL，同一 configDir 重建 Host 后 `session/load` 可恢复。

## 系统能力工具：x-device 扩展（已实现）

设备能力只有 client（app 进程）可调用，走 **client-ward 扩展方法**（Zed `fs/*` 模式）。
方法名带 `x-` 前缀 = goworker 应用层扩展，**不进 `ai-dispatch/protocol` 标准面**；
VS Code/ZCode 等标准客户端不认识 → 自动 method-not-found → 无设备工具，零兼容负担。

```
每次 prompt 的工具收集时（app-service 路径，mobile Host 属于该路径；旧
ServeACPWorker 是 session/new 后一次性探测）：agent ── x-device/tools（探测请求）──→ client
                 ← {tools: [{name, description, schema, risk_level}]}
                 （method-not-found / 超时 / 非法条目 → 视为无能力，不注册工具）

LLM 调 sys_<name> → 统一 HITL 门控（risk_level × 沙箱模式，见下）→ 通过后：
                 agent ── x-device/call {name, arguments} ──→ client 执行
                 ← {result: "<文本结果回给 LLM>"}
```

- **工具名强制 `sys_` 前缀**（worker 添加）：与 bash/mcp 命名空间隔离。
- **能力门控（app-service 路径）**：门控规则与报文见「能力协商（initialize）」一节
  —— 客户端在 initialize 的 clientCapabilities 里声明 `x-device` 才会被探测。
- **探测时机**：probe 在每次 prompt 的工具收集时进行（Session.Run 合并按请求
  注入的 ToolSource），session/new 后不做一次性探测注册。旧 ServeACPWorker
  路径（session/new 后一次性探测）仅剩 stdio worker（ZCode dispatcher）形态。
- **metadata 声明**：客户端应发送 `risk_level: benign..reserved`；需要统一门控的工具还应发送 `requires_permission: true`。worker 为 gated 工具补充稳定的 `allow_always_key: sys_<name>`。
- **兼容旧客户端**：`never → low`（直通）、`mode → unclassified`、`always → critical + permission_policy=always_hitl`；旧值只在 relay 边界转换，runtime 内部统一使用 benign-reserved。

  | mode | benign/low | medium-reserved |
  |---|---|---|
  | normal | 放行 | HITL |
  | auto | `<= auto_max_risk` 放行，否则 HITL | HITL |
  | strict / readonly | 放行 | 拒绝 |
  | off | 放行（`always_hitl` 仍询问） | 放行（`always_hitl` 仍询问） |

- **实现位置**：worker 中继 `daemon/internal/core/acpworker/sys_relay.go`；统一裁决 `ai-runtime/middlewares/hitl.go`；执行器在 Kotlin `DeviceTools` 注册表
- **每加一个能力的成本 = Kotlin 一处**（注册描述符 + handler），Go/协议零改动、无需重绑 AAR 之外的任何东西

**客户端义务**：对不认识的带 id 请求必须自动回 -32601（JSON-RPC 标准行为）——
worker 的探测靠这个快速失败；沉默客户端会把探测拖到超时（2s）。

## Android 构建与 Host 适配

AAR 默认只编入 Agent：

```bash
daemon/mobile/build-aar.sh
# 等价于 gomobile bind -target=android -tags=goworker_no_dispatcher -o build/goworker-agent.aar ./mobile
```

脚本在 bind 前检查依赖图：必须包含 `daemon/internal/plugins/agent`，不得包含
`daemon/internal/plugins/dispatcher`。可用 `GOWORKER_MOBILE_GO_TAGS` 覆盖 Go tags，
但脚本仍会拒绝包含 Dispatcher 的组合，避免把 full 产物伪装成 Agent-only AAR。

Host 只负责进程内管道和 ACP 帧桥接，Android 侧负责能力适配：

- `configDir` 应指向 app 的持久化目录；同一目录用于进程重建后的 session/config 恢复。
- 文件、权限、设备 Tool 与 MCP 能力通过 `x-device/*` client-ward 请求提供；不支持的
  方法必须返回 JSON-RPC `-32601` 或明确的工具错误，不能回退到桌面文件系统/命令执行。
- Activity/Service 停止时调用 `Close()`，销毁后不得继续 `Write()`；重新创建 Host 时复用
  持久化目录并重新完成 `initialize`，不要持有旧 Host 或旧回调引用。
- `OnData`/`OnClose` 来自 Go 后台 goroutine，回调实现必须切回 Android 主线程后再更新 UI。

AAR 的 API 只暴露 `NewHost`、`Start`、`Write`、`Close` 和 `Callbacks`，不把 Go 的
`context.Context`、channel 或 runtime 内部对象直接穿过 gobind 边界。

## 演进规则

- 协议语义以 `ai-dispatch/protocol` 为唯一事实源，本文档只做映射，不定义新语义
- 新增 client-ward 能力方法：先在 `ai-dispatch/protocol` 落方法常量与类型
  （编辑器端可复用），再在 Kotlin 客户端实现
- 破坏性变更：`protocolVersion` 递增，Kotlin 客户端按 initialize 协商降级
