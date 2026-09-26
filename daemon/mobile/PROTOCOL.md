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
│  │      io.Pipe 桥 → service.ServeACPWorker(rwc, deps)          │  │
│  └──────────────────────────┬───────────────────────────────────┘  │
│                             ▼                                      │
│        ACP worker（ai-runtime Session：LLM/工具/沙箱/HITL）          │
└───────────────────────────────────────────────────────────────────┘
```

## 帧格式

换行分隔的 JSON-RPC 2.0（与 `ai-dispatch/protocol.Conn` 一致，每条消息一行）。
`Host.Write` 负责出方向定界：**一次 Write 一条完整消息**，追加 `\n`；
入方向 `OnData` 是原始字节切片，**可能含半条消息**，客户端必须按行累积组帧。

## 方法面（client → agent）

| 方法 | 说明 |
|---|---|
| `initialize` | 能力协商，首条必发；响应含 agentCapabilities（loadSession 等） |
| `session/new` | 建会话，params `{cwd, mcpServers}`；响应 `{sessionId, modes?}` |
| `session/load` | 回放历史会话（checkpoint 锚定，update 先于响应到达） |
| `session/list` | 会话清单（当前 + 归档，`isCurrent` 显式区分） |
| `session/prompt` | 提交一轮对话，阻塞到回合结束返回 stop reason |
| `session/cancel` | **通知**（无响应）：取消进行中的 prompt —— Android 停止按钮发这个 |
| `session/set_mode` | 未注册（method-not-found 诚实失败） |

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
NewHost(configDir, callbacks)   装载配置/日志（GOWORKER_CONFIG_DIR 注入，
                                GOWORKER_SANDBOX_MODE 可收紧沙箱门禁）
  → Start()                     io.Pipe 桥 + worker 后台启动
  → initialize → session/new → (session/prompt ⇄ update/permission)* → …
  → Close()                     幂等；之后 OnClose 恰好回调一次
```

配置文件落在 `configDir`（`config.yaml`），LLM endpoint 等可经 ACP 无关路径
预置（app 首启引导生成），或后续经 agent 的命令通道设置。

## 系统能力工具：x-device 扩展（已实现）

设备能力只有 client（app 进程）可调用，走 **client-ward 扩展方法**（Zed `fs/*` 模式）。
方法名带 `x-` 前缀 = goworker 应用层扩展，**不进 `ai-dispatch/protocol` 标准面**；
VS Code/ZCode 等标准客户端不认识 → 自动 method-not-found → 无设备工具，零兼容负担。

```
session/new 后：agent ── x-device/tools（探测请求）──→ client
                 ← {tools: [{name, description, schema, risk_level}]}
                 （method-not-found / 超时 / 非法条目 → 视为无能力，不注册工具）

LLM 调 sys_<name> → HITL 门控（risk_level × 沙箱模式，见下）→ 通过后：
                 agent ── x-device/call {name, arguments} ──→ client 执行
                 ← {result: "<文本结果回给 LLM>"}
```

- **工具名强制 `sys_` 前缀**（worker 添加）：与 bash/mcp 命名空间隔离，且自动进 HITL 门控
- **risk_level 声明**（`never | mode | always`，缺省 mode）：

  | | normal | strict / readonly | off |
  |---|---|---|---|
  | never | 放行 | 放行 | 放行 |
  | mode（默认） | **询问** | 拒绝 | 放行 |
  | always | **询问** | 拒绝 | **询问** |

- **实现位置**：worker 中继 `daemon/internal/core/service/sys_relay.go`；裁决 `ai-runtime/middlewares/hitl.go checkSys`（读 `BeforeToolEvent.ToolDef.Metadata["risk_level"]`）；执行器在 Kotlin `DeviceTools` 注册表
- **每加一个能力的成本 = Kotlin 一处**（注册描述符 + handler），Go/协议零改动、无需重绑 AAR 之外的任何东西

**客户端义务**：对不认识的带 id 请求必须自动回 -32601（JSON-RPC 标准行为）——
worker 的探测靠这个快速失败；沉默客户端会把 session/new 拖到探测超时（2s）。

## 演进规则

- 协议语义以 `ai-dispatch/protocol` 为唯一事实源，本文档只做映射，不定义新语义
- 新增 client-ward 能力方法：先在 `ai-dispatch/protocol` 落方法常量与类型
  （编辑器端可复用），再在 Kotlin 客户端实现
- 破坏性变更：`protocolVersion` 递增，Kotlin 客户端按 initialize 协商降级
