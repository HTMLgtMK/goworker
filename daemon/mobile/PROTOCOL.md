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

## 系统能力工具（设计方向，待实现）

通知/联系人等 Android 能力只有 app 进程可调用，走 ACP 的 client-ward 方法
（Zed `fs/*` 模式）：worker 侧把这类工具的执行请求经协议发给 client，
app 执行后回传结果。具名工具按 `sys_<capability>` 命名，声明风险等级
（never / always / mode），在 HITL 权限链路统一门控；`Execute` 内不做用户
确认（worker 侧工具执行有 60s 硬超时）。

## 演进规则

- 协议语义以 `ai-dispatch/protocol` 为唯一事实源，本文档只做映射，不定义新语义
- 新增 client-ward 能力方法：先在 `ai-dispatch/protocol` 落方法常量与类型
  （编辑器端可复用），再在 Kotlin 客户端实现
- 破坏性变更：`protocolVersion` 递增，Kotlin 客户端按 initialize 协商降级
