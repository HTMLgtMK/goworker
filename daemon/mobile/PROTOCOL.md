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
当前会话。反方向同理：prompt 在飞期间，B 的 `session/new` 与对当前会话的
`session/load` 返回 -32000 "session busy"（归档 load 只读不受限），等 prompt 结束
或取消后重试即可；各连接的 prompt 在 daemon 侧串行执行，连接断开时排队中的
prompt 以错误退出。

## x-goworker 扩展协议（后台 Run 与事件流）

标准 ACP 方法面向同步交互（`session/prompt` 阻塞到回合结束）。`x-goworker/*`
是 daemon 侧的**增量扩展协议**（`x-` 前缀 = goworker 应用层扩展，与 x-device
同款先例，不进 `ai-dispatch/protocol` 标准面）：把「提交一轮对话」变成后台
Run（立即返回，不等 LLM 完成），并把会话事件流（工具调用/流式文本/审批/
run 生命周期）以持久化游标暴露给任意连接——断线重连不丢事件。

**标准方法语义逐字不变**：本节全部是增量能力，标准 6 方法与
`session/update`、`session/request_permission` 的报文和行为与本文件其余章节
完全一致；不调用 x- 方法的旧客户端零感知。

### 方法面

| 方法 | 方向 | 入参 | 响应 | control lease |
|---|---|---|---|---|
| `x-goworker/run/start` | 请求 | `{sessionId, input, requestId}` | `{runId, state}` | **必须持有** |
| `x-goworker/run/get` | 请求 | `{sessionId, runId}` | `{state, result?, latestSeq, replayIntegrity}` | 不需要（只读观察面） |
| `x-goworker/run/cancel` | 请求 | `{sessionId, runId}` | `{accepted}` | **必须持有** |
| `x-goworker/session/attach` | 请求 | `{sessionId, afterSeq, mode?}` | `{currentRun, latestSeq, replayIntegrity, tookOver?}` | `mode:"control"` 获取/接管；observe 不动 lease |
| `x-goworker/session/detach` | 请求 | `{sessionId, mode?}` | `{detached}` | `mode:"control"` 归还（仅持有者本人有效） |
| `x-goworker/approval/respond` | 请求 | `{sessionId, requestId, decision}` | `{accepted}` | **必须持有**（声明的 `sessionId` 的 lease） |
| `x-goworker/session/events` | **服务端通知** | `SessionEvent`（原样 JSON，蛇形键；合并记录带 `from_seq`） | — | attach 建立订阅后下行；observe/control 均可收 |

- **`state` 取值**：`queued` / `running` / `waiting_input`（HITL 等审批）/
  `waiting_capability`（设备工具等重绑）/ `completed` / `failed` /
  `cancelled`。
- **`requestId` 是幂等键，与 JSON-RPC id 不同**：连接断开后客户端不知道
  run/start 是否成功时，用**同一个 requestId** 重试——daemon 返回原 runId
  与该 run 的当前状态快照，绝不创建第二个 Run。requestId 缺省/空串被 -32000
  拒绝（放弃幂等键 = 放弃重试保护）。注意 `approval/respond` 里的
  `requestId` 寻址的是**待决审批**（`permission.requested` 事件 payload 的
  `id` 字段，`req-N` 命名空间），与 run/start 的 requestId 是两个命名空间。
- **run/start 立即返回**：Run 被接受并持久化登记后即响应（提交时 state 恒
  `queued`），不等 LLM 完成；后续进展经 `x-goworker/session/events` 或
  run/get 观察。
- **错误约定**：沿用 -32000 + 明确文案（不新增标准错误码）。无 control
  lease → `"x-goworker: control lease held by another connection ..."`；
  游标超前 → 文案含 `cursor expired`；未知 run → 文案含 `not found`。

### Observe / Control（多客户端控制权限）

control lease **按会话记账**（拍板②原文「每会话至多一个 control 持有者」）：
同一会话可有多个观察者、至多一个 control 持有者；两连接各控各会话互不影响，
接管只发生在同一会话上：

- `attach` 不带 `mode` 或 `mode:"observe"` = **只读观察**：收事件流，不碰
  lease；run/start|cancel 与 approval/respond 一律 -32000。
- `attach` `mode:"control"` = **获取该会话的 control lease**：该会话空闲时
  获取（响应不含 `tookOver`）；他人持有该会话时**显式接管**（响应
  `tookOver:true`，daemon 侧记 slog 审计日志）。attach 的准备阶段
  （会话/游标校验与订阅建立）失败时 lease 与既有订阅原样保持——接管只在
  订阅建立成功后提交。
- run/start|cancel 与 approval/respond 守卫**被操作会话**：请求 `sessionId`
  的 lease 持有者才被授权（持有其他会话的 lease 不构成授权）。approval/respond
  的 `sessionId` 声明审批归属会话，daemon 再按服务端记录的审批归属（审批注册
  时落账的会话）校验两者一致——**服务端记录为准，不信客户端声明**：声明别的
  会话（即便持有其 lease）应答本会话审批 → -32000 越权拒绝，审批不被消费。
- V1 采用本机信任的宿主授权（单用户 daemon，无账号体系）：不做 lease 过期
  与授权协商；**断开连接即释放该连接全部会话的 lease**（审批不悬死在已死
  连接上）；`detach` `mode:"control"` 按会话主动归还。权限检查全部在 daemon
  侧完成，不依赖客户端 UI 自律。

### attach：原子 Replay+Live 与 latestSeq 语义

- `attach {sessionId, afterSeq}` **原子建立「回放 + 实时」订阅**：先重放
  `(afterSeq, latest]` 的持久化事件，再无缝续上此后实时产生的事件——无缺、
  无重、无乱序。`afterSeq` 是客户端已确认游标（0 = 从保留最早开始）。
- **`replayIntegrity` 是回放完整性三态**（attach 响应与 run/get 响应均携带，
  取值 `complete` / `unknown` / `incomplete`；I4 修正后替代旧 `eventsComplete`
  布尔——布尔在 daemon 重启后失真：旧纪元的落盘失败不延续，新进程恒报「完
  整」）：
  - `complete` = daemon 本进程未观察到任何事件写侧失败，且无可疑历史（空
    历史/纯内存）——本进程观察面即全部；
  - `unknown` = journal 存在跨进程纪元的既有历史（daemon 重启后 attach 到旧
    会话即此形态），旧纪元内是否发生过落盘失败无从验证——诚实优先，客户端
    按「可能缺段」消费或按 `afterSeq:0` 全量重放后自行核对终态；
  - `incomplete` = 本进程发生过事件落盘失败（关键事件 durable-first 拒绝，
    或高频 delta 合并落盘失败——重放缺 delta 段），粘滞标记，不因后续成功
    翻回。
  非 `complete` 时**不得把收到的事件当完整历史**。
- **响应的 `latestSeq` 仅描述 attach 时刻事件流的位置，不是游标授权**：
  客户端不得把本地游标直接跳到 latestSeq——只有真正应用了对应事件之后，
  才能按逐事件 `seq` 推进本地 cursor。
- 游标超前保留窗口 → -32000（文案含 `cursor expired`），订阅不建立；客户端
  修正游标（或按 0 全量重放）后重新 attach。attach 失败（会话/游标校验）时
  该连接的既有订阅与 control lease **原样保持**——换代只在订阅建立成功后
  提交。
- **run/start|status|cancel 与 attach 按 sessionId 寻址**（C3 修正，不再限当
  前会话）——**多 Session 路由**：x-goworker 方法寻址**任意已注册会话**
  （daemon 侧会话注册表权威，不限当前会话；与标准方法「单活动会话」的约束
  是两回事，标准面行为不变）。`session/new` 建新会话时旧会话被归档并从注册
  表**收口**——此后对已收口旧 sessionId 或任意未注册 id 的 x-goworker 寻址
  **诚实失败**：-32000（文案含 `session not found`），归档历史走
  `session/load` 只读重放。分工：`session/load` = 历史对话重放，
  `x-goworker/session/attach` = 事件游标（Replay+Live）。`requestId` 幂等键
  按 (sessionId, requestId) 组合寻址，幂等作用域是**会话**：同会话同键同
  input 重试返回原 run、同键不同 input 返回 -32000 显式冲突；不同会话各自
  局部递增的 requestId 互不影响（跨会话复用同一 requestId 是两个独立操作，
  组合键保证绝不误返他会话的 run）。
- 同连接重复 attach 同一会话 = **换代**：旧订阅收掉、按本次 afterSeq 重建，
  每连接每会话至多一条事件流；`detach` 或断连收掉订阅。
- **与标准方法的共存语义**：后台 Run 运行期间，任何连接的标准
  `session/prompt` 会阻塞等待该 Run 结束（排队，不报 busy 错误）；反之标准
  prompt 在飞时 run/start 返回 -32000（同会话单活动 Run）。同步入口与后台
  入口靠同一把会话执行权互斥，不存在并发写会话。

### 事件批量语义（SessionEvent.from_seq 与游标推进）

`x-goworker/session/events` 下行的每条 `SessionEvent` 按 `from_seq` 字段区分
单条与批量（实现侧事实源：`ai-runtime/session/journal.go` 的 SessionEvent
注释、`ai-runtime/agent/event_hub.go` 的回放注释）：

- **`from_seq` 缺省（=0）= 单条事件**：`seq` 即该事件自身序号。实时流恒为
  单条（逐事件投递）；回放流中的关键事件（tool_call / permission.* /
  assistant.message_finalized / run.*）与未进合并窗的 delta 也是单条。
- **`from_seq > 0` = 批量聚合事件**：高频 delta（`text.delta` /
  `thinking.delta` / `usage.progress`）在持久化侧按窗（50ms/4KB）合并——
  `seq` = 区间末事件序号，`from_seq` = 区间首事件序号，payload 为窗内逐事件
  payload 的**聚合结果**：文本类 = 窗内全部增量的拼接字符串（客户端按批量
  语义一次性还原窗内文本），对象类（usage.progress）= 窗内最后一条 payload
  （最终状态）。
- **游标以实际收到的事件 `seq` 逐条推进，不得假设区间内没有其他类型事件**：
  合并窗按类型分桶，而关键事件在发布时逐条落盘，其 `seq` 可能落在某个合并窗
  的 `[from_seq..seq]` 区间之间——回放流里区间边界之间会出现其他类型的单条
  记录（回放按 `seq` 升序）。范围声明只描述本条聚合记录自身的覆盖范围，不是
  「区间内仅此一条」的承诺；客户端逐事件按 `seq` 记账游标（批量记录推进到其
  `seq`），禁止按区间端点或 `latestSeq` 批量前移。

### 交互示例（对齐总方案 §7，键名为 daemon 实际 wire 形状）

```json
→ {"jsonrpc":"2.0","id":1,"method":"x-goworker/run/start","params":{"sessionId":"session-A","input":"分析当前项目架构","requestId":"client-req-001"}}
← {"jsonrpc":"2.0","id":1,"result":{"runId":"run-001","state":"queued"}}
← {"jsonrpc":"2.0","method":"x-goworker/session/events","params":{"session_id":"…","run_id":"run-001","seq":1,"type":"run.started","time":"…","payload":{"state":"running"}}}
```

断线重连（从 `permission.requested` 事件 payload 的 `id` 取审批 requestID）：

```json
→ {"jsonrpc":"2.0","id":2,"method":"x-goworker/session/attach","params":{"sessionId":"session-A","afterSeq":120,"mode":"control"}}
← {"jsonrpc":"2.0","id":2,"result":{"currentRun":{"runId":"run-001","state":"waiting_input"},"latestSeq":145,"replayIntegrity":"unknown","tookOver":true}}
→ {"jsonrpc":"2.0","id":3,"method":"x-goworker/approval/respond","params":{"sessionId":"session-A","requestId":"req-7","decision":{"interrupt_id":"req-7","type":"approve"}}}
← {"jsonrpc":"2.0","id":3,"result":{"accepted":true}}
```

### 能力通告与降级

daemon **不经 agentCapabilities 通告 x-goworker 能力**（V1 决策，与 x-device
的方法面同策略）：客户端按 JSON-RPC 标准行为降级——收到
`-32601 method not found` 即视为对端无此扩展（老 daemon / stdio worker 形态）。
实现 x-goworker 的客户端：先试 attach，吃 -32601 就回退纯标准面（prompt 阻塞
语义 + session/update 流），不要假设方法一定存在。

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
- `x-goworker/session/events` 是**异步服务端通知**，消费注意（与上一条同样
  独立于 request/response 挂起请求表）：通知可能在任意时刻到达（不限于某个
  请求在飞），Kotlin 读循环收到后分发订阅回调即可，不要在回调里反压读循环
  ——daemon 侧对慢消费者直接背压断流（断线窗口内的事件由下次 attach 带已
  确认游标补齐），事件处理应投递到独立队列/协程
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

- **断连等待（P5 行为变化，声明 x-device 的客户端必须知晓）**：设备工具调用
  时发现执行通道已断（连接退出/`x-device/call` 失败）→ 不再「立即把错误交给
  模型」，而是**等待重绑**（默认 45s）：
  - 预算内同会话出现具备相同工具能力的新绑定（新连接 prompt 探测成功即重绑）
    → 工具在新连接上执行完成；重绑后的调用照常走统一 HITL 门控（per-call
    裁决，危险操作重绑后仍需确认）；
  - **审批语义边界（P4 评审 issue6）**：统一 HITL 门控在每次工具调用前裁决
    （ai-core OnBeforeTool，一次调用一次审批）。**单次工具调用内的审批随重绑
    转移到新连接执行**——调用获批后连接断开、预算内重绑成功的，本次调用以
    既有授权直接在新连接上执行完成，不重复弹审批；**再裁决发生在下一次调用**
    （重绑后的新一次工具调用照常走门控，危险操作仍需确认，见上条）；
  - **能力等待总预算受 ai-core toolCtx 60s 约束（预算钳 55s）**：等待发生在
    ai-core 工具执行预算（toolCtx 60s）之内，重绑后再失败回到等待也不复位
    计时（总等待 ≤ 预算）；daemon 把显式配置 >55s 的预算**钳到 55s** 并记
    warn（给 Invoke 留 5s 余量）——超配不生效，否则等待会被 60s 掐断、呈现
    为普通工具超时而非下面的明确失败文案；
  - 预算耗尽 → 返回**明确失败文本**（`device tool sys_<name> unavailable:
    connection lost, waited Ns`）交给模型决定下一步，run 正常继续；
  - **绝不把工具请求静默路由到其他连接**（含已断开的旧连接）；
  - 等待期间 run 状态为 `waiting_capability`（x-goworker 的 run/get /
    session/events 可观测）。
- **`capability_wait_timeout` 配置**：上述等待预算，秒为单位，`config.yaml`
  的 `session.capability_wait_timeout`，默认 45；**daemon 侧钳制（P4 评审
  issue6）**：显式配置 >55 被钳到 55 并记 slog.Warn（硬约束：等待发生在
  ai-core 工具执行 60s 预算内，55s 给 Invoke 留余量——配置方无需自律，超配
  只是无效而非错误）；<=0 回落 45s 兜底。
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

## 仓库外依赖：Android App 跟进清单（goworker-android 仓库）

本文件上述 P5 协议面落码后，Android App（独立仓库）需要跟进的改动清单；
daemon 侧已全部实现并有协议级集成测试覆盖（`xgw_integration_test.go`）：

1. **事件流消费（`x-goworker/session/events`）**：注册通知回调，按 `seq`
   逐事件推进本地 cursor（`latestSeq` 只描述位置，不得跳游标，见
   「x-goworker 扩展协议」节）；断线重连走 `session/attach {afterSeq: 已确认
   游标, mode}` 原子续传，吃 `-32601` 回退纯标准面。
2. **后台 Run 与审批应答（`run/start` / `approval/respond`）**：run/start 携带
   客户端生成的 `requestId`（幂等键，重试复用同一值）；HITL 决策可从
   `permission.requested` 事件取 `id` 经 `approval/respond` 应答（attach
   `mode:"control"` 后；请求须带 `sessionId` = 审批归属会话，见「x-goworker
   扩展协议」节方法表），或继续走标准 `session/request_permission` 同步应答
   ——两条路都在，按 UI 形态选。
3. **断连等待文案**：设备工具断连后 run 会停在 `waiting_capability` 等待重绑
   （默认 45s，见「系统能力工具」节），UI 需要新的等待/失败文案与状态展示；
   失败文本形如 `device tool sys_<name> unavailable: connection lost, waited Ns`。
4. **断连=宿主停机的边界不变**：Host.Close → 整个后端销毁，后台 Run 存活与
   `waiting_capability` 等待期依赖 Android 侧 RuntimeService（进程内形态会被
   进程死亡截断）——需要后台 Run 在断连后继续跑的话，属 Android 侧独立工程项。

## 演进规则

- 协议语义以 `ai-dispatch/protocol` 为唯一事实源，本文档只做映射，不定义新语义
- 新增 client-ward 能力方法：先在 `ai-dispatch/protocol` 落方法常量与类型
  （编辑器端可复用），再在 Kotlin 客户端实现
- **x-goworker 归属**：`x-goworker/*` 是 daemon 侧 agent-ward 的应用层扩展
  （agent 发起/承载的后台 Run 控制与事件流），**不进 `ai-dispatch/protocol`
  标准面**——方法常量与 wire 类型在 daemon 侧自持
  (`daemon/internal/core/service/acp/xproto.go`)，与 x-device（client-ward
  扩展，不进标准面）同款先例；标准面回归由集成测试钉死
  （`TestXGWStandardMethodsUnaffected` 等）
- 破坏性变更：`protocolVersion` 递增，Kotlin 客户端按 initialize 协商降级
