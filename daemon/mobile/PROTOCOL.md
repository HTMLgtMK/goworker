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
| `session/new` | **真新建**会话，params `{cwd, mcpServers}`；响应 `{sessionId, modes?}`。返回的 `sessionId` 是 `ai-session.SessionManager` 的 canonical ID，同时用于持久化、runtime、标准 ACP 和扩展协议；新会话只新增本连接的绑定，不影响既有会话或其他连接 |
| `session/load` | 按请求中的 canonical `sessionId` 恢复/重开历史会话，响应可带 `{modes?}`；归档会话只读重放；仅当 agentCapabilities 声明 `loadSession` 时可用 |
| `session/list` | 会话清单（全仓并集扫描，`updatedAt` 降序，条目含三态 `state`，见下） |
| `session/prompt` | 提交一轮对话，阻塞到回合结束返回 stop reason |
| `session/cancel` | **通知**（无响应）：取消进行中的 prompt —— Android 停止按钮发这个 |
| `session/set_mode` | 未注册（method-not-found 诚实失败） |

**会话三态（session/list 条目的 `state` 字段，Plan A3）**：

- `active` = 后端仍视为 live：有连接绑定、有在飞 run/待完成的延迟收口，或是
  当前 session。`x-goworker/session/attach` 的 control lease 本身不构成清单活跃引用。
  字段 omitempty 兼容旧客户端（不识别的实现按无字段组收下）。
- `idle` = 无引用无在飞（收口前后过渡态）。
- `archived` = 显式归档终态（见 x-goworker/session/archive），只读保留。
- `cwd`：会话最近一次声明的执行目录——active 会话以运行时声明为最新事实；
  idle/archived 会话从持久化 workspace 记录读（重启后仍有值）。

**多会话并行语义**：ACP `sessionId` 就是 canonical session ID，不存在 transport→runtime ID 映射。同一连接可持有多个不同会话；多个连接也可绑定同一 canonical 会话。不同 session 可并行执行，单个 session 同时只接受一个 run（标准 prompt 与后台 Run 共用该执行权）。

- **按 canonical ID 路由**：`session/prompt`、`session/load` 和 `x-goworker/*` 使用请求中的同一个 `sessionId` 寻址，不使用连接级「最近活动会话」或全局 current。`session/new` 返回 manager 分配的 canonical ID，并只新增该连接对它的绑定；不会改写 ID、顶掉或解绑已有会话。对未知或已收口 ID 的 prompt 明确失败，不会悄悄路由到 current；成功 `session/load` 可重新建立可写绑定。
- **管理门禁**：`session/new` / `session/load`（可写 load 分支）/
  `x-goworker/session/archive` 是管理操作，全局互斥——并发管理操作返回
  -32000 "session busy"（瞬态，稍后重试）。管理操作不与 prompt 共用此门禁，
  因此在飞 prompt 不会阻塞新建或加载；归档只读重放不需要管理门禁。
- **CLI `/new` 与 ACP `session/new` 共用同一 `ai-session.SessionManager` 创建会话，但选择行为不同**：CLI `/new` 创建并选择新会话作为 CLI current；ACP `session/new` 创建新会话并将新 ID 绑定到响应连接，旧 ID 和其他连接绑定原样保留。
- 在 ACP `session/prompt` 中执行 `/new` 会明确报错并提示调用标准 ACP `session/new`。它不会创建会话、修改 CLI current、清空绑定或使旧 session 失效；客户端应通过 `session/new` 获得新 ID。
- 同一 runtime 的并发 prompt/后台 Run 不排队：先成功预留执行权的 run 执行，另一
  个请求失败；不同 runtime 可并行，但仍受后台 Run 的全局并发/排队限额约束。

**缺省执行域（tmp workspace，Plan A4 已知语义）**：从未声明 cwd 的会话
（`session/new` 不带 cwd、`session/load` 无声明可恢复、CLI `/new`），daemon 自动
注入 `<系统 tmp>/goworker-workspaces/<uid>/<sessionID>/`（MkdirAll 0700）作为
工具执行域——**重启即失是有意语义**（系统 tmp，避免遗留垃圾；tmp 只是执行域，
不存历史，会话数据仍在 sessions 目录）。要点：

- **显式声明始终覆盖注入**：`session/new` 的 cwd、`session/load` 的重申落到
  workspace 记录（持久事实）；注入域不写记录——清单 cwd 与重启恢复以用户
  显式声明为唯一持久事实，从未声明的会话重启后回到注入域（新建的空目录）。
- **沙箱第二边界**：配置了 `AllowedWorkDir` 的部署上，缺省执行域根自动成为
  第二放行边界（否则注入即无效标记）；显式声明越界（两个边界之外）仍拒。
- **启动迁移（一次性、幂等）**：旧布局（根槽 `current.jsonl`、根目录
  `archive/*.jsonl` 存量归档、v1 文件名/目录名错位）在 daemon 启动时自动迁入
  per-id 目录——存量归档以 `archived` 态回归 `session/list`；迁移失败条目直接
  丢弃（逐条独立，不阻断启动）。

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

| 方法 | 方向 | 入参 | 响应 | 权限边界 |
|---|---|---|---|---|
| `x-goworker/capabilities` | 请求 | `{}` | `{methods: [...], notifications: [...]}` | 不需要（只读发现面） |
| `x-goworker/run/start` | 请求 | `{sessionId, input, requestId}` | `{runId, state}` | control lease |
| `x-goworker/run/get` | 请求 | `{sessionId, runId}` | `{state, result?, latestSeq, replayIntegrity}` | 不需要（只读观察面） |
| `x-goworker/run/cancel` | 请求 | `{sessionId, runId}` | `{accepted}` | control lease |
| `x-goworker/session/attach` | 请求 | `{sessionId, afterSeq, mode?}` | `{currentRun, latestSeq, replayIntegrity, tookOver?}` | `mode:"control"` 获取/接管；observe 不动 lease |
| `x-goworker/session/detach` | 请求 | `{sessionId, mode?}` | `{detached}` | `mode:"control"` 归还（仅持有者本人有效） |
| `x-goworker/session/archive` | 请求 | `{sessionId}` | `{archived}` | control lease（声明的 `sessionId` 的 lease） |
| `x-goworker/session/title/get` | 请求 | `{sessionId}` | `{title, titleVersion}` | 不需要（只读，支持清单 50 条之外的会话） |
| `x-goworker/session/title/set` | 请求 | `{sessionId, title}` | `{updated}` | control lease（声明的 `sessionId` 的 lease）；标题去除首尾空白，1–80 个 Unicode 字符 |
| `x-goworker/session/title/generate` | 请求 | `{sessionId}` | `{title, source}` | control lease（声明的 `sessionId` 的 lease）；按需用会话首个 user/assistant 轮生成并追加标题，标题为纯 last-wins，手动与自动生成不设优先级 |
| `x-goworker/approval/respond` | 请求 | `{sessionId, requestId, decision}` | `{accepted}` | control lease（声明的 `sessionId` 的 lease） |
| `x-goworker/session/modes` | 请求 | `{}` | `{currentModeId, availableModes}` | 不需要（只读展示面） |
| `x-goworker/providers` | 请求 | `{}` | `{revision, current, providers}` | 不需要（只读） |
| `x-goworker/provider/detail` | 请求 | `{name}` | `{revision, current, provider}` | 不需要（只读） |
| `x-goworker/provider/current/get` | 请求 | `{}` | `{revision, current}` | 不需要（只读当前选择） |
| `x-goworker/provider/current/set` | 请求 | `{revision, provider, model}` | `{revision, current}` | 独立的 connection-level provider-write 授权 |
| `x-goworker/provider/upsert` | 请求 | `{revision, name, type, endpoint, endpointAction?, authType?, secretAction, apiKey?, models, current?}` | `{revision, current, provider}` | 独立的 connection-level provider-write 授权 |
| `x-goworker/provider/remove` | 请求 | `{revision, name}` | `{revision}` | 独立的 connection-level provider-write 授权 |
| `x-goworker/session/events` | **服务端通知** | `SessionEvent`（原样 JSON，蛇形键；逐事件投递；标题更新为 `seq:0` 的 live-only 通知，不进入回放） | — | attach 建立订阅后下行；observe/control 均可收 |

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
- **`x-goworker/session/archive`（Plan A3）= 显式归档终态**：
  - 语义：把会话落为 `archived` 终态（session/list 立即反映）——**只读保留、
    不可逆、无 unarchive**。归档后 `session/load` 仍可只读重放历史；prompt 与
    run/start 一律拒绝（写提交面同源执法，-32000，文案含 `archived`/
    `read-only`），无论该连接此前是否 load 过它；run/cancel、run/get、attach
    等只读/终态管理面照常可用（只读保留 ≠ 下线不可触达）。
  - fencing 双保险：调用方必须持有该会话的 control lease（run/cancel 同款——
    lease 校验与落盘同一受保护临界区，接管后旧持有者一律拒绝）；且归档是管理
    操作，受全局管理门禁（满即 -32000 "session busy"）。
  - run 在飞 → -32000 busy（文案含 `busy`）：在飞 run 即事实活跃，等 run 终态
    后重试；终态与在飞 run 并存会让只读执法与事件流失真。
  - 绑定中的会话**允许**归档：绑定保留、prompt 转只读拒绝、引用归零后由
    daemon 正常收口（数据保留）。
- **错误约定**：沿用 -32000 + 明确文案（不新增标准错误码）。无 control
  lease → 文案含 `control lease`（lease 校验与操作提交在 daemon 侧同一
  受保护临界区内原子完成——接管后旧持有者的 run/start|cancel、
  approval/respond 一律拒绝，不存在「校验通过后被接管仍落地」的窗口）；
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
- **接管只影响后续控制操作，不撤销已成功启动的后台 AgentRun**（DE2-I2）：control
  lease 授权的是**控制操作的提交时刻**，不是 Run 的持续所有权——旧控制者已成功
  启动的 run 继续执行至终态，不随接管自动取消；新控制者作为当前 lease 持有者经
  run/get 观察、attach 订阅其事件流、run/cancel 接管管理（是否取消由新控制者
  **显式决定**，前端不得假设接管即停旧 Run）。测试佐证：
  `TestControlFencingTakeoverStorm` 与 `TestCrossSessionStartRunNotBlockedByFsync`
  （daemon/internal/plugins/agent/runcontrol_test.go——前者钉死接管后旧持有者的
  StartRun/ResolveApproval/CancelRun 一律拒绝；后者断言已提交的后台 Run 在接管后
  继续收敛 completed、不被撤销）。
- V1 的本机信任授权（单用户 daemon，无账号体系）仅描述 Session control lease 的
  宿主策略；Provider 写权限另由上文的连接级 authorizer 决定。Session lease 不做过期
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
- **标题更新不回放**：`session.title.updated` 是 `seq:0` 的 live-only 通知，payload 为 `{title, source, titleVersion}`，不进入事件 journal，也不推进 `latestSeq` 或客户端游标。`session/list` 的每项和 `title/get` 快照都携带 `titleVersion`；清单最多 50 条，任意会话使用 `title/get`。
- **标题版本合并**：每次成功追加标题，版本按该会话持久化顺序递增；旧记录及首条用户消息回退标题的版本为 0。客户端先 attach 再获取 `title/get`，为每个会话维护独立的「已初始化」标记和版本：未初始化时接受首个快照或事件，即使版本为 0；之后通常只接受版本严格更大的快照或事件。**唯一例外**：已初始化的标题为空、版本为 0，收到非空标题的版本 0 快照时应接受（首条消息可能在空快照之后才落盘）；反向的空快照不得覆盖已有非空标题。回退标题不是标题更新事件；初始标题为空时，首轮结束后应重新读取 `title/get` 或刷新清单。快照与事件可能任意顺序到达，断线重连后重新 attach 并读取快照。`title/set` 和 `title/generate` 的响应不是带版本的快照，不要用响应中的标题覆盖已有状态；通过事件或 `title/get` 更新。忽略版本的旧客户端仍可能显示乱序通知中的过期标题。
- **响应的 `latestSeq` 仅描述 attach 时刻事件流的位置，不是游标授权**：
  客户端不得把本地游标直接跳到 latestSeq——只有真正应用了对应事件之后，
  才能按逐事件 `seq` 推进本地 cursor。
- 游标超前保留窗口 → -32000（文案含 `cursor expired`），订阅不建立；客户端
  修正游标（或按 0 全量重放）后重新 attach。attach 失败（会话/游标校验）时
  该连接的既有订阅与 control lease **原样保持**——换代只在订阅建立成功后
  提交。
- **run/start|status|cancel 与 attach 按 sessionId 寻址**（C3 修正，不再限当
  前会话）——**多 Session 路由**：x-goworker 方法寻址**任意已注册会话**
  （daemon 侧会话注册表权威，不限当前会话；标准面行为不变）。会话在引用归零
  （绑定解除且无在飞 run）后从注册表**收口**——此后对已收口旧 sessionId 或
  任意未注册 id 的 x-goworker 寻址
  **诚实失败**：-32000（文案含 `session not found`），历史走
  `session/load` 重开或只读重放。分工：`session/load` = 历史对话重放/重开，
  `x-goworker/session/attach` = 事件游标（Replay+Live）。`requestId` 幂等键
  按 (sessionId, requestId) 组合寻址，幂等作用域是**会话**：同会话同键同
  input 重试返回原 run、同键不同 input 返回 -32000 显式冲突；不同会话各自
  局部递增的 requestId 互不影响（跨会话复用同一 requestId 是两个独立操作，
  组合键保证绝不误返他会话的 run）。
- 同连接重复 attach 同一会话 = **换代**：旧订阅收掉、按本次 afterSeq 重建，
  每连接每会话至多一条事件流；`detach` 或断连收掉订阅。
- **与标准方法的共存语义**：`x-goworker/run/start` 与标准
  `session/prompt` 最终都通过同一 runtime 的 SessionManager 预留执行权。该
  runtime 已有活动 run 时，竞争的 prompt 或 run/start 会被拒绝，不会等待/排队；
  反之先进入的 prompt 或后台 Run 也不会被后来的同会话请求并发执行。它们不共享
  ACP 前端的 session 管理门禁，但共享 per-runtime 单活动 run 限制。

### 事件语义（逐事件投递与断点恢复）

`x-goworker/session/events` 下行的普通 `SessionEvent` 均为**单条逐事件**形状：
`seq` 即该事件自身序号，wire 上单条与批次不可区分，不存在批量/聚合形态。
标题更新是例外：使用 `seq:0` 的 live-only 通知，不进入回放游标。
高频 delta（`text.delta` / `thinking.delta` / `usage.progress`）在**持久化侧**按窗（50ms/4KB）合并为批次
记录以压缩落盘条数，但批次只存在于存储面——服务端回放时把批次展开为原始
per-seq 事件后逐条下行，与实时流形状完全一致。

- **游标只按普通事件推进**：客户端仅对 `seq > 0` 的事件逐条推进本地 cursor；`seq:0` 的标题 live 通知不得推进游标。禁止按 `latestSeq` 批量前移。
- **断点恢复精确到 seq**：断线重连 `attach(afterSeq=N)` 时，服务端重放所有
  `seq > N` 的事件——合并批次内 `seq > N` 的部分逐条重发、`seq <= N` 的部分
  不再重发，不重复、不丢失（游标停在批次中间也能精确续传）。

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

- **`x-goworker/session/modes`（Plan B 任务3）**：数据源 cfg.LLM providers
  （`currentModeId` = current provider），纯展示面，无门禁。
- **Provider 管理面（连接级授权 + revision 乐观并发）**：`providers`/`provider/detail` 返回
  全局 Provider 注册表与 `current {provider, model}`；模型属性
  （`name`（可选展示名；客户端可在缺省时显示模型 ID）/`contextWindow`/`thinking`/`maxTokens`）
  挂在每个 model 条目下，models 的 JSON key 是模型 ID（身份）。
  **写权限独立于 session control lease**：每条 ACP 连接由服务端注入的
  `ProviderWriteAuthorizer(connectionID)` 单独授权；未授权连接的
  `provider/current/set`、`provider/upsert`、`provider/remove` 不注册，也不会出现在
  该连接的 `x-goworker/capabilities` 方法清单中。调用未注册方法得到 method-not-found。
  revision 只做并发冲突检测，绝不是授权凭证；即使持有某个 Session 的 control
  lease 或拿到最新 revision，也不会因此获得 Provider 写权。读取方法仍可单独发现与调用。
  默认信任路径是 daemon 自己创建的 Unix socket（socket mode 0600；新建父目录 0700，
  按单用户本机信任模型视为本地可信）；经 `WithACPConnection` 注入的 pipe/其他来源
  缺少可验证的 peer identity，默认拒绝 Provider 写操作。仓库内 Android `mobile.Host`
  只把自己创建并持有的进程内管道视为可信，并显式启用写授权。其他宿主可通过
  `app.WithACPProviderWriteAuthorizer` 注入逐连接策略；授权仅基于宿主实际信任的
  连接身份/传输事实，不能把客户端自报字段当身份。
  模型切换走 `provider/current/set`（高频操作，不扛整个 upsert）：带读取时的
  `revision` 切换到已注册的 `{provider, model}`，同一事务内校验模型归属。
  已授权写操作（current/set、upsert、remove）必须携带读取时的 `revision`，
  版本不匹配返回 -32000（文案含 revision/conflict），不执行变更。约束：不允许
  删除 current provider、不允许空 `models`、`current.model` 必须属于对应
  provider——切换与删除在同一笔提交中原子完成。`secretAction` 三态：
  `keep`（保留现有 key，不可携带 apiKey）/ `replace`（必携新 key）/ `clear`
  （清除）；**明文 key 永不出现在任何响应中**（仅 `hasApiKey` 状态位）。API
  key 写入即时持久化，运行中 Run 使用启动时的配置快照不受影响，下一轮 Run 生效。
  `endpointAction` 可选为 `keep` 或 `replace`：`keep` 保留现有 endpoint，不能用于新建
  Provider；`replace` 将 `endpoint` 作为真实值写入。旧客户端省略该字段时维持兼容语义：
  对已有 Provider，若 `endpoint` 等于读取时返回的脱敏值，则视为 round-trip 并保留原值；
  否则视为替换。由于该脱敏值也可能是合法真实 URL（如路径恰为 `/%5Bredacted%5D`），
  新客户端必须在有意设置此类值时传 `endpointAction: "replace"`，不能依赖字段省略。

### 能力通告与降级（两阶段发现）

**initialize 通告 + ready 后动态发现**，与 x-device 的"声明后探测"同型：

1. **握手通告**：daemon 在 initialize 响应的 `agentCapabilities` 中声明
   `x-goworker: {version: 1}`（发现协议版本）。未声明即视为对端无此扩展
   （老 daemon / stdio worker 形态），客户端不要假设方法存在。
2. **动态发现**：initialize 响应收到后调用 `x-goworker/capabilities`，daemon
   返回**本连接实际可用**的方法与通知清单——清单来自连接级注册表，与真实
   注册同源：后端接口缺失或授权缺失的方法不注册、不列出（如无 RunControlSource 时无
   run/* 方法，无 ProviderControl 时无 provider 方法；Provider 写授权缺失时只省略
   provider 写方法）。客户端按发现结果启用功能，发现清单不构成授权——Session 控制
   lease 与 Provider 写授权仍分别在服务端执行。
3. **降级路径**：initialize 未声明 x-goworker 时，旧客户端可保留试探性 attach
   （吃 `-32601` 即回退纯标准面：prompt 阻塞语义 + session/update 流）；
   新实现以握手通告为准，不做方法级试探。

`version: 1` 定义发现协议本身的兼容性；方法清单向后兼容地增删，未知方法由
客户端忽略，不因增方法升版本。已知方法目录的唯一事实源是
`daemon/xgoworker/protocol`（gomobile 侧 `mobile.XGWorkerMethod*` 导出同一
目录），**本地目录 ≠ 当前端支持**——运行时可用性以发现响应为准。

## 能力协商（initialize）

完整报文示例（客户端视角，逐字段可抄）：

```json
→ {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
     "protocolVersion":1,
     "clientCapabilities":{"x-device":{"tools":true}}
   }}
← {"jsonrpc":"2.0","id":1,"result":{
     "protocolVersion":1,
     "agentCapabilities":{"loadSession":true,"x-goworker":{"version":1}}
   }}
```

**clientCapabilities（client 声明）**

- `x-device` key 存在性即门控，value 当前不解析。`{"x-device":{"tools":true}}`
  是现行声明形状；服务端不会检查嵌套的协议版本或 `sessionRunIdentity`。
- 声明 `x-device` 等同于承诺完整实现 `x-device/tools` 和当前版本的 `x-device/call`
  （含 `sessionId` / `runId` 必填字段及不确定结果不重试语义）。只支持旧
  `{name, arguments}` 形状的客户端已不兼容，必须先更新处理器再声明该 key。
  发送 initialize **之前**必须完成反向请求 handler 注册：服务端写出 initialize
  响应后即可发起探测，客户端应用层可能尚未处理该响应。
- **不声明的后果（app-service 路径）**：agent 不发探测请求，设备工具静默为零
  —— 不报错、不降级提示，LLM 只是看不到任何 `sys_*` 工具。这是能力协商的
  预期行为（VS Code 等标准客户端就是这条路）。
- 旧 AAR（ServeACPWorker 路径）不检查此字段；但没有 managed runtime identity 的
  worker relay 会在发出 `x-device/call` 前 fail closed。

**agentCapabilities（agent 声明）**

- 客户端**必须保存** initialize 响应并据此降级，不要对能力面做硬编码假设：
  - `loadSession` 缺失 → 藏掉/禁用「恢复历史会话」入口，别发 `session/load`
    吃 -32000。
  - 两条 Go 路径的能力面不同（ServeACPWorker 无 load/list；app-service 全有），
    客户端同一份代码要能对着任意一份 AAR 跑。（当前 mobile Host 已切 app-service
    路径；ServeACPWorker 仅剩 stdio worker 形态在用 —— ZCode dispatcher spawn
    的无人值守进程。）

**客户端迁移清单（旧 AAR → capability 门控版 AAR）**

1. initialize params 补 `clientCapabilities: {"x-device": {"tools": true}}`；仅在客户端已实现本节的新 `x-device/call` wire contract 后声明该 key。现有服务端不解析嵌套版本字段。
2. 在发送 initialize 前注册 `x-device/tools` / `x-device/call` handler，处理 `sessionId`、`runId`；设备调用的 `sessionId` 是 canonical session ID，可据此关联该会话的 workspace。
3. 保存 initialize 响应的 `agentCapabilities`，UI 按能力降级；保留 -32601 兜底、权限对话框语义。对已发出但结果不确定的设备调用不得自动重试。
4. 新 AAR（app-service 路径）要求客户端声明 `clientCapabilities`（见上节），未声明时设备工具静默为零；Host 侧已强制启用 ACP service —— 显式注入连接时 `frontend.acp.enabled` 开关无效，装配层无视该开关注册 service。

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
不支持该扩展的标准 ACP 客户端会对探测返回 method-not-found，agent 不会注册设备工具。

```
initialize 响应写入 transport 后（app-service 路径）：
                 agent ── x-device/tools（探测请求）──→ client
                 ← {tools: [{name, description, schema, risk_level}]}
                 （method-not-found / 超时 → 无设备工具；非法条目跳过）

LLM 调 sys_<name> → 统一 HITL 门控（risk_level × 沙箱模式，见下）→ 通过后：
                 agent ── x-device/call ──→ client 执行
                 ← {result: "<文本结果回给 LLM>"}
```

### Canonical session identity

`session/new` returns the ID created by `ai-session.SessionManager`. That exact canonical ID is the ACP `sessionId`, persisted session ID, runtime ID, and ID accepted by x-goworker and x-device. `session/load` addresses the same ID; no `_meta` alias or transport-to-runtime mapping is used.

Clients use the canonical `sessionId` for ACP prompts and x-goworker calls. For device workspace selection, clients may maintain an explicit canonical-session-ID-to-workspace association; they must not infer a session identity from cwd, recent selection, or process-global current. Archived read-only sessions have no executable runtime and cannot invoke device tools.

### `x-device/call` wire contract

实际 JSON 字段使用 camelCase；Go 结构体字段名分别是 `SessionID` / `RunID`：

```json
{
  "sessionId": "session_0123456789abcdef0123456789abcdef",
  "runId": "run-opaque-123",
  "name": "read_file",
  "arguments": {"path": "src/main.go"}
}
```

- `sessionId` 和 `runId` 均为必填、非空的顶层字符串；旧 `{name, arguments}` 形状不兼容。身份来自 Go 侧 managed Run 的可信 context，不是模型可填写的 `arguments`。
- `sessionId` 是本次调用所属的 canonical ACP/persisted/runtime session ID；没有独立的 transport ID 映射。`runId` 是该 managed Run 的执行 ID；同一 Run 的所有设备工具调用复用该值。
- Go 在没有有效 managed Run identity 时会在 client 调用前失败，不会发送缺字段或空字段请求。`acpWorker`/raw relay 若没有可信 session/run identity，设备调用 fail closed；不能把 cwd 或 process-global current 冒充 session identity。
- 这是 wire 破坏性变更：旧客户端若只理解 `{name, arguments}` 不兼容，必须先升级 handler，再声明 `clientCapabilities.x-device`。现有 initialize 门控只检查 `x-device` key 是否存在，嵌套值不解析；当前没有单独的 `sessionRunIdentity` 版本门控。不要让旧客户端与新 agent 执行设备工具。
- 客户端按 canonical `sessionId` 查找该会话关联的 workspace，再应用原有 workspace/path 边界；未知、空值或有歧义的关联必须拒绝。`runId` 只用于执行关联，不是授权凭证。

**调用结果不确定时禁止自动重试**：能力代理在 Invoke 前找不到绑定时，可以按现有等待策略等待同一 runtime session 的兼容绑定；但一旦 `x-device/call` 已发出，返回错误/超时无法证明设备端没有执行。托管 capability proxy 会使当前失败绑定失效并将错误交回工具调用；raw relay 也只返回调用错误。两条路径都不会把同一请求重发到替换连接。客户端和宿主不得因结果不确定而自动重放副作用调用；由上层决定是否以新的用户/模型操作重试。

- **工具名强制 `sys_` 前缀**（worker 添加）：与 bash/mcp 命名空间隔离。
- **能力门控（app-service 路径）**：客户端在 initialize 的 `clientCapabilities` 中声明 `x-device` 才会探测；value 当前不解析。设备工具清单只在连接 initialize 后探测，按 runtime session 绑定；`session/new`、`session/load`、`x-goworker/session/attach` 和 `x-goworker/run/start` 在会话确定后使用该清单。探测失败时会话仍可用，只是没有已绑定能力；连接关闭时清理该连接绑定。
- **metadata 声明**：客户端应发送 `risk_level: benign..reserved`；需要统一门控的工具还应发送 `requires_permission: true`。worker 为 gated 工具补充稳定的 `allow_always_key: sys_<name>`。
- **兼容旧描述符值**：`never → low`（直通）、`mode → unclassified`、`always → critical + permission_policy=always_hitl`；旧值只在 relay 边界转换，runtime 内部统一使用 benign-reserved。

  | mode | benign/low | medium-reserved |
  |---|---|---|
  | normal | 放行 | HITL |
  | auto | `<= auto_max_risk` 放行，否则 HITL | HITL |
  | strict / readonly | 放行 | 拒绝 |
  | off | 放行（`always_hitl` 仍询问） | 放行（`always_hitl` 仍询问） |

- **等待重绑与不确定结果边界**：Invoke 前查不到会话对应的能力绑定时，设备工具可等待重绑（默认 45s）；新绑定必须属于同一个 runtime session 且工具 schema/policy 兼容。等待期间 run 状态为 `waiting_capability`。一旦 Invoke 已调用，任何传输错误都不重发，哪怕刚好有新连接绑定；设备可能已完成副作用但响应丢失。
- **单次调用权限语义**：正常工具策略仍在每次工具调用前裁决。一次已获批的调用在 Invoke 前等待重绑时沿用本次裁决，不会再弹一次；重绑后的下一次调用重新裁决。身份字段不绕过权限策略。
- **等待预算**：`config.yaml` 的 `session.capability_wait_timeout`，默认 45 秒；显式值 >55 秒钳到 55 秒并记录 warning，给 ai-core 60 秒工具执行预算中的 Invoke 留 5 秒；<=0 回落到默认值。等待预算只覆盖尚未进入 Invoke 的能力等待，不是失败后的自动重试预算。
- **多绑定选择**：同一 runtime session 有多个兼容绑定时，registry 当前按 connection ID 字典序选择；不保证优先选择原连接或当前控制连接。实际使用约定仍是一个 Session 对应一个设备。
- **实现位置**：中继 `daemon/internal/core/acpworker/sys_relay.go`；托管 identity `ai-runtime/agent`；能力绑定 `ai-runtime/agent/capability.go`；客户端执行器在 Android `DeviceTools`。

**客户端义务**：对不认识的带 id 请求必须自动回 `-32601`（JSON-RPC 标准行为），避免探测无谓等待；支持 `x-device` 的客户端还必须实现上述新 `x-device/call` 字段和失败语义。

## Android 构建与 Host 适配

AAR 默认只编入 Agent：

```bash
daemon/mobile/build-aar.sh
# 等价于在 daemon 目录执行：
# gomobile bind -target=android -javapkg=dev.tinguo.goworker -tags=goworker_no_dispatcher -o ../build/goworker-agent.aar ./mobile
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
