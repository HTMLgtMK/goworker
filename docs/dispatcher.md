# Dispatcher 设计：goworker 作为 commit 调度中心

> 分支：`research/multi-agent-delegation`（基于 feature/llm-provider）
> 定位：dispatcher **完全以 plugin 方式接入**，不影响现有 REPL / agent 功能。

## 1. 目标与非目标

**目标**

- goworker 作为 dispatcher：接收任务 → 派发给 worker agent（Claude Code / Codex / ZCode / workbuddy）→ worker 在隔离工作区产出 commit → HITL 审批后合入主线
- 双角色 ACP：
  - **Client 角色**：dispatcher 作为 ACP Client，spawn 并驱动 worker agent 子进程
  - **Agent 角色**：dispatcher 同时暴露 ACP Agent 端点，接受外部（Codex 编排层、其他 goworker 实例、任何 ACP Client）提交任务
- REPL 命令入口：`/dispatch` 系列命令添加/管理任务
- 合并策略：所有 commit 合入必须过 HITL 人工审批，无自动 merge

**非目标（第一版）**

- 不做 worker 的智能路由/负载均衡（先支持手动指定 + 默认 worker）
- 不做跨机器分发（worker 全部为本机子进程）
- 不改变现有 /agent 的任何行为

## 2. 为什么是 ACP

- JSON-RPC 2.0 over stdio，天然匹配 goworker 的子进程 worker 模型
- 协议自带任务语义：`initialize`（能力协商）→ `session/new` → `session/prompt`（提交任务）→ `session/update`（流式进度）→ stop reason（结束）→ `session/request_permission`（权限回调）
- 官方适配器现成：Claude Code 走 `zed-industries/claude-agent-acp`，Codex 走 `agentclientprotocol/codex-acp`，Gemini CLI / OpenCode / Cursor CLI 原生支持
- goworker 自身已 SDK 化（ai-runtime/agent Session 零宿主协议依赖），补一个 ACP agent 端适配即可让 ZCode 被其他 dispatcher 调用

## 3. 模块布局

遵循现有分层（独立协议层零依赖，daemon 做插件适配）：

```
ai-dispatch/                      ← 新独立 module（stdlib only，零 goworker 依赖）
  protocol/                       ← ACP wire 协议：JSON-RPC 2.0 编解码 + 双端方法集
    jsonrpc.go                    ←   frame 读写、request/notification/error
    methods.go                    ←   initialize/session.new/session.prompt/session.update/
    spec.go                       ←   类型定义（AgentCard/SessionUpdate/PermissionRequest…）
  task/                           ← 任务模型与队列
    task.go                       ←   Task + 状态机
    store.go                      ←   JSONL 追加持久化（崩溃恢复重放），风格对齐 ai-runtime/session
  client.go                       ← ACP Client 角色：spawn 子进程、驱动一个任务到终点
  server.go                       ← ACP Agent 角色：对外任务入口（stdio / unix socket）

daemon/internal/dispatcher/       ← dispatcher 插件（daemon 侧适配）
  plugin.go                       ← plugin.Plugin 生命周期 + 命令注册 + 事件广播
  workers.go                      ← worker 注册表（config → launch 描述）
  worktree.go                     ← git worktree 隔离与清理
  review.go                       ← HITL 审批门禁（复用 hitl.Decision 通道）
  commands.go                     ← /dispatch /workers 命令实现
```

依赖方向：`daemon/internal/dispatcher → ai-dispatch → (stdlib)`；`ai-runtime` 不感知 dispatcher。

## 4. 任务模型与状态机

```go
type Task struct {
    ID         string    // task_<ulid>
    Source     string    // "repl" | "acp:<client-name>"
    Kind       string    // "code" | "general"，决定隔离要求与产物形态
    Prompt     string    // 任务描述（自然语言）
    Repo       string    // code 任务必填且必须为 git 仓库；general 任务可选
    Worker     string    // worker 名称
    Status     Status
    BaseCommit string    // 仅 code 任务：派发时的 HEAD SHA，commit 收集边界
    Worktree   string    // 仅 code 任务：.goworker/dispatch/<ID>/
    Branch     string    // 仅 code 任务：dispatch/<ID>
    Commits    []string  // 仅 code 任务：完成后收集的 commit 摘要
    Error      string
    CreatedAt / UpdatedAt / ExpiresAt time.Time
}
```

**两种任务类型，隔离要求不同：**

- **`code` 任务**：目标是改动代码仓库。必须在 git 仓库中派发，**worktree 隔离是硬性要求**
  ——并行不互踩、主工作区不受污染、commit 边界（`BaseCommit..dispatch/<ID>`）天然干净
- **`general` 任务**：文档、调研、分析等非代码任务。不需要 git、不需要 worktree，
  worker 在指定目录下工作；产物是 worker 的回答/产出文件，没有 commit 收集

状态机（全部落 JSONL，进程重启重放恢复）：

```
queued ──→ dispatching ──→ working ──→ awaiting_review ──→ merging ──→ done
   │            │             │              │                
   │            │             │              └── rejected（弃置任务产物）
   └────────────┴─────────────┴──────────────────→ failed / cancelled
```

- `working`：消费 worker 的 `session/update`（agent_message_chunk / tool_call / plan）广播为 `EventTaskProgress`，statusbar 可订阅
- `awaiting_review`：worker prompt 返回 stop reason 后发起 HITL——code 任务附 commit
  清单（`BaseCommit..HEAD`）+ diff stat；general 任务附 worker 自述/产物路径（产物确认）
- 超时沿用现有约定：`ExpiresAt` 为唯一超时来源（对齐 ef5a57b 的 HITL 决策）

## 5. 双角色 ACP 接入

### 5.1 Client 角色（派发给 worker）

config 新增（`dispatch.enabled: true` 时才注册插件）：

```yaml
dispatch:
  enabled: true
  workers:
    - name: claude
      command: npx
      args: ["-y", "@zed-industries/claude-agent-acp"]
    - name: codex
      command: codex-acp            # agentclientprotocol/codex-acp 适配器
    - name: zcode
      command: goworker
      args: ["acp"]                 # goworker 自身原生 ACP agent 端（后续子命令）
  default_worker: claude
  max_parallel: 2
```

派发流程（ai-dispatch/client.go）：

1. 按 `Kind` 校验前置：code 任务必须是 git 仓库，记录 `BaseCommit` 并建 worktree（§6）；
   general 任务跳过隔离，直接用指定目录
2. spawn worker 子进程 → `initialize`（协商 `loadSession` 等能力）→ `authenticate`（如需）
3. `session/new`（cwd = worktree 路径或 repo 本体）→ `session/prompt`（任务 Prompt）
4. 流式消费 `session/update` → 转发事件；worker 若发起 `session/request_permission`，
   有人的 REPL 会话在时转发给前端弹批，无人值守按 worker 配置的 fallback policy 决绝
5. stop reason 返回 → 收集 commits → 进入 `awaiting_review`

### 5.2 Agent 角色（接受外部任务）

入口（已实现）：

- **dispatcher ingress**：`dispatch.enabled` 的插件 Start 时监听
  `<DispatchDir>/acp.sock`（unix socket），外部 ACP Client（Codex 编排层、
  另一个 goworker）连接后 `session/prompt` 的内容即任务，落 `Source: "acp"`；
  进度经 `session/update` 流式回给提交方，任务完成即 prompt 返回 stop reason
- **ZCode worker 化**：`goworker acp` 以 stdio ACP Agent 模式暴露 Session SDK
  （provider/工具装配与 agent 插件共用 ProviderFactory），任意 ACP Client
  （包括本项目 dispatcher 的 worker 配置 `command: goworker, args: ["acp"]`）
  都能驱动 ZCode；HITL 请求无人值守一律拒绝

长任务与 HTTP 不同：ACP 是随连接存活的会话，提交方断连 = 取消（`session/cancel`）。

## 6. 工作区隔离（由任务类型决定）

| 任务类型 | 隔离 | 产物收集 |
|---|---|---|
| `code`（git 仓库） | **worktree 必须**：`git worktree add <repo>/.goworker/dispatch/<ID> -b dispatch/<ID>`，worker 全程在 worktree 内 | `BaseCommit..HEAD` commit 清单 + diff stat |
| `general` | 无隔离，worker 在指定目录工作 | worker 自述 / 产出文件路径 |

code 任务的收尾：done → 人工合入（fast-forward / merge / cherry-pick 由人决定）后
`git worktree remove`；rejected/failed → 直接 remove（保留分支可选）。
general 任务无 git 收尾，approve 即完成。

## 7. HITL 审批

复用现有 HITL 协议（decision channel + ExpiresAt）：

- 触发：任务进入 `awaiting_review`，向 REPL 推送审批请求——code 任务附 commit 清单 +
  diff stat；general 任务附 worker 自述与产出路径（产物确认）
- 决策：`approve`（code：执行合并；general：标记完成）、`reject`（code：弃置 worktree；
  general：记录后关闭）、`edit`（人接管，dispatcher 挂起）
- 无人在场：任务挂起等待，不超时自动决策（与 bash 命令审批不同，这是重决策）
- 每个决策写 `audit/dispatch.jsonl`（复用 audit 风格）

## 8. REPL 命令与事件

```
/dispatch <prompt>              ← 当前目录是 git 仓库 → code 任务（worktree）；否则 general 任务
/dispatch --general <prompt>    ← 强制 general 任务
/dispatch @claude <prompt>      ← 指定 worker
/dispatch ls [status]           ← 任务列表
/dispatch show <id>             ← 详情（commits、进度、错误）
/dispatch approve|reject <id>   ← 审批
/dispatch cancel <id>
/workers                        ← worker 清单与连接状态
```

事件契约（进 ai-dispatch/task 包，daemon statusbar 订阅渲染）：

- `EventTaskStatus{TaskID, From, To}`
- `EventTaskProgress{TaskID, Kind, Content}`（worker 流式输出节流后转发）

## 9. 里程碑

| 阶段 | 内容 | 验收 |
|---|---|---|
| M1 | ai-dispatch：ACP protocol + task 状态机 + JSONL store（单测全覆盖） | go test；fake ACP agent/client 互测 |
| M2 | client.go 派发链路 + worktree 隔离（code 任务）+ general 任务直跑 | fake worker 跑通 code 任务（worktree + commit 收集）与 general 任务 |
| M3 | dispatcher 插件 + REPL 命令 + HITL 审批 + statusbar 事件 | REPL 全流程：加任务→进度→审批→合入 |
| M4 | ACP Agent 端点（接受外部任务）+ ZCode 自身 ACP 化 | 两个 goworker 实例互派任务 |

## 10. 风险与开放问题

- **worker 的 bash 不受 ai-sandbox 门禁**：worker 是独立进程，自己的工具自成沙箱；
  dispatcher 的安全边界是 worktree 隔离 + 合并审批。若要更紧，M4 后可对 zcode worker
  下发 sandbox 配置（它本身就是 goworker）
- **request_permission 无人值守策略**：默认拒绝并记审计，避免 worker 卡死等不到人
- **MCP 对照**：MCP 方案更简单但缺任务生命周期语义（无 permission 回调标准流、无 session 恢复）；
  ACP 的 `session/load` 支持崩溃恢复 dispatcher 侧任务，这是选 ACP 的核心收益之一

## 11. 规划：/dispatch 进度接 statusbar 渲染

### 现状与关键差异

现有 statusbar 事件链（usage/iteration）：

```
Session SDK ──RunCallbacks.Publish──→ Bar.Publish ──→ addon.OnRegister 订阅
                                                          │
                              addon.Tick 消费 channel ←───┘（200ms 一帧）
```

`Publish` 是命令执行时经 `ctx.Publish` 注入的（stdin.go:288），随命令结束失效。
dispatcher 任务跑在**后台 goroutine**，没有命令 Context —— 需要常驻发布路径。

### 方案：经 Engine 事件总线桥接（零协议改动）

```
dispatcher plugin ──hub.Notify(plugin.Event)──→ Engine.Notify ──AddEventListener──┐
                                                                                   ▼
                                        stdin 前端桥接 listener ──sb.Publish──→ Bar
                                                                                   ▼
                                                            TaskAddon.OnRegister 订阅
```

- dispatcher 插件只依赖已有的 `hub.Notify`（daemon 内部协议），不感知 statusbar
- 前端持有 engine 引用，启动 Bar 时 AddEventListener 把
  `task_status` / `task_progress` 两类事件桥接到 `sb.Publish`
  ——桥接 listener 只做一次 Publish（轻），不会拖慢 dispatcher goroutine
- 符合「事件契约倒置」惯例：契约跟类型走，定义在 ai-dispatch/task，
  daemon 两侧（dispatcher 发布、stdin 渲染）各自 import

### 1. 事件契约（ai-dispatch/task/events.go，新增）

```go
const (
    EventTaskStatus   = "task_status"   // 状态迁移（低频，必达）
    EventTaskProgress = "task_progress" // 执行进度（高频，可丢）
)

type StatusEvent struct {
    TaskID string
    From, To Status
    Kind     Kind
    Worker   string
}

type ProgressEvent struct {
    TaskID  string
    Kind    string // session/update 子类型（agent_message_chunk/tool_call/…）
    Summary string // 文本摘要（Content.Text 截断），空表示结构化更新
}
```

### 2. 发布点（daemon/internal/dispatcher）

- **task_status**：不改 orchestrator。插件 `save()` 是全部落盘的汇聚点，
  在此对比 store 旧快照与新任务的 Status，不同即 `hub.Notify` 一次
- **task_progress**：`orch.SetProgress` 现有闭包里，除 ACP 路由外
  追加 `hub.Notify(EventTaskProgress, ProgressEvent{...})`
- 节流：起步不做（orchestrator 透传的是消息块级而非 token 级）；
  addon 端 channel 缓冲 + drop 兜底（对齐 IterationAddon 的 15 缓冲策略）。
  若后续接流式 worker 再在插件端加 200ms 窗口合并

### 3. 渲染（daemon/internal/frontend/stdin/task_addon.go，新增 TaskAddon）

```go
// 状态：running（atomic.Int64）、review（atomic.Int64）、latest（atomic.Value 存摘要）
// Tick 排水事件 channel 更新状态；Render 拼接输出
```

Render 输出形态：

| 场景 | 输出 |
|---|---|
| 无任务 | `""`（不占位） |
| 1 个运行中 | `dispatch 1 run` |
| 混合 | `dispatch 2 run 1 review` |
| 有待审批 | `!1 review` 段高亮（审批等待是用户必须看的） |

状态映射：`To == working` → running++；终态 → running--；
`To == awaiting_review` → review++；approve/reject 后 review--。
与 agent addon 的差异：**Reset 清摘要但不清计数**——dispatcher 是常驻的，
计数跨 /new 生命周期。

### 4. 接线与测试

- stdin.go `sb.Use(...)` 追加 `NewTaskAddon()`
- 测试：
  - TaskAddon 单测：事件序列（working→progress→awaiting_review→approve）
    → 逐帧 Render 快照
  - dispatcher 插件：save diff 发 status 事件、progress 转发，hub.Notify 断言
  - 桥接：stdin 前端集成测试，Engine.Notify → addon 收到
- 验收：REPL 派发任务后状态栏实时出现 `dispatch 1 run`，worker 进度摘要滚动，
  完成后 `!1 review` 高亮提示审批

### 5. 开放问题

- 多任务并跑时摘要行拥挤：V1 只显示最新一条；V2 可按任务轮换或只显计数
- TUI/Web 前端复用：桥接逻辑在 stdin 前端内，TUI 实现时按同样模式自行桥接
- 「有任务时 statusbar 是否常驻启动」：目前 Bar.Start 在 /agent 时触发，
  dispatcher 事件到达时 Bar 可能未启动——桥接 listener 需惰性 Start 或丢弃早到事件
