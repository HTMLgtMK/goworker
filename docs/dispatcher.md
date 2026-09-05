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
    Prompt     string    // 任务描述（自然语言）
    Repo       string    // 目标仓库绝对路径（可以不是 git 仓库）
    Worker     string    // worker 名称
    Isolation  string    // "worktree" | "shared"，默认 worktree（git 仓库时）
    Status     Status
    BaseCommit string    // 派发时的 HEAD SHA，commit 收集边界（git 仓库时）
    Worktree   string    // 仅 worktree 模式：.goworker/dispatch/<ID>/
    Branch     string    // 仅 worktree 模式：dispatch/<ID>
    Commits    []string  // 完成后收集的 commit 摘要
    Error      string
    CreatedAt / UpdatedAt / ExpiresAt time.Time
}
```

状态机（全部落 JSONL，进程重启重放恢复）：

```
queued ──→ dispatching ──→ working ──→ awaiting_review ──→ merging ──→ done
   │            │             │              │                
   │            │             │              └── rejected（弃置任务产物）
   └────────────┴─────────────┴──────────────────→ failed / cancelled
```

- `working`：消费 worker 的 `session/update`（agent_message_chunk / tool_call / plan）广播为 `EventTaskProgress`，statusbar 可订阅
- `awaiting_review`：worker prompt 返回 stop reason 后，dispatcher 收集 commit 清单（`BaseCommit..HEAD`，worktree 模式下即 `base..dispatch/<ID>`）发起 HITL；非 git 仓库则收集为空，审批看 worker 自述
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

1. 记录 `BaseCommit`（git 仓库时）；按 `Isolation` 决定是否建 worktree（见 §6）
2. spawn worker 子进程 → `initialize`（协商 `loadSession` 等能力）→ `authenticate`（如需）
3. `session/new`（cwd = worktree 路径或 repo 本体）→ `session/prompt`（任务 Prompt）
4. 流式消费 `session/update` → 转发事件；worker 若发起 `session/request_permission`，
   有人的 REPL 会话在时转发给前端弹批，无人值守按 worker 配置的 fallback policy 决绝
5. stop reason 返回 → 收集 commits → 进入 `awaiting_review`

### 5.2 Agent 角色（接受外部任务）

`goworker acp --listen stdio|socket`：dispatcher 以 ACP Agent 身份对外服务。

- 外部 ACP Client（Codex 编排层、另一个 goworker）spawn 本进程或连 socket，
  `session/prompt` 的内容即任务，落 `Source: "acp:<client>"`
- 进度通过 `session/update` 流式回给提交方；任务完成即 prompt 返回 stop reason
- 长任务与 HTTP 不同：ACP 是随连接存活的会话，提交方断连 = 取消（`session/cancel`）
  ——设计上明确「提交方需保持连接」，或后续加持久化任务队列的外部补偿接口

## 6. 工作区隔离（可选策略，非前置条件）

**worktree 不是派发的必要条件**，是 `Isolation` 的默认选项；派发只要求一个工作目录。

| 模式 | 行为 | 适用 |
|---|---|---|
| `worktree`（git 仓库默认） | `git worktree add <repo>/.goworker/dispatch/<ID> -b dispatch/<ID>`；worker 全程在 worktree 内 | 并行多任务、不想污染主工作区 |
| `shared` | worker 直接在 repo 工作区干活，能看见未提交的本地改动 | 串行任务、轻任务、需要本地上下文 |
| 非 git 目录 | 强制 shared；BaseCommit/Commits 留空 | 文档、分析类任务 |

commit 收集不依赖 worktree：派发前记 `BaseCommit`（HEAD SHA），收工后 `BaseCommit..HEAD`
即为该任务产出（shared 模式下若主区有用户未提交改动混入，由 HITL 审批环节兜底把关）。

worktree 模式的收尾：done → 人工合入（fast-forward / merge / cherry-pick 由人决定）后
`git worktree remove`；rejected/failed → 直接 remove（保留分支可选）。

## 7. HITL 合并审批

复用现有 HITL 协议（decision channel + ExpiresAt）：

- 触发：任务进入 `awaiting_review`，向 REPL 推送审批请求（commit 清单 + diff stat；
  shared/非 git 模式无 commit 清单，审批看 worker 自述与进度记录）
- 决策：`approve`（worktree 模式执行合并；shared 模式仅标记完成，commit 已在原地）、
  `reject`（worktree 弃置清理；shared 模式仅记录，改动由人自行处理）、
  `edit`（人接管，dispatcher 挂起）
- 无人在场：任务挂起等待，不超时自动决策（与 bash 命令审批不同，合并是重决策）
- 每个决策写 `audit/dispatch.jsonl`（复用 audit 风格）

## 8. REPL 命令与事件

```
/dispatch <prompt>              ← 用默认 worker 在当前仓库加任务
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
| M2 | client.go 派发链路 + 隔离策略（worktree 默认，shared 可选） | fake worker 真实跑通一个任务并产出 commit；shared 与 worktree 两模式 |
| M3 | dispatcher 插件 + REPL 命令 + HITL 审批 + statusbar 事件 | REPL 全流程：加任务→进度→审批→合入 |
| M4 | ACP Agent 端点（接受外部任务）+ ZCode 自身 ACP 化 | 两个 goworker 实例互派任务 |

## 10. 风险与开放问题

- **worker 的 bash 不受 ai-sandbox 门禁**：worker 是独立进程，自己的工具自成沙箱；
  dispatcher 的安全边界是 worktree 隔离 + 合并审批。若要更紧，M4 后可对 zcode worker
  下发 sandbox 配置（它本身就是 goworker）
- **request_permission 无人值守策略**：默认拒绝并记审计，避免 worker 卡死等不到人
- **MCP 对照**：MCP 方案更简单但缺任务生命周期语义（无 permission 回调标准流、无 session 恢复）；
  ACP 的 `session/load` 支持崩溃恢复 dispatcher 侧任务，这是选 ACP 的核心收益之一
