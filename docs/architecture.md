# Architecture

```
daemon/
├── cmd/goworker/
│   └── main.go                  # 入口：生命周期、信号管理
│
├── internal/
│   ├── spec/                    # 纯类型定义
│   │   ├── types.go             # Plugin, Command, Tool, Context, Session, Event, Middleware
│   │   └── context.go           # NewContext 构造
│   │
│   ├── core/                    # Engine 实现
│   │   ├── engine.go            # Engine：注册、路由、Eval、事件、生命周期
│   │   └── middleware.go        # 内置中间件（Logging, Session）
│   │
│   ├── frontend/
│   │   ├── stdin/               # headless REPL（当前唯一前端）
│   │   │   └── stdin.go
│   │   ├── tui/                 # Bubble Tea（占位）
│   │   └── web/                 # WebSocket + REST（占位）
│   │
│   └── plugins/
│       └── agent/               # ReAct agent 插件
│           ├── plugin.go        # /agent, /model 命令 + 配置管理
│           ├── agent.go         # ReAct 循环 + tool 执行
│           ├── llm.go           # Message, ToolCall, Token 等类型
│           └── provider.go      # OpenAI API 客户端（chat, stream, SSE）
│
├── docs/
│   └── architecture.md
│
├── TODO.md                      # 生产就绪待办
├── CLAUDE.md
├── README.md
└── go.mod
```

---

## 生命周期架构

所有前端的生命周期由 main 统一管理，前端只负责 I/O 和交互逻辑。

```
┌──────────────────────────────────────────────┐
│                   main()                      │
│  NewEngine() + defer StopAll()               │
│  Register plugins → StartAll()               │
│  Launch frontend goroutine ← errCh           │
│  select { ← sigCh                            │
│    case <-errCh:  ← frontend 正常/异常退出     │
│    case <-sigCh:  ← OS 信号                   │
│  }                                            │
│  ↓ return → defer StopAll()                  │
└──────────────────────────────────────────────┘
         │ signal (SIGINT/SIGTERM)
         ▼
┌──────────────────┐     ┌─────────────────────┐
│   frontend/      │     │   core.Engine        │
│   (goroutine)    │     │   - plugin lifecycle │
│   stdin / tui    │     │   - middleware chain │
│   只做 I/O       │     │   - command routing  │
│   不碰信号/退出  │     │   - event broadcast  │
└──────────────────┘     └─────────────────────┘
```

### 关键约定

1. **前端只做 I/O** — 不注册信号监听、不调用 StopAll、不发起进程退出。所有信号统一由 main 处理
2. **生命周期归 Engine** — `defer engine.StopAll()` 在 main 入口注册，覆盖所有退出路径
3. **前端通过 errCh 向 main 报告退出** — 正常退出发 nil，异常退出发 error
4. **Error 不 panic** — 启动阶段的错误用 `log.Printf` + `return`（defer 会自动触发 StopAll），不直接用 `log.Fatal`

---

## 中间件链（洋葱模型）

```
注册: [middleware A, middleware B, middleware C]
执行: A → B → C → handler → C → B → A
```

中间件注册顺序 = 进入顺序。每个 middleware 调 `next()` 进入下一层，`next()` 返回后执行收尾逻辑。

---

## Agent Plugin

`/agent` 命令启动 ReAct 循环：

```
User Input → system prompt + tools → LLM
  ↑                                    ↓
  │                              Tool Calls?
  │                              ├─ no  → 返回结果
  │                              └─ yes → 执行工具 → 追加结果
  └──────────────────────────────────────────┘
```

最大 15 轮迭代。工具集可扩展：默认内置 bash、read_file、write_file，插件可通过 Hub 注册额外工具。

### 命令安全层（sandbox）

bash 工具执行前经结构化决策链路（`daemon/internal/sandbox`）：

```text
命令 → Assess（风险分级 R0-R7 + 副作用 Effects）→ Policy 决策矩阵 → allow / hitl / deny
```

- **Rule ≠ Model**：正则规则引擎负责确定性判定；未知命令（R6）默认进入 HITL（Unknown ≠ Safe），不静默放行。
- **deny > hitl > allow**：strict 拒全部风险、readonly 拒全部写；预批准规则（`allow_rules`，token 前缀匹配 + 副作用子集校验）只覆盖 normal 模式的确认，不越过模式硬拒。
- 注入向量检测（命令替换/子shell/解释器 `-c`/eval/heredoc）与换行拆分封堵绕过。
- 审计：`audit_log: true` 时每次决策（含用户最终选择）落 `audit/audit.jsonl`，是未来模型训练数据。

---

## 配置级联

```
In-memory config (runtime override)
  ↓ (fallback)
LLM_* 环境变量
  ↓ (fallback)
~/.config/goworker/.env 文件
```

`/model set` 写入 `.env` 文件，覆盖优先级最高的 in-memory config。
