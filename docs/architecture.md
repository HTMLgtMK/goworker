# Architecture

```
ai-memory/                      [module github.com/tinguo/goworker/ai-memory] 零依赖
  memory.go store.go retriever.go consolidate.go ...  # MTM + LTM，逻辑独立

ai-sandbox/                     [module github.com/tinguo/goworker/ai-sandbox] stdlib only
  spec.go config.go safe.go policy.go assess.go risk.go audit.go  # 命令安全层

ai-core/                        [module github.com/tinguo/goworker/ai-core] 零 goworker 依赖
  core/             # Tool, Message, Usage, Compressor, RuntimeEvent（spec.go 集中类型）
  agent/            # ReAct 引擎本体（无 DefaultTools，sandbox/memory/config 无关）

ai-runtime/                     [module github.com/tinguo/goworker/ai-runtime] 聚合层
  config/           # LLM/Memory/Sandbox/Session/MCP 配置 + Paths + 事件契约
  hitl/             # HITL 协议与 DecisionProvider
  agent/            # agent SDK：Session + SessionDeps + RunRequest/RunCallbacks + DefaultTools
                    # （纯 Go API，零宿主 plugin 协议依赖；宿主自行做 plugin 封装）
  middlewares/      # HITL/usage/iteration/compression/memory runtime 中间件
  session/          # 会话持久化 store（checkpoint/rewind/compact）
  mcp/ skills/ logger/ fakeserver/

daemon/                         [module github.com/tinguo/goworker/daemon] agent 后端 + 前端壳
  cmd/goworker/main.go          # CLI 壳：app.New 装配 + stdin 前端 + OS 信号
  mobile/                       # Android 壳占位（gobind bind 目标；共享 app 装配）
  internal/app/                 # 装配层：Application（配置→日志→Engine→插件→StartAll→Stop）
  internal/app/config/          # 顶层平铺 config.yaml 解析 + ToRuntime/ApplyRuntime + SaveConfig
  internal/core/engine/         # Engine：插件生命周期、命令路由、中间件链、事件广播
  internal/core/model/          # 插件/命令/前端上下文协议：Hub, Command, Plugin, Context, RenderKind
  internal/core/service/        # /agent 插件适配器：把 ai-runtime Session SDK 装配成 plugin.Plugin
                                # （资源装载 skill/MCP/memory/store + 命令注册 + 生命周期）
  internal/cli/                 # stdin REPL + statusbar（addon 订阅 ai-runtime 事件）
```

依赖方向（禁止反向）：

```
daemon ──→ ai-runtime ──→ ai-core ──→ (zero goworker deps)
                 ├──→ ai-memory
                 └──→ ai-sandbox
```

- ai-core/agent 与 ai-memory、ai-sandbox、ai-runtime 零耦合：DefaultTools 在 ai-runtime/agent，MemoryClient 为 runtime 本地接口 + ai-runtime adapter。
- 配置不跨层上溯：LLM/Memory/Sandbox/Session/MCP 在 ai-runtime/config；daemon 负责 YAML 兼容（risky_patterns 双格式）与本机路径派生。
- 事件契约倒置：ai-runtime/config 定义 EventUsage/EventIteration + UsageEvent，daemon/statusbar 订阅渲染，statusbar 不进 SDK。
- SDK/插件边界：ai-runtime/agent 是纯 Session SDK（零宿主 plugin 协议依赖），plugin.Plugin 适配器在 daemon/internal/core/service —— 宿主换协议（HTTP/MCP/mobile 等）只需重写 adapter，SDK 不动。

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

bash 工具执行前经结构化决策链路（`ai-sandbox` 独立 module）：

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
