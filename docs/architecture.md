## 目录结构

```
daemon/
├── cmd/goworker/
│   └── main.go                  # 入口
│
├── internal/
│   ├── spec/                    # 纯类型定义
│   │   ├── types.go             # Plugin, Command, Tool, Context, Session, Event, Middleware
│   │   └── context.go           # NewContext 构造
│   │
│   ├── core/                    # Engine 实现
│   │   ├── engine.go            # Engine：注册、路由、Eval、事件
│   │   └── middleware.go        # 内置中间件
│   │
│   ├── frontend/
│   │   ├── tui/
│   │   │   ├── model.go         # Bubble Tea Model
│   │   │   ├── update.go        # Update 循环
│   │   │   ├── view.go          # View 渲染
│   │   │   └── styles.go        # 样式定义
│   │   ├── stdin/
│   │   │   └── stdin.go         # headless
│   │   └── web/                 # （未来）
│   │       ├── handler.go
│   │       └── ws.go
│   │
│   └── plugins/                 # 业务插件
│       └── ...
│
├── docs/
│   └── architecture.md
│
└── go.mod
```

---

## 当前状态 vs 目标

| 文件 | 当前 | 目标 |
|---|---|---|
| `spec/types.go` | Context 有 Hub 字段，无 Session | Context 无 Hub，有 Session + Values |
| `core/engine.go` | 不存在 | Engine：注册 + 路由 + 中间件 + 事件 + Eval |
| `core/middleware.go` | 不存在 | + 内置中间件 |
| `spec/context.go` | 不存在 | + NewContext |
| `internal/plugin/` | hub.go + types.go | 拆为 spec/ + core/，删 hub.go |
| `cmd/main.go` | 内联 PluginA/B | + Engine + 前端 |

---

## 下一步实施建议

1. **删 `internal/plugin/` 和 `internal/repl/`** — 旧结构清场
2. **建 `internal/spec/types.go`** — Plugin, Command, Context, Session, Middleware, Event
3. **建 `internal/spec/context.go`** — NewContext 方法
4. **建 `internal/core/engine.go`** — Engine 结构体 + 注册 + 路由 + Eval + 中间件链
5. **建 `internal/core/middleware.go`** — 内置中间件
6. **写 `frontend/stdin/`** — 最简单验证
7. **写 `frontend/tui/`** — Bubble Tea
8. **更新 `cmd/main.go`** — 新架构串起来

每个步骤都是可运行的独立增量，不用一步到位。
