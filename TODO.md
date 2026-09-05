# Production Readiness TODO

优先级 P0 = 必须修 / P1 = 强烈建议 / P2 = 按需投入

---

## P0 — 核心 Bug & 安全

- [x] **修复优雅关闭** — frontend 去除信号监听，main 接管生命周期，`defer StopAll` 覆盖所有退出路径
- [x] **bash 工具沙箱** — 三层安全模型：denylist 拦截、risk 检测弹确认、workdir 限制。支持 normal/strict/readonly/off 模式
- [x] **HITL 重构** — 替换阻塞回调为 interrupt token + decisions channel 握手，支持 approve/edit/reject/respond 四种决策
- [x] **Context 传递断裂** — spec.Context 新增 Ctx 字段，`handleAgent` 改用 `ctx.Ctx` 代替 `context.Background()`
- [x] **配置写入非原子 + 迁移 YAML** — 替换 .env 为 config.yaml，写入用 tmp+rename 保证原子

---

## P1 — 生产基础设施

- [x] **结构化日志** — 替换 `log.Printf`，引入 log level（debug/info/warn/error）、结构化字段（request_id, plugin, cmd）、可插拔输出后端（slog Handler）。`internal/logger/`：`Setup` 装配、按大小轮转、按天清理，`config.yaml` 新增 `log:` 段（level/file/max_size_mb/max_age_days）
- [ ] **LLM 调用限流 & 重试** — 429/5xx 自动重试 + exponential backoff，可配置的 rate limit，防止 API 被打爆
- [x] **构建编排（CMake 替代 Makefile）** — `CMakeLists.txt` 驱动 Go toolchain：build（版本注入）/test/test-race/vet/run/install 目标、`-DGOOS/-DGOARCH` 交叉编译、`instance` 目标生成多运行时隔离目录；脱离 workspace 的独立编译由 daemon/go.mod require+replace 支持
- [x] **版本信息** — `main.version` 变量 + `-v/--version/version` 子命令；CMake 构建经 `git describe` 注入（`ldflags -X main.version=...`），源码直跑为 dev
- [ ] **CI 流程** — `.github/workflows/`：go vet、golangci-lint、race detector、build 检查，合 PR 前自动跑

---

## P2 — 功能补全 & 架构打磨

- [ ] **Session/Auth 实现** — `SessionProvider` 接口已有但有实现，补一个真实的后端（token 签发、校验、过期）
- [ ] **配置系统升级** — 结构化 Config 对象、配置校验、支持多种来源（文件/env/flag）合并、hot-reload 接口
- [ ] **TUI 前端** — Bubble Tea 前端，补上 `internal/frontend/tui/` 的 model/update/view/styles
- [ ] **Web 前端** — WebSocket 实时输出 + REST 接口，支持远程访问
- [ ] **插件热加载** — 运行时动态加载/卸载插件，不需要重启进程

---

## P3 — 质量工程（用户指定放后面）

- [ ] **单元测试** — Engine 路由、中间件链、Agent 循环、Provider 请求解析
- [ ] **集成测试** — 模拟 LLM 端到端 ReAct 循环、插件生命周期
- [ ] **Fuzz 测试** — 边界输入（空命令、超大参数、畸形 JSON）

---

> 按依赖顺序：P0 可以并行修，P1 建议从 Makefile → 结构化日志 → CI → 限流/重试 依次推进。
> 每个任务完工后回 TODO.md 打勾 ✅，并更新相关文档。
