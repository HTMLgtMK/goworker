# Production Readiness TODO

优先级 P0 = 必须修 / P1 = 强烈建议 / P2 = 按需投入

---

## P0 — 核心 Bug & 安全

- [x] **修复优雅关闭** — frontend 去除信号监听，main 接管生命周期，`defer StopAll` 覆盖所有退出路径
- [x] **bash 工具沙箱** — 三层安全模型：denylist 拦截、risk 检测弹确认、workdir 限制。支持 normal/strict/readonly/off 模式
- [ ] **Context 传递断裂** — `plugin.go:144` 用 `context.Background()` 而不是继承请求链，超时传播链断了
- [ ] **配置写入非原子** — `saveEnvFile()` 直接覆写文件，写入中途 crash 会丢数据或留下残缺文件

---

## P1 — 生产基础设施

- [ ] **结构化日志** — 替换 `log.Printf`，引入 log level（debug/info/warn/error）、结构化字段（request_id, plugin, cmd）、可插拔输出后端
- [ ] **LLM 调用限流 & 重试** — 429/5xx 自动重试 + exponential backoff，可配置的 rate limit，防止 API 被打爆
- [ ] **Makefile** — 常用命令封装（build/test/lint/run/clean），不用手敲 `go run` 长路径
- [ ] **版本信息** — `-version` 标志 + `ldflags` 注入版本号/commit/构建时间，方便线上定位
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
