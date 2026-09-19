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

---

## Android 前端（`android-frontend-acp` 分支 + goworkerandroid 工程）

架构：gomobile 管道桥（`daemon/mobile`）+ 进程内嵌入 ACP worker，Kotlin 实现 ACP Client。
协议见 `daemon/mobile/PROTOCOL.md`，构建见 `docs/mobile-build.md`。
真机已验证：worker 进程内启动 / 流式对话 / thinking 渲染 / write_file 真执行 / bash→HITL 对话框 / 错误路径。

### 系统工具协议（client-ward capabilities）

- [x] **协议定义** — 落地为 `x-device/*` 应用层扩展方法（`x-device/tools` 探测 + `x-device/call` 转发），**不进 ai-dispatch/protocol 标准面**（用户决策：具体能力不写入共享协议）；方法常量在 `service/sys_relay.go`，机制复用 `Server.Call`
- [x] **worker 侧工具注册** — `service/sys_relay.go`：ProbeDeviceTools（2s 超时 / method-not-found=无能力 / 条目清洗 + 32 上限）+ RelayDeviceTools（sys_ 前缀 + risk_level 元数据 + 转发 Execute）；acpWorker 经 SessionServerAware 捕获通道、CollectTools 组合
- [x] **风险分级门控** — `ai-core`：Tool.Metadata + BeforeToolEvent.ToolDef（通用字段，声明随工具走）；`hitl.go checkSys`：risk_level(never/mode/always) × 沙箱模式 裁决矩阵；SessionDeps 零改动
- [x] **Kotlin 侧执行器** — `DeviceTools` 注册表（描述符 + handler 同处声明）+ `AcpClient` client-ward 分发（未知方法自动 -32601）；send_notification 含 POST_NOTIFICATIONS 运行时权限
- [x] **真机 e2e** — deepseek 触发 sys_send_notification → HITL 对话框（declared risk_level=mode 展示）→ Allow once → 系统通知真实弹出（dumpsys + 用户确认）

### UI 打磨

- [ ] **会话管理** — `session/list`（历史列表 UI）+ `session/load`（回放渲染，update 先于响应到达）+ 新建会话入口
- [ ] **Markdown 渲染** — assistant 正文（代码块/列表/粗体）；代码块等宽字体
- [ ] **thinking 折叠** — 当前灰色平铺，改为可折叠（"思考过程"默认收起）
- [ ] **工具行按 toolCallId 关联** — tool_call 与 tool_result 成对展示（现状不同工具的 RESULT 文本会拼接进同一条）
- [ ] **usage 状态条** — 订阅 `usage` 事件渲染 tokens/上下文窗口百分比（事件已在流里，UI 未接）
- [ ] **输入体验** — 多行输入优化、发送中禁用态、错误气泡与 agent 消息的视觉分层

### 工程收尾

- [ ] **Android 工程推远端** — goworkerandroid 已 git init（main@4dab96b），建 GitHub 仓库并 push
- [ ] **make aar 目标** — goworker 仓库 Makefile/CMake 封装 gomobile bind + 拷贝到 Android 工程（现手工命令，见 docs/mobile-build.md）
- [ ] **CI** — Android assembleDebug + goworker go test/gofmt；host_test 补 -race 与 Write-after-Close 用例
- [ ] **rebase 到 master** — 等 feat/task-detail-acp-observer 合并后，android-frontend-acp rebase 清理分叉
- [ ] **发布形态** — release 签名 + minify 时 gobind JNI keep 规则（proguard-rules.pro 现为空）
