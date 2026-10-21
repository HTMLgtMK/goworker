# Phase 0 审计：插件分层与编译期裁剪

日期：2026-10-13
分支：`feature/plugin-registry-build-tags`
依据：`/Users/tinguo/Downloads/PLAN (2).md`

## 当前依赖与入口

- Go workspace 位于根目录 `go.work`，包含 `ai-core`、`ai-dispatch`、`ai-memory`、`ai-runtime`、`ai-sandbox`、`daemon` 六个 module。
- CMake 当前只有一个 `goworker` 构建产物。`GOWORKER_NO_PLUGINS` 会转换为 `goworker_no_<name>` tags，但仍通过 `daemon/internal/app/plugins_<name>.go` 与对应 `_off.go` 裁剪入口；没有 full/agent/dispatcher/minimal 四个独立 target。
- `daemon/internal/app/application.go` 直接 import `daemon/internal/core/service`，并在 `Application.Agent` 持有 `*service.AgentPlugin`。
- `daemon/internal/app/plugins_agent.go` 直接构造 `service.NewPlugin`；`plugins_dispatcher.go` 直接构造 `dispatcher.NewPlugin`。两个 off 文件只提供空 `register*` 函数。
- `daemon/internal/core/engine/engine.go` 只依赖 `core/model` 的 `Plugin`/`DependentPlugin`/`Hub` 契约，但 `Register` 立即按当前 map 检查依赖；`StartAll`、`StopAll`、`Notify` 遍历 map，生命周期顺序不确定。
- `daemon/internal/app/acpworker.go` 不调用 `app.New`，只通过 `daemon/internal/core/service.ServeACPWorker` 提供轻量 ACP worker；这一条轻启动约束必须保留。
- `daemon/mobile/host.go` 当前也是直接 import `core/service`，通过 `ServeACPWorker` 建立 Android ACP 管道；它不是完整 Engine/Plugin 装配入口。
- VS Code 前端构造点是 `daemon/cmd/goworker/main.go:startVSCodeFrontend`，传入 `appl.Agent`。消费者接口实际需要 `ListSessions`、`CurrentSessionID`、`SetSession`、`ReloadCurrentSession`、`CurrentSessionHistory`、`ArchivedSessionHistory`、`SessionModes` 七个方法，定义在 `daemon/internal/cli/vscode.SessionSource`。
- Agent 的 fallback 入口是 `daemon/internal/core/service/plugin.go` 调用 Hub 的 `SetFallbackHandler`；本轮只迁移注册/装配边界，不改变 fallback 路由。
- no-agent 依赖图仍包含 `github.com/tinguo/goworker/daemon/internal/core/service`，说明 build tag 还没有切断具体 Agent 实现所在包的静态依赖。

## 基线结果

命令均从仓库根目录执行：

| 检查 | 结果 | 说明 |
|---|---|---|
| `go test ./daemon/...` | PASS | full 基线通过 |
| `go test -tags=goworker_no_dispatcher ./daemon/...` | PASS | agent-only 基线通过 |
| `go test -tags=goworker_no_agent ./daemon/...` | FAIL | `internal/app/application_test.go:46` 仍无条件断言 Agent 已注册 |
| `go test -tags=goworker_no_agent,goworker_no_dispatcher ./daemon/...` | FAIL | 同一条 Agent 无条件断言 |
| `go build ./daemon/cmd/goworker` | PASS | full 构建通过 |
| `go build -tags=goworker_no_dispatcher ./daemon/cmd/goworker` | PASS | agent-only 构建通过 |
| `go build -tags=goworker_no_agent ./daemon/cmd/goworker` | PASS | dispatcher-only 构建通过 |
| `go build -tags=goworker_no_agent,goworker_no_dispatcher ./daemon/cmd/goworker` | PASS | minimal 构建通过 |
| `go list -deps -tags=goworker_no_agent ./daemon/cmd/goworker \| grep '/core/service\|/plugins/agent'` | FAIL（命中） | 当前命中 `daemon/internal/core/service`，正是待消除的宿主耦合 |
| `gomobile version` | 不可用 | 当前环境没有 `gomobile` 命令；Android AAR 不能在本机完成验证 |

构建通过不代表裁剪正确：当前 `core/service` 同时承载 Agent 和 ACP worker，所以 no-agent 仍会把该包带入依赖图。

## 阶段边界与风险

- Phase 1 只建立 registry、descriptor/factory 和 builtin 条件 import；Factory 注册不得触发 IO 或实例化插件。
- Phase 2 迁移目录并切断 `app`、Engine、CLI 到具体 Agent 类型的引用；VS Code 以 `SessionSource` 等最小消费者契约接入。
- Phase 3 必须把插件依赖解析、启动顺序和逆序停止做成显式稳定顺序，不能继续依赖 map 遍历。
- Phase 4 需要把 CMake 的插件组合映射为互不覆盖的独立目标，并保留 `GOWORKER_NO_PLUGINS` 兼容入口。
- Phase 5 受本机缺少 `gomobile` 阻塞；先确保 Agent-only 绑定入口和 tag 传递可验证，再补真实 AAR/demo 验收。Android 仍是发布阻断项。

## Phase 0 完成判定

已完成依赖/入口审计、四种 Go build/test 基线和 Android 工具可用性记录；本阶段没有修改源码行为。后续实现必须保持 ACP worker 不经 `app.New`，并以本文件的失败项作为迁移期间的已知基线，不能用删除测试断言的方式掩盖。
