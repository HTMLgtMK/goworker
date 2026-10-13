# Implementation Plan: Unified Tool Risk Policy

**Branch**: `005-unified-tool-risk-policy` | **Date**: 2026-09-27 | **Spec**: [spec.md](spec.md)

## Summary

在 001 的 bash 命令决策引擎之上，为所有受策略控制的工具统一 `benign-reserved` 风险分类；把 critical 从“天然 deny”改成可 HITL；把 denylist hard-deny、运行模式、auto 阈值、HITL 和用户持久化批准拆成独立层。新增外部工具 metadata 接入和 tool allow rule，不污染 bash `AllowRule`。

## Technical Context

**Language**: Go 1.26 workspace（ai-core、ai-runtime、ai-sandbox、daemon）

**Storage**: 现有 config.yaml；新增 `sandbox.auto_max_risk` 和 `sandbox.tool_allow_rules`。Agent plugin 写回配置；ACP worker 至少更新进程内配置，是否落盘取决于其宿主配置能力。

**Protocol**: 现有 `session/request_permission`；runtime interrupt payload additive 增加 allow_always 能力 key，旧客户端缺字段时只显示 once 选项。

**Testing**: table-driven unit/integration tests；必须运行 `go test -race ./...`，覆盖 mode × benign-reserved、metadata 传播、ACP option 和并发持久化。

## Constitution Check

- **模块边界**：bash 规则仍留在 ai-sandbox；外部 tool approval 使用独立类型，不在 `AllowRule` 上增加 ToolName。
- **安全默认**：缺失/非法风险=unclassified；无 responder/持久化失败=reject；hard-deny/strict/readonly 优先。
- **兼容性**：`Check`、旧 `allow_rules`、bash command rule 保持；旧 sys risk strings 只经显式 adapter。
- **可观测性**：审计区分 risk、hard deny、engine decision、user decision、tool allow key（不记录原始 args）。
- **测试优先**：每个阶段先加 RED 测试，再接实现；旧回归零改动通过。

## Project Structure

```text
specs/005-unified-tool-risk-policy/
├── spec.md
├── research.md
├── data-model.md
├── quickstart.md
├── plan.md
├── tasks.md
└── contracts/
    ├── risk-policy.md
    ├── external-tool-approval.md
    └── acp-permission.md
```

## Implementation Areas

- `ai-sandbox/spec.go`, `assess.go`, `policy.go`, `config.go`, `defaults.go`: HardDeny、canonical risk、auto threshold 和 decision matrix；保持 bash rule 独立。
- `ai-runtime/config/spec.go` 与 `daemon/internal/app/config/config.go`: YAML DTO、双向转换、默认值和校验。
- `ai-core/core/spec.go`: metadata key 常量/契约（若需要）；provider 不序列化 metadata。
- `ai-runtime/hitl/spec.go`: additive `AllowAlwaysKey` 等字段。
- `ai-runtime/middlewares/hitl.go`: 通用 tool policy resolver、metadata normalization、HITL/allow_always 接线。
- `ai-runtime/agent/default_tools.go`, `daemon/internal/core/service/mcp.go`, `sys_relay.go`, `tools.go`: metadata 生产与传播。
- `ai-runtime/mcp/types.go`: 如 MCP tool schema 需要承载可信风险声明，扩展内部 DTO；没有声明时桥接默认 unclassified。
- `daemon/config.yaml.example`: 增加 auto、auto_max_risk、tool_allow_rules 示例，并移除“mcp 不受 sandbox 管控”的过时描述。
- `README.md`, `docs/architecture.md`, `docs/dispatcher.md`, `daemon/mobile/PROTOCOL.md`: 更新 MCP 门控、benign-reserved、critical HITL、auto 阈值和 ACP capability 契约。
- `daemon/internal/core/model/permission.go`: capability-driven ACP option generation。
- `daemon/internal/core/service/plugin.go`, `acp_worker.go`, `ai-runtime/agent/spec.go/session.go`: 独立 tool rule 持久化和 session runtime snapshot。

## Phases

### Phase 0 - Contract and migration baseline

冻结 canonical metadata、RiskAssessment.HardDeny、auto threshold 默认值、旧 sys descriptor adapter 和 off 语义；补 RED contract tests。

### Phase 1 - Risk and mode policy

实现 metadata normalization、HardDeny 与 critical HITL；把 ModeAuto 改为阈值策略；补所有 benign-reserved × mode 表格测试。

### Phase 2 - Tool registration propagation

让 MCP/sys/custom registered tool 产生 metadata，并确保 hub/skill/memory/profile 包装不丢失；未 opt-in 内部工具维持直通。

### Phase 3 - Generic HITL and approval state

引入独立 `ToolAllowRule`，实现 allow_always key、风险上限、持久化失败 fail-closed、并发去重和当前 session 快照更新。

### Phase 4 - ACP and compatibility

让 ACP 根据 capability key 展示 allow_always；迁移旧 sys `never/mode/always`；保持旧 bash option/config/Check 行为。

### Phase 5 - Verification and docs

跑 race、集成测试、配置 round-trip、工具 metadata 传播测试；更新 dispatcher/README/architecture 文档与 quickstart。

## Risks

- 旧 sys client 仍发送字符串风险值：必须 adapter 或清晰迁移错误，不能当 benign。
- `ToolDef` metadata 在包装层丢失会导致外部工具绕过策略。
- allow_always 并发写配置可能丢更新，需要宿主级 mutex/原子保存。
- auto 模式中没有 responder 时必须拒绝，不能因为“自动模式”误放行。
- off 与 hard-deny 的关系当前历史语义不完全一致，必须在 Phase 0 固定并测试。
