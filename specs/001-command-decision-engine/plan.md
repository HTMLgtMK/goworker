# Implementation Plan: Command Decision Engine

**Branch**: `001-command-decision-engine` | **Date**: 2026-08-17 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-command-decision-engine/spec.md`

## Summary

把 goworker 现有的正则三态命令安全层（`daemon/internal/sandbox.Check`）升级为结构化风险分级 + 策略决策引擎：命令 → `RiskAssessment`（R0-R7 分级 + Effects 副作用位集 + 稳定 reason 码）→ Policy 决策矩阵（normal/strict/readonly/off × 风险等级 → allow/hitl/deny）。Unknown≠Safe（R6 默认 HITL）。Normalize 强化封堵换行绕过、命令替换、解释器 `-c`/`-e` 代码执行。新增用户预批准规则（AllowRules）与审计 jsonl。不含 tiny model / confidence calibration / gVisor（macOS）。

## Technical Context

**Language/Version**: Go 1.26.1（workspace：daemon + memory 双 module）

**Primary Dependencies**: 无新增。stdlib + 现有 `gopkg.in/yaml.v3`。明确不引入 mvdan/sh（见 research.md）

**Storage**: 审计 jsonl 落盘到 `config.DefaultDir()/audit/audit.jsonl`（append + fsync，仿 `session/store.go`）

**Testing**: `go test -race`，table-driven；迁移门禁为旧回归测试（`safe_test.go`/`regression_test.go`/`mw_hitl_test.go`）零改动通过

**Target Platform**: macOS (darwin)，CLI REPL daemon

**Project Type**: CLI/daemon（内部库 `sandbox` 包 + agent 中间件）

**Performance Goals**: 命令评估 < 1ms（Assess/Decide 为纯函数、无 I/O，审计异步/独立文件）

**Constraints**: 本版本无 OS 级进程隔离；审计默认关闭（`audit_log: false`）；迁移期行为除 Unknown→HITL 外逐字节不变

**Scale/Scope**: 单机 REPL；一次一个命令评估，无并发热点（审计用 mutex 保护）

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **I. 模块边界** ✅ — `sandbox` 包保持不依赖 agent/engine，所有新类型留在包内；`mw_hitl.go` 通过已有 `BeforeTool` 钩子接入，无跨模块内部依赖。
- **II. 无重依赖** ✅ — 手写解析器扩展，mvdan/sh 明确不引入（research.md 论证）。
- **III. 安全默认** ✅ — Unknown→HITL（FR-004）、deny > hitl > allow、confidence 不可作裁决依据、HITL 超时默认拒绝，全部落地。
- **IV. 测试优先** ✅ — table-driven + `-race`；迁移门禁 FR-012/SC-005 明确。
- **V. 可观测性** ✅ — 审计 jsonl（FR-010），人类决策进记录。

Phase 1 复核：全部通过，无违规，无需 Complexity Tracking justification。

## Project Structure

### Documentation (this feature)

```text
specs/001-command-decision-engine/
├── plan.md              # This file
├── research.md          # Phase 0：mvdan/sh 取舍 / R-level 粒度 / 审计格式
├── data-model.md        # Phase 1：实体与关系
├── quickstart.md        # Phase 1：端到端验证指南
├── contracts/           # Phase 1：sandbox 包 API 契约 + HITL 消息契约
├── checklists/          # Phase 0：spec 质量清单
└── tasks.md             # Phase 2（/speckit-tasks 生成）
```

### Source Code (repository root)

```text
daemon/internal/sandbox/           # 核心：全部新增/修改
├── risk.go                        # 新增：RiskLevel(R0-R7)、Effects 位集、RiskAssessment、Source、Reason、CommandRequest
├── normalize.go                    # 新增：换行拆分、子shell/命令替换/解释器 -c/heredoc 检测、结构化写命令表
├── assess.go                       # 新增：Assess() 分类器（吸收 safe.go 三态逻辑）
├── policy.go                       # 新增：Decision、Outcome、AllowRule、Policy.Decide、Evaluate、Check shim
├── audit.go                        # 新增：AuditLogger（jsonl append + fsync）
├── config.go                       # 修改：Check→Evaluate shim、AllowRules 接入、移除死 AllowList
├── safe.go                         # 修改：三态逻辑迁 assess.go，保留 splitMain/envResolve/validateFlags 等 helpers
├── defaults.go                     # 修改：riskPatternLevel/riskPatternEffects 等级映射
└── *_test.go                       # 新增：risk/normalize/assess/policy/audit table-driven 测试

daemon/internal/config/config.go    # 修改：AllowRuleConfig、AllowRules、AuditLog 字段
daemon/internal/plugins/agent/middlewares/mw_hitl.go  # 修改：改 Evaluate/Outcome、审计钩子、风险信息进 InterruptRequest
daemon/internal/plugins/agent/session.go              # 修改：buildMiddlewareChain 构造审计并注入 HITL 中间件
daemon/internal/spec/hitl.go        # 修改：InterruptRequest 加 additive 字段 RiskLevel/Effects
daemon/internal/frontend/stdin/hitl_consumer.go       # 可选修改：渲染 [R4] 风险标签
```

**Structure Decision**: 所有决策类型留在现有 `sandbox` 包（拆文件实现 Assess/Decide 纯函数分离，而非新建 decision 包）——沿用包注释契约"不依赖 agent 或 engine，可被任何插件复用"，避免新 import 图；审计日志复用 `session/store.go` 的 append+fsync 模式。若后续 policy+audit 超 2k 行再提升为独立包。

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

无违规。`RiskLevel` 的 R6/R7 两级语义（unclassified/reserved）在 data-model.md 中说明，不视为额外复杂度——它是 future model 的契约，避免未来 schema 翻动。
