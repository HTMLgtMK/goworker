# Tasks: Command Decision Engine

**Input**: Design documents from `specs/001-command-decision-engine/`

**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/ ✅

**Tests**: TDD 强制（宪法 IV）——每个用户故事的测试任务 MUST 先写先失败（RED），再实现（GREEN）。

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to
- Include exact file paths in descriptions

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 建立迁移基线，确认无新依赖

- [X] T001 运行 `cd daemon && go test ./internal/sandbox/... ./internal/plugins/agent/middlewares/...` 记录当前全绿状态作为迁移基线
- [X] T002 [P] 确认 `daemon/go.mod` 无新增依赖需求（mvdan/sh 不引入，研究决策已定）

**Checkpoint**: 基线确立——后续所有重构以"旧测试零改动通过"为门禁

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 基础类型层——所有用户故事依赖的风险分级/副作用类型

**⚠️ CRITICAL**: 用户故事实现开始前必须完成

- [X] T003 创建 `daemon/internal/sandbox/risk.go`：`RiskLevel`（R0-R7，含 String/ParseRiskLevel/MarshalText）、`Effects` 位集（8 个 Effect + Has/Names/MarshalJSON）、`Source`、`Reason{Code,Pattern,Detail}`、`CommandRequest`、`RiskAssessment{Level,Effects,Reasons,Source,Confidence}`（按 data-model.md）
- [X] T004 [P] 创建 `daemon/internal/sandbox/risk_test.go`：table-driven 断言 String/Parse 往返、"R6" 语义、Effects.Names 顺序稳定、非法 Parse 报错

**Checkpoint**: 类型层就绪，`go build ./internal/sandbox/...` 通过

---

## Phase 3: User Story 1 - 结构化风险分级与策略决策 (Priority: P1) 🎯 MVP

**Goal**: 命令有明确风险等级与副作用，Assess/Decide 分离，Unknown→HITL，决策矩阵生效
**Independent Test**: REPL 里 `git status` 放行、`rm -rf /` 直拒、`pip install` 弹确认（spec US1 验收 1-6）

### Tests for User Story 1 (TDD - 先写先失败) ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T005 [P] [US1] 创建 `daemon/internal/sandbox/assess_test.go`：已知命令→{Level,Effects} 映射 table-driven——`git status`→R1、`git push`→R3、`curl -s`→R1、`curl -o`→R2、`rm -rf /tmp/x`→R3、`sudo apt install`→R4、`rm -rf /`→R5、`openssl x509 -text`→R6、`env FOO=bar /usr/bin/curl -s`→R1（env 解析保留）
- [X] T008 [P] [US1] 创建 `daemon/internal/sandbox/policy_test.go`：决策矩阵全组合 + Effects floor 升级（CodeExecution→R4）+ readonly/strict 硬拒 + **Unknown→hitl**（`pip install foo` 在 normal 必 hitl）+ R5 恒 deny

### Implementation for User Story 1

- [X] T006 [US1] 创建 `daemon/internal/sandbox/assess.go`：`Assess(cmd, cfg) RiskAssessment` 纯函数，吸收 `safe.go` 三态逻辑与 defaults 等级映射（T010 依赖）
- [X] T007 [US1] 迁移门禁：重构后 `daemon/internal/sandbox/safe_test.go`、`regression_test.go` 零改动通过
- [X] T009 [US1] 创建 `daemon/internal/sandbox/policy.go`：`Decision`、`Outcome{Decision,Level,Effects,Reasons,Override}`、`Outcome.Error()` 还原三态、`Policy.Decide` 决策矩阵、`Evaluate(req,cfg) Outcome`；改造 `config.go` 的 `Check` 为 `Evaluate(...).Error()` shim
- [X] T010 [US1] 扩展 `daemon/internal/sandbox/defaults.go`：`riskPatternLevel`/`riskPatternEffects` 映射（sudo→R4、rm/mv→R3、`>`/`>>`→R2、`|`→R2、kill→R4、dd/mkfs/`rm -rf /`→R5），供 Assess 结构化推导
- [X] T011 [US1] 修改 `daemon/internal/spec/hitl.go`：`InterruptRequest` 增 additive 字段 `RiskLevel string`、`Effects []string`（JSON omitempty，老前端兼容）
- [X] T012 [US1] 修改 `daemon/internal/plugins/agent/middlewares/mw_hitl.go`：`checkBash` 改 `sandbox.Evaluate` + `Outcome`，deny→block、hitl→confirm（RiskLevel/Effects 填入 InterruptRequest）、allow→放行
- [X] T013 [US1] 修改 `daemon/internal/plugins/agent/session.go` `buildMiddlewareChain`（约 L165）：`NewHITLMiddleware` 构造处接入新签名（审计参数先传 nil，US4 填充）
- [X] T014 [US1] 更新 `daemon/internal/plugins/agent/middlewares/mw_hitl_test.go`：断言 InterruptRequest 带 RiskLevel/Effects、deny/allow 分支

**Checkpoint**: US1 完成——决策矩阵生效、Unknown→HITL 落地、迁移门禁通过（P1 MVP 可交付）

---

## Phase 4: User Story 2 - 命令解析强化（封堵绕过） (Priority: P2)

**Goal**: 换行绕过封死，命令替换/子shell/解释器 `-c` 升级风险，引号内字面量不误报
**Independent Test**: `echo hi\nsudo rm -rf /tmp` 不再放行（spec US2 验收 1-4）

### Tests for User Story 2 (TDD) ⚠️

- [X] T015 [P] [US2] 创建 `daemon/internal/sandbox/normalize_test.go`：换行拆分（`echo hi\nsudo rm -rf /tmp` 必须升级）、`$(...)`/反引号/`${x}`→R4+CodeExecution、`bash -c 'rm -rf /'`/`python3 -c`/`perl -e`→R4、`eval`→R4、heredoc `<<`；**负例**：`echo "$(date)"`→R1、quoted 反引号不升级、`echo "a > b"` 保持 allow

### Implementation for User Story 2

- [X] T016 [US2] 创建 `daemon/internal/sandbox/normalize.go`：`splitShellSegments` 补 `\n`/`\r` 拆分（修真实绕过 bug）、子shell/命令替换/解释器 `-c`/`-e`/eval/heredoc 引号感知检测、结构化写命令表替换 `hasWriteOps` 正则（rm→Destructive、mv/cp/touch/ln→FileWrite、chmod/chown→FileWrite+Privileged、kill/pkill→ProcessSpawn）
- [X] T017 [US2] 重跑 R1/R2 回归：`go test ./internal/sandbox/...` 确认 normalize 强化无回退

**Checkpoint**: 绕过封死，负例守门

---

## Phase 5: User Story 3 - 用户预批准规则 (Priority: P2)

**Goal**: 配置驱动免确认规则，token 前缀匹配，effects 子集校验，strict/readonly 不被覆盖
**Independent Test**: `git push` 配置后免确认、`git push --force` 仍弹确认（spec US3 验收 1-5）

### Tests for User Story 3 (TDD) ⚠️

- [X] T018 [P] [US3] 创建 `daemon/internal/config/config_test.go` 增 `allow_rules` YAML round-trip 用例：`{match,max_risk,effects,desc}` 解析正确
- [X] T019 [P] [US3] 创建 `daemon/internal/sandbox/policy_test.go` 增 override 用例：`git push`→allow（normal）、`git push --force`（destructive 超 effects 子集）→hitl、strict/readonly 下规则不覆盖硬拒、token 前缀不误中 `git fetch`

### Implementation for User Story 3

- [X] T020 [US3] 修改 `daemon/internal/config/config.go`：`SandboxConfig` 增 `AllowRules []AllowRuleConfig`、`AuditLog bool`；`AllowRuleConfig{Match,MaxRisk,Effects,Desc}`
- [X] T021 [US3] 修改 `daemon/internal/sandbox/config.go` `NewFromConfig`：编译 `AllowRules` 为 `[]AllowRule{MatchTokens,MaxRisk,Effects,Desc}`，**移除死代码 `Config.AllowList`**
- [X] T022 [US3] 修改 `daemon/internal/sandbox/policy.go` `Policy.Decide`：normal 模式 AllowRule 命中且 `assessed.Effects ⊆ rule.Effects` 且 `level ≤ rule.MaxRisk` 时降级 hitl→allow；strict/readonly 不适用

**Checkpoint**: 预批准规则生效且不越界

---

## Phase 6: User Story 4 - 决策审计追踪 (Priority: P3)

**Goal**: 每次决策落审计 jsonl，含用户最终选择，审计默认关闭
**Independent Test**: 开启审计后批准一次命令，`audit/audit.jsonl` 出现带 user_decision 的记录（spec US4 验收 1-3）

### Tests for User Story 4 (TDD) ⚠️

- [X] T023 [P] [US4] 创建 `daemon/internal/sandbox/audit_test.go`：OpenAudit 建目录、Record 追加 + fsync、jsonl 逐行可解析且字段齐全、并发 Record 无数据竞争（`-race`）

### Implementation for User Story 4

- [X] T024 [US4] 创建 `daemon/internal/sandbox/audit.go`：`AuditEntry{Timestamp,Command,Cwd,RiskLevel,Effects,Reasons,Source,EngineDecision,UserDecision,Outcome}`、`AuditLogger`（mutex + O_APPEND|O_CREATE + Sync，仿 `session/store.go` appendRecord），路径 `config.DefaultDir()/audit/audit.jsonl`
- [X] T025 [US4] 修改 `daemon/internal/plugins/agent/session.go` `buildMiddlewareChain`：`cfg.Sandbox.AuditLog` 为 true 时 `sandbox.OpenAudit(...)`，注入 `NewHITLMiddleware`
- [X] T026 [US4] 修改 `daemon/internal/plugins/agent/middlewares/mw_hitl.go`：决策闭环（含 HITL 用户决策）后 `audit.Record`，`UserDecision` 填 approve/edit/reject/respond，未走 HITL 留空

**Checkpoint**: 审计链路通，默认关闭不干扰现有行为

---

## Phase 7: Polish & Cross-Cutting Concerns

- [X] T027 [P] 可选：修改 `daemon/internal/frontend/stdin/hitl_consumer.go` 渲染 `[R4]` 风险标签（新前端字段，老逻辑兼容）
- [X] T028 跑通 `specs/001-command-decision-engine/quickstart.md` 全部 5 个验证场景（含 REPL 端到端）
- [X] T029 收尾：gofmt/goimports + `go vet ./...` + `go test -race ./internal/sandbox/... ./internal/plugins/agent/middlewares/...` 全绿
- [X] T030 更新 `docs/architecture.md` 与 `README.md`：sandbox 层从"正则三态"描述升级为"Risk Assessment + Policy Engine"

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: 无依赖，可立即开始
- **Foundational (Phase 2)**: 依赖 Setup——BLOCKS 所有用户故事
- **US1 (Phase 3)**: 依赖 Foundational——MVP 核心
- **US2 (Phase 4)**: 依赖 Foundational；可与 US1 并行（normalize.go 独立文件，但 Assess 接线需 US1 的 assess.go 结构，建议 US1 后）
- **US3 (Phase 5)**: 依赖 US1（Policy.Decide 的 override 接入）
- **US4 (Phase 6)**: 依赖 US1（mw_hitl 改造）+ US3（AuditLog 配置字段）
- **Polish (Phase 7)**: 依赖所有用户故事

### User Story Dependencies

- **US1 (P1)**: 无其他故事依赖——MVP 单独可交付
- **US2 (P2)**: 仅依赖 Foundational，技术独立于 US1（换行 bug 可在 US1 前修）
- **US3 (P2)**: 依赖 US1 的 Policy.Decide
- **US4 (P3)**: 依赖 US1 中间件改造 + US3 配置字段

### Within Each User Story

- 测试 MUST 先写并 FAIL，再实现（宪法 IV）
- 类型 → 纯函数 → 接线 → 中间件
- 故事完成才进入下一优先级

### Parallel Opportunities

- T004 与 T003 顺序（类型先写方法后测）；T005/T008 可并行（不同文件）
- T015/T018/T019 可并行（独立测试文件）
- T027 可选独立
- US2 的 normalize.go 与 US1 的 assess.go 若分人可并行（但接线需串行）

---

## Implementation Strategy

### MVP First (US1 Only)

1. Setup（T001-T002）
2. Foundational（T003-T004）
3. US1（T005-T014）
4. **STOP and VALIDATE**: 迁移门禁 + 决策矩阵行为
5. 可交付：结构化分级 + Unknown→HITL + 决策矩阵

### Incremental Delivery

1. US1 → 结构化分级 + 决策矩阵（MVP）
2. US2 → 绕过封死
3. US3 → 预批准规则
4. US4 → 审计
5. 每故事独立验证，不破坏前序

---

## Notes

- **[P] 任务** = 不同文件，无依赖
- 迁移门禁：`safe_test.go`/`regression_test.go`/`mw_hitl_test.go` 零改动通过（宪法 IV）
- 行为变化点（Unknown→HITL）只在 US1 落地，可定位可回滚
- 每逻辑单元一个 commit（conventional commits）
