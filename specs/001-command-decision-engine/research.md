# Research: Command Decision Engine

**Date**: 2026-08-17

本文件记录 Phase 0 研究的决策与取舍。所有决策基于对现有源码（`daemon/internal/sandbox/`、`daemon/internal/plugins/agent/`）的核实与设计评审。

## 1. shell 解析：mvdan/sh vs 手写扩展

**Decision**: 不引入 mvdan/sh，扩展现有手写解析器（`safe.go` 的 `splitShellSegments`/`splitMain`/`envResolve`）。

**Rationale**:
- 项目无重依赖偏好（宪法 II），`daemon/go.mod` 最小化；mvdan/sh 是仓库最大的新增传递依赖。
- 分类器只需要"检测"不需要"解析"：对 `$(`、反引号、`bash -c`，只需判断"这段含注入向量"→ 整段升级 R4 + CodeExecution，无需走嵌套脚本 AST。
- 项目特有豁免逻辑（伪设备 `pseudoDevRedirectOnly`、内置 `/dev/` 规则作用域、`env` 包装器契约）与 mvdan/sh 节点类型映射生硬，引入易回归 R1/R2 golden 测试。

**Alternatives considered**:
- mvdan/sh（`syntax.NewParser`）：完整 AST，能精确解析但重、且与项目豁免逻辑不匹配。
- 仅加正则：成本最低，但换行绕过等结构性漏洞正则防不住。

## 2. 风险等级粒度

**Decision**: R0-R7 八级，其中 **R6 = unclassified（信任缺口）**，R7 = reserved（模型预留）。

**Rationale**:
- R0-R5 覆盖 只读 → 工作区写 → 提升/破坏 → 提权/密钥/代码执行 → denylist 的语义阶梯，与现有 `safeReadonlyCmds`/`flagCheckCmds`/denylist 能力映射自然。
- R6 独立于安全等级：它是"无法分类"的状态，默认进 HITL（宪法 III：Unknown≠Safe），不是"介于 R5/R7 之间的风险"。
- R7 reserved 为 future model 的分类输出留位，避免后续 schema 翻动。

**Alternatives considered**:
- 三态布尔（现状）：无法表达"工作区内写 vs 提权"的差异，策略只能一刀切。
- 连续分数 0-1：精确但难以配置、难解释，对无模型的 MVP 过度设计。

## 3. 审计格式与落点

**Decision**: jsonl append + fsync，路径 `config.DefaultDir()/audit/audit.jsonl`，记录含用户最终决策（user_decision）。落点在 HITL 中间件内（注入 `*sandbox.AuditLogger`），非独立 AuditMiddleware。

**Rationale**:
- 复用 `session/store.go` 的 `appendRecord` 模式（append-only + fsync），零新依赖、进程崩溃不丢记录。
- 独立 AuditMiddleware 在链上跑在 HITL 前后，**拿不到用户 approve/edit/reject 的终局**——而用户决策正是未来训练数据的标签（宪法 V）。放在 HITL 中间件内，决策闭环后才落盘。
- 每条 `AuditEntry` 即训练样本：完整命令 + 结构化评估 + 引擎建议 + 人类标签。

**Alternatives considered**:
- 决策引擎内直接写：`Assess`/`Decide` 是纯函数（无 I/O），为可测试性必须保持纯。
- 结构化日志 syslog：无本地 syslog 依赖，且 jsonl 更贴近训练数据消费。

## 4. AllowRules 匹配语义

**Decision**: token 前缀匹配（非正则），`AllowRule.Match` 归一化后与主命令 token 前缀比较（`git push` 命中 `git push --force origin`，不命中 `git fetch`）。

**Rationale**:
- 避免配置驱动的正则注入（FR 3 验收 5）；确定性、可预测。
- 与分类器现有归一化（`splitMain` token 化）一致，无需新解析。
- `Effects` 子集校验防止规则越界放行（`git push` 规则不放过破坏性副作用），strict/readonly 模式覆盖一切规则（宪法 III）。

**Alternatives considered**:
- 正则匹配：灵活但注入风险 + 与分类器归一化语义不一致。
- 前缀字符串匹配：实现最简但会误命中 `git` → `gitty`，token 级更严谨。

## 5. 迁移策略：Check shim

**Decision**: `Check` 变成 `Evaluate(...).Error()` 的兼容 shim，`Outcome.Error()` 精确还原三态契约（nil / NeedsConfirmationError / error）。

**Rationale**:
- 现有 `errors.As(&NeedsConfirmationError)` 调用点（`mw_hitl.go`）和全部 sandbox 回归测试零改动通过（宪法 IV 迁移门禁，FR-012）。
- Assess/Decide 拆为纯函数，各自可独立 table-test；行为差异（Unknown→HITL）只在 Phase 2 一步落地，可定位可回滚。

**Alternatives considered**:
- 直接改 `Check` 签名：调用方全部破坏，违背"每阶段独立可测"。
- 新包完全替换：增加 import 图，违背宪法 I 的最小改动原则。
