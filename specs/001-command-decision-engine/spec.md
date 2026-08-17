# Feature Specification: Command Decision Engine

**Feature Branch**: `001-command-decision-engine`

**Created**: 2026-08-17

**Status**: Draft

**Input**: User description: "把 goworker 现有的正则三态命令安全层升级为结构化风险分级 + 策略决策引擎。核心：RiskLevel R0-R7 + Effects 维度 + Assess/Decide 分离 + Unknown→HITL + Normalize 强化 + AllowRules 配置 + 审计 jsonl。不含 tiny model / confidence calibration / gVisor 隔离。"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 结构化风险分级与策略决策 (Priority: P1)

作为 goworker 用户，我希望代理执行的每条命令都有明确的风险等级和副作用标注，并由独立于分类的决策策略决定放行/确认/拒绝——这样安全边界不再依赖"正则碰运气"，而是可解释、可审计、可调策略。

**Why this priority**: 这是整个方案的地基。没有分级和决策分离，后续的预批准、审计、模型接入都无从谈起。它把当前"三态布尔 + 单点 Check"升级为可扩展的决策链路。

**Independent Test**: 在 REPL 里跑 agent 触发命令执行即可验证：只读命令直接放行、高危命令直接拒绝、无法分类的命令弹出确认。不需要配置任何新项。

**Acceptance Scenarios**:

1. **Given** 沙箱处于 normal 模式，**When** 代理执行 `git status`，**Then** 命令直接放行且不弹确认
2. **Given** 沙箱处于 normal 模式，**When** 代理执行 `rm -rf /`，**Then** 命令被直接拒绝且不弹确认
3. **Given** 沙箱处于 normal 模式，**When** 代理执行无法分类的命令（如 `pip install foo`），**Then** 弹出用户确认而不是静默放行
4. **Given** 沙箱处于 readonly 模式，**When** 代理执行任何写操作命令，**Then** 命令被直接拒绝
5. **Given** 沙箱处于 strict 模式，**When** 代理执行任何中高风险命令，**Then** 命令被直接拒绝
6. **Given** 用户确认界面，**When** 命令需要确认，**Then** 界面展示命令的风险等级和副作用清单

---

### User Story 2 - 命令解析强化（封堵绕过） (Priority: P2)

作为 goworker 用户，我希望命令安全层能识别复杂 shell 结构（换行拼接、命令替换、子shell、解释器代码执行），而不是只看表面的主命令——因为当前的解析会把 `echo hi` 换行后接 `sudo rm -rf /tmp` 误判为安全命令直接放行。

**Why this priority**: 这是实际存在的安全绕过。虽然优先级低于核心分级（P1），但它直接关涉"是否真的安全"，必须在预批准规则上线前堵住，否则规则会建立在漏检之上。

**Independent Test**: 构造恶意命令字符串验证拦截效果，不需要启动代理即可用命令级检查验证（`echo hi\nsudo rm -rf /tmp` 必须不再放行）。

**Acceptance Scenarios**:

1. **Given** 一条含换行拼接的命令，**When** 前面是安全命令、换行后是高危命令，**Then** 整体按最高风险评估而非按首段放行
2. **Given** 一条含命令替换的命令（`$(...)` 或反引号），**When** 执行，**Then** 风险被升级并进入确认
3. **Given** 一条含解释器代码执行命令（如 `bash -c '...'`、`python -c '...'`），**When** 执行，**Then** 风险被升级并进入确认
4. **Given** 一条引号内包含 `$(...)` 字面量的命令，**When** 执行，**Then** 不被误报为命令替换（保持原风险评估）

---

### User Story 3 - 用户预批准规则 (Priority: P2)

作为 goworker 用户，我希望能为信任的命令族配置预批准规则（命令 + 风险上限 + 允许的副作用），命中规则的命令跳过确认——因为日常高频命令（如 `git push`）每次弹确认很烦，但又不希望规则本身成为安全漏洞。

**Why this priority**: P1 的 Unknown→HITL 会让未分类命令多弹确认。预批准规则是用户缓解弹窗、又不牺牲安全的出口，必须紧随核心分级上线。

**Independent Test**: 在配置文件里加一条规则，然后在 REPL 里执行对应命令验证是否免确认；再执行规则允许范围之外的命令验证是否仍弹确认。

**Acceptance Scenarios**:

1. **Given** 配置了 `git push` 的预批准规则（风险上限 R4），**When** 代理执行 `git push origin main`，**Then** 命令免确认放行
2. **Given** 上一条规则，**When** 代理执行 `git fetch`，**Then** 规则不命中，按常规评估
3. **Given** 一条预批准规则的允许副作用不包含破坏性操作，**When** 代理执行带破坏性副作用的命令，**Then** 仍进入确认而非被规则放行
4. **Given** 沙箱处于 strict/readonly 模式，**When** 代理执行被预批准规则覆盖但本应拒绝的命令，**Then** 规则不覆盖模式硬拒，命令仍被拒绝
5. **Given** 规则匹配是基于命令词（而非正则），**When** 配置含特殊字符的命令名，**Then** 不会引入正则注入

---

### User Story 4 - 决策审计追踪 (Priority: P3)

作为 goworker 用户，我希望每次命令评估与决策都有结构化审计记录（含我的最终选择），这样安全事件可回溯，且积累的数据能用于未来训练更好的分类器。

**Why this priority**: 审计本身不改变执行行为，是长期价值（安全可追溯 + 训练数据沉淀）。在核心链路稳定后补上，不影响 MVP 交付。

**Independent Test**: 开启审计后执行一次需要确认的命令并作出选择，检查审计文件里出现带用户决策标记的记录。

**Acceptance Scenarios**:

1. **Given** 开启审计，**When** 代理执行一条命令并作出决策，**Then** 审计文件新增一条记录，含命令、风险等级、副作用、引擎建议决策和用户最终选择
2. **Given** 审计开启，**When** 连续执行多条命令，**Then** 每条记录按时间顺序追加且不丢失
3. **Given** 审计未开启，**When** 代理执行命令，**Then** 不产生审计文件

---

### Edge Cases

- 引号内的命令替换字面量（`echo "$(date)"`）不应误报为高危
- 写向伪设备（`> /dev/null`）不应误报为写操作
- `env` 包装器（`env FOO=bar /usr/bin/curl -s`）应解析真实命令后评估
- HITL 确认超时时应默认拒绝而非放行
- 空命令、nil 配置不应触发异常
- 换行拆分后不应产生空段导致误判

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: 系统 MUST 对每条待执行命令输出结构化风险等级（R0-R7，R6 表示无法分类）
- **FR-002**: 系统 MUST 识别命令的副作用维度：文件读/写、网络、提权、破坏性、密钥访问、进程生成、代码执行
- **FR-003**: 系统 MUST 根据风险等级与运行模式（normal/strict/readonly/off）的矩阵决定放行/确认/拒绝，分类与决策 MUST 相互独立
- **FR-004**: 无法分类的命令（unknown）MUST 进入用户确认，禁止静默放行
- **FR-005**: denylist 命令（如 `rm -rf /`）在 normal 模式 MUST 直接拒绝，不进确认
- **FR-006**: 系统 MUST 按换行拆分命令段并逐段评估，避免前段安全命令掩盖后段高危命令
- **FR-007**: 系统 MUST 识别命令替换、子shell、解释器 `-c`/`-e` 代码执行，并将风险升级至确认档
- **FR-008**: 用户 MUST 能配置预批准规则（命令族 + 风险上限 + 允许副作用），命中规则在 normal 模式免确认
- **FR-009**: 预批准规则 MUST NOT 覆盖 strict/readonly 模式的硬拒，且 MUST 做副作用子集校验
- **FR-010**: 系统 MUST 记录每次评估与决策（含用户最终选择）到结构化审计文件
- **FR-011**: 现有三类模式语义（off 绕过、strict 拒全部风险、readonly 拒全部写）MUST 保持不变
- **FR-012**: 旧命令安全回归测试 MUST 在迁移后零改动通过

### Key Entities *(include if feature involves data)*

- **命令风险评估**: 命令对应的风险等级、副作用集合、判定来源与原因（稳定机器码）
- **预批准规则**: 命令族匹配词、风险上限、允许副作用集合、描述
- **审计记录**: 时间戳、命令、风险等级、副作用、引擎建议、用户最终决策、执行结果

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 已知只读命令（`git status`、`ls`、`grep` 等）零干扰直接放行，不新增确认
- **SC-002**: denylist 高危命令（`rm -rf /` 等）100% 被拦截
- **SC-003**: 换行拼接绕过样本（安全命令 + 换行 + 高危命令）100% 被拦截或转确认
- **SC-004**: 开启审计时，100% 的命令决策产生带用户决策的审计记录
- **SC-005**: 迁移门禁：现有命令安全回归测试 100% 零改动通过
- **SC-006**: 预批准规则命中时 100% 免确认；越界命令 0% 被规则误放行

## Assumptions

- 主运行平台为 macOS，本版本不做 OS 级进程隔离（容器/gVisor），"沙箱"指策略层 + 工作目录限制
- 本版本不含机器学习模型，confidence 字段预留但不可作裁决依据
- 审计功能默认关闭，避免无谓的磁盘写放大
- 现有 `allowed_work_dir` 工作目录限制继续生效
- 现有 HITL 交互协议（批准/编辑/拒绝/回复）复用，仅补充风险信息展示
