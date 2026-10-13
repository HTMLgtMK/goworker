# Feature Specification: Unified Tool Risk Policy

**Feature Branch**: `005-unified-tool-risk-policy`

**Created**: 2026-09-27

**Status**: Draft

**Input**: Extend `001-command-decision-engine` so bash, MCP, client-ward (`sys_*`), and explicitly registered tools share one semantic risk vocabulary and one mode policy. `benign` through `reserved` are classifications only; `critical` may enter HITL. Hard deny, automatic execution, HITL, and user-persisted approval are separate decisions.

**Relationship to 001**: This feature does not replace the command engine or its `Check` compatibility shim. It consumes and extends its risk/policy contracts for non-bash tools.

## User Scenarios & Testing

### User Story 1 - 统一工具风险等级 (Priority: P1)

作为工具注册方，我希望所有受策略控制的工具都用 `benign-reserved` 描述风险，这样策略层不需要知道工具是 bash、MCP 还是设备中继。

**Independent Test**: 为每类工具构造 `benign-reserved`、缺失和非法 metadata，验证解析与默认行为，不启动真实 LLM。

**Acceptance Scenarios**

1. **Given** 工具声明 `risk_level=low`，**When** 运行时读取工具定义，**Then** 得到 `RiskLow`。
2. **Given** 工具声明 `risk_level=critical`，**When** normal 模式评估，**Then** 进入 HITL，而不是因为等级为 critical 直接拒绝。
3. **Given** 外部工具缺少或声明非法 `risk_level`，**When** 评估，**Then** 按 unclassified（unclassified）处理，不得自动放行。
4. **Given** 工具 metadata 通过 provider 发送给模型，**When** 请求序列化，**Then** 策略 metadata 不出现在模型工具 schema 中。

### User Story 2 - 风险与硬拒分离 (Priority: P1)

作为系统维护者，我希望 denylist 仍然能硬拒，但普通 critical 工具可以请求用户确认——风险等级不应该同时承担硬拒语义。

**Acceptance Scenarios**

1. **Given** 命令命中 deny pattern，**When** 运行在 normal/auto/strict/readonly，**Then** 结果为 deny，不进入 HITL。
2. **Given** 工具风险为 critical 但没有 hard-deny 标记，**When** 运行在 normal，**Then** 结果为 HITL。
3. **Given** 评估结果为 critical 且来源是普通 risky pattern，**When** 运行在 normal，**Then** 结果为 HITL。
4. **Given** critical 工具在 strict/readonly，**When** 执行，**Then** 仍被模式拒绝。

### User Story 3 - auto 模式风险阈值 (Priority: P1)

作为无人值守运行的用户，我希望低风险工具能自动执行，同时高风险工具仍能进入确认流程，而不是把 `auto` 做成无差别放行。

**Acceptance Scenarios**

1. **Given** `mode=auto`、`auto_max_risk=low`，**When** benign/low 工具执行，**Then** 自动放行。
2. **Given** 同一配置，**When** medium-reserved 工具执行，**Then** 进入 HITL；若没有可用 responder，则默认拒绝。
3. **Given** critical 工具，**When** auto 模式执行且没有 hard-deny，**Then** 可以进入 HITL。
4. **Given** `auto_max_risk` 缺失，**When** 加载配置，**Then** 使用安全默认值 low。
5. **Given** 工具风险高于阈值，**When** strict/readonly 执行，**Then** 不受 auto 阈值影响，直接按严格模式拒绝。

### User Story 4 - 外部工具显式接入策略 (Priority: P1)

作为工具注册方，我希望自定义工具通过 metadata 显式声明需要策略控制；没有声明的内部工具不应因为“不是 bash”而被强行弹窗。

**Acceptance Scenarios**

1. **Given** MCP 工具，**When** 转换为 `core.Tool`，**Then** 自动携带风险等级和策略接入 metadata。
2. **Given** `sys_*` 工具，**When** 从 client descriptor 转换，**Then** 使用 benign-reserved 风险等级，不再使用 `never/mode/always` 作为 risk_level。
3. **Given** 自定义注册工具声明 `requires_permission=true`，**When** 运行，**Then** 进入统一风险策略。
4. **Given** 普通内部工具没有策略 metadata，**When** 运行，**Then** 保持现有直通行为。
5. **Given** hub/skill/memory/profile 工具经过包装，**When** 进入 agent，**Then** metadata 不丢失。

### User Story 5 - 外部工具 allow_always (Priority: P2)

作为用户，我希望对一个明确声明授权身份的外部工具选择 `allow_always`，后续相同授权身份的调用在 normal 模式免确认，同时不把调用参数写入配置。

**Acceptance Scenarios**

1. **Given** 工具声明 `allow_always_key`，**When** 产生 HITL 请求，**Then** ACP 请求包含该能力并展示 `allow_always`。
2. **Given** 用户选择 `allow_always`，**When** 持久化失败，**Then** 当前工具调用被拒绝且不执行。
3. **Given** 持久化成功，**When** 当前调用继续，**Then** 当前 middleware 更新内存状态并执行调用。
4. **Given** 后续调用使用相同 key，**When** normal 模式评估，**Then** 免确认放行。
5. **Given** 后续调用使用不同 key 或工具未声明 key，**When** 执行，**Then** 不命中该授权。
6. **Given** 工具风险等级升高超过批准时的等级，**When** 再次调用，**Then** 不得静默复用旧授权，必须重新评估。
7. **Given** strict/readonly 模式，**When** 存在 tool allow rule，**Then** 规则不得覆盖模式硬拒。

### User Story 6 - ACP 与旧 bash 行为兼容 (Priority: P2)

作为现有前端用户，我希望 bash 的命令级预批准、ACP 请求、旧配置和 `Check` API 不因外部工具策略改造而回归。

**Acceptance Scenarios**

1. 旧 `sandbox.allow_rules` 仍按命令 token 前缀工作。
2. bash 的 `allow_always` 仍保存命令规则，不混入 tool rule。
3. ACP 只根据请求能力字段展示 `allow_always`，不再硬编码 bash 名称。
4. 未知 permission option 默认拒绝，不能因为新增字段而 fail-open。

## Requirements

### Functional Requirements

- **FR-001**: 所有受策略控制的工具 MUST 使用 `benign-reserved` 风险等级；`risk_level` 不得再使用 `never/mode/always`。
- **FR-002**: 缺失或非法的外部工具风险 metadata MUST 解析为 unclassified；unclassified 默认不得自动放行。
- **FR-003**: RiskLevel MUST 只表示风险分类；硬拒 MUST 由独立的 `HardDeny`/deny rule 表示。
- **FR-004**: deny pattern MUST 在策略启用时保持硬拒，不得因 critical 改为 HITL。
- **FR-005**: normal 模式 MUST 对 benign/low 自动放行，对 medium-reserved 进入 HITL；硬拒优先。
- **FR-006**: auto 模式 MUST 对 `risk_level <= auto_max_risk` 自动放行；超出阈值进入 HITL；无 responder 时默认拒绝。
- **FR-007**: strict/readonly MUST 拒绝 medium-reserved；任何 allow_always 或 auto threshold 都不能覆盖该拒绝。
- **FR-008**: `auto_max_risk` MUST 可配置，缺省为 low，且 MUST 通过 benign-reserved 校验。
- **FR-009**: MCP、sys 和显式声明 `requires_permission=true` 的注册工具 MUST 进入统一工具策略；未声明的内部工具保持现有行为。
- **FR-010**: 工具 metadata MUST 在 default tools、MCP、sys、hub、skill、memory、profile 的包装链路中保持，不得被静默丢弃。
- **FR-011**: `allow_always_key` MUST 是稳定、非敏感的授权身份；默认不得把 tool args 写入配置。
- **FR-012**: 外部工具 allow_always MUST 在持久化成功后才执行当前调用；失败 MUST 拒绝执行。
- **FR-013**: tool allow rule MUST 只在 normal 模式覆盖重复 HITL，不得覆盖 strict/readonly 或 hard deny。
- **FR-014**: ACP permission option MUST 根据 HITL 请求中的能力字段生成；不得通过工具名硬编码判断。
- **FR-015**: 旧 bash `AllowRule`、旧 `Check` API、旧 `allow_rules` YAML MUST 保持兼容。
- **FR-016**: 旧 sys descriptor 的 `never/mode/always` MUST 经过显式兼容转换或被明确拒绝并给出迁移错误，不得静默当作 benign。
- **FR-017**: risk、hard deny、policy decision、user decision MUST 可在审计中区分，不能只记录最终 allow/deny。

### Key Entities

- **RiskLevel**: benign-reserved 的有序风险分类。
- **RiskAssessment**: 风险等级、副作用、来源、原因和独立 hard-deny 标志。
- **ToolPolicyMetadata**: 工具的风险、是否进入策略、allow_always 授权 key。
- **ToolAllowRule**: 用户持久化的外部工具授权身份与批准时风险上限。
- **PolicyConfig**: mode 与 auto_max_risk 等运行策略。
- **PermissionRequest**: ACP/HITL 请求的工具信息、风险信息和授权能力。

## Success Criteria

- **SC-001**: 所有内置、MCP、sys 和显式注册工具的策略测试覆盖 benign-reserved；非法/缺失值全部落 unclassified。
- **SC-002**: deny pattern 样本 100% hard deny；普通 critical 样本 100% 在 normal 进入 HITL。
- **SC-003**: auto 模式阈值边界（benign、阈值、阈值+1、critical、unclassified、reserved）100% 符合矩阵。
- **SC-004**: allow_always 持久化失败时 0 次工具执行；成功后同 key 调用 100% 免重复确认。
- **SC-005**: strict/readonly 下 0 条持久化授权能够绕过拒绝。
- **SC-006**: 旧 bash 回归测试、旧 YAML 配置和 `Check` API 零改动通过。
- **SC-007**: `go test -race ./...` 通过，权限状态并发更新无丢失或重复写入。

## Assumptions & Out of Scope

- 不在本 feature 引入 OS 级隔离、模型分类器或参数级动态风险分析。
- `risk_level` 是工具/请求级声明；工具若因参数导致风险显著升高，注册方必须声明更高等级或提供独立工具。
- tool allow rule 默认按 metadata key 匹配，不把原始参数写入配置。
- `off` 保持 001 的关闭沙箱语义；hard deny 是否在 off 下仍生效必须在实现契约中明确并加测试，不能由 `RiskLevel` 隐式决定。
