# Data Model: Command Decision Engine

**Date**: 2026-08-17

## Overview

命令决策链：`CommandRequest` → `Assess` → `RiskAssessment` → `Policy.Decide` → `Outcome` →（执行/拒绝）→ `AuditEntry`（含用户决策）。所有类型落在 `daemon/internal/sandbox` 包内。

```text
CommandRequest ──Assess()──▶ RiskAssessment ──Policy.Decide()──▶ Outcome
      │                            │                                │
      │                    Confidence (预留)                   Decision + 命中 AllowRule
      │                            │                                │
      └───────────────────────────┴──────────┬─────────────────────┘
                                              ▼
                                          AuditEntry (jsonl，含 user_decision)
```

## Entities

### RiskLevel

- **Purpose**: 命令的风险分级。R0-R5 语义阶梯，R6 是"无法分类"（信任缺口，非安全等级），R7 模型预留。
- **Values**: R0 benign / R1 low / R2 medium（工作区内写）/ R3 elevated（工作区内破坏·移动·权限）/ R4 high（提权·密钥·代码执行）/ R5 critical（denylist）/ R6 unclassified / R7 reserved
- **Validation**: 序列化为 "R0".."R7"；`ParseRiskLevel` 拒绝非法输入。

### Effects (uint32 位集)

- **Purpose**: 命令副作用维度，供 floor 升级与 AllowRule 子集校验。
- **Bits**: FileRead / FileWrite / Network / Privileged / Destructive / SecretAccess / ProcessSpawn / CodeExecution
- **Methods**: `Has(Effect)`、`Names() []string`（审计/HITL 展示用，如 `["network","file_write"]`）。

### RiskAssessment

| Field | Type | Note |
|---|---|---|
| Level | RiskLevel | 最终分级（含 floor 升级前原始值见 Source） |
| Effects | Effects | 副作用位集 |
| Reasons | []Reason | 稳定 reason 码 + 命中的 pattern + 中文描述 |
| Source | Source | bypass / rule / classifier / heuristic / unclassified |
| Confidence | float64 | **恒 1.0（规则命中）或 0（unclassified）**；策略层禁止据此裁决，仅未来模型契约 |

**Validation**: 至少一个 Reason（unclassified 时 Code="unclassified"）；Source 与 Level 语义一致。

### Reason

| Field | Type | Note |
|---|---|---|
| Code | string | 稳定机器码：`deny_pattern` / `risky_pattern` / `write_redirect` / `unsafe_flag` / `subshell` / `code_eval` / `heredoc` / `unclassified` |
| Pattern | string | 命中的正则源文本（无则空） |
| Detail | string | 人类可读中文描述 |

### CommandRequest

| Field | Type | Note |
|---|---|---|
| Command | string | 整条 shell 命令（必填） |
| Cwd | string | 预留（未来 containment） |
| Workspace | string | 预留 |

### Decision / Outcome

- `Decision`: allow / sandbox（保留枚举，MVP 不产出）/ hitl / deny
- `Outcome`: `{Decision, Level, Effects, Reasons, Override *AllowRule}` + `Error() error`（还原旧三态契约）
- **Validation**: `Outcome.Error()` 保证 nil ↔ allow/sandbox、`NeedsConfirmationError` ↔ hitl、其他 error ↔ deny。

### AllowRule / AllowRuleConfig

| Field | Type | Note |
|---|---|---|
| MatchTokens | []string | 归一化主命令 token 前缀（`git push`） |
| MaxRisk | RiskLevel | 命中可放行的最高等级 |
| Effects | Effects | 允许的副作用子集；0 = 全部 |
| Desc | string | 人类可读描述 |

**Validation**:
- `MatchTokens` 非空，首 token 非 flag。
- **只在 normal 模式降级 hitl→allow**；strict/readonly 硬拒优先。
- `assessed.Effects ⊆ rule.Effects` 才放行；`git push --force`（destructive）不被仅允许 `[network,file_write]` 的规则放过。

### AuditEntry

| Field | Type | Note |
|---|---|---|
| Timestamp | time.Time | 记录时间 |
| Command | string | 原始命令 |
| Cwd | string, omitempty | 工作目录 |
| RiskLevel | string | "R3" |
| Effects | []string | Names() |
| Reasons | []Reason | 含稳定 code |
| Source | string | 判定来源 |
| EngineDecision | string | allow/hitl/deny |
| UserDecision | string, omitempty | approve/edit/reject/respond（未走 HITL 时空） |
| Outcome | string | executed / blocked / aborted |

**Validation**: jsonl 每行一个对象；`user_decision` 是训练标签，是未来模型数据的关键字段。

## State Transitions

命令决策没有跨请求状态机（每命令独立评估）。唯一的"状态"是配置态：

```text
SandboxConfig (YAML) ──NewFromConfig──▶ sandbox.Config（编译 AllowRules/regex，惰性）
    ▲                                           │
    │ /config set 运行时修改                     ▼
    └────────────────────────────── 每命令评估：Config 是只读快照
```

审计 Logger 是进程级单例（`OpenAudit` 一次，复用文件句柄），mutex 保护并发 `Record`。
