# Data Model: Unified Tool Risk Policy

**Date**: 2026-09-27

## Overview

```text
core.Tool.Metadata
        │
        ▼
ToolPolicyMetadata ──normalize──▶ RiskAssessment
        │                              │
        │                              ├── RiskLevel benign-reserved
        │                              ├── Effects
        │                              └── HardDeny
        ▼                              │
PolicyConfig ───────────────────▶ Decision
                                       │
                 ┌─────────────────────┼──────────────────┐
                 ▼                     ▼                  ▼
              allow                  HITL               deny
                                       │
                                       ▼
                              PermissionRequest
                                       │
                                       ▼
                               ToolAllowRule state
```

## Entities

### RiskLevel

有序枚举：

| Level | Meaning |
|---|---|
| benign | benign，无副作用或可忽略副作用 |
| low | low，只读或低影响外部访问 |
| medium | medium，工作区内写入 |
| elevated | elevated，破坏/移动/权限相关操作 |
| high | high，提权、秘密访问、代码执行 |
| critical | critical，严重影响，但可进入 HITL |
| unclassified | unclassified，未知风险，默认不自动放行 |
| reserved | reserved，未来分类器/模型保留等级 |

序列化固定为 `benign`..`reserved`。任何工具 metadata 的非法值都归一到 unclassified，并保留解析失败原因供审计。

### ToolPolicyMetadata

由 `core.Tool.Metadata` 解析出的内部快照，不发送给 provider：

| Field | Type | Meaning |
|---|---|---|
| RiskLevel | `RiskLevel` | 工具声明的风险等级，缺失=unclassified |
| RequiresPermission | `bool` | 是否进入通用工具策略；MCP/sys 由桥接层默认 true |
| AllowAlwaysKey | `string` | 稳定、非敏感的用户授权身份；空=不提供 always |
| ForceHITL | `bool` | 兼容旧 sys `always` 语义；strict/readonly 仍优先拒绝 |
| ParseReason | `string` | metadata 缺失/非法时的审计原因 |

约定 metadata：

```text
risk_level:        benign..reserved
requires_permission: "true"
allow_always_key:  stable-key
permission_policy: "always_hitl"  # 仅兼容旧 sys always 或明确注册需求
```

### RiskAssessment

在 001 的 `RiskAssessment` 上增加独立硬拒语义：

| Field | Type | Note |
|---|---|---|
| Level | `RiskLevel` | 风险分类，不决定硬拒 |
| Effects | `Effects` | 副作用位集 |
| Reasons | `[]Reason` | 稳定 reason code |
| Source | `Source` | metadata / rule / classifier / heuristic / unclassified |
| Confidence | `float64` | 仅观测，不可单独授权 |
| HardDeny | `bool` | denylist 或其他明确不可执行规则命中 |

不变量：`HardDeny=true` 时 Policy 必须返回 deny；`Level=critical` 本身不得推出 `HardDeny=true`。

### PolicyConfig

| Field | Type | Default |
|---|---|---|
| Mode | `normal/auto/strict/readonly/off` | `normal` |
| AutoMaxRisk | `RiskLevel` | `low` |
| AllowRules | `[]AllowRule` | empty，bash 命令规则，继承 001 |
| ToolAllowRules | `[]ToolAllowRule` | empty，外部工具用户授权 |

### ToolAllowRule

外部工具用户批准状态，不复用 bash `AllowRule`：

| Field | Type | Meaning |
|---|---|---|
| Key | `string` | 与 ToolPolicyMetadata.AllowAlwaysKey 精确匹配 |
| MaxRisk | `RiskLevel` | 用户批准时的最高风险 |
| Desc | `string` | 可选的人类描述 |

匹配条件：normal 模式、key 相同、当前风险 `<= MaxRisk`、无 HardDeny。strict/readonly 和 hard deny 优先。

### PermissionRequest

HITL 请求增加：

| Field | Type | Meaning |
|---|---|---|
| AllowAlways | `bool` | command-style approval capability；ACP 只看能力，不看工具名 |
| AllowAlwaysKey | `string` | 外部工具稳定授权身份；非空才向 ACP 展示 allow_always |
| ToolName/Command | existing | 展示上下文 |

ACP 层不得再根据 tool name 判断 allow_always。

## Policy Matrix

| Mode | benign/low | medium-reserved | HardDeny |
|---|---|---|---|
| normal | allow | HITL | deny |
| auto | allow when `<= AutoMaxRisk`; otherwise HITL | HITL when above threshold | deny |
| strict | allow | deny | deny |
| readonly | allow | deny | deny |
| off | legacy bypass behavior | legacy bypass behavior | legacy behavior must be explicit in contract/test |

`ToolAllowRule` only changes normal `HITL → allow` and never changes the other columns.

## State Transitions

```text
ToolCall
  │
  ▼
Normalize metadata
  │
  ├── RequiresPermission=false → existing direct execution
  │
  ▼
Assess risk + HardDeny
  │
  ├── HardDeny → deny
  ├── mode policy → allow
  ├── normal + matching ToolAllowRule → allow
  └── otherwise → HITL
                    │
                    ├── allow_once → execute
                    ├── reject_once → deny
                    └── allow_always
                          ├── persist failure → deny
                          └── persist success → update local rule + execute
```

配置加载必须生成不可变运行快照；并发的 allow_always 写入需要由宿主 callback 串行化并去重。
