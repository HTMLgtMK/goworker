# Contract: sandbox 包公开 API

**Date**: 2026-08-17
**Scope**: `daemon/internal/sandbox` 对外的稳定接口。包注释契约：不依赖 agent 或 engine，可被任何插件复用。

## 函数契约

### `Check(cmd string, cfg *Config) error`（兼容 shim）

- **返回**（三态契约，保持不变）：
  - `nil` → 放行
  - `*NeedsConfirmationError` → 需用户确认（HITL）
  - 其他 `error` → 拒绝
- **语义**: `cfg == nil` 或 `cfg.Mode == ModeOff` 恒放行。
- **兼容性**: 现有调用点（`mw_hitl.go` 的 `errors.As`）与全部回归测试零改动通过。

### `Evaluate(req CommandRequest, cfg *Config) Outcome`

- **入口**: 分类 + 决策的组合（内部 `Assess` → `Policy.Decide`）。
- **返回**: `Outcome{Decision, Level, Effects, Reasons, Override}`。
- **约定**: `Evaluate(...).Error()` 与 `Check(...)` 完全等价。

### `Assess(cmd string, cfg *Config) RiskAssessment`

- **纯函数**: 无 I/O，输入命令 → 输出风险评估。
- **返回**: `{Level, Effects, Reasons, Source, Confidence}`。
- **约定**: 只升不降的检测规则在此累积（denylist/risky/classifier/normalize 检测）。

### `NewFromConfig(cfg *config.SandboxConfig) *Config`

- **编译**: 正则、AllowRules（token 前缀编译）、默认等级映射。
- **约定**: 空字段回退到内置默认（denied/risky patterns、工作目录 = 当前目录）。

## 类型不变量

| 类型 | 不变量 |
|---|---|
| `RiskLevel` | `String()` 输出 "R0".."R7"；`ParseRiskLevel` 可逆 |
| `Effects` | `Names()` 顺序稳定（审计/HITL 展示一致） |
| `Outcome.Error()` | nil ↔ allow/sandbox；`NeedsConfirmationError` ↔ hitl；其他 ↔ deny |
| `AllowRule` | `MatchTokens` 非空；`Effects` 子集校验在 Decide 内强校验 |
| `RiskAssessment.Confidence` | 规则命中恒 1.0、unclassified 恒 0.0；**消费方禁止据此裁决** |

## 配置契约

```yaml
sandbox:
  mode: normal            # normal | strict | readonly | off
  allowed_work_dir: ""    # 空 = 当前目录
  denied_patterns: []
  risky_patterns: []      # 支持 string 或 {pattern, desc}
  safe_commands: []
  allow_rules:            # 新增
    - match: "git push"
      max_risk: "R4"
      effects: ["network", "file_write"]
      desc: "..."
  audit_log: false        # 新增，默认关闭
```

**语义**:
- `allow_rules` 只覆盖 normal 模式的 hitl→allow；strict/readonly 硬拒优先。
- `audit_log: true` 时写入 `config.DefaultDir()/audit/audit.jsonl`。
