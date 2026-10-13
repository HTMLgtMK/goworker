# Contract: Unified Risk Policy

**Date**: 2026-09-27

## Tool Metadata Contract

`core.Tool.Metadata` 是 runtime 内部策略声明，不进入 LLM provider 的工具 schema。

```text
risk_level: benign | low | medium | elevated | high | critical | unclassified | reserved
requires_permission: "true" | "false"
allow_always_key: stable non-sensitive string
permission_policy: "always_hitl"  # optional
```

规则：

- 缺失/非法 `risk_level` → unclassified。
- `requires_permission=true` 的工具进入统一策略；MCP/sys 桥接自动设置。
- `allow_always_key` 为空时不展示 allow_always。
- key 禁止包含调用参数、secret 或用户输入；注册方负责稳定性。
- `permission_policy=always_hitl` 只能加强策略，不能绕过 strict/readonly/hard deny。

## Risk Assessment Contract

```text
Assess(tool/command) -> RiskAssessment
RiskAssessment.Level    = benign..reserved
RiskAssessment.HardDeny = independent bool
```

- `HardDeny=true` 必须返回 deny。
- critical 但 `HardDeny=false` 在 normal 模式必须进入 HITL。
- denylist 命中必须产生 `HardDeny=true` 和稳定 reason code `deny_pattern`。
- risky pattern 命中只提供风险等级，不自动产生 hard deny。

## Policy Contract

```text
Decide(assessment, policyConfig, toolAllowRules) -> allow | hitl | deny
```

- normal：benign/low allow，medium-reserved HITL。
- auto：当前等级 `<= auto_max_risk` allow，否则 HITL；无 responder 时拒绝。
- strict/readonly：benign/low allow，medium-reserved deny。
- tool allow rule 只在 normal 下把相同 key 且风险不超上限的 HITL 降级为 allow。
- HardDeny 和 strict/readonly 优先于所有 allow rule。
- `auto_max_risk` 缺省 low，比较为闭区间 `<=`。

## Configuration Contract

```yaml
sandbox:
  mode: auto
  auto_max_risk: low
  allow_rules:
    - match: "git push"
      max_risk: high
      effects: [network, file_write]
  tool_allow_rules:
    - key: "mcp_filesystem_write"
      max_risk: medium
      desc: "trusted filesystem writer"
```

`allow_rules` 继续只表示 bash 命令规则；`tool_allow_rules` 是新增的外部工具批准状态。

旧 command-only `allow_rules` 必须原样解析。旧 sys `risk_level` 值 `never/mode/always` 只能由兼容 adapter 处理，不得进入新的 canonical metadata。

## HITL/ACP Contract

`InterruptRequest` 增加 additive 字段：

```json
{
  "risk_level": "critical",
  "effects": ["network"],
  "allow_always": false,
  "allow_always_key": "mcp_filesystem_write"
}
```

- `allow_always_key` 非空 → `PermissionFromInterrupt` 添加 `allow_always`。
- 空值 → 只返回 `allow_once` / `reject_once`。
- `DecisionFromOption` 对 allow_always 返回 `DecisionApproveAlways`。
- 未知 option id、过期、连接断开、无 responder → reject。
- ACP 不检查 `ToolName == "bash"`。

## Persistence Contract

allow_always 的顺序必须是：

```text
derive key + max risk
  → persist and dedupe
  → update current runtime snapshot
  → execute current call
```

任一步持久化失败都不得执行当前工具调用。持久化 callback 必须保证并发审批不会丢更新；daemon plugin 还要写回 config 文件，ACP worker 至少更新进程内 runtime config。
