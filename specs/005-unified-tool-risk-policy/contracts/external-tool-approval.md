# Contract: External Tool Approval

**Date**: 2026-09-27

## Scope

本契约定义 MCP、sys 和显式注册工具如何接入 HITL 与 allow_always。bash 继续使用 `001-command-decision-engine` 的命令规则契约。

## Registration Examples

MCP bridge MUST attach a policy declaration:

```text
risk_level=unclassified                 # 若 MCP server 没有可信分类
requires_permission=true
allow_always_key=mcp_<server>_<tool>
```

A client-ward tool descriptor MUST use canonical values:

```text
risk_level=benign..reserved
requires_permission=true
allow_always_key=sys_<name>
```

An explicit hub tool opts in:

```text
Metadata:
  risk_level: medium
  requires_permission: "true"
  allow_always_key: "my-stable-operation"
```

Default/internal tools without `requires_permission=true` retain their existing direct execution behavior.

## Matching

The persisted key is the only default identity matcher. Tool arguments are not persisted or compared. A tool that needs narrower authorization must register separate stable tool identities; parameter-level policy is out of scope for this feature.

The persisted `MaxRisk` prevents a later risk downgrade/upgrade from silently broadening approval. Current risk must be `<= MaxRisk`.

## Mode Boundaries

- normal + matching rule → execute
- auto → `auto_max_risk` decides; tool rule does not bypass auto policy
- strict/readonly → reject for medium-reserved even when matching rule exists
- hard deny → reject regardless of matching rule
