# Contract: ACP Permission Capability

**Date**: 2026-09-27

## Request Shape

`session/request_permission` continues to use the existing permission option wire format. The runtime interrupt payload carries additive capability fields:

```json
{
  "id": "req-1",
  "tool_name": "mcp_filesystem_write",
  "risk_level": "critical",
  "allow_always_key": "mcp_filesystem_write"
}
```

Bash command approval uses `allow_always: true`; external tools use a stable `allow_always_key`. Neither field contains tool arguments or secrets.

## Option Generation

| `allow_always=false` and empty key | allow_once, reject_once |
| `allow_always=true` or non-empty key | allow_once, reject_once, allow_always |

The model layer MUST derive the option set from these capability fields. It MUST NOT special-case bash, MCP, sys, or any other tool name.

## Decision Translation

```text
allow_once   → DecisionApprove
allow_always → DecisionApproveAlways
reject_once  → DecisionReject
unknown      → DecisionReject
```

`DecisionApproveAlways` is valid only when the originating request had `allow_always=true` or a non-empty capability key. Middleware MUST reject an invalid or missing capability instead of persisting an empty rule.

## Failure Semantics

- Request timeout → reject.
- ACP connection failure → reject.
- Empty option response → reject.
- Persistence failure after allow_always → reject tool execution.
- No interactive responder → reject.
