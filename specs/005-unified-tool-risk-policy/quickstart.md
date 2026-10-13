# Quickstart: Unified Tool Risk Policy

**Date**: 2026-09-27

## Configuration

```yaml
sandbox:
  mode: auto
  auto_max_risk: low
  tool_allow_rules: []
```

benign/low 工具在 auto 模式直接执行；medium-reserved 进入 HITL。critical 不再因为等级本身被硬拒。

## Tool Metadata

MCP/sys bridge 自动设置策略 metadata。显式注册工具需要声明：

```text
risk_level=medium
requires_permission=true
allow_always_key=my-stable-operation
```

缺失或非法 risk_level 按 unclassified 处理。

## Local Verification

```bash
go test ./ai-sandbox/... ./ai-runtime/middlewares/... ./ai-runtime/hitl/...

go test ./daemon/internal/core/model/... ./daemon/internal/core/service/... ./daemon/internal/app/config/...

go test -race ./ai-core/... ./ai-dispatch/... ./ai-memory/... ./ai-runtime/... ./ai-sandbox/... ./daemon/...

go vet ./ai-core/... ./ai-dispatch/... ./ai-memory/... ./ai-runtime/... ./ai-sandbox/... ./daemon/...
```

## Acceptance Checks

- normal + low → no interrupt
- normal + critical without hard deny → interrupt
- deny pattern + critical + HardDeny → deny without interrupt
- auto + `auto_max_risk=low` + low → allow
- auto + medium/critical/unclassified/reserved → HITL, no responder → reject
- strict/readonly + persisted tool rule + medium → reject
- custom tool without `requires_permission` → existing pass-through
- custom tool with `allow_always_key` → ACP includes allow_always
- persistence failure → current tool call does not execute
- old bash `allow_rules` and `Check` tests remain green
