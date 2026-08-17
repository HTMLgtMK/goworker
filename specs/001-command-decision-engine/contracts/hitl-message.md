# Contract: HITL 中断消息

**Date**: 2026-08-17
**Scope**: agent 中间件 ↔ 前端之间的 interrupt token / decision 消息格式（`daemon/internal/spec` + `daemon/internal/plugins/agent/core`）。

## `InterruptRequest`（中间件 → 前端）

| Field | Type | Note |
|---|---|---|
| ID | string | `req-<n>` 唯一 |
| ToolName | string | "bash" 或 "mcp_<name>" |
| Command | string | 待执行命令（MCP 工具为 args JSON） |
| RiskReason | string | 人类可读风险原因 |
| Description | string, omitempty | 补充描述（MCP） |
| RiskLevel | string, omitempty | **新增**："R0".."R7"，未走分级时为 "" |
| Effects | []string, omitempty | **新增**：副作用 Names()，如 `["destructive"]` |
| CreatedAt / ExpiresAt | time.Time | 30s 有效期 |

**兼容性**: `RiskLevel`/`Effects` 为 additive 字段，老前端忽略、新前端用于展示 `[R4]` 标签，不破坏现有 `hitl_consumer.go` 渲染。

## `HITLDecision`（前端 → 中间件）

| Field | Type | Note |
|---|---|---|
| InterruptID | string | 回填请求 ID |
| Type | DecisionType | approve / reject / edit / respond |
| Command | string, omitempty | edit 时的新命令 |
| Message | string, omitempty | respond 时的用户指令 |

## 决策语义

- `approve`: 原样执行。
- `edit`: 仅 bash 支持，重写 `Args["command"]` 后执行；MCP 视为批准。
- `reject`: 中止执行，回填 `⛔ rejected by user`。
- `respond`: 中止执行，用户消息作为 tool result 回填（带 ToolCallID，满足 OpenAI 兼容后端多轮约束）。
- **超时/失败默认 `reject`**（宪法 III：永不静默放行）。

## 审计标签映射

HITL 中间件在决策闭环后写 `AuditEntry.UserDecision`：approve/edit/reject/respond。未走 HITL 的命令该字段为空。
