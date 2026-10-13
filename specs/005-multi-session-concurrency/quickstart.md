# Quickstart: Multi-Live-Session Concurrency

This guide validates the feature from storage primitives through ACP/daemon behavior. It assumes Go 1.26.1 and a configured local goworker checkout.

## Focused Tests

Run the session persistence and runtime tests:

```bash
go test ./ai-runtime/session/...
go test ./ai-runtime/agent/...
```

Run dispatch and daemon session tests:

```bash
go test ./ai-dispatch/...
go test ./daemon/internal/core/service/...
go test ./daemon/internal/cli/vscode/...
```

Run the complete race-enabled validation after implementation:

```bash
go test -race ./ai-runtime/... ./ai-dispatch/... ./daemon/...
```

## Scenario: Two Independent Live Sessions

1. Start the ACP worker or daemon ingress with persistence enabled.
2. Create session A and session B with different working directories or prompts.
3. Submit one prompt to each before either completes.
4. Verify both executions make progress without waiting for a global execution gate.
5. Verify every update, tool call, permission request, cancellation, and final response carries the originating session identity.
6. Inspect the session root and verify there is one history JSONL per session, with no shared `current.jsonl` branch.

Expected result: A and B overlap in execution and their histories contain no records from the other session.

## Scenario: Same-Session Single Flight

1. Create one live session.
2. Submit a long-running prompt.
3. Submit another prompt to the same session before the first completes.
4. Cancel the first prompt and verify the second follows the existing duplicate-prompt contract.

Expected result: the session never has two active prompts mutating the same conversation, and cancellation does not affect another session.

## Scenario: Restart and Load

1. Create at least two sessions with distinct user messages.
2. Archive one session and leave another live.
3. Stop the service cleanly.
4. Start it again and call session/list.
5. Load each session by stable ID.
6. Verify the live session is promptable, the archived session is discoverable and read-only, and histories are unchanged.

Expected result: lifecycle state and history survive independently.

## Scenario: Legacy Migration

1. Seed a temporary storage root with the previous `current.jsonl` plus multiple `archive/*.jsonl` files.
2. Start the service once and record the resulting session IDs and histories.
3. Stop and start it again.
4. List sessions again and compare IDs, statuses, titles, and history counts.

Expected result: each legacy history appears once, current is live, old archive entries are archived, and the second startup creates no duplicates or additional migrations.

## Required Regression Checks

- Compact and rewind affect only the selected session.
- Checkpoints and cursor advancement survive restart.
- HITL responses are delivered to the waiting session only.
- Persistence-disabled mode keeps independent in-memory sessions without creating files.
- Shared audit, memory, MCP, and configuration paths pass `go test -race`.
