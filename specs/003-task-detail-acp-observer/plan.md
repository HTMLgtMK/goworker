# Implementation Plan: Task Detail ACP Observer

## Critical Files

| File | Change |
|---|---|
| `daemon/internal/dispatcher/plugin.go` | turn single `acpRoute` values into per-task route slices; add safe add/remove/snapshot helpers; fan out updates outside the mutex |
| `daemon/internal/dispatcher/ingress.go` (or current ingress implementation file) | parse `--attach`, replay EventLog update bodies, tail with cursor and complete on stream-ending status |
| `daemon/internal/dispatcher/ingress_test.go` | add ACP-level replay, tail, cancellation, status and malformed-payload tests |
| `daemon/internal/dispatcher/plugin_test.go` | cover multi-route lifecycle and fanout isolation if existing test helpers make this more direct |
| `specs/003-task-detail-acp-observer/*` | preserve protocol and verification contract |

## Execution Steps

1. Add failing tests for an attached completed task. Assert raw native `session/update` output, observer session-ID remapping, and `end_turn`; include a status record that produces no update.
2. Implement strict attach argument parsing and task lookup. Reuse existing directive error helpers and `tailComplete` rather than inventing a second task completion definition.
3. Implement a replay helper that decodes only `EventUpdate` payloads, logs/skips malformed stored data, and calls the existing reporter `Update` method.
4. Implement the subscribe/replay/tail loop using existing `EventLog.Subscribe` and `EventsAfter`. Always defer unsubscribe; advance cursor even when an event is skipped.
5. Refactor ACP route storage to `map[string][]acpRoute`. Register/remove an exact route identity and snapshot routes while locked, then send updates outside the mutex.
6. Add failing tests for snapshot-to-tail no-gap behavior, `awaiting_review`/`merging` behavior, missing input/task errors, cancellation isolation, and original submitter plus observer behavior.
7. Run focused unit tests, then `go test -race` on affected packages. Fix only real defects found by those checks.
8. Perform a local Unix-socket ACP smoke test against a running dispatcher: submit a task from one client, attach another, verify original typeful update stream and observer cancellation isolation.

## Implementation Constraints

- Keep `--events` unchanged. Its JSON envelope is a diagnostic protocol, not a renderer protocol.
- Do not introduce an observer into `acpRoutes` merely to bridge history and live output; EventLog cursor tail is the sole lossless bridge.
- Do not alter TaskEvent persistence schema or make Task status into chat content.
- Do not add permission/HITL behavior in this branch.

## Verification

```bash
go test ./daemon/internal/dispatcher ./ai-dispatch/task
go test -race ./daemon/internal/dispatcher ./ai-dispatch/task
go test ./...
```

Manual smoke path after test success:

1. Connect two ACP clients to the configured local Unix socket.
2. Submit a task through client A and wait for at least one worker update.
3. Create a fresh session through client B and prompt `--attach <task_id>`.
4. Confirm B receives history plus subsequent worker updates with B's session ID and native ACP update shapes.
5. Cancel B's session and verify A continues receiving progress and the task continues normally.
