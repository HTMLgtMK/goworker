# Session Lifecycle and Concurrency Contract

## Scope

This contract describes the behavior expected by ACP clients and the daemon's session-aware ingress. Existing method names remain unchanged unless an additive lifecycle status field is required by the implementation.

## Session Identity

- `session/new` returns a stable `sessionId`.
- The ID remains valid after restart while its session history exists.
- `session/load` requires an explicit ID and never falls back to the current session on failure.
- An unknown or unsafe ID returns an error without opening or mutating another session.

## Session States

- `live`: accepts one active prompt at a time.
- `archived`: can be listed and loaded for history replay but rejects new prompts in v1.
- `isCurrent`: identifies the selected session independently of lifecycle state.

A response that lists sessions should expose enough information for clients to distinguish lifecycle state from current selection. If the existing protocol type cannot represent status, the implementation must add it additively or expose an equivalent documented field without changing existing fields' meaning.

## Prompt Concurrency

- Prompts for different live session IDs may execute concurrently.
- A second prompt for a session with an active prompt follows the existing duplicate-prompt error/cancellation contract.
- `session/cancel` cancels only the identified session's active prompt.
- Every `session/update`, permission request, tool result, and prompt response uses the originating session ID.

## Lifecycle Operations

- Archiving changes session status and leaves its history addressable in place.
- Loading a live session selects it for the caller and restores its own runtime context.
- Loading an archived session replays history but does not silently reactivate it.
- Lifecycle failures are returned explicitly and do not change another session.

## Persistence and Migration

- One session has one durable JSONL history file.
- Restart reconstructs history, cursor, compact state, checkpoints, and lifecycle status from that file.
- Legacy current and archive files are migrated once, with current mapped to `live` and archived files mapped to `archived`.
- Migration is safe to retry and never duplicates a legacy history.
