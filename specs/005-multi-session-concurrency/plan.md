# Implementation Plan: Multi-Live-Session Concurrency

**Branch**: `005-multi-session-concurrency` | **Date**: 2026-09-27 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/005-multi-session-concurrency/spec.md`

## Summary

Replace the current single `current.jsonl` conversation store and singleton daemon session with independently addressable session histories. Keep `session.Store` scoped to one explicit JSONL file, persist lifecycle transitions in that file, and add a daemon-side session manager that owns one runtime Session, Store, lifecycle state, and single-flight execution guard per session. Remove global execution serialization so different live sessions can run concurrently while preserving one active prompt per session. Migrate the legacy current/archive layout idempotently and retain compact, rewind, checkpoint, streaming, tool, and HITL semantics.

## Technical Context

**Language/Version**: Go 1.26.1; workspace contains the daemon and memory Go modules.

**Primary Dependencies**: Go standard library, existing ACP dispatch/protocol packages, existing runtime agent, sandbox, memory, and MCP packages; no new external dependency.

**Storage**: Append-only JSONL; one file per session under the configured session root. Legacy `current.jsonl` and `archive/*.jsonl` are migrated into that layout. Lifecycle state is recorded in the session file.

**Testing**: Existing table-driven Go tests, integration tests, and `go test -race` across `ai-runtime`, `ai-dispatch`, and `daemon`.

**Target Platform**: macOS local daemon/CLI, with behavior kept portable through standard filesystem APIs.

**Project Type**: Multi-module Go runtime library plus daemon/ACP service.

**Performance Goals**: At least two live sessions must make observable progress concurrently; no global execution gate may serialize independent session IDs. Per-session persistence retains the existing append-and-sync durability behavior.

**Constraints**: Single-process coordination only; no distributed locking or database. Same-session prompts remain single-flight. Archived sessions remain read-only in v1. Existing compact, rewind, checkpoint, streaming, tool, and HITL contracts must not regress. Unsafe session IDs must never become filesystem paths.

**Scale/Scope**: Multiple live sessions within one daemon process; migration of one existing current file plus any number of archived files; no redesign of provider, memory, MCP, or ACP protocols beyond additive session status information.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **I. Module boundaries**: PASS. Generic one-file persistence and record replay remain in `ai-runtime/session`; daemon-specific lifecycle ownership stays in daemon service. No memory-to-daemon dependency is introduced.
- **II. No unnecessary dependencies**: PASS. The design uses existing standard-library JSONL/filesystem primitives and does not add a dependency.
- **III. Security defaults**: PASS. Session IDs are validated before path use; load failures never fall back to another session; HITL behavior and conservative cancellation semantics are retained.
- **IV. Test first**: PASS. Store isolation, migration idempotency, same-session single-flight, cross-session routing, and race tests are explicit gates. Existing regression assertions remain mandatory.
- **V. Observability**: PASS with design action. Audit writes will use one explicit shared synchronization boundary or session-scoped files and include session identity so concurrent decisions remain attributable.

## Phase 0: Research Findings

The detailed decisions are recorded in [research.md](research.md):

- A Store remains single-session and receives an explicit file path.
- Lifecycle records live in the session's own JSONL; no archive directory or global manifest is introduced.
- A daemon-side manager owns runtime sessions and per-session execution gates.
- Legacy files move atomically and idempotently into the new layout.
- Shared audit, memory, MCP, and configuration resources receive explicit concurrency checks.

## Phase 1: Design

The model and external behavior are documented in:

- [data-model.md](data-model.md)
- [contracts/session-lifecycle.md](contracts/session-lifecycle.md)
- [quickstart.md](quickstart.md)

### Implementation structure

```text
ai-runtime/session/
├── record.go              # lifecycle record kind and replay/format compatibility
├── store.go               # explicit one-file Store; remove current/archive rotation
├── migrate.go             # legacy current/archive discovery and idempotent migration
└── *_test.go              # isolated stores, lifecycle replay, migration, compact/rewind regression

ai-runtime/agent/
├── spec.go                # session dependency/optional shared resource wiring
├── session.go              # per-session execution and audit ownership adjustments
└── *_test.go              # session isolation and concurrent dependency behavior

daemon/internal/core/service/
├── session_manager.go     # managed session ownership, lifecycle, selection, per-session gates
├── acp_worker.go          # map ACP IDs to managed sessions; remove global runMu
├── plugin.go              # replace singleton Session/Store with manager
├── commands.go            # route /agent, /new, and maintenance commands by selected session
├── sessions.go            # list/load summaries from session files and lifecycle state
└── *_test.go              # manager, ACP, migration, list/load, and shared-resource tests

daemon/internal/cli/vscode/
├── frontend.go            # session-aware load/run behavior using manager state
└── *_test.go              # current/live/archived selection and read-only behavior

ai-sandbox/
├── audit.go               # shared logger or explicit cross-instance synchronization
└── spec.go                # session attribution in audit entries if needed
```

## Implementation Phases

### Phase A: One-file Store and lifecycle records

- Change Store construction to accept a validated explicit session file path and create only its parent directory.
- Add lifecycle record encoding/replay without changing active conversation reconstruction.
- Remove `Archive()`'s file rename/reopen semantics; retain compact, cursor, checkpoint, and head behavior.
- Add tests for two interleaved Store instances, lifecycle replay, malformed/unknown records, and existing record-format compatibility.

### Phase B: Session manager and migration

- Add managed-session ownership with stable IDs, status, per-session runtime dependencies, and per-session single-flight state.
- Add safe path construction and stable ID validation.
- Implement legacy current/archive discovery and atomic, retry-safe migration into one file per session.
- Add manager tests for create/load/archive/select/close, restart reconstruction, and migration failure/retry behavior.

### Phase C: ACP worker concurrency

- Replace the ACP worker's runtime-session map and global `runMu` with managed sessions and per-session gates.
- Reuse the existing dispatch server's per-session active-prompt protection and cancellation map.
- Verify parallel execution with a barrier-controlled provider and verify event/HITL/cancel routing by session ID.

### Phase D: Normal daemon agent ingress

- Replace AgentPlugin's singleton Session/Store pointers and global run gate with the manager.
- Route command, `/new`, `/compact`, `/rewind`, usage, memory, and history operations to the selected or requested session.
- Preserve archived read-only behavior and separate `isCurrent` from lifecycle status.
- Update VSCode ingress session/list and session/load handling without making archived files special.

### Phase E: Shared-resource and regression hardening

- Establish one safe synchronization boundary for audit writes and add session attribution. Protect each Session's lazy audit initialization from duplicate first-run opens.
- Make `Session.Conversation()` return a real immutable snapshot rather than its backing slice before concurrent readers are introduced.
- Verify or harden concurrent memory and MCP usage; the memory checkpoint path may serialize compaction globally, which is acceptable only if it cannot mix session history. Keep global configuration reads and allow-rule persistence under an explicit consistent synchronization/snapshot policy.
- Verify MCP tool closures and provider/config snapshots do not share mutable per-session state.
- Run focused tests, full race tests, migration tests, and the quickstart scenarios.
- Recheck all constitution gates and update documentation only where the session contract changed.

## Critical Files and Reusable Existing Logic

- Reuse `buildActiveView`, `activePathIDs`, `DetectCompact`, and existing Store locking rather than introducing a second history graph.
- Reuse `dispatch.Server`'s `cancels[sessionID]` single-prompt and cancellation behavior.
- Reuse `AgentPlugin`'s cwd validation and `persistAllowRule` lock; move their ownership to the selected managed session where appropriate.
- Reuse existing session summary/title parsing, changing only discovery from current/archive paths to session files.
- Preserve `TokenToUpdate` and Reporter session IDs; changes should be routing/ownership changes, not a new token protocol.

## Verification

- `go test ./ai-runtime/session/...`
- `go test ./ai-runtime/agent/...`
- `go test ./ai-dispatch/...`
- `go test ./daemon/internal/core/service/... ./daemon/internal/cli/vscode/...`
- `go test -race ./ai-runtime/... ./ai-dispatch/... ./daemon/...`
- Execute all scenarios in [quickstart.md](quickstart.md), including two-session overlap, same-session single-flight, restart/load, legacy migration retry, compact/rewind isolation, and HITL/cancellation routing.

## Complexity Tracking

No constitution violations. The manager is a necessary ownership boundary because runtime execution dependencies and daemon transport lifecycle are currently split across packages; making Store itself multi-session would be more complex and was explicitly rejected in [research.md](research.md).
