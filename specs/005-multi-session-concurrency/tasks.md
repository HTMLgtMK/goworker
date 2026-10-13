# Tasks: Multi-Live-Session Concurrency

**Input**: Design documents from `/specs/005-multi-session-concurrency/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/session-lifecycle.md, quickstart.md

**Tests**: Required by the feature specification and project constitution. Write focused tests first and verify they fail before implementation.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel with other tasks in the same phase after its dependencies are met
- **[Story]**: User story traced to `spec.md`
- Every task names the concrete file or file group it changes.

## Phase 1: Setup (Shared Test Infrastructure)

**Purpose**: Prepare deterministic fixtures for isolated storage and overlapping session execution.

- [X] T001 [P] Add explicit-path Store fixture helpers and two-session temporary-root setup in `ai-runtime/session/store_test.go`.
- [X] T002 [P] Add a barrier-controlled fake provider and session-tagged event recorder for concurrency tests in `ai-runtime/agent/session_integration_test.go`.
- [X] T003 [P] Add ACP multi-session request/notification transport helpers in `daemon/internal/core/service/acp_worker_test.go`.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Establish the one-file Store contract, lifecycle record format, safe path rules, and runtime snapshot invariants required by every story.

**Checkpoint**: No user story implementation starts until the persistence and single-session invariants pass.

### Tests first

- [X] T004 [P] Add failing lifecycle record round-trip and unknown-record compatibility tests in `ai-runtime/session/record_format_test.go`.
- [X] T005 [P] Add failing tests proving two Store instances on different files keep independent heads, cursors, views, compact operations, and checkpoints in `ai-runtime/session/store_test.go`.
- [X] T006 [P] Add failing tests for safe session identifiers, malformed paths, and idempotent legacy migration fixtures in `ai-runtime/session/migrate_test.go`.
- [X] T007 [P] Add failing tests for immutable Conversation snapshots and one-time audit initialization in `ai-runtime/agent/session_test.go`.

### Implementation

- [X] T008 [P] Add lifecycle record fields, constants, marshal/unmarshal, and replay helpers in `ai-runtime/session/record.go`.
- [X] T009 Refactor Store construction to open one validated explicit session file and remove implicit `current.jsonl` and archive rotation from `ai-runtime/session/store.go` (depends on T008).
- [X] T010 [P] Implement stable session ID validation, safe path construction, lifecycle replay, and legacy current/archive migration in `ai-runtime/session/migrate.go` (depends on T008-T009).
- [X] T011 [P] Make runtime Conversation reads return immutable snapshots and make lazy audit setup single-initialization-safe in `ai-runtime/agent/session.go` (depends on T007).
- [X] T012 Add the daemon-side managed-session type, session registry, per-session status, selection, and single-flight ownership in `daemon/internal/core/service/session_manager.go` (depends on T009-T011).

---

## Phase 3: User Story 1 - Run Multiple Live Sessions in Parallel (Priority: P1) 🎯 MVP

**Goal**: Different live session IDs execute concurrently with isolated runtime state, while each session still permits only one active prompt.

**Independent Test**: Create sessions A and B, submit overlapping prompts, and verify both progress concurrently with session-correct events; submit a second prompt to A and verify the existing single-prompt contract.

### Tests for User Story 1

- [X] T013 [P] [US1] Add failing manager tests for concurrent A/B execution and same-session single-flight in `daemon/internal/core/service/session_manager_test.go`.
- [X] T014 [P] [US1] Add failing ACP integration tests for cross-session overlap, per-session cancellation, and session-tagged token/HITL routing in `daemon/internal/core/service/acp_worker_test.go`.
- [X] T015 [P] [US1] Add failing runtime tests proving concurrent session histories, usage, cwd, and tool decisions cannot cross-contaminate in `ai-runtime/agent/session_integration_test.go`.

### Implementation

- [X] T016 [US1] Implement managed-session creation and lookup with one runtime Session, Store, cwd, and execution guard per ID in `daemon/internal/core/service/session_manager.go` (depends on T013).
- [X] T017 [US1] Replace the ACP worker's global `runMu` and raw runtime-session map with managed sessions and per-session execution guards in `daemon/internal/core/service/acp_worker.go` (depends on T014 and T016).
- [X] T018 [US1] Preserve dispatch Server's same-session active-prompt rejection and ensure cancellation state is scoped to the originating session in `ai-dispatch/server.go` and `ai-dispatch/protocol/jsonrpc.go` (depends on T014 and T017).
- [X] T019 [US1] Route normal agent command execution through the selected managed session instead of the singleton Session and global run gate in `daemon/internal/core/service/commands.go` and `daemon/internal/core/service/plugin.go` (depends on T015-T016).
- [X] T020 [US1] Verify the MVP overlap scenario and session-specific event routing with the focused ACP and runtime tests in `daemon/internal/core/service/acp_worker_test.go` and `ai-runtime/agent/session_integration_test.go`.

**Checkpoint**: ACP worker and normal agent execution can run independent live sessions concurrently without history or event crossover.

---

## Phase 4: User Story 2 - Persist and Reopen Independent Sessions (Priority: P1)

**Goal**: Multiple session histories and lifecycle states survive restart and can be reopened by stable ID.

**Independent Test**: Persist two distinct sessions, restart the service, list/load both, and compare history, status, cursor, compact, and checkpoint state.

### Tests for User Story 2

- [X] T021 [P] [US2] Add failing restart/reopen tests for multiple live session files and independent lifecycle replay in `ai-runtime/session/migrate_test.go` and `daemon/internal/core/service/session_manager_test.go`.
- [X] T022 [P] [US2] Add failing persistence tests for compact, rewind, cursor, checkpoint, and usage state per session in `ai-runtime/agent/session_integration_test.go`.
- [X] T023 [P] [US2] Add failing tests that unknown or unsafe IDs cannot fall back to or mutate another session in `daemon/internal/core/service/session_manager_test.go`.

### Implementation

- [X] T024 [US2] Add manager startup discovery, file opening, lifecycle restoration, and stable-ID loading in `daemon/internal/core/service/session_manager.go` (depends on T021 and T010).
- [X] T025 [US2] Integrate persistence-enabled and persistence-disabled manager lifecycles, including closing every managed Store during shutdown, in `daemon/internal/core/service/plugin.go` and `daemon/internal/core/service/session_manager.go` (depends on T024).
- [X] T026 [US2] Preserve per-session compact, rewind, checkpoint, and usage operations when routing maintenance commands through the manager in `daemon/internal/core/service/commands.go` and `daemon/internal/core/service/rewind.go` (depends on T022 and T025).
- [X] T027 [US2] Complete restart/load integration coverage and verify no session file is opened as another session in `daemon/internal/core/service/session_manager_test.go` and `ai-runtime/session/store_test.go`.

**Checkpoint**: Restarting the service restores every persisted session independently without changing existing history semantics.

---

## Phase 5: User Story 3 - Manage Session Lifecycle Without Moving History (Priority: P2)

**Goal**: Listing, selecting, loading, and archiving sessions are lifecycle operations independent from physical history placement.

**Independent Test**: Create multiple live sessions, archive one, list them, load the live and archived sessions, and verify current-selection and read-only behavior.

### Tests for User Story 3

- [X] T028 [P] [US3] Add failing list tests for live/archived/current distinctions, stable IDs, titles, timestamps, and one-file-per-session discovery in `daemon/internal/core/service/sessions_test.go`.
- [X] T029 [P] [US3] Add failing load tests for live session selection, archived read-only replay, and unknown session errors in `daemon/internal/cli/vscode/frontend_test.go`.
- [X] T030 [P] [US3] Add failing protocol serialization tests for additive lifecycle status information in `ai-dispatch/protocol/spec_test.go`.

### Implementation

- [X] T031 [US3] Add lifecycle status and current-selection fields to session summary types without changing existing field meanings in `ai-dispatch/protocol/spec.go`.
- [X] T032 [US3] Replace current/archive directory scanning with session-file discovery and manager-backed lifecycle summaries in `daemon/internal/core/service/sessions.go`.
- [X] T033 [US3] Implement manager archive/select/load rules while keeping archived sessions read-only and leaving history files in place in `daemon/internal/core/service/session_manager.go` and `daemon/internal/core/service/commands.go`.
- [X] T034 [US3] Update VSCode ingress session source, load replay, promptability, and read-only maps to use manager lifecycle state in `daemon/internal/cli/vscode/frontend.go`.
- [X] T035 [US3] Complete list/load/current-selection regression coverage in `daemon/internal/core/service/sessions_test.go` and `daemon/internal/cli/vscode/frontend_test.go`.

**Checkpoint**: Clients can distinguish lifecycle state from current selection, and archiving no longer moves or hides a session history.

---

## Phase 6: User Story 4 - Preserve Existing Session History (Priority: P2)

**Goal**: Existing current and archive files migrate once into independent sessions without data loss or behavior changes.

**Independent Test**: Seed the legacy layout, start the service twice, and verify the same complete set of histories, statuses, record IDs, and history operations both times.

### Tests for User Story 4

- [X] T036 [P] [US4] Add failing migration tests for current plus multiple archive files, empty files, malformed lines, ID collisions, and retry after interruption in `ai-runtime/session/migrate_test.go`.
- [X] T037 [P] [US4] Add failing compatibility tests for legacy message, compact, rewind, checkpoint, and cursor records after migration in `ai-runtime/session/store_test.go` and `ai-runtime/session/archiveview_test.go`.
- [X] T038 [P] [US4] Add failing daemon regression tests proving migrated sessions appear once with correct live/archived status in `daemon/internal/core/service/sessions_test.go`.

### Implementation

- [X] T039 [US4] Integrate idempotent legacy migration before session discovery and preserve safe generated IDs for unsafe or colliding legacy names in `daemon/internal/core/service/session_manager.go` and `ai-runtime/session/migrate.go`.
- [X] T040 [US4] Remove legacy archive-path assumptions from archived history replay while preserving checkpoint segmentation and update ordering in `daemon/internal/core/service/sessions.go`.
- [X] T041 [US4] Complete migration compatibility coverage and verify repeated startup produces no duplicate files, records, or sessions in `ai-runtime/session/migrate_test.go` and `daemon/internal/core/service/sessions_test.go`.

**Checkpoint**: Upgraded installations retain all old session histories and existing replay/rewind semantics.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Close shared-resource race windows, harden boundaries, and run the complete verification loop.

- [X] T042 [P] Make audit writes use one explicit synchronization boundary and include session identity in decision records in `ai-sandbox/audit.go`, `ai-sandbox/spec.go`, and `ai-runtime/agent/session.go`.
- [X] T043 [P] Verify and harden shared memory checkpoint/search behavior under concurrent sessions in `ai-memory/store.go` and `ai-runtime/memory_adapter.go`.
- [X] T044 [P] Verify MCP transport multiplexing and tool closures under concurrent session calls in `ai-runtime/mcp/client.go`, `ai-runtime/mcp/transport.go`, and `ai-runtime/agent/session.go`.
- [X] T045 [P] Add path-traversal, malformed-ID, corrupt-file, and explicit-error regression coverage in `ai-runtime/session/migrate_test.go` and `daemon/internal/core/service/session_manager_test.go`.
- [X] T046 Run focused package tests for Store, runtime agent, dispatch, daemon service, and VSCode ingress using the commands in `specs/005-multi-session-concurrency/quickstart.md`.
- [X] T047 Run `go test -race ./ai-runtime/... ./ai-dispatch/... ./daemon/...` and fix every race or cross-session routing failure before completion.
- [X] T048 Execute every quickstart scenario, perform a final constitution review, and update affected session protocol documentation in `specs/005-multi-session-concurrency/quickstart.md` and `specs/005-multi-session-concurrency/contracts/session-lifecycle.md`.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No implementation dependency; test fixtures can be prepared immediately.
- **Foundational (Phase 2)**: Depends on Setup and blocks all user stories. Store format and manager ownership must be stable before ingress changes.
- **User Story 1 (Phase 3)**: Depends on Foundational; this is the MVP and enables concurrent execution.
- **User Story 2 (Phase 4)**: Depends on US1 manager ownership; adds restart discovery and persistence guarantees.
- **User Story 3 (Phase 5)**: Depends on US2 stable IDs and restored lifecycle state; changes list/load and selection behavior.
- **User Story 4 (Phase 6)**: Depends on the new Store and manager discovery from Foundational/US2; validates legacy compatibility.
- **Polish (Phase 7)**: Depends on all desired stories and is the release gate.

### User Story Dependencies

- **US1**: Depends only on Foundational; independently delivers the concurrency MVP.
- **US2**: Depends on US1's manager and per-session runtime ownership.
- **US3**: Depends on US2's stable session discovery and lifecycle persistence.
- **US4**: Depends on the new persistence layout and manager startup path, then validates compatibility.

### Parallel Opportunities

- T001-T003 can run in parallel because they add isolated test fixtures.
- T004-T007 can run in parallel because they are independent failing tests.
- T008-T011 can run in parallel after their related tests are written, except T009 depends on T008 and T010 depends on T008-T009.
- Within US1, T013-T015 can run in parallel; T017 and T019 can proceed in parallel after manager ownership is available.
- Within US2, T021-T023 can run in parallel before T024.
- Within US3, T028-T030 can run in parallel before T031-T034.
- Within US4, T036-T038 can run in parallel before T039.
- T042-T045 can run in parallel after story implementations stabilize.

## Parallel Example: User Story 1

```text
T013: Manager overlap and same-session single-flight tests
T014: ACP cross-session routing/cancellation tests
T015: Runtime history/cwd/HITL isolation tests

After T016 establishes manager ownership:
T017: ACP worker per-session execution
T019: Normal agent command routing
```

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Setup and Foundational phases.
2. Complete US1 tests first and implement the manager, ACP worker, and normal agent routing.
3. Stop at the US1 checkpoint and validate two-session overlap, same-session single-flight, event routing, and `go test -race` for the affected packages.

### Incremental Delivery

1. Add US2 restart/load persistence and verify compact/rewind/checkpoint state.
2. Add US3 lifecycle listing, selection, archived read-only behavior, and protocol status.
3. Add US4 legacy migration and repeated-startup idempotency.
4. Complete cross-cutting shared-resource hardening and the full race suite.

### Notes

- `[P]` marks tasks that touch separate files or test surfaces and have no unfinished dependency.
- Every story has failing tests before implementation tasks.
- Do not weaken existing archive, compact, rewind, checkpoint, or HITL assertions to make concurrency tests pass.
- Commit each logical phase or migration boundary separately using the project's conventional commit format.
