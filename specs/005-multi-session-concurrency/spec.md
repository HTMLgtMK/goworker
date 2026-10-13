# Feature Specification: Multi-Live-Session Concurrency

**Feature Branch**: `005-multi-session-concurrency`
**Created**: 2026-09-27
**Status**: Draft
**Input**: User description: "为 goworker 增加多 live session 并发支持。每个 session 一个独立 JSONL 文件，archived 是 session 层生命周期状态而不是 archive 目录；不同 live session 可并发执行，同一 session 仍只能同时执行一个 prompt；支持持久化、重启恢复、session/list、session/load、旧 current/archive 数据迁移；保留现有 compact、rewind、checkpoint、HITL 语义。"

## User Scenarios & Testing

### User Story 1 - Run Multiple Live Sessions in Parallel (Priority: P1)

As an orchestrator or client, I want to create and run multiple live sessions at the same time so independent tasks do not wait behind one another or share conversation state.

**Why this priority**: Concurrent task execution is the primary capability and removes the current single-session bottleneck.

**Independent Test**: Create two live sessions, submit one prompt to each, and verify both prompts make progress concurrently while each response uses only its own conversation and working context.

**Acceptance Scenarios**:

1. **Given** two distinct live sessions, **When** both receive prompts before either finishes, **Then** both sessions execute concurrently and each client receives updates tagged for the correct session.
2. **Given** one live session with an active prompt, **When** a second prompt is submitted to the same session, **Then** the second prompt is rejected or waits according to the existing single-prompt contract, and it cannot corrupt the first prompt's history.
3. **Given** two live sessions using tools or HITL, **When** one session requests a tool decision, **Then** the decision and resulting tool activity remain associated with that session only.
4. **Given** two live sessions, **When** one session compacts, rewinds, or checkpoints its history, **Then** the other session's history and cursor remain unchanged.

---

### User Story 2 - Persist and Reopen Independent Sessions (Priority: P1)

As a client, I want each session's history and lifecycle to survive a daemon restart so I can reopen a specific session without losing its context.

**Why this priority**: Concurrent in-memory sessions are not useful for real workflows if a restart merges or loses their state.

**Independent Test**: Create multiple sessions, write distinct histories and lifecycle states, restart the service, list sessions, and load each session independently.

**Acceptance Scenarios**:

1. **Given** multiple sessions with distinct histories, **When** the service restarts, **Then** each session can be listed and reopened with exactly its own history.
2. **Given** a session marked live or archived, **When** the service restarts, **Then** its lifecycle state is restored independently from every other session.
3. **Given** a session is loaded by its stable identifier, **When** a prompt is submitted, **Then** the prompt uses that session's history and does not replace or mutate another session's selected state.
4. **Given** an unknown or malformed session identifier, **When** a client attempts to load it, **Then** the request fails clearly and no other session is opened or modified.

---

### User Story 3 - Manage Session Lifecycle Without Moving History (Priority: P2)

As a client, I want to list, select, and archive sessions as lifecycle operations while keeping each session's history intact and discoverable.

**Why this priority**: Lifecycle state must be independent from the currently selected session and from physical history storage.

**Independent Test**: Create live sessions, archive one, list all sessions, select another, and verify the archived session remains available for read-only history loading.

**Acceptance Scenarios**:

1. **Given** several live sessions, **When** the client lists sessions, **Then** the response identifies each session, its lifecycle state, its title/updated time, and which one is currently selected without treating selection as lifecycle state.
2. **Given** a live session is archived, **When** the client lists sessions, **Then** it remains discoverable as archived and its history remains intact.
3. **Given** an archived session, **When** the client attempts to submit a new prompt, **Then** the request follows the existing read-only archived-session behavior and does not append new conversation history.
4. **Given** a live session is selected, **When** another session is archived or loaded, **Then** the selected session changes only when explicitly requested.

---

### User Story 4 - Preserve Existing Session History (Priority: P2)

As an existing user, I want sessions created before this feature to remain available after upgrading without losing conversation history or session controls.

**Why this priority**: Storage migration must not turn an infrastructure improvement into data loss or a broken history view.

**Independent Test**: Seed the previous current-session and archived-session layout, run migration, and verify all sessions appear once with their original histories and expected lifecycle states.

**Acceptance Scenarios**:

1. **Given** an existing current session and zero or more archived histories, **When** the service starts after upgrade, **Then** all histories are discovered as separate sessions.
2. **Given** migration is interrupted or retried, **When** the service starts again, **Then** already migrated histories are not duplicated or deleted.
3. **Given** a migrated history contains compact, rewind, or checkpoint records, **When** it is loaded, **Then** those operations retain their prior behavior.

### Edge Cases

- Two sessions are created or loaded at the same time and receive distinct stable identifiers.
- A session file is empty, partially written, contains malformed records, or has no user message; listing remains safe and the affected session reports a clear degraded state.
- A session's history is being committed while another client lists sessions; the list must not expose cross-session records or panic.
- One session is cancelled while another is waiting for or answering HITL; cancellation must not release, reject, or redirect the other session's decision.
- A session is archived while a prompt is active; the lifecycle operation must follow an explicit safe rule and must not lose the active run's final history.
- A legacy current or archived file has an identifier that is not safe as a path component; migration must use a generated safe identifier.
- Shared configuration, memory, MCP, or audit resources are used by concurrent sessions; one session must not observe another session's mutable conversation state.

## Requirements

### Functional Requirements

- **FR-001**: The system MUST support more than one independently addressable live session within the same running service.
- **FR-002**: The system MUST allow prompts belonging to different live sessions to make progress concurrently.
- **FR-003**: The system MUST permit at most one active prompt per session and MUST preserve the existing cancellation and error behavior for duplicate prompts.
- **FR-004**: The system MUST keep conversation history, usage, working context, lifecycle state, and history cursors isolated per session.
- **FR-005**: The system MUST persist each session's complete history and lifecycle state independently so that reopening one session cannot read or modify another session's records.
- **FR-006**: The system MUST assign each session a stable identifier that remains usable across service restarts and MUST reject unsafe or unknown identifiers without touching other sessions.
- **FR-007**: The system MUST represent live, archived, and currently selected as separate concepts; changing the selected session MUST NOT change lifecycle state.
- **FR-008**: The system MUST expose session listing and loading that report each session's identifier, lifecycle state, title or equivalent summary, last update time, and current-selection status.
- **FR-009**: The system MUST preserve the existing read-only behavior for archived sessions unless an explicit future resume operation is introduced.
- **FR-010**: The system MUST preserve compact, rewind, checkpoint, streaming, tool execution, and HITL behavior independently for each session.
- **FR-011**: The system MUST prevent concurrent sessions from cross-routing token updates, tool results, permission decisions, cancellation, or errors.
- **FR-012**: The system MUST migrate existing current-session and archived-session data into independently addressable sessions without data loss, duplication, or repeated migration side effects.
- **FR-013**: The system MUST keep shared resources that remain process-wide concurrency-safe, including configuration updates, memory access, MCP calls, and audit writes.
- **FR-014**: The system MUST report persistence, migration, invalid-session, and lifecycle failures explicitly to the caller and logs; it MUST NOT silently fall back to another session.
- **FR-015**: The system MUST preserve existing behavior when persistence is disabled, except that independent in-memory live sessions may be created and run concurrently.

### Key Entities

- **Live Session**: An independently addressable conversational execution context with its own history, usage, working context, active prompt, and lifecycle state.
- **Session History**: The durable ordered record of one session's messages, tool activity, checkpoints, cursor, and compaction state.
- **Session Lifecycle**: The state of a session such as live or archived, independent of whether the session is currently selected by a client.
- **Session Selection**: The client's current working-session choice; selection is not a lifecycle transition and does not change other sessions.
- **Migration Result**: The durable mapping from legacy current/archived histories to stable session identifiers, including idempotency information.

## Success Criteria

### Measurable Outcomes

- **SC-001**: Two or more live sessions can have active prompts making observable progress at the same time, with no global execution gate blocking one session behind another.
- **SC-002**: Across a concurrency test with at least 20 interleaved prompt, token, tool, and cancellation events, 100% of delivered events are attributed to the originating session and no session contains another session's history.
- **SC-003**: After a clean restart, 100% of successfully persisted sessions can be listed and reopened with the same messages, lifecycle states, and history operations available before restart.
- **SC-004**: Re-running migration on the same legacy data produces no additional sessions, duplicate messages, or changed history.
- **SC-005**: Existing session, compact, rewind, checkpoint, streaming, and HITL regression tests pass without weakening their assertions.
- **SC-006**: A client can list and select any non-corrupt persisted session without affecting the active execution or history of another live session.

## Assumptions

- The feature targets multiple live sessions within one daemon process; distributed multi-process coordination is out of scope.
- Archived sessions remain read-only in this version, matching current behavior. Reactivating an archived session is a separate future capability.
- Existing session protocol method names and response compatibility are retained unless a status field is required to expose the lifecycle state.
- Each session persists its history in one dedicated JSONL file; no archive directory or per-session history rotation is required.
- Shared memory, MCP, configuration, and audit facilities remain process-wide unless concurrency analysis shows a resource must be scoped per session.
- Migration is performed locally at startup or first session-store access and must be safe to retry.
- When persistence is disabled, session state lasts only for the lifetime of the running service.
