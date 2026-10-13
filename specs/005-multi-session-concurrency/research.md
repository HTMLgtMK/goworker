# Research: Multi-Live-Session Concurrency

## Decision 1: Keep `Store` single-session and open an explicit file path

- **Decision**: One `session.Store` instance owns one session history file. Replace the implicit `dir/current.jsonl` convention with an explicit path under a session storage root, such as `sessions/<safe-session-id>.jsonl`.
- **Rationale**: The current `Store` already has exactly one `head`, `cursor`, record set, and append handle. Making it multi-session would turn compact, rewind, cursor, and parent-chain semantics into a shared-branch problem. One store per file preserves those invariants and makes isolation structural.
- **Alternatives considered**: Adding a session ID to every existing record in one shared JSONL was rejected because every active head/cursor/compact operation would need a partitioned index and would greatly expand the failure surface.

## Decision 2: Persist lifecycle transitions in the session's own JSONL

- **Decision**: Add a session lifecycle record kind that carries the stable session ID, status, and timestamps. The record is ignored by conversation reconstruction but replayed by the session manager.
- **Rationale**: This keeps the one-session/one-JSONL invariant and avoids a second metadata database or a global manifest. Append-only status transitions remain crash-visible and can be replayed to the latest state.
- **Alternatives considered**: A global `sessions.json` manifest was rejected for the first version because it creates a second consistency boundary and a new recovery protocol. Directory location was rejected as lifecycle state because moving files is not a semantic session transition.

## Decision 3: Put lifecycle and runtime ownership in a manager above `Store`

- **Decision**: Add a daemon-side manager that owns `sessionID -> managed session`, where each managed session contains one runtime agent Session, one Store, lifecycle status, working-directory state, and a per-session execution gate. Keep generic persistence primitives in `ai-runtime/session`.
- **Rationale**: Provider, memory, MCP, HITL, and frontend wiring are daemon concerns. The runtime Store should not know how a client selects or archives a session. The manager can serve both ACP worker and normal agent ingress without duplicating lifecycle logic.
- **Alternatives considered**: Putting a global multi-session registry inside `ai-runtime/agent` was rejected because it would pull daemon-specific lifecycle and transport concerns into the reusable runtime package.

## Decision 4: Use per-session single-flight execution

- **Decision**: Preserve one active prompt per session and replace global execution gates with a lock or active-run state owned by each managed session. Different session IDs do not share that gate.
- **Rationale**: `dispatch.Server` already rejects duplicate prompts for one session ID. The runtime still needs protection for command paths that bypass that server guard, while different sessions must be able to overlap.
- **Alternatives considered**: Removing all locks was rejected because `Session.Run` snapshots and commits conversation history as a unit; concurrent runs on one session can lose one result or corrupt ordering.

## Decision 5: Migrate old files atomically and idempotently

- **Decision**: On first access, discover legacy `current.jsonl` and `archive/*.jsonl`, assign safe stable IDs, append lifecycle metadata with the appropriate live/archived state, and move each file into the new session file location using an idempotent migration marker/state check. A retry must skip a destination that has already been completed.
- **Rationale**: Existing histories must survive without being merged. Rename-based moves avoid copying a potentially large history twice; lifecycle records make the destination self-describing.
- **Alternatives considered**: Rebuilding every history through `Commit` was rejected because it would change record IDs, parent chains, compact nodes, and checkpoint semantics. Blindly renaming without a status record was rejected because archived state would be lost after restart.

## Decision 6: Share process-wide resources deliberately

- **Decision**: Keep configuration rule persistence serialized by its existing lock; share a single audit logger instance or otherwise give all writers a common lock and include session identity in audit entries; verify memory and MCP transport concurrency with race tests before allowing parallel execution.
- **Rationale**: `AuditLogger` currently locks per logger instance while `OpenAudit` opens a common file, so one logger per session is not a sufficient synchronization boundary. Memory and MCP are shared dependencies even after conversation state is isolated.
- **Alternatives considered**: Assuming `O_APPEND` makes multiple independently locked writers semantically safe was rejected because record ordering, close behavior, and logical attribution would remain undefined.

## Decision 7: Preserve archived read-only behavior in v1

- **Decision**: Lifecycle status `archived` remains discoverable and loadable for history replay but rejects new prompts, matching the existing frontend contract. Reactivation is out of scope.
- **Rationale**: It keeps this change focused on isolation and concurrency instead of inventing a new resume policy while changing storage.
- **Alternatives considered**: Automatically turning an archived session live on load was rejected because it changes existing user-visible semantics and could unexpectedly resume old work.
