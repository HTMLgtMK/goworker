# Implementation Plan: ai-session and Unified Session Identity

**Branch**: `feat/x-device-call-session-run-id`  
**Status**: Planning only  
**Related**: `specs/007-x-device-call-session-run-id/` (current run/device-call contract)

## Summary

Add `ai-session` as the single shared owner of session lifecycle and canonical identity. Its `SessionManager` handles create/load/get/list and holds the authoritative in-process session registry; every caller receives the same manager instance. An injected storage interface is the persistence boundary, implemented only by the existing `ai-runtime/session.Repository`. Use the same immutable ID as ACP `sessionId`, every `x-goworker` session selector, the session record/runtime key, and `x-device/call.sessionId`. Keep execution/run IDs separate. Remove transport-ID-to-runtime-ID translation.

`/new` and ACP `session/new` share the same manager-backed creation operation. Their caller-specific selection remains outside the generic manager: CLI `/new` selects the created session as its default; ACP returns and binds that ID for the connection. Neither operation mutates or archives the previous session implicitly.

The generic manager owns session identity, registration, and create/load/get/list lifecycle only. It owns no agent Run, approval/HITL, device capability, ACP connection, connection lease, workspace authorization, or daemon policy. `ai-runtime/session.Repository` remains the unique persistence adapter; `ai-runtime/agent.SessionManager` is narrowed to execution runtime registration/admission and must not act as a second session-identity authority.

## Current workspace constraints

- The worktree already contains extensive uncommitted 007 implementation changes in `ai-acp/`, `ai-runtime/agent/`, and `daemon/`. Treat them as existing user work: implementation must inspect and reconcile them, not reset or overwrite them wholesale.
- `ai-acp/unified_session_id_test.go` is an untracked test added earlier by the main agent. Preserve it; do not delete, replace, or treat it as verified. Incorporate its intended assertion into the test plan, then review and run it during implementation.
- `ai-runtime/agent/run_identity_context_test.go` and the two `daemon/internal/core/service/acp/xgw_device_call_*_integration_test.go` files are also untracked current-worktree tests; preserve and reconcile them.
- `specs/007-x-device-call-session-run-id/` already has a plan/spec/tasks. This plan extends the architecture; do not silently rewrite those files as part of implementation.

## Technical decisions and boundaries

### `ai-session` manager and storage boundary

Create workspace module `ai-session` (`github.com/tinguo/goworker/ai-session`) with no dependency on `ai-acp`, `ai-runtime`, daemon, or agent packages. It owns one reusable `SessionManager` instance with the generic lifecycle surface:

```go
type ID string

type Storage interface {
    Create(context.Context, Session) error
    Load(context.Context, ID) (Session, error)
    Get(context.Context, ID) (Session, error)
    List(context.Context) ([]Session, error)
    Delete(context.Context, ID) error // rollback of an unpublished create / explicit lifecycle removal only
}

type SessionManager interface {
    Create(context.Context, CreateOptions) (Session, error)
    Load(context.Context, ID) (Session, error)
    Get(context.Context, ID) (Session, error)
    List(context.Context) ([]Session, error)
}
```

The concrete manager owns canonical ID generation/validation, atomic publication into its in-memory registry, and the shared create/load/get/list semantics. `CreateOptions`/`Session` remain protocol-neutral; the manager may expose a short lifecycle initializer hook for host-specific runtime attachment, but the hook cannot choose or replace the ID. No mutable process-wide `current` lives in this module.

The injected `Storage` boundary persists generic session identity/metadata. The existing `ai-runtime/session.Repository` is the sole storage adapter and source for durable records; do not add another repository, ID registry, or parallel persistence implementation. Its adapter may keep richer JSONL/workspace/archive data internally, but all create/load/get/list calls flow through the shared `ai-session.SessionManager`. A failure before publication must roll back the created store and any partially attached runtime. Existing stored IDs (including `session_<hex>` and stable `legacy_<hex>` identities) remain unchanged; no directory rename or historical record rewrite.

`ai-runtime/agent.SessionManager` is an execution-runtime registry, not the session manager: it accepts only IDs allocated/resolved by `ai-session`, may map an ID to an attached runtime for Run admission, and must not independently create, load, list, generate, or declare session identity. The daemon composition root constructs exactly one `ai-session.SessionManager` around the repository adapter and injects the same instance into plugin/session creation and ACP adapters.

### One ID across protocol and runtime

- `ai-acp` receives the shared manager through a narrow session-lifecycle interface (`Create`, `Load`, `Get`, `List` with `ai-session.ID`/`Session` values); it does not construct a second manager. The generic ACP handler does not generate IDs. For `session/new`, the application/manager commits creation and returns the canonical ID; only then does ACP serialize that ID as `sessionId`.
- ACP `session/prompt`, `session/cancel`, `session/update`, and `session/request_permission` all use the same canonical ID. `session/load` resolves that exact persisted ID through the shared manager; no alias lookup or current-session fallback.
- Every `x-goworker` request already carrying `sessionId` uses the same canonical ID as the session manager and `SessionRuntime.ID`. `runId` remains an independent, agent-issued execution identity.
- `x-device/call` keeps its required top-level `{sessionId, runId, name, arguments}` shape. `sessionId` comes from the trusted managed Run context and must equal the actual runtime/store ID. Do not source it from tool arguments or look it up from a connection's later binding.
- Remove only the transport/runtime identity translation. ACP connections still own lifecycle references and per-session capabilities/leases; those are business-layer ownership records, not identity mappings. The generic manager knows nothing about connections.

### `/new` semantics

Use the one daemon-composed `ai-session.SessionManager` for both callers. ACP injects it through the narrow lifecycle interface; the plugin's `/new` handler calls the same instance. The manager runs the generic create transaction against its injected storage adapter and returns the committed canonical `ai-session.ID`. The daemon/plugin initializer attaches workspace/runtime-specific state before the new session becomes visible, but cannot supply a separate ID. `/new` then selects that ID for its CLI/default caller; ACP returns that ID and records a connection-owned reference without changing the process-wide default. The prior session remains loadable and is not archived, deleted, or assigned a new identity.

## Target files and responsibilities

| File/module | Planned responsibility |
|---|---|
| `go.work` | Add `./ai-session` as a workspace module. |
| `ai-session/go.mod`, `id.go`, `session.go`, `manager.go`, tests | Canonical ID, immutable session value, shared generic `SessionManager` lifecycle (create/load/get/list), injected `Storage` contract, atomic registry publication and rollback tests; no protocol or business policy. |
| `ai-runtime/session/id.go`, `repository.go`, `store.go`, related tests | Implement the `ai-session.Storage` boundary on the existing Repository; keep it the only persistence adapter and preserve JSONL, locking, archive, journal, and migration behavior. Do not add a parallel repository or authority. |
| `ai-runtime/agent/manager.go`, `runtime.go`, `run.go`, `supervisor.go`, `capability.go`, `approval.go` | Narrow the existing manager to runtime execution registration/admission only. Consume canonical IDs supplied by ai-session; retain agent-issued `runId`, private execution context identity, approvals and capability registry. It must not create/load/list or mint session IDs. |
| `ai-acp/protocol/spec.go`, `server.go`, `client.go`, `session.go`, tests | Inject the shared manager through a narrow lifecycle interface; use `ai-session.ID` at protocol boundaries; remove transport-side ID generation and runtime-ID metadata bridge. Generic handlers never mint a second ID; preserve wire key spelling `sessionId`. |
| `daemon/internal/core/model/capability.go`, `model/types.go` | Keep daemon adapter ports protocol-neutral; pass canonical IDs directly. Keep leases, run control, workspace checks, and binding interfaces in daemon business layer. |
| `daemon/internal/core/service/acp/{handler,mapping}.go`, tests | Route directly by canonical ID; remove transport→runtime route records and global-current fallback. Retain connection-owned reference sets and close/replacement cleanup. |
| `daemon/internal/plugins/agent/{commands,lifecycle,boundsession,plugin,sessions}.go` | Composition root creates/injects exactly one shared ai-session manager around the Repository; both `/new` and ACP adapters use it. Plugin hooks attach runtime/workspace state; runtime selection stays caller-specific. |
| `daemon/internal/agentcontrol/{bindings,sessions,service}.go` and tests | Keep connection references, control leases, approval ownership, and run admission keyed by canonical ID; no transport alias ownership. |
| `daemon/internal/core/acpworker/sys_relay.go`, `daemon/xgoworker/protocol/protocol.go`, `daemon/mobile/PROTOCOL.md` | Validate/document one canonical `sessionId` on ACP, x-goworker, and x-device; keep `runId` distinct and existing fail-closed/no-retry rules. |
| Current untracked test files listed above | Preserve; update only after reviewing their assertions against the unified-ID contract. |

## Implementation tasks

Each task is a small reviewable change. **After every task, perform an architecture review before starting the next task**: check import direction, ownership of identity/lifecycle, and whether any parallel ID source or business policy leaked into `ai-session`. After all tasks, perform one overall architecture review across modules and protocol flows.

### T1 — Define the shared session manager and RED tests

- Add `ai-session` to `go.work`; implement `ID`, immutable `Session`, injected `Storage`, and one `SessionManager` with create/load/get/list. Test canonical ID validation, one ID per created session, duplicate/load behavior, and registry consistency.
- Define create/load as transactions: reserve identity privately, persist through Storage, run an optional host initializer before publication, then publish once. On any persistence/initializer failure, return no visible session and roll back the store/runtime attachment; test failure at each boundary.
- Add failing ACP tests proving `session/new` returns the manager-assigned ID and no generic ACP handler generates a transport ID; assert the runtime-ID metadata bridge is not needed.
- Review untracked `ai-acp/unified_session_id_test.go` and other tests without overwriting them; incorporate valid assertions and run them only during implementation verification.
- **Architecture review gate**: ai-session depends on no protocol/business modules; the manager owns session lifecycle but not storage or agent policy.

### T2 — Make the existing Repository the sole Storage adapter

- Implement the ai-session `Storage` interface with the existing `ai-runtime/session.Repository`/Store code. Do not introduce another repository, file format, manager, or identity registry. Preserve JSONL layout, locking, archive data, workspaces, and run journals.
- Keep persisted IDs stable, including existing `session_*` and `legacy_*`; new IDs are generated by the shared manager. Add migration/reopen/list tests proving old IDs and paths remain stable.
- Wire the application composition root to construct exactly one `ai-session.SessionManager` around the Repository and inject that same instance into plugin creation and the ACP adapter. Do not independently construct managers in `ai-acp`, the agent plugin, or runtime packages.
- **Multi-module assembly constraint**: `ai-session` has no imports from ai-runtime/daemon; `ai-runtime/session` adapts persistence to its storage port; daemon/plugin assembly owns the one shared instance and supplies business initialization; `ai-acp` imports only the neutral ai-session contract / narrow consumer interface. Preserve an acyclic graph `ai-session <- ai-runtime/session <- daemon composition`, with ai-acp depending on ai-session but never on runtime/daemon.
- **Architecture review gate**: verify there is one manager pointer in production wiring, one persistence adapter, and no second authoritative session registry.

### T3 — Make ACP use the injected manager, without ID generation

- Define/inject a narrow ACP-facing session lifecycle interface backed by the shared manager. It accepts/returns `ai-session.ID` and supports only the lifecycle methods ACP needs; ACP must not own another manager instance.
- Change `session/new` so the generic handler delegates creation, receives the committed ID, and serializes it as `sessionId`. Remove `Server.newSessionID`, the `SessionCreatorWithRuntimeID` / `SessionLoaderWithRuntimeID` dual-ID bridge, and `_meta["x-goworker.runtimeSessionId"]` emission/consumption.
- Make `session/load` use the exact requested ID; standard prompt/cancel/update/permission paths preserve the same ID. Keep connection-specific references in the daemon adapter, not generic ACP.
- **Architecture review gate**: ACP transports/delegates lifecycle and wire data only; it neither generates IDs nor chooses daemon current session or manages runs/leases.

### T4 — Separate generic session lifecycle from agent execution runtime

- Adapt `ai-runtime/agent.SessionManager` to consume IDs already allocated/resolved by ai-session and manage only `SessionRuntime` registration, per-session Run admission, Run IDs, and shutdown.
- Remove any session create/load/list/ID-generation responsibility from the agent runtime manager. Runtime lookup failure must be an explicit attachment/runtime error, never an alternative session identity resolution.
- Keep approvals, private managed Run identity, capability/device bindings, and authorization in `ai-runtime/agent` and daemon business layers.
- **Architecture review gate**: ai-session is the session identity/lifecycle authority; agent runtime manager is an execution subsystem attached by canonical ID.

### T5 — Route daemon protocols and `/new` through the shared manager

- Refactor `FrontendHandler` from transport→runtime translation to direct canonical-ID routing. Remove transport aliases where no longer needed; retain connection-owned reference sets and exact close/replacement cleanup.
- Keep multiple sessions on one connection; ACP create/load/prompt and every x-goworker session method address the same ID. Unknown IDs fail, with no `active`, process-current, cwd, or transport-ID fallback.
- Make `/new` call the same injected SessionManager instance and creation transaction as ACP `session/new`. The CLI selects the returned session; ACP returns/binds it for that connection without changing global default. Previous session stays intact and archival remains explicit.
- **Architecture review gate**: lifecycle refs/control leases remain daemon-owned; shared manager does not absorb connection or control policy.

### T6 — Enforce x-device identity and remove compatibility shims

- Preserve required top-level `sessionId` and `runId`; validate that trusted execution context ID equals the canonical session ID used to attach the runtime/device proxy before dispatch. Keep unmanaged calls fail-closed and uncertain Invoke outcomes non-retryable.
- Update serializers, fixtures, `daemon/mobile/PROTOCOL.md`, and repository-owned examples. Require client upgrade before deploying the sender; do not add a legacy `{name, arguments}` adapter or negotiate a second identity wire version.
- **Architecture review gate**: session identity is routing/correlation identity, never an authorization token; authorization remains per-call in agent policy/client workspace mapping.

### T7 — Verification and final architecture review

- Run focused tests for ai-session, its Repository adapter, ACP injection, and daemon lifecycle; then all `go.work` module tests, `go test -race ./...` per module, and `go vet ./...` per module. Confirm previously unrun untracked tests are reviewed and executed without losing them.
- Audit every ACP, x-goworker, and x-device `sessionId`; assert all point to the exact SessionManager session and persisted/runtime ID. Audit for duplicate manager construction, runtime-owned session creation/listing, remaining `runtimeSessionId` metadata, transport ID generators, translation maps, and fallback paths.
- Run one overall architecture review across module imports, single-manager wiring, persistence adapter uniqueness, API ownership, atomic create/load, concurrency/lifecycle behavior, and protocol docs. Resolve findings before declaring completion.

## Concurrency and lifecycle invariants

- A created canonical ID is immutable. Creation either returns a fully mounted session with that ID or returns an error and cleans up newly created resources; never publish a protocol ID before application creation commits.
- `ai-session.SessionManager` is the sole in-process authority for canonical identity and generic lifecycle. The Repository owns durable persistence; the agent runtime manager owns runtime admission. ACP/session-management locks must not be held across long-running prompts; per-runtime Run gates remain the execution serializer.
- Create reserves an ID privately, persists and completes host attachment, then publishes once. Any failure before publication leaves no discoverable session and rolls back partial storage/runtime resources. Load stages storage/runtime state before registry publication; a failed load cannot replace an already registered session or partially overwrite its state. Concurrent duplicate create/load operations must converge or return a clear conflict, never publish competing objects for one ID.
- A prompt resolves the exact canonical ID once at admission and pins it for that execution. Later connection detach, another session creation, or a `/new` cannot redirect it.
- Connection lifecycle references are keyed by canonical ID and are reference-counted/idempotent. Closing/rebinding one connection cannot close a session still referenced by another; final release waits for active Runs before retiring runtime/events/store.
- Run identity is the immutable pair `{sessionId, runId}` injected by the agent manager. Approval ownership uses `{sessionId, runId, requestId}`; capability bindings use canonical session ID plus connection ID. These business-layer keys remain separate from the generic ai-session skeleton.
- No request path may fall back from an unknown/blank ID to `current`, `active`, cwd, or a transport-generated ID. No x-device invocation is replayed when the upstream result is uncertain.

## Protocol and migration constraints

- Intentional protocol/source incompatibility: remove the runtime-ID metadata bridge and transport/runtime ID split; do not retain dual-ID compatibility shims. Update all in-repository callers and require ACP/x-goworker/x-device clients to upgrade before release.
- Wire field names stay camelCase `sessionId` and `runId`; x-device requires both. Existing ACP and x-goworker method names and unrelated fields remain unchanged.
- Persisted data migration is compatibility-preserving: keep existing session IDs, JSONL rows, store paths, archive names, and run journals as-is. `legacy_*` values remain resolvable. Only newly created session identity generation is centralized.
- `/new` does not mean archive, delete, rewind, or rotate the old persisted record; it creates a new session as `session/new` does and only changes the initiating caller's selected session.
- Android client implementation is outside this repository. Update the repository protocol contract and treat the external client upgrade as a release prerequisite, not a server-side negotiation feature.

## Main risks and mitigations

- **ACP owns an independent ID generator today**: changing only the daemon leaves split identity or breaks client prompts. Change the creator API and test full `session/new → prompt → x-device/call` propagation together.
- **Current dirty 007 implementation encodes transport/runtime mappings**: preserve it, then replace behavior incrementally under tests; avoid broad file rewrites and verify every existing test before removing the mapping abstraction.
- **Breaking wire/API change leaves stale clients**: no silent compatibility mode; document exact contract and block rollout until clients upgrade.
- **ID type adoption creates broad compile churn**: keep the type small, migrate module-by-module, and convert to/from JSON strings only at explicit wire/storage boundaries.
- **Lifecycle ownership can split again during wiring**: the daemon composition root must construct one manager and inject that exact instance into `/new` and ACP; add wiring tests and reject manager construction in adapters/plugins.
- **Atomic creation crosses storage and runtime attachment**: keep the ID unpublished until both succeed, define rollback/cleanup ownership at the composition boundary, and test each failure point plus concurrent create/load. Do not add a second repository or persistence mechanism to achieve atomicity.
- **Breaking wire/API change leaves stale clients**: no silent compatibility mode; document exact contract and block rollout until clients upgrade.
- **ID type adoption creates broad compile churn**: keep the type small, migrate module-by-module, and convert to/from JSON strings only at explicit wire/storage boundaries.
- **Concurrent `/new`, load, close, and prompt can orphan or retire active sessions**: retain lifecycle fencing and connection-reference commits; add race tests for creation failure, close-vs-bind, prompt-vs-detach, and final release after Run completion.
