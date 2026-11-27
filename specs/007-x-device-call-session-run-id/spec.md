# Feature Specification: x-device Call Session and Run Identity

**Feature Branch**: `feat/x-device-call-session-run-id`  
**Created**: 2026-11-25  
**Status**: Draft  
**Input**: Approved design discussion and `/Users/tinguo/Downloads/x-device-session-id.md`.

## Goal

Make every `x-device/call` identify the runtime session and execution that owns it. Standard ACP transport session IDs remain transport routing keys; they are not implicitly treated as runtime/store IDs. A single ACP connection may bind and use multiple sessions concurrently without a mutable “current session” deciding a device workspace.

This spec defines a future implementation contract. This documentation change does not implement the feature or modify the Android repository.

## User Scenarios & Testing

### Story 1 — One connection uses multiple sessions safely (P1)

An ACP client creates or loads multiple sessions on one connection. Each `session/prompt` is routed using its own standard ACP `sessionId`, and each device call resolves to that prompt's runtime session and workspace, regardless of request interleaving.

**Independent test**: Create two sessions on one connection, map them to different workspaces, run prompts concurrently, and assert each `x-device/call` has the matching runtime `sessionId` and accesses only its workspace.

### Story 2 — Each device call is attributable to one execution (P1)

A client receives a required, non-empty runtime `sessionId` and `runId` on every `x-device/call`. Calls within one execution share a `runId`; another execution receives a different one. Identity comes from trusted Go execution context, never model arguments or a global current-session lookup.

**Independent test**: Make multiple device calls in one managed run and one standard prompt, then assert stable identity within each execution and distinct run IDs across executions.

### Story 3 — Missing or conflicting identity fails closed (P1)

A missing runtime mapping, stale binding, unknown runtime session, conflicting proxy/session identity, or managed execution without identity must fail before dispatching the device call. The system must not guess a workspace or retry an invocation whose result is uncertain. Legacy clients that only understand `{name, arguments}` are incompatible and must be upgraded before declaring `x-device`; the current initialize gate does not negotiate an identity-wire version.

**Independent test**: Exercise missing identity, unknown session, conflicting binding, an unmanaged worker relay, and disconnect/rebind races; assert no identity-less call is sent and an Invoke error is not replayed.

### Story 4 — Disconnect and rebinding preserve execution ownership (P2)

When a connection closes or a session binding is replaced, its lifecycle reference and device capability binding are released without redirecting an in-flight execution. A permitted call waiting for a compatible rebind retains its original session/run identity; the rebind does not create new authorization.

**Independent test**: Start a call, disconnect the client, bind a compatible client for the same runtime session, and assert the resumed call carries its original identity; calls for another session remain isolated.

## Functional Requirements

- **FR-001**: Every outbound `x-device/call` MUST include top-level, non-empty `sessionId` and `runId` fields. The request MUST fail before client invocation if either value is missing or blank.
- **FR-002**: `sessionId` MUST identify the actual runtime/store session owning the execution. A transport ID MUST NOT be substituted merely because it is available.
- **FR-003**: `runId` MUST be the manager-issued ID of the current managed execution. All device calls in the same Run share it; a separate Run gets a distinct value. Paths without a managed Run ID MUST fail closed rather than minting a per-call or transport-derived substitute.
- **FR-004**: Identity MUST be captured from trusted Go execution context and MUST NOT be accepted from model-visible tool arguments, mutable global state, or a later connection binding lookup.
- **FR-005**: A runtime execution identity MUST be immutable for that execution. If identity in execution context disagrees with a proxy's bound runtime session, the call MUST fail closed.
- **FR-006**: A standard ACP connection MUST support multiple independent transport-session-to-runtime-session mappings. `session/new` adds a mapping; `session/load` commits/replaces only its target mapping after successful load; neither operation may silently invalidate unrelated mappings.
- **FR-007**: `session/prompt` MUST route by the request's ACP transport `sessionId` to its exact runtime session. Unknown, closed, stale, or superseded mappings MUST return a clear protocol error, not fall back to a process-wide current session.
- **FR-008**: A runtime session ID MUST be returned in ACP response metadata under `_meta["x-goworker.runtimeSessionId"]` when the session creator/loader provides a non-empty runtime ID. `session/new` and writable `session/load` in the daemon app-service path provide it. A read-only archived load or backend without a runtime ID may omit it; such a session MUST NOT be used to select a device workspace.
- **FR-009**: A path without a trusted managed runtime identity MUST fail closed before sending `x-device/call`. It MUST NOT invent a runtime identity from the ACP transport ID or cwd. The current stdio `acpWorker` raw relay has no managed Run identity and therefore does not send device calls.
- **FR-010**: Closing a connection MUST release all of that connection's mapping references and device capability bindings. Replacing one mapping MUST release only the replaced reference. Existing in-flight execution remains pinned to its resolved runtime identity until it ends or is cancelled.
- **FR-011**: Capability rebinding MUST match the execution's runtime session and compatible tool schema/policy. Rebinding MUST NOT change `sessionId`/`runId` or replay authorization. If an invocation has already been sent and its result is uncertain, it MUST NOT be retried automatically.
- **FR-012**: The existing initialize gate is the presence of `clientCapabilities.x-device`; its nested value is not parsed and there is no `sessionRunIdentity` version gate. A client MUST implement the new call shape before declaring `x-device`; old clients are operationally incompatible and MUST NOT be used with the new sender.
- **FR-013**: The Android client contract MUST validate both required wire fields, resolve `sessionId` through its explicit runtime-session-to-workspace map, and reject unknown/ambiguous mappings. It MUST never select a default/current workspace as fallback.
- **FR-014**: `runId` is correlation identity, not an authorization token. Permission checks remain server-side and MUST be evaluated for each tool call under the existing policy.
- **FR-015**: The exact JSON fields are `sessionId` and `runId` (Go fields `SessionID` and `RunID`), both required and non-empty. The change is intentionally not wire-backward-compatible with `{name, arguments}` clients; no identity-less executable call is defined.

## Key Entities

- **ACP transport session ID**: Standard ACP routing identifier used by `session/prompt`, updates, and permission messages.
- **Runtime session ID**: Trusted identity of the runtime/store session; device clients use it to select the associated workspace.
- **Connection mapping**: Connection-owned mapping from each ACP transport session ID to a runtime session ID, with independent lifecycle reference and device-tool binding.
- **Execution identity**: Manager-issued immutable pair `{sessionId, runId}` attached to a managed Run context and propagated to each device call; unmanaged contexts have no device-call identity.
- **Device call request**: `{sessionId, runId, name, arguments}`; wire identity is top-level protocol metadata, not part of model arguments.

## Success Criteria

- **SC-001**: 100% of generated `x-device/call` requests contain non-empty runtime `sessionId` and execution `runId`; zero malformed requests reach a client handler.
- **SC-002**: Concurrent prompts for two sessions on one connection produce zero cross-session calls or workspace access.
- **SC-003**: Every call in one execution has the same `runId`; separate executions never share one.
- **SC-004**: Missing/malformed managed identity, conflicting identity, stale mapping, or unknown client-side workspace mapping produces an explicit failure and no identity-less dispatch or workspace fallback.
- **SC-005**: Disconnect, remap, and rebinding race tests pass under Go's race detector without redirecting or duplicating calls.
- **SC-006**: The wire contract and `daemon/mobile/PROTOCOL.md` document the exact camelCase request fields, conditional ACP metadata mapping, worker fail-closed behavior, uncertain-result no-retry rule, existing capability gate, and intentional client compatibility break.

## Scope and Assumptions

- In scope: Go identity propagation, standard ACP session mapping/routing, x-device wire contract, permission and rebinding boundaries, tests, and protocol documentation.
- Android implementation is a separate user-executed task. This feature's implementation must not modify the Android project; this spec records its compatibility contract and prerequisite.
- ACP transport IDs and runtime IDs may happen to have equal strings in some paths, but equality is not a contract and must not be relied upon.
- Tool registration/probing remains separate from per-call execution identity; `x-device/tools` does not carry a runtime session or run ID.
