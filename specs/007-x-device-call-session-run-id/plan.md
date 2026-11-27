# Implementation Plan: x-device Call Session and Run Identity

**Branch**: `feat/x-device-call-session-run-id` | **Spec**: [spec.md](spec.md)

## Summary

Carry the manager-issued immutable `{runtimeSessionID, runID}` from each managed execution context to `x-device/call`. Route standard ACP prompts by their own transport session mapping rather than a connection-global “current session”. Publish runtime IDs through ACP response `_meta` when the handler returns them, document the client upgrade requirement under the existing key-only capability gate, and fail closed when managed identity is absent.

This file is an implementation plan only. No feature code or Android changes are part of this documentation task.

## Technical Context

- **Language/runtime**: Go workspace (`ai-acp`, `ai-runtime`, `daemon`); no new dependency expected.
- **Wire**: The Go `deviceCallRequest` uses the required top-level JSON fields `sessionId` and `runId` (Go fields `SessionID` and `RunID`), plus `name` and `arguments`. `ai-acp` returns runtime identity under `_meta["x-goworker.runtimeSessionId"]` when the creator/loader returns a non-empty ID; archived read-only load may omit it.
- **Execution sources**: Managed Runs reuse their manager-issued Run ID. Standard ACP prompts that reach device tools must carry a managed identity. Unmanaged paths such as the current stdio `acpWorker` raw relay have no trusted Run identity and fail closed; do not mint a substitute from ACP transport IDs.
- **Safety**: No global-current fallback, no model-supplied identity, no identity-less call, and no automatic resend after an invoked call has an uncertain result. Existing initialize capability checks only for presence of `x-device`; it does not negotiate a wire version, so old clients must be upgraded before declaring the capability.
- **Verification**: Table-driven boundary tests, ACP integration tests with concurrent sessions, and `go test -race` on affected Go modules/packages.

## Implementation Areas

| Area | Planned change |
|---|---|
| `ai-runtime/agent` execution context | Add a private-key helper for immutable execution identity; validate required fields and expose read-only lookup. |
| Run/session manager and run-control entry points | Inject the actual runtime session ID and managed Run ID into each Run context; ensure identity survives callback/tool execution. |
| `daemon/internal/core/service/acp/handler.go` | Maintain multiple connection-owned ACP transport→runtime mappings; route prompt by the requested transport ID; pin execution identity before evaluation; preserve transport ID for ACP updates/permissions. |
| `ai-acp/protocol/spec.go` and `ai-acp/server.go` | Verify the metadata contract is preserved for both `session/new` and `session/load`; extend only where needed. Use the established key `x-goworker.runtimeSessionId`. |
| `daemon/internal/core/acpworker/sys_relay.go` | Add required wire fields and reject absent/blank/conflicting execution identity before invoking `x-device/call`. |
| `daemon/internal/core/acpworker/acp_worker.go` | Verify that the stdio worker's unmanaged Run context cannot invoke device calls without manager-issued identity; preserve fail-closed behavior. |
| Raw relay, legacy binder, and tool proxy paths | Trace every caller; provide manager-issued identity for managed paths or fail closed. Never derive identity from a transport ID or mint a run ID per tool invocation. |
| Device capability negotiation | Keep the existing `x-device` key-presence handshake accurate in docs. Require client upgrade operationally; do not claim a nested identity-version gate is enforced. |
| `daemon/mobile/PROTOCOL.md` | Document the new handshake requirement, metadata mapping, request shape, lifecycle, errors, and non-backward-compatible device-call change. Android implementation remains external. |

## Phases

### Phase 0 — Freeze protocol and identity boundaries

- Confirm the wire keys (`sessionId`, `runId`), ACP metadata location/availability, fail-closed worker behavior, and uncertain-result semantics from `contracts/`.
- Add failing protocol tests for runtime metadata on new/writable-load responses and its omission for archived read-only loads; test that a managed call without valid identity is not dispatched.

### Phase 1 — Multi-session standard ACP routing

- Replace last-selected-session prompt routing with an exact transport-ID mapping lookup.
- Make new/load mapping updates atomic with connection lifecycle registration; keep unrelated mappings intact.
- Pin the selected runtime ID for a prompt and return a clear error for stale/unknown mappings.
- Preserve transport session IDs in standard ACP notifications and permission requests.

### Phase 2 — Execution identity propagation

- Add execution identity context helpers and inject identity at managed Run start.
- Trace synchronous, raw relay, `acpWorker`, proxy and legacy binder call chains. Only manager-issued identity is trusted; any path without it must fail closed rather than mint an ID from transport state.

### Phase 3 — Device call boundary and capability gate

- Encode required `sessionId` and `runId` fields in the top-level device-call request and reject missing managed identity before transport invocation.
- Check context identity against any runtime-bound proxy; reject mismatch.
- Preserve the existing `x-device` key-presence capability gate. Do not document a nested identity-version flag as enforced; release/use of the new sender requires an updated client because old clients are incompatible.
- Keep permission checks independent and ensure an uncertain result after Invoke is returned without replaying the call.

### Phase 4 — Lifecycle, races, and compatibility verification

- Cover close, mapping replacement, in-flight prompt, cancellation, concurrent sessions, disconnect and compatible rebind.
- Update only repository-owned protocol documentation and examples. Record Android coordination as an external prerequisite; do not edit its repository.
- Run focused tests, race detection, and broader Go verification as available.

## Design Constraints

- Transport session ID and runtime session ID are distinct concepts even if their string values coincide.
- A handler returns the runtime ID in `_meta["x-goworker.runtimeSessionId"]` when the optional runtime-ID interface supplies it; archived read-only load has no writable runtime ID. Missing metadata is not permission to guess.
- Mapping lookup happens once at execution admission. Later connection rebinding cannot retarget an in-flight execution.
- Workspace authorization stays in the client, based on the explicit runtime ID mapping and its existing path boundary checks. No workspace path is supplied as identity.
- `runId` is for execution attribution, not authorization. Existing per-call HITL/policy behavior remains authoritative.
- Existing initialize negotiation checks presence of `x-device`, not an identity-wire version. Legacy `{name, arguments}` clients are incompatible and must be upgraded before they declare that capability; do not claim the server detects legacy versions.

## Verification Targets

- Unit: identity context validation, propagation, uniqueness/reuse semantics, proxy binding mismatch, request serialization.
- ACP integration: response metadata for create/load; two sessions and two workspaces on one connection; concurrent standard prompts; notification IDs remain transport IDs.
- Lifecycle/race: failed load leaves prior mapping intact; successful replacement affects only its target; connection close releases all references; in-flight execution remains pinned; rebind does not alter identity.
- Security: missing/malformed managed identity and proxy/session mismatch produce zero client invocation; client mapping errors never fall back to a workspace. Validate the legacy-client incompatibility in the Android client contract/deployment checklist, not as a server-side capability-version check.
- Go: focused packages plus `go test -race` for affected packages/modules, then the repository's normal full test suite.
