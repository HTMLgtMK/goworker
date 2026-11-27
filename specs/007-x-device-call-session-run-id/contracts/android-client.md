# Contract: Android Client Coordination (External Implementation)

This contract is owned here for coordination only. The Android implementation is a separate user-executed task; this repository task does not modify Android sources or AAR artifacts.

## Required Client Behavior

- Declare the existing `clientCapabilities.x-device` key only when the handler fully supports the current identity-bearing call shape. The agent checks only key presence; it does not parse `sessionRunIdentity` or another nested version flag.
- Register the `x-device/tools` and `x-device/call` handlers before sending `initialize`, as required by the existing reverse-request flow.
- After successful `session/new` or writable `session/load`, read `_meta["x-goworker.runtimeSessionId"]` when present and store the explicit runtime-session-to-workspace association. Keep the ACP transport `sessionId` for standard ACP requests. Missing metadata means no device-workspace binding; archived read-only loads intentionally have no runtime ID.
- Support multiple mappings on one connection; do not use a mutable global/current workspace to process calls.
- On every `x-device/call`, validate non-empty top-level `sessionId` and `runId`, resolve the exact runtime session mapping, and reject unknown or ambiguous sessions. Never fall back to a default workspace.
- Apply the existing workspace/path boundary and permission behavior after resolving the mapping. `runId` is correlation data, not authorization.
- If a request has been received but the response is lost or times out, do not automatically execute it again: the original operation may already have completed. Report the uncertain outcome to the caller.
- On session unload/replacement or connection close, release only the corresponding mappings; do not accidentally erase another live session's mapping.

## Compatibility Boundary

The current `deviceCallRequest` emits `sessionId` and `runId` alongside `name` and `arguments`. Clients that only understand `{name, arguments}` are incompatible; update the handler before declaring `x-device`. The existing initialize gate checks only whether the `x-device` key exists, so it does not detect an old handler. The Android client must be updated and verified separately before using this sender.

## Suggested Verification in the Android Repository

- A response with missing runtime metadata disables device execution for that session and surfaces a clear error.
- Two sessions mapped to different workspaces can invoke the same tool concurrently and access only their respective workspace.
- Two sessions mapped to one workspace still retain distinct session/run identity.
- Empty identity, unknown session, stale mapping, and path escape are rejected without default-workspace fallback.
- The handler supports the added top-level fields and does not deserialize identity from model `arguments`.
