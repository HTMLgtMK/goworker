# Contract: x-device/call Execution Identity

This contract matches the Go sender's current `deviceCallRequest` JSON tags and managed Run identity boundary. The wire change is not backward-compatible with clients that only understand `{name, arguments}`.

## Negotiation and Client Compatibility

The existing initialize capability signal is the presence of the `x-device` key:

```json
{
  "clientCapabilities": {
    "x-device": {"tools": true}
  }
}
```

The server checks key presence; it does not parse a nested `sessionRunIdentity` flag or negotiate a version for this wire change. A client must only declare `x-device` after its handler supports the request below. Old clients are incompatible and must not be used with the new sender. This is a client/deployment compatibility requirement, not a server-side version gate.

## Request

```json
{
  "sessionId": "runtime-session-42",
  "runId": "run-opaque-9a5f",
  "name": "read_file",
  "arguments": {"path": "src/main.go"}
}
```

The exact wire keys are `sessionId` and `runId`. In the Go `deviceCallRequest` struct, these map to `SessionID` and `RunID`.

| Wire field | Type | Required | Meaning |
|---|---|---:|---|
| `name` | string | yes | Client-declared device tool name, without the worker's `sys_` prefix. |
| `arguments` | JSON value | yes | Model-visible tool arguments; contains no trusted identity fields. |
| `sessionId` | non-empty string | yes | Runtime/store session that owns this invocation. Never infer it from transport ID, workspace path, or current-session state. |
| `runId` | non-empty string | yes | Managed execution ID shared by all device calls in the same Run. Not an authorization token. |

The Go sender obtains identity from the private manager-issued Run context. It rejects a missing/malformed managed identity before calling the client. Identity fields are top-level protocol fields and must never be read from `arguments`. No legacy identity-less request shape is defined.

## Identity Source and Lifetime

- The managed `SessionManager` installs the runtime session ID and its Run ID in a private execution context. The identity is immutable for that Run; all device calls in it use the same pair.
- An unmanaged caller cannot manufacture this context identity from an ACP transport ID, prompt text, or tool arguments.
- The current stdio `acpWorker` raw relay has no trusted managed runtime identity; it may discover descriptors, but execution fails closed before sending `x-device/call`.
- Connection rebinding cannot change the identity captured by an admitted Run.

## Capability Wait and Uncertain Results

- If no compatible binding exists before `Invoke`, the proxy may wait for a binding for the same runtime session within the existing capability-wait budget.
- Once `x-device/call` is invoked, any transport error or timeout has an uncertain outcome: the client may have performed the operation even if the response was lost. The binding is invalidated as appropriate, the error is returned, and the call is **not automatically retried** on a replacement connection.
- A caller may choose a new retry as a separate operation after reviewing the result; the protocol does not promise idempotency for device tools.

## Errors and Safety

| Condition | Required behavior |
|---|---|
| Missing or blank managed identity | Explicit local error; do not call the client. |
| Missing runtime session-to-workspace mapping on client | Client rejects; never select a default/current workspace. |
| Context runtime ID conflicts with a session-bound proxy | Reject before client invocation and log useful server-side context without leaking secrets. |
| Client implements only the legacy `{name, arguments}` shape | Incompatible; upgrade before declaring `x-device`. The current initialize gate does not negotiate a wire version. |
| Connection disconnects before a compatible binding is selected | Existing capability wait may wait for a binding for the same runtime session. |
| `x-device/call` returns transport error/timeout after invocation | Outcome is uncertain; return error and do not replay automatically. |
| Permission policy rejects or requires approval | Enforce existing server-side per-call policy; identity does not grant permission. |

The client resolves `sessionId` through an explicit runtime-session-to-workspace mapping and enforces its existing workspace/path authorization. Unknown or ambiguous mapping fails closed. `runId` may be used for correlation/audit only; it must not select or authorize a workspace.
