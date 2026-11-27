# Contract: Standard ACP Session-to-Runtime Mapping

## Identities

- **ACP transport session ID**: Standard ACP `sessionId`, used to route `session/prompt`, `session/cancel`, `session/update`, and permission requests.
- **Runtime session ID**: Trusted Go runtime/store identity used by execution routing and by the device client to choose a workspace.

These identities have different meanings. Their string values may coincide in a particular backend, but clients and servers must not rely on that coincidence.

## Response Metadata

A successful `session/new` reports its transport ID as usual and, when the creator returns a non-empty runtime ID, includes it in ACP response metadata:

```json
{
  "sessionId": "acp-session-a",
  "_meta": {
    "x-goworker.runtimeSessionId": "runtime-session-a"
  }
}
```

A successful writable `session/load` includes the same `_meta` key when the loader returns a non-empty runtime ID. The current daemon app-service handler provides a runtime ID for new and writable loaded sessions. Archived read-only loads return no runtime ID; protocol adapters without the optional runtime-ID creator/loader may also omit `_meta`. The key is the literal dotted string `x-goworker.runtimeSessionId` inside `_meta`, not a nested `x-goworker` object.

A client that needs `x-device` MUST persist the explicit mapping it receives and treat missing/blank runtime metadata as unavailable for device workspace selection. It must not guess from `cwd`, session-list order, or process-global selection. The metadata key and response serialization are provided by `ai-acp`; handler support determines whether a runtime ID is available.

## Connection Mapping and Routing

- A connection owns zero or more mappings: `ACP transport session ID → runtime session ID`.
- `session/new` adds its new mapping; it does not invalidate previously declared sessions on the same connection.
- `session/load` resolves the requested session and commits/replaces only that target mapping after successful load. Failed load leaves prior routing and lifecycle references unchanged.
- Every `session/prompt` resolves the exact transport ID in that connection's map. It never routes through “last session selected” or global current state.
- The mapping is pinned when a prompt is admitted. A later load, disconnect, or rebind cannot redirect that in-flight execution to another runtime.
- Updates and permission messages continue to use the ACP transport session ID. Runtime ID is used only for backend execution/tool binding and identity-bearing device calls.
- Multiple transport mappings may target the same runtime. Each mapping owns its own lifecycle reference; removal releases only that reference.

## Lifecycle and Errors

- Connection close releases all mappings and device-capability bindings owned by that connection. A runtime remains alive under the repository's existing reference/run lifecycle rules.
- Replacing a mapping releases its previous reference only after the new mapping is successfully committed.
- Unknown, stale, closed, or non-promptable transport IDs produce an explicit ACP error. There is no fallback to another session.
- A runtime without a trusted managed Run identity cannot make an executable device call. Do not invent an execution identity from the ACP transport session ID or cwd. The current stdio `acpWorker` raw relay fails closed before sending `x-device/call`.
- Reconnection creates new connection-owned mappings by successful `session/load` (or another explicitly supported restore operation); clients must not reuse stale connection state as authority.

## Device Binding Relationship

Device tool capability descriptors may be discovered at connection initialization, but execution binding is runtime-session-specific. For each prompt, the server binds or resolves the negotiated capability for the mapped runtime ID. Concurrent prompts on the same connection must not share mutable routing state. Disconnect/rebind can move a waiting invocation only to a compatible client binding for the same runtime ID; it does not change the invocation's Run ID or repeat/skip permission checks.
