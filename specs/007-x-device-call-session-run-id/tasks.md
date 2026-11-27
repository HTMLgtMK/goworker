# Tasks: x-device Call Session and Run Identity

**Input**: Design documents in `specs/007-x-device-call-session-run-id/`  
**Status**: Go implementation and verification are substantially complete; unchecked items below identify gaps not fully covered by current evidence. Android implementation and real-client end-to-end verification remain external prerequisites.  
**Implementation boundary**: Go repository only. Android work is external and user-executed.

## Phase 1 — Contract and failing tests

- [x] T001 Add wire serialization tests requiring top-level non-empty `sessionId` and `runId` JSON fields on every managed `x-device/call` request. Evidence: `daemon/internal/core/acpworker/sys_relay_test.go` (`TestRelayDeviceToolsForSessionWritesManagedIdentityToWire`); ACP device-call integration test.
- [ ] T002 Add ACP protocol tests for runtime metadata on `session/new` and writable `session/load`, and for omission on archived read-only load or handlers without runtime-ID support. `ai-acp/client_server_test.go` covers new + writable load; archived/no-runtime-ID metadata omission is not explicitly asserted.
- [x] T003 Add tests proving a missing/malformed managed Run identity never dispatches `x-device/call`; document that initialize checks only `x-device` key presence (no identity-wire version gate). Evidence: `daemon/internal/core/acpworker/sys_relay_test.go` missing/mismatched identity tests; `daemon/mobile/PROTOCOL.md` documents key-presence gate.
- [x] T004 Add failing standard ACP tests for multiple transport→runtime mappings on one connection and exact `session/prompt` routing by requested `sessionId`. Evidence: `daemon/internal/core/service/acp/handler_test.go` (`TestFrontendHandlerRoutesConcurrentTransportSessionsAndTools`); ACP identity integration test exercises standard prompts.
- [ ] T005 Add failing identity tests for blank/missing values, unknown sessions, proxy/session conflict, model-argument spoofing, and a legacy path without a trustworthy identity. Missing/blank, unknown-session, proxy mismatch, and unmanaged relay are covered; model-argument spoof rejection is not directly tested.

## Phase 2 — Multi-session ACP mapping and lifecycle

- [x] T006 Implement connection-owned mappings from ACP transport IDs to runtime IDs; do not let `session/new` supersede unrelated mappings. Evidence: `daemon/internal/core/service/acp/handler.go`; concurrent mapping test in `handler_test.go`.
- [x] T007 Make successful `session/load` replace only its target mapping; failed load leaves existing mappings and lifecycle references unchanged. Evidence: `handler.go`; load/commit and lifecycle tests in `handler_test.go` and `binding_test.go`.
- [x] T008 Route prompts by the requested transport ID, pin the resolved runtime for that prompt, and return explicit errors for unknown/stale mappings. Evidence: `handler.go`; `TestFrontendHandlerRejectsUnknownSession` and `TestFrontendHandlerRoutesConcurrentTransportSessionsAndTools`.
- [x] T009 Preserve ACP transport IDs in `session/update` and `session/request_permission` while using runtime IDs for execution and device routing. Evidence: `daemon/internal/core/service/acp/handler.go`; ACP routing tests in `handler_test.go`.
- [x] T010 Release exactly the affected mapping reference on replacement and all connection-owned references on close; keep already-admitted execution pinned until completion/cancellation. Evidence: `handler.go`; `TestNewClearsOnlyItsTransportAndUnbindsLastRuntimeReference`, create/load-close tests in `handler_test.go`.

## Phase 3 — Execution identity propagation and device relay

- [x] T011 Add private-context execution identity helpers with required-field validation and immutable `{sessionID, runID}` semantics. Evidence: `ai-runtime/agent/run.go`; `TestRunIdentityFromContext` in `run_identity_context_test.go`.
- [x] T012 Inject managed runtime session ID and managed Run ID at run start; add tests proving all tool calls in that Run share the pair. Evidence: `ai-runtime/agent/supervisor.go`; ACP identity integration test checks active Run ID and repeated calls.
- [x] T013 Add standard ACP prompt integration coverage proving the prompt is routed to the mapped runtime and its managed identity reaches every device call; unmanaged entry paths must fail closed rather than minting a substitute ID. Evidence: `daemon/internal/core/service/acp/xgw_device_call_identity_integration_test.go`; unmanaged relay tests in `sys_relay_test.go`.
- [x] T014 Verify that the current stdio `acpWorker` raw relay has no trusted managed identity and fails closed before sending a device call; do not assign its ACP transport ID as a substitute. Evidence: `daemon/internal/core/acpworker/sys_relay.go`; unmanaged relay assertions in `sys_relay_test.go`.
- [x] T015 Trace and update raw relay, client-tool proxy and legacy binder paths so each supplies complete identity or returns an explicit error. Evidence: `sys_relay.go`; `ai-runtime/agent/capability.go`; `daemon/internal/plugins/agent/runcontrol.go`; their fail-closed and identity tests.
- [x] T016 Add required top-level `sessionId` and `runId` JSON fields to `x-device/call`; reject missing managed identity and proxy/runtime mismatch before invoking the client. Evidence: `deviceCallRequest` in `sys_relay.go`; missing/mismatch tests in `sys_relay_test.go`.
- [x] T017 Preserve the existing `x-device` key-presence gate; document that nested capability values do not negotiate this wire version. Keep permission checks independent and do not retry an invocation with an uncertain result. Evidence: `daemon/mobile/PROTOCOL.md`; capability and no-retry tests in `ai-runtime/agent/capability_test.go`.

## Phase 4 — Concurrency and integration verification

- [x] T018 Add integration coverage for two concurrent sessions/workspaces on one connection, including same-named tools and out-of-order completion. Go fake-client coverage only: `xgw_device_call_workspace_isolation_integration_test.go` and `xgw_device_call_identity_integration_test.go`; real Android client/workspace integration remains an external prerequisite and is not verified here.
- [x] T019 Add lifecycle tests for connection close, mapping replacement, cancellation, stale prompts, disconnect, and compatible rebind; assert no cross-session routing and stable execution identity. Evidence spans `handler_test.go`, `service_test.go`, `ai-runtime/agent/capability_test.go`, and ACP identity integration test.
- [ ] T020 Add end-to-end tests for managed Run, standard prompt, synchronous path, raw relay, `acpWorker`, and legacy binder fail-closed behavior. Unit/integration evidence exists for several paths, but the complete cross-path end-to-end set is not verified.
- [x] T021 Run focused Go tests and race detection on affected packages/modules; resolve any identity/lifecycle races. Verified: `go test ./...`, `go test -race ./...`, and `go vet ./...` across all seven `go.work` modules passed.
- [x] T022 Run the repository's normal Go test suite and verify no identity-less `x-device/call` request remains in any sender/test fixture. Verified all workspace module tests; source audit found the sender in `daemon/internal/core/acpworker/sys_relay.go` emits the required fields, with no legacy identity-less sender fixture.

## Phase 5 — Repository protocol documentation

- [x] T023 Update `daemon/mobile/PROTOCOL.md` with request fields, metadata mapping, capability requirement, errors, lifecycle semantics, and explicit non-backward compatibility.
- [x] T024 Verify repository-owned examples and fixtures use the new request shape and do not document a current-workspace fallback.
- [x] T025 Record Android client implementation as a separate external prerequisite; do not modify the Android repository as part of this plan.

## Dependencies

```text
T001-T005 → T006-T010 → T011-T017 → T018-T022 → T023-T025
```

T001-T005 may be developed in parallel. Lifecycle tests in T018-T020 depend on both ACP mapping and execution identity contracts being in place.
