# Tasks: Unified Tool Risk Policy

**Input**: Design documents from `specs/005-unified-tool-risk-policy/`

**Prerequisites**: `spec.md`, `research.md`, `data-model.md`, `contracts/`, `plan.md`

**Testing**: TDD mandatory. Every test task is RED before its implementation task.

## Phase 1: Setup and baseline

- [X] T001 [P] Run existing sandbox, middleware, ACP, config tests and record baseline without modifying old tests.
- [X] T002 [P] Verify no new third-party dependency is required; preserve workspace module boundaries.
- [X] T003 [P] Add `specs/005-unified-tool-risk-policy/quickstart.md` verification commands to CI/local checklist.

## Phase 2: Canonical risk and hard deny

### Tests first

- [X] T004 [P] Add table-driven metadata normalization tests for benign-reserved, missing, invalid, and legacy sys values in `ai-sandbox` or the chosen policy package.
- [X] T005 [P] Extend `ai-sandbox/policy_test.go` for critical normal→HITL, critical auto threshold, strict/readonly, and hard-deny orthogonality.
- [X] T006 [P] Add deny-pattern regression asserting `rm -rf /` remains hard-deny without HITL.

### Implementation

- [X] T007 Add an independent hard-deny field/reason to `ai-sandbox.RiskAssessment`; keep critical as a classification only.
- [X] T008 Add `auto_max_risk` parsing/default/validation and update `Policy.Decide` matrix; default low.
- [X] T009 Update `assess.go` deny-pattern path to set hard deny; risky patterns must not set it.
- [X] T010 Preserve `Check`/`Evaluate` compatibility and update comments/data-model references that currently define critical as deny.

## Phase 3: Config and metadata contracts

### Tests first

- [X] T011 [P] Add YAML load/save round-trip tests for `auto_max_risk` and `tool_allow_rules` in `daemon/internal/app/config/config_test.go`.
- [X] T012 [P] Add metadata propagation tests for MCP, sys, hub, skill, memory, and profile wrappers.
- [X] T013 [P] Add provider serialization tests proving policy metadata is not sent to LLM tool schema.

### Implementation

- [X] T014 Add runtime and daemon config DTOs for `auto_max_risk` and independent `tool_allow_rules`.
- [X] T015 Define canonical metadata keys/values and normalization contract.
- [X] T016 Update `daemon/internal/core/model/types.go` and `daemon/internal/core/service/tools.go` so registered-tool metadata reaches `core.Tool`.
- [X] T017 Update `ai-runtime/mcp/types.go`, `daemon/internal/core/service/mcp.go` and `sys_relay.go` to emit/normalize benign-reserved metadata; add explicit legacy adapter for `never/mode/always`.
- [X] T018 Audit default/skill/memory/profile tool construction and preserve metadata without opt-in side effects.

## Phase 4: Generic middleware policy

### Tests first

- [X] T019 [P] Add `ai-runtime/middlewares/hitl_test.go` mode × benign-reserved table for MCP/sys/custom permission tools.
- [X] T020 [P] Add tests for missing/invalid risk → unclassified and no responder → reject.
- [X] T021 [P] Add tests proving internal unmarked tools remain pass-through.

### Implementation

- [X] T022 Add shared tool-policy resolution in `ai-runtime/middlewares/hitl.go` using ToolDef metadata, not tool-name special cases.
- [X] T023 Route MCP/sys and explicitly marked tools through the same risk/mode resolver while retaining sys legacy force-HITL behavior through policy metadata.
- [X] T024 Make nil/absent decision providers fail closed without panic.

## Phase 5: Generic allow_always state

### Tests first

- [X] T025 [P] Add independent `ToolAllowRule` matching tests: same key, different key, risk upgrade, hard deny, strict/readonly.
- [X] T026 [P] Extend middleware tests for allow_once, reject_once, allow_always success/failure and next-call bypass.
- [X] T027 [P] Extend plugin and ACP worker persistence tests for tool rules, dedupe, rollback, and concurrent duplicate approvals under `-race`.
- [X] T028 [P] Add config reload test proving tool rules survive restart without persisting raw args.

### Implementation

- [X] T029 Define independent `ToolAllowRule` type and compile/match functions; do not add ToolName to bash `AllowRule`.
- [X] T030 Add metadata-key-driven `AllowAlwaysKey` to `InterruptRequest` and generic HITL persistence callback.
- [X] T031 Update `plugin.go` and `acp_worker.go` persistence to support tool rules with serialized key/max risk and atomic dedupe.
- [X] T032 Update session construction to share immutable tool-rule snapshots and refresh current middleware after successful persistence.

## Phase 6: ACP compatibility

### Tests first

- [X] T033 [P] Update `daemon/internal/core/model/permission_test.go` to table-drive bash/MCP/sys/custom capability options.
- [X] T034 [P] Extend dispatcher permission tests for allow_always forwarding and unknown-option rejection.
- [X] T035 [P] Add backward-compat tests for clients with no `allow_always_key` field.

### Implementation

- [X] T036 Change `PermissionFromInterrupt` to use capability key, never tool name.
- [X] T037 Keep old bash command allow_always behavior and make external tool approval use independent rule type.
- [X] T038 Verify ACP error/timeout/no-responder paths reject and never execute.

## Phase 7: Verification and documentation

- [X] T039 Run `gofmt`, `go vet ./...`, and targeted package tests after each phase.
- [X] T040 Run `go test -race ./...` and inspect race output for config writes/session snapshots.
- [X] T041 Run quickstart acceptance matrix including critical HITL and denylist hard-deny.
- [X] T042 Update `docs/dispatcher.md`, architecture docs, README/config examples, and cross-reference 001.
- [X] T043 Update `daemon/config.yaml.example` and `daemon/mobile/PROTOCOL.md` for auto/risk metadata/permission capability migration; remove stale MCP-not-gated claims.
- [X] T044 Perform final diff review for secrets in tool keys/config and confirm original `daemon/mobile/build-aar.sh` remains untouched.

## Dependencies

```text
T004-T010 → T011-T018 → T019-T024 → T025-T032 → T033-T038 → T039-T044
```

T012/T013 and T025/T027 can run in parallel within their phase. No implementation task starts before its corresponding RED tests exist.
