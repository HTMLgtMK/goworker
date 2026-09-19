# Task List: Task Detail ACP Observer

## Phase 1 — Contract and Test Design

- [ ] Add failing ACP ingress tests for completed task history replay and observer session-ID mapping.
- [ ] Add failing tests proving EventStatus is not rendered as a session update.
- [ ] Add failing tests for snapshot-to-tail ordering and exactly-once delivery.
- [ ] Add failing tests for malformed stored updates, missing task/arguments, cancellation and completion statuses.

## Phase 2 — Dispatcher Implementation

- [ ] Convert dispatcher ACP route storage to a per-task route slice with exact route removal.
- [ ] Fan out live progress from a lock-protected route snapshot outside the plugin mutex.
- [ ] Add `--attach <task_id>` directive parsing and task validation.
- [ ] Replay valid EventUpdate payloads as native ACP updates using the observer session ID.
- [ ] Tail EventLog with Subscribe cursor and EventsAfter until the worker trace completes or observer cancels.

## Phase 3 — Regression Verification

- [ ] Prove original submitter and observer receive the same live update without overriding each other.
- [ ] Prove observer cancellation does not cancel the task or remove another route.
- [ ] Run focused tests and race detection for dispatcher and EventLog packages.
- [ ] Run broader Go test suite and manual local Unix-socket ACP smoke test.

## Out of Scope

- [ ] No task console or custom trace renderer in this feature.
- [ ] No permission/HITL broker, reply/resume, or `session/load` changes.
