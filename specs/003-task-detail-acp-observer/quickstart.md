# Quickstart: Observe a Dispatcher Task Through ACP

## Prerequisites

- Start the goworker daemon with dispatcher enabled.
- Use an ACP client that can connect to the local Unix socket configured for dispatcher. Do not expose this socket through TCP.
- Have a task ID from a task dispatched through the same daemon.

## Attach to a Completed Task

1. Create a new ACP session with the client working directory.
2. Send a text prompt exactly in this form:

   ```text
   --attach task_abc123
   ```

3. Inspect received `session/update` notifications. Their `sessionId` must equal the new observer session ID; their `update` payloads contain the original agent trace.
4. Verify prompt completion has `stopReason: "end_turn"`.

## Observe a Running Task

1. Client A submits a normal task and retains its session.
2. After the task emits at least one update, Client B creates a fresh session and sends `--attach <task_id>`.
3. Verify Client B first receives historical update notifications, then receives later worker updates as they occur.
4. Cancel Client B's prompt/session.
5. Verify Client A still receives subsequent updates and the task continues until its normal stream completion.

## Expected Completion

- `awaiting_review` ends this observer prompt because worker execution has finished.
- `done`, `rejected`, `failed`, and `cancelled` replay remaining history then end the prompt.
- `merging` stays open until it reaches a terminal status.

## Failure Diagnosis

- Invalid or missing task IDs return dispatcher RPC status errors.
- A warning in daemon logs about a malformed stored update means that single record was skipped; subsequent valid trace events should still be observable.
- If an ACP client renders raw text JSON rather than thought/tool cards, ensure it is using `--attach`, not the diagnostic `--events` directive.
