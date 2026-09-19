# ACP Attach Contract

## Transport

The dispatcher remains an ACP server on its existing local Unix socket. An ACP client creates a normal session and uses the prompt directive below.

## Request

```text
session/prompt
{
  "sessionId": "observer-session-id",
  "prompt": [{"type": "text", "text": "--attach task_abc123"}]
}
```

Grammar:

```text
--attach <task_id>
```

Exactly one non-empty task ID is required. The directive does not accept a cursor in V1: the intended product behavior is full trace archaeology from the beginning.

## Success Response

For each valid stored task update, the server emits a normal ACP notification before prompt completion:

```text
session/update
{
  "sessionId": "observer-session-id",
  "update": {
    "sessionUpdate": "...",
    "...": "original body fields"
  }
}
```

The `update` body is the stored worker `SessionUpdateBody`. Its contents, including unknown update forms and tool-call identifiers, stay unchanged. Only the wrapper session ID is remapped to the observer's session.

When the trace is complete, the prompt response uses:

```json
{"stopReason": "end_turn"}
```

## Live Semantics

- The server subscribes before it begins emitting the snapshot.
- The snapshot is emitted in EventLog order.
- Each live notification prompts an `EventsAfter(cursor)` query; all returned events are emitted in order and cursor advances to the returned next position.
- A signal is a wakeup hint, not an event count.
- `update` events emit native ACP updates. `status` events only determine whether to complete the prompt.

## Errors

| Condition | Result |
|---|---|
| missing task ID | existing dispatcher RPC status error |
| extra attach argument | existing dispatcher RPC status error |
| task does not exist | existing dispatcher RPC status error |
| malformed persisted update | warning logged; event skipped; attach remains active |
| client cancellation / disconnect | no error message required; prompt ends and subscription is removed |

## Cancellation and Isolation

`session/cancel` targets only the observer prompt context. It removes the observer's EventLog subscription but does not cancel the worker task, remove unrelated ACP routes, or resolve future permission requests.

## Non-goals

This contract does not make a task a loadable ACP session, expose a pagination/cursor resume API, or define permission/HITL responses. Those require separate protocol work.
