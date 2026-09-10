# Research: Task Detail ACP Observer

## Decision

Task detail uses a dedicated observer ACP session, started through:

```text
session/new → session/prompt("--attach task_xxx")
```

The observer reads durable task history from EventLog and then live-tails the same log. It does not become the task owner and it does not rely on a timing-sensitive conversion from replay to direct routing.

## Existing Building Blocks

### `TaskEventLog` is the trace authority

`ai-dispatch/task/event_log.go` provides:

```go
Subscribe(taskID) ([]TaskEvent, int, <-chan struct{}, func())
EventsAfter(taskID, cursor) ([]TaskEvent, int)
```

`Subscribe` holds the EventLog mutex while it clones history, sets the cursor to history length, and registers the subscriber signal. That makes the boundary atomic: any update before registration belongs in history; any update after belongs to `EventsAfter(cursor)`.

The live channel is deliberately coalescing. Missing a signal is harmless because each wakeup reads all durable events after the cursor. The observer must therefore never treat one signal as one event.

### `EventUpdate` already contains the real ACP payload

`TaskEvent.Update` is raw JSON produced by `updatePayload(protocol.SessionUpdateBody)`. The observer decodes it and delegates output to `dispatch.Server.Update(observerSessionID, body)`. That produces a normal outbound ACP notification:

```text
session/update {
  sessionId: observer session,
  update: original SessionUpdateBody
}
```

This preserves renderer-visible types such as agent message, agent thought, tool call and tool update. The outer session ID must change because the original downstream/submitting session does not belong to the observing client session.

### `EventStatus` is control-plane state, not trace UI

Task status transitions do not have a matching ACP `SessionUpdateBody`. The observer uses them only to end the prompt after `awaiting_review` or a terminal state. It must not emit a fake update or serialize status as a textual JSON chunk.

`awaiting_review` is complete for this stream: the worker has stopped and future review/merge handling is a separate lifecycle. `merging` remains open until it becomes terminal.

## Why `session/load` is not attach

ACP `session/load` restores an agent's own persisted session. Dispatcher currently uses the client-side load mechanism to work with downstream worker persistence; its ingress server neither advertises nor handles task-as-session loading.

A task may contain work across dispatch, review and merge state, while attach is a read-only subscription to one execution trace. Reusing `session/load` would confuse ownership, capabilities and cancellation semantics. A prompt directive is explicit, backwards-compatible and fits the current server handler model.

## Why `acpRoutes` are not the observer source

`acpRoutes` exists to push live progress to currently connected submitters. Existing code has a single route per task; it must become `map[string][]acpRoute` so one session does not overwrite another.

However, routes have no history and cannot provide an atomic transition from replay to live. Registering a route only after replay loses updates; registering before replay can duplicate them. Attach remains EventLog-backed. Route fanout is still needed for original submitters and future direct live subscribers.

## Malformed persisted payload policy

EventLog startup recovery tolerates malformed JSONL records. A record may still be structurally a TaskEvent but have unusable `Update` bytes. During attach:

1. attempt to decode `protocol.SessionUpdateBody`;
2. on failure log task ID and event cursor at warning level;
3. skip the bad event and advance cursor;
4. keep processing later durable events.

The decoder must not impose a subtype whitelist: valid forward-compatible raw update forms must flow through unchanged.

## Explicitly Excluded

- task console / VS Code WebView / custom timeline rendering;
- `session/load` capability changes;
- EventLog schema migrations;
- permission broker, ordinary HITL, reply/resume;
- TCP listeners or externally reachable ports.
