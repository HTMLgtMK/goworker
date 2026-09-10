# Data Model: Task Detail ACP Observer

## Existing Persistent Data

No persistent schema changes are required.

```go
type TaskEvent struct {
    TaskID    string
    Timestamp time.Time
    Type      EventType
    Update    json.RawMessage
    From      Status
    To        Status
}
```

| Field | Observer use |
|---|---|
| `TaskID` | attaches the event to the requested task |
| `Type == update` | decode `Update` as `protocol.SessionUpdateBody` and emit it |
| `Type == status` | determine whether the stream has ended |
| `Timestamp` | diagnostics only; never used as a replay cursor |

`Update` stays the single durable payload for ACP trace data. It is deep-copied by EventLog before returning snapshots and tail batches.

## In-memory Route Model

```go
type acpRoute struct {
    server    *dispatch.Server
    sessionID string
}

acpRoutes map[string][]acpRoute
```

| Entity | Identity | Lifecycle |
|---|---|---|
| route | `{server, sessionID}` | registered for a running synchronous dispatch; removed only by its owner |
| route collection | task ID | contains all current low-latency targets for that task |
| route snapshot | copied slice | made while holding plugin mutex, dispatched after unlocking |

Routes are transport optimization only. They do not store replay progress and do not participate in an attach observer's durable cursor.

## Observer State Machine

```text
new ACP session
  │
  ├─ prompt "--attach <task_id>"
  │    ├─ task missing / invalid arguments → RPC error
  │    └─ task found
  │         │
  │         ├─ EventLog.Subscribe → {history, cursor, live, unsubscribe}
  │         ├─ replay update events in history
  │         ├─ status ends worker trace → end_turn
  │         └─ wait live
  │              ├─ live signal → EventsAfter(cursor) → replay batch → advance cursor
  │              ├─ terminal / awaiting_review status → end_turn
  │              └─ context canceled → unsubscribe → end_turn
```

## Outbound Mapping

```text
TaskEvent(EventUpdate)
  └─ JSON decode → protocol.SessionUpdateBody
       └─ reporter.Update(observerSessionID, body)
            └─ session/update(sessionId=observerSessionID, update=body)
```

The observer does not alter `body`. Changing the body would break renderer pairing such as tool-call/update associations. The outer `sessionId` belongs to the observation session because ACP clients route notifications by that ID.

## Completion States

| Task status | Observer behavior |
|---|---|
| `queued`, `dispatching`, `working` | continue waiting for tail events |
| `awaiting_review` | trace is complete; return `end_turn` |
| `merging` | continue waiting until a terminal state |
| `done`, `rejected`, `failed`, `cancelled` | replay available updates then return `end_turn` |

## Corruption Handling

A failed `SessionUpdateBody` decode creates no outbound update and no client-visible synthetic message. It produces a server warning, advances the cursor, and continues. That keeps a damaged historical record from turning the rest of an execution trace invisible.
