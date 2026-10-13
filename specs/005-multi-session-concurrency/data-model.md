# Data Model: Multi-Live-Session Concurrency

## Session History File

A session history file is the complete append-only record stream for exactly one stable session ID. It contains the existing message, compact, head, cursor, and checkpoint records plus lifecycle records. The file is stored under the configured session root with a generated safe filename derived from the session ID.

### Invariants

- One file maps to one session ID.
- A record is never interpreted as belonging to another session.
- There is one active head and one cursor per Store instance.
- Conversation reconstruction considers only message and compact records; lifecycle records do not enter the model context.
- Unknown or malformed records are handled using the existing tolerant-read policy without silently selecting another session.

## Session Lifecycle Record

| Field | Meaning | Validation |
|---|---|---|
| `kind` | Record discriminator | Must identify a lifecycle record |
| `session_id` | Stable session identity | Non-empty, safe identifier; immutable within a file |
| `status` | Latest lifecycle state | `live` or `archived` in v1 |
| `created_at` | Session creation time | Set once; valid timestamp |
| `updated_at` | Last lifecycle transition time | Monotonic for valid transitions |

The last valid lifecycle record determines the persisted status. A file without a lifecycle record is treated as legacy input only during migration; normal newly-created files always receive one before being exposed.

## Managed Session

The daemon's session manager owns the runtime state associated with a session file.

| Field | Meaning | Ownership |
|---|---|---|
| `id` | Stable client-visible ID | Manager |
| `agent` | Conversation execution state | One runtime Session per managed session |
| `store` | Durable history | One Store per session file |
| `status` | Live or archived | Manager, persisted through Store lifecycle record |
| `cwd` | Validated working directory | Session-specific runtime state |
| `run state` | Active prompt/cancel/single-flight guard | Manager/server |
| `audit` | Decision audit sink | Shared process sink with session attribution, or explicitly session-scoped sink |

### Lifecycle Transitions

```text
(new) ──create──> live ──archive──> archived
                    │                    │
                    └──load/select───────┘ (read-only in v1)
```

Loading a live session selects it for the caller without changing its status. Loading an archived session replays history but does not make it promptable.

## Session Selection

Selection is process/client state, not lifecycle state. The manager may keep a current session ID for legacy command paths and UI display. Selection changes must not archive, revive, or mutate any other session.

## Migration Mapping

| Legacy source | New identity | New status |
|---|---|---|
| `current.jsonl` with history | Generated safe stable ID recorded in the file | `live` |
| `archive/<name>.jsonl` | Safe ID derived from `<name>` or generated on collision | `archived` |
| Empty legacy current file | No session unless it contains meaningful metadata | None |

Migration must be idempotent: the destination identity and completed transition are recognized on retry, and one source cannot produce two visible sessions.

## Concurrency Invariants

- Different managed session IDs may execute concurrently.
- A single managed session has at most one active prompt.
- Store operations for one session serialize through that Store's mutex.
- Session A's head, cursor, usage, conversation, cwd, HITL decision, cancellation, and emitted events cannot be read or written through session B.
- Process-wide configuration, memory, MCP, and audit resources use their own explicit concurrency contracts.
