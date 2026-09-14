# Data Model

## Catalog

`TaskSummary` is the sidebar projection. It contains identity, source, kind, status, worker, prompt, and lifecycle timestamps.

`TaskDetail` extends it with repo/worktree/branch/base commit/worker session/commits/error. The DTO is owned by `daemon/internal/dispatcher`, not by the generic ACP protocol.

## Trace state

The WebView owns immutable `TraceState`:

```text
TraceState
  items: TraceItem[]
  completed: boolean

TraceItem
  id
  kind: message | thought | tool | plan | unknown
  text/title/status/toolCallId
  uncertain
  raw ACP update
```

Adjacent message and thought chunks merge. Tool updates use `toolCallId`; no ID uses adjacent unfinished tool fallback and is marked uncertain.
