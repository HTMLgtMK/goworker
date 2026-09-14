# Task Console Renderer Contract

## Scope

VS Code extension reads dispatcher state through two one-shot ACP updates and observes execution through the existing `--attach <task_id>` stream. It never parses human-oriented `--ls` or `--status` output and never reads the EventLog directly.

## Catalog updates

### Task list

Prompt: `--console/tasks`

```json
{
  "sessionUpdate": "goworker_task_list",
  "version": 1,
  "tasks": [
    {
      "id": "task_abc",
      "source": "acp",
      "kind": "code",
      "status": "working",
      "worker": "claude",
      "prompt": "…",
      "created_at": "2026-09-14T12:00:00Z",
      "updated_at": "2026-09-14T12:01:00Z"
    }
  ]
}
```

Tasks are sorted by `updated_at` descending.

### Task detail

Prompt: `--console/task <task_id>`

```json
{
  "sessionUpdate": "goworker_task_detail",
  "version": 1,
  "task": {
    "id": "task_abc",
    "repo": "/repo",
    "worktree": "/worktree",
    "branch": "dispatch/task_abc",
    "commits": ["abc summary"]
  }
}
```

The detail payload contains all list fields plus code-task evidence and error state when present.

## Trace updates

Prompt: `--attach <task_id>` remains the sole trace source. The extension host forwards native `session/update` bodies to the WebView in arrival order. The renderer:

- merges adjacent message and thought chunks;
- updates tools by `toolCallId`;
- visibly marks tool fallback correlation as uncertain when an ID is absent;
- keeps unknown updates as expandable raw JSON;
- renders task detail as read-only until `reply` / `resume` exists server-side.

Unknown catalog versions are rejected, not heuristically decoded.
