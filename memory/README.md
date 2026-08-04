# goworker/memory

Standalone, zero-dependency Go memory component for LLM agents. Two-layer model with a
mem0-like retrieval API: **MTM** (task archive, cross-session work state) + **LTM**
(distilled facts), retrieved on every query and injected into the prompt.

Designed as a small, independently-releasable module — no imports outside the standard
library, so it drops into any Go project.

## Why this design

- **Retrieve on every query** — the agent searches top-K relevant memory per user query
  and stitches it into the prompt (mem0-style GET).
- **Two layers, one search** — `Task` holds actionable work state (open/closed, summary,
  next steps); `Fact` holds durable knowledge. A unified `Search` returns both.
- **Pluggable retrieval** — keyword scoring ships by default; `Retriever` is an interface,
  so RAG / embedding / knowledge-graph backends drop in without touching callers.

## Install

```bash
go get github.com/tinguo/goworker/memory
```

## Quick start

```go
import (
    "context"

    "github.com/tinguo/goworker/memory"
)

func main() {
    ctx := context.Background()

    // open (or create) the store
    client, err := memory.NewClient("~/.config/goworker/memory", 50 /* taskKeep */)
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // write
    client.AddFact(&memory.Fact{
        Content: "project uses yaml config",
        Topic:   "config",
        Source:  "user",
    })
    client.UpsertTask(&memory.Task{Title: "fix config parser", Status: "open"})

    // retrieve — merged, score-desc, capped per kind
    results, _ := client.Search(ctx, "config parser", /* taskTopK */ 3, /* factTopK */ 8)
    for _, r := range results {
        switch {
        case r.Task != nil:
            fmt.Println("task:", r.Task.Title)
        case r.Fact != nil:
            fmt.Println("fact:", r.Fact.Content)
        }
    }
}
```

## Data model

| Type   | Layer | Contents                                                        | Persisted to |
|--------|-------|-----------------------------------------------------------------|--------------|
| `Task` | MTM   | Title, Status (`open`/`closed`), cross-checkpoint `Summary`, `NextSteps`, `Runs`, token usage | `<dir>/mtm/tasks.jsonl` |
| `Fact` | LTM   | `Content` (1–3 sentences), `Topic`, `Source`, timestamps         | `<dir>/ltm/facts.jsonl` |
| `Result` | —  | A unified search hit — exactly one of `Task` / `Fact` non-nil, with a `Score` | (in-memory) |

Both files are JSONL; each write mutates the in-memory slice then atomically rewrites
(tmp + rename). Existing data survives reopens and module upgrades — the on-disk format
is part of the compatibility contract.

## Retrieval

```go
type Retriever interface {
    SearchFacts(ctx context.Context, query string, topK int) ([]Result, error)
    SearchTasks(ctx context.Context, query string, topK int) ([]Result, error)
}
```

- **`KeywordRetriever`** (default, shipped) — literal keyword matching over
  `Content+Topic` (facts) and `Title+Summary+NextSteps` (open tasks only), scored as
  *matched-token-count × recency* (`1/(1+hours/24)`).
- CJK query text is tokenized into **bigrams** so "长沙" matches a task titled
  "长沙部署". Fuzzy recall where the query omits the answer's entity words is out of
  scope for keywords — that's the job of an embedding-backed `Retriever`.
- Closed tasks are never retrieved by search; historical recall is LTM's job.

To plug in a vector/RAG backend, implement `Retriever` and pass it to the client:

```go
client, err := memory.NewClientWithRetriever(dir, taskKeep, myVectorRetriever)
```

## LLM consolidation (write path)

At checkpoint time (session end, `/compact`, exit), one LLM call distills the
conversation into task updates + fact decisions:

```go
// 1. wrap your LLM client in the tiny memory.LLM interface
var llm memory.LLM          // Model() + Chat(ctx, *ChatRequest) (*ChatResponse, error)

cp := memory.NewCheckpointer(llm, cwd)
res, err := cp.Run(ctx, conversation /* []memory.Message */, openTasks, facts)
// 2. apply to the store; the summary carries what was actually persisted,
//    so callers can echo it back to the user (e.g. "Memory updated: N task(s), M fact(s)")
sum, err := memory.ApplyCheckpoint(client, res, openTasks, facts, runID, tokens, cwd)
// sum.UpdatedTasks / sum.ClosedTasks / sum.Facts / sum.DeletedFacts —
//   persisted task titles, added/updated fact lines, and deleted fact lines
```

Fact writes are decision-based (mem0-style): the model emits `add` / `update` /
`delete` / `noop` per candidate fact (`ApplyDecisions`), so the fact set stays deduped
and contradiction-free instead of blind appends.

## Storage format

```
<dir>/ltm/facts.jsonl      # one JSON Fact per line
<dir>/mtm/tasks.jsonl      # one JSON Task per line
```

Corrupt or oversize lines are skipped on load (crash-tolerant). `taskKeep` trims the
oldest tasks when the archive exceeds its cap.

## Dependencies

**None outside the standard library.** `go.mod` is module + go version only — the whole
retrieval + storage + LLM-consolidation surface is self-contained.

## License / status

Part of the [goworker](https://github.com/tinguo/goworker) workspace; extracted so it
can be released and consumed on its own.
