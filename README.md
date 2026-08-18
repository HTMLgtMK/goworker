# goworker

A modular REPL agent terminal in Go — plugin-based, middleware-driven, LLM-ready.

## Architecture

Five Go modules in one workspace (`go.work`), layered as an SDK. `ai-core` / `ai-sandbox` / `ai-memory` are standalone, independently-releasable; `ai-runtime` aggregates them into an out-of-the-box agent for external projects; `daemon/` is the REPL shell consuming `ai-runtime`.

```
ai-memory/                     ← standalone: MTM task archive + LTM facts (mem0-like), zero deps
ai-sandbox/                    ← standalone: command safety layer (risk R0-R7 → policy → allow/hitl/deny), stdlib only
ai-core/                       ← pure engine kernel: spec protocol + ReAct engine + core types + engine middlewares
│  ├── spec/                   ← Hub, Command, Plugin, Context (pure protocol)
│  ├── core/                   ← Tool, Message, Usage, Compressor, NewMsgID
│  ├── config/                 ← LLMConfig/MemoryConfig + validators (engine needs)
│  ├── agent/                  ← ReAct loop (zero sandbox/memory deps)
│  └── middlewares/            ← usage/iteration/compression/memory (MemoryClient as local interface)
ai-runtime/                    ← aggregation: out-of-the-box agent for external projects
│  ├── config/                 ← aggregated Config{LLM,Memory,Sandbox,Session,MCP} + Paths + event contract
│  ├── agent/                  ← AgentPlugin (NewPlugin(cfg, paths)), session, tools, commands, DefaultTools
│  ├── middlewares/            ← HITL middleware (sandbox decision gating)
│  ├── session/                ← conversation store (checkpoint/rewind)
│  ├── mcp/ skills/ logger/    ← moved from daemon, reusable
daemon/                        ← REPL shell: core.Engine + frontend + config parsing + path hub
│  ├── cmd/goworker/           ← entry point
│  ├── internal/config/        ← top-level flattened config.yaml + ToRuntime()/ApplyRuntime()
│  ├── internal/core/          ← Engine: plugin lifecycle, command routing, middleware chain
│  └── internal/frontend/      ← stdin REPL + statusbar (subscribes ai-runtime events)
docs/architecture.md           ← detailed architecture doc
```

```
daemon ──→ ai-runtime ──→ ai-core ──→ (zero goworker deps)
                 ├──→ ai-memory
                 └──→ ai-sandbox
```

### Core Concepts

- **Engine** — single center of the system. Handles plugin registration, command routing, middleware chain, event broadcasting.
- **Plugin** — self-contained module with `Init(hub)`, `Start()`, `Stop()`. Registers commands & tools through Hub.
- **Hub** — adapter that exposes a limited API from Engine to plugins (function-pointer struct pattern).
- **Middleware** — onion model middleware chain wrapping every command execution.
- **Command** — slash commands like `/help`, `/agent`, `/model`.
- **ReAct Agent** — Think→Act→Observe loop with OpenAI-compatible LLM + tool calling.

### Agent Plugin

The `/agent` command runs a ReAct agent with built-in tools:

- `bash` — execute shell commands
- `read_file` — read files with offset/limit
- `write_file` — write files with auto-mkdir

Command safety (`sandbox`): every `bash` call goes through a structured decision
chain — `Assess` (risk level R0-R7 + side-effect dimensions) → `Policy` matrix →
`allow / hitl / deny`. Unknown commands default to HITL confirmation (Unknown ≠
Safe); regex rules stay deterministic, pre-approval rules (`allow_rules`) only
soften normal-mode confirmations, and strict/readonly modes hard-deny. Injection
vectors (subshell, interpreter `-c`, eval) and newline-joined commands are
flagged. Set `audit_log: true` to append every decision (with the human verdict)
to `audit/audit.jsonl`.

Configure LLM endpoint via `/model`:

```
/model                     ← view config
/model set endpoint=http://localhost:8000/v1
/model set model=gpt-4o
/model set api_key=sk-xxx
/model set context_window=32768   ← model context window (tokens), enables ctx % in status bar
/model set compress_at=0.8        ← auto-compact threshold (0-1), 0 disables
/model set compact_keep=10        ← keep last N messages verbatim when compacting
```

History compaction:
- `/compact` — LLM-rolls up old messages into a summary, keeps the recent tail
- Auto — fires before a model call when estimated usage ≥ `compress_at` × window
- `/history` — inspect the current conversation contents

Skills:
- Drop `SKILL.md` files in `~/.config/goworker/skills/<name>/` (user) or `.goworker/skills/<name>/` (project)
- Each skill registers as a `skill_<name>` tool; the agent loads the instructions on demand (not resident in the system prompt)
- `/skills` — list loaded skills

SKILL.md format (frontmatter `name` is required, `description` guides when the agent loads it):

```markdown
---
name: gofmt
description: 用 gofmt 格式化 Go 代码
---
1. Run `gofmt -w .` on the target package
2. ...
```

MCP servers:
- Add stdio servers under `mcp.servers` in `config.yaml`:
  ```yaml
  mcp:
    servers:
      - name: filesystem
        command: npx
        args: ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
  ```
- Server tools register as `mcp_<server>_<tool>` and are available to the agent
- `/mcp` — check server connection status
- ⚠️ MCP tools run in external processes, **not gated by the local sandbox** — only connect servers you trust


Memory (`ai-memory/`):
- Two-layer model: MTM (task archive, open/closed, cross-session summary) + LTM (distilled facts)
- Every query retrieves top-K relevant memory (open tasks + facts) and injects it into the prompt
- Standalone module with zero external deps — `Retriever` interface leaves room for RAG/embedding backends
- `/memory` manage facts, `/task` manage task archive, `/new` / `/compact` consolidate the session

Config cascades: in-memory → `$LLM_*` env vars → `~/.config/goworker/.env`.

## Getting Started

```bash
cd daemon
go run cmd/goworker/main.go     # go.work resolves the ai-* modules; standalone builds use the replaces in daemon/go.mod
```

## Why goworker?

Built from scratch to understand how REPL agents work under the hood — plugin systems, middleware chains, streaming LLM APIs, ReAct loops. Inspired by coworker (Kotlin), OpenCode, and Claude Code.
