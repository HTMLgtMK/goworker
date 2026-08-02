# goworker

A modular REPL agent terminal in Go — plugin-based, middleware-driven, LLM-ready.

## Architecture

```
cmd/goworker/main.go        ← entry point
internal/
├── spec/                   ← interfaces & types (Hub, Command, Plugin, Context)
├── core/                   ← Engine: middleware chain, plugin lifecycle, command routing
├── plugins/
│   └── agent/              ← ReAct agent plugin (OpenAI-compatible LLM, tool calling)
└── frontend/
    ├── stdin/              ← stdin REPL frontend
    ├── tui/                ← TUI frontend (placeholder)
    └── web/                ← web frontend (placeholder)
docs/architecture.md        ← detailed architecture doc
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


Config cascades: in-memory → `$LLM_*` env vars → `~/.config/goworker/.env`.

## Getting Started

```bash
cd daemon
go run cmd/goworker/main.go
```

## Why goworker?

Built from scratch to understand how REPL agents work under the hood — plugin systems, middleware chains, streaming LLM APIs, ReAct loops. Inspired by coworker (Kotlin), OpenCode, and Claude Code.
