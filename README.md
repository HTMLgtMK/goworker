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
/model                            ← view config
/model list                       ← list providers
/model use <provider>             ← switch default provider
/model set endpoint=http://localhost:8000/v1
/model set model=gpt-4o
/model set api_key=sk-xxx
/model set context_window=32768   ← model context window (tokens), enables ctx % in status bar
/model set global.compress_at=0.8     ← auto-compact threshold (0-1), 0 disables
/model set global.compact_keep=10     ← keep last N messages verbatim when compacting
/model set global.max_iterations=20   ← ReAct loop iteration cap
/model set global.thinking_show=true  ← stream the model's thinking to the terminal
```

Provider fields: `endpoint`, `model`, `api_key`, `context_window`,
`thinking_request_mode`, `thinking_effort`, `auth_type`, `max_tokens`.
Global fields are prefixed with `global.` — see `/model help`.

History compaction:
- `/compact` — LLM-rolls up old messages into a summary, keeps the recent tail
- Auto — fires before a model call when estimated usage ≥ `compress_at` × window
- `/history` — inspect the current conversation contents
- `/rewind` — list checkpoints, or `/rewind <n>` to restore that conversation view
- `/usage` — token usage breakdown for the current session
- `/new` — end the session: consolidate memory → clear STM → inject reminders next run

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

## Configuration

Everything lives in a single `~/.config/goworker/config.yaml`, written atomically
(tmp + rename) by `/config` and `/model set`. Set `GOWORKER_CONFIG_DIR` to
relocate the whole directory — skills, logs and audit logs derive from it.

```yaml
llm:
  default_provider: openai
  compress_at: 0.8
  providers:
    openai:
      type: openai
      endpoint: https://api.openai.com/v1
      model: gpt-4o
      api_key: sk-xxx
      context_window: 128000
      thinking:
        request_mode: auto    # auto | enable_thinking | reasoning_effort — required
        effort: medium        # low | medium | high
```

⚠️ A provider entry you define in `config.yaml` **replaces** the built-in
default wholesale — defaults are not merged in, so you have to spell out every
required field yourself. `thinking.request_mode` is one of them: leave it out
and the whole config is rejected at startup (the process exits with
`配置无效`, logging the reason above it). Easiest route is to let `/model set`
write the file for you.

(This sample is pinned by a test — `TestReadmeSampleConfigParses` — so it can't
drift away from what the loader actually accepts.)

## Getting Started

```bash
cd daemon
go run cmd/goworker/main.go     # go.work resolves the ai-* modules; standalone builds use the replaces in daemon/go.mod
```

Build with the version stamped in:

```bash
cd daemon
go build -ldflags "-X github.com/tinguo/goworker/daemon/internal/version.version=v0.1.0" \
  -o goworker ./cmd/goworker
./goworker -version
```

`commit` / commit date / dirty-worktree flag come from Go's own build metadata
(`-buildvcs`, on by default) — no ldflags needed, and they work for local
`go run` builds too. Only the semver string has to be injected.

### CMake orchestration (optional)

CMake drives the same `go build` for local development — version stamping,
common targets, cross-compilation, multi-instance bootstrap:

```bash
cmake -B build && cmake --build build      # -> build/bin/goworker (version via git describe)
cmake --build build --target test          # go test across the workspace (module list read from go.work)
cmake --build build --target vet
cmake -B build -DGOOS=linux -DGOARCH=amd64 && cmake --build build   # cross-compile
cmake --build build --target instance      # bootstrap an isolated instance dir
```

**Plugin selection at build time**: `GOWORKER_PLUGINS` lists the plugins compiled
into the daemon (default `agent`). Plugins left out of the list are removed at
compile time via `goworker_no_<name>` build tags:

```bash
cmake -B build -DGOWORKER_PLUGINS=""       # minimal shell: engine + builtin commands, no plugins
cmake -B build -DGOWORKER_PLUGINS=agent    # default, full build
```

The wiring lives in `daemon/cmd/goworker/plugins.go` (one `plugin_<name>.go` /
`plugin_<name>_off.go` pair per plugin). A bare `go build` without tags — what
CI and release use — always compiles every plugin in. CMake cache is sticky:
after changing `GOWORKER_PLUGINS`, a plain `cmake -B build` won't revert to the
default; pass `-D` explicitly or delete `build/`.

## CI & Release

| Workflow | Trigger | Does |
|----------|---------|------|
| `ci.yml` | push / PR to `master` | gofmt, `go vet`, `go test -race`, build — module list read live from `go.work` |
| `release.yml` | push to `master` | auto-bump patch tag → **manual approval gate** → build linux/darwin × amd64/arm64 → publish release + checksums |
| `pr-review.yml` | PR opened / updated | fetch diff → LLM review → one review comment with a 修改点 / 是否准入 breakdown plus inline comments on the changed lines; findings that can't be anchored collapse at the bottom |

Release needs a `release` environment with required reviewers configured (the
approval gate), plus a `LLM_API_KEY` secret for PR review. `LLM_BASE_URL` and
`LLM_MODEL` are optional repo variables — without them the review falls back to
`https://api.openai.com/v1` and `gpt-4o-mini`.

Windows binaries are not published: the bash tool's process-group handling
(`Setpgid` / `kill(-pid)`) is Unix-only, so `GOOS=windows` doesn't compile yet.

## Why goworker?

Built from scratch to understand how REPL agents work under the hood — plugin systems, middleware chains, streaming LLM APIs, ReAct loops. Inspired by coworker (Kotlin), OpenCode, and Claude Code.
