# goworker

A modular REPL agent terminal in Go — plugin-based, middleware-driven, LLM-ready.

## Architecture

Six Go modules in one workspace (`go.work`), layered as an SDK. `ai-core` / `ai-sandbox` / `ai-memory` / `ai-dispatch` are standalone, independently-releasable; `ai-runtime` aggregates core+memory+sandbox into an out-of-the-box agent for external projects; `daemon/` is the agent backend consuming `ai-runtime` — one shared assembly (`internal/app`) behind per-frontend shells (CLI `cmd/goworker`, Android `mobile/` via gobind) and the multi-agent dispatcher host (`internal/plugins/dispatcher`).

```
ai-memory/                     ← standalone: MTM task archive + LTM facts (mem0-like), zero deps
ai-sandbox/                    ← standalone: command safety layer (risk benign-reserved → policy → allow/hitl/deny), stdlib only
ai-core/                       ← pure engine kernel: spec protocol + ReAct engine + core types + engine middlewares
│  ├── spec/                   ← Hub, Command, Plugin, Context (pure protocol)
│  ├── core/                   ← Tool, Message(+Thinking/Custom), Usage, Compressor
│  ├── config/                 ← LLMConfig/MemoryConfig + validators (engine needs)
│  ├── agent/                  ← ReAct loop (zero sandbox/memory deps; emits thinking tokens)
│  └── middlewares/            ← usage/iteration/compression/memory (MemoryClient as local interface)
ai-dispatch/                   ← standalone: ACP (Agent Client Protocol) dual-role + commit-dispatcher core, stdlib only
│  ├── protocol/               ← JSON-RPC 2.0 conn + ACP REV_1 types/methods (initialize/session.*/permissions)
│  ├── task/                   ← Task state machine (code/general) + JSONL snapshot store + event contract
│  └── dispatch                ← Client (drive worker subprocess) / Server (accept task submissions)
ai-runtime/                    ← aggregation: out-of-the-box agent for external projects
│  ├── config/                 ← aggregated Config{LLM,Memory,Sandbox,Session,MCP,Dispatch} + Paths + event contract
│  ├── provider/               ← LLM protocol adapters: BaseProvider + openai/ + anthropic/
│  ├── agent/                  ← agent SDK: Session + SessionDeps + RunRequest/RunCallbacks + DefaultTools
│  ├── middlewares/            ← HITL middleware (sandbox decision gating)
│  ├── session/                ← conversation store (checkpoint/rewind)
│  ├── mcp/ skills/ logger/    ← moved from daemon, reusable
daemon/                        ← agent backend: engine + plugins + shell per frontend + dispatcher host
│  ├── cmd/goworker/           ← CLI 壳：daemon/tui 运行模式 + acp/status/web/service 子命令 + 信号处理
│  ├── mobile/                 ← Android Agent-only 壳（gobind 入口，ACP Host）
│  ├── internal/app/           ← 装配层：Application（配置→日志→Engine→RegisterPlugins→StartAll）+ config/
│  ├── internal/core/          ← engine + model（通用 Plugin/Capability 协议）
│  ├── internal/plugins/agent/ ← Agent 内置插件（可由 Build Tag 裁剪）
│  ├── internal/plugins/dispatcher/ ← Dispatcher 内置插件（可由 Build Tag 裁剪）
│  └── internal/cli/           ← stdin REPL + statusbar (subscribes ai-runtime events); external editors connect via the app ACP service (frontend/acp.sock)
docs/architecture.md           ← detailed architecture doc
docs/dispatcher.md             ← dispatcher design (ACP dual-role, worktree isolation, HITL merge)
docs/vscode-acp-integration.md ← VS Code 扩展接入对接（frontend/acp.sock、能力协商）
daemon/mobile/PROTOCOL.md      ← ACP 客户端协议契约（能力协商、session 面、x-device 设备能力扩展）
```

```
daemon ──→ ai-runtime ──→ ai-core ──→ (zero goworker deps)
      │         ├──→ ai-memory
      │         └──→ ai-sandbox
      └──→ ai-dispatch ──→ (stdlib only)
```

### Core Concepts

- **Engine** — single center of the system. Handles plugin registration, command routing, middleware chain, event broadcasting.
- **Plugin** — self-contained module with `Init(hub)`, `Start()`, `Stop()`. Registers commands & tools through Hub.
- **Hub** — adapter that exposes a limited API from Engine to plugins (function-pointer struct pattern).
- **Middleware** — onion model middleware chain wrapping every command execution.
- **Command** — slash commands like `/help`, `/agent`, `/model`.
- **ReAct Agent** — Think→Act→Observe loop with multi-provider LLM (OpenAI-compatible / Anthropic Messages) + tool calling; thinking tokens stream end-to-end.
- **Dispatcher** — commit-dispatching center as a plugin: accepts tasks (REPL `/dispatch` or external ACP clients), drives worker agents (Claude Code / Codex / ZCode) over ACP in isolated git worktrees, gates every merge behind human review.
- **ACP dual role** — goworker speaks the Agent Client Protocol on both sides: as Client it spawns worker agents; as Agent it serves the main conversation to external editors over `<config-dir>/frontend/acp.sock` (same service Android connects to in-process), accepts task submissions (`<config-dir>/dispatch/acp.sock`), and exposes itself as a worker via `goworker acp`. Client-facing protocol contract: [`daemon/mobile/PROTOCOL.md`](daemon/mobile/PROTOCOL.md).

### Agent Plugin

The `/agent` command runs a ReAct agent with built-in tools:

- `bash` — execute shell commands
- `read_file` — read files with offset/limit
- `write_file` — write files with auto-mkdir

Command safety (`sandbox`): every `bash` call goes through a structured decision
chain — `Assess` (risk level benign-reserved + side-effect dimensions) → `Policy` matrix →
`allow / hitl / deny`. Unknown commands default to HITL confirmation (Unknown ≠
Safe); regex rules stay deterministic, pre-approval rules (`allow_rules`) only
soften normal-mode confirmations, and strict/readonly modes hard-deny. Injection
vectors (subshell, interpreter `-c`, eval) and newline-joined commands are
flagged. Set `audit_log: true` to append every decision (with the human verdict)
to `audit/audit.jsonl`.

Multiple LLM providers are supported per config (`llm.providers.<name>.type`: `openai` or `anthropic`, with `auth_type`/`max_tokens`/thinking options for Anthropic); `/model` edits the default one.

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
- MCP bridges attach `risk_level=unclassified`, `requires_permission=true`, and a stable `allow_always_key` by default when the server provides no trusted classification
- normal/auto/strict/readonly sandbox modes apply to MCP tools; `auto_max_risk` controls automatic execution in auto mode
- Bash command rules remain compatible with the `001-command-decision-engine`; external-tool approval uses independent `tool_allow_rules`
- Full policy contract: `specs/005-unified-tool-risk-policy/`
- `/mcp` — check server connection status


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
  current: {provider: openai, model: gpt-4o}
  compress_at: 0.8
  providers:
    openai:
      type: openai
      endpoint: https://api.openai.com/v1
      api_key: sk-xxx
      models:
        gpt-4o:
          context_window: 128000
          thinking:
            request_mode: auto    # auto | enable_thinking | reasoning_effort
            effort: medium        # low | medium | high
```

When `providers` is present it replaces the built-in registry. OpenAI model
context window and thinking settings have defaults; Anthropic models require
`context_window` and `max_tokens`. Older single-model LLM formats are rejected;
use `current` and `models` as shown above. Existing installations must manually
rewrite legacy settings before upgrading; see the [LLM configuration migration
guide](daemon/docs/config-migration.md) for field mappings, credential handling,
verification, and rollback. No automatic migration or compatibility parsing is
provided.

(This sample is pinned by a test — `TestReadmeSampleConfigParses` — so it can't
drift away from what the loader actually accepts.)
Dispatcher (`docs/dispatcher.md`, `dispatch.enabled: true` in config.yaml):
- `/dispatch <prompt>` — queue a task: git repo → code task (mandatory worktree isolation, commits collected via `BaseCommit..HEAD`); otherwise general task (no git needed)
- `/dispatch @claude <prompt>` — target a specific worker; `ls/show/approve/complete/reject/cancel` manage the lifecycle
- Workers are ACP subprocesses (`dispatch.workers`): Claude Code via `claude-agent-acp`, Codex via `codex-acp`, ZCode via `goworker acp`
- Every merge waits for human review (`approve` = ff-only merge attempt; conflicts → manual merge + `complete`); decisions land in `audit/dispatch.jsonl`
- External ACP clients submit tasks to `<config-dir>/dispatch/acp.sock`; progress streams back as `session/update`
- Status bar shows live task state (`dispatch 2 run !1 review · summary`)

Config: YAML at `~/.config/goworker/config.yaml` (`GOWORKER_CONFIG_DIR` relocates the whole runtime dir — one dir per instance when running multiple goworkers).

## Getting Started

```bash
cmake -B build && cmake --build build   # -> build/bin/goworker（版本号经 git describe 注入）
./build/bin/goworker                    # daemon 模式（默认）：service 形态，banner 后安静运行
./build/bin/goworker --tui              # 交互式终端 REPL（原默认行为）
./build/bin/goworker status             # 查看运行中 daemon 的状态（--json 输出原始快照）
./build/bin/goworker web                # 托管启动 web 网关（Node，见下）
./build/bin/goworker service install    # 注册 macOS LaunchAgent 服务（见下）
./build/bin/goworker acp                # ACP worker 模式（被 dispatcher/编辑器驱动）
./build/bin/goworker -version           # 版本信息

cmake --build build --target test       # 全量测试
cmake --build build --target vet
cmake --build build --target instance   # 生成本地隔离子实例目录（多运行时）

# 交叉编译
cmake -B build -DGOOS=linux -DGOARCH=amd64 && cmake --build build

# 无 CMake 时：go run（workspace 内，开发用；临时二进制不能注册 service）
cd daemon && go run cmd/goworker/main.go
```

## Usage

| 命令 | 说明 |
|------|------|
| `goworker`（= `--daemon`） | daemon 模式（默认）：service 形态跑后台服务，banner 后安静运行 |
| `goworker --tui` | 交互式终端 REPL（原默认行为） |
| `goworker status [--json]` | 查看运行中 daemon 的状态 |
| `goworker web [flags]` | 托管启动 web 网关（Node） |
| `goworker service <action>` | 注册/管理 macOS LaunchAgent 常驻服务 |
| `goworker acp` | ACP worker 模式（被 dispatcher/编辑器经 stdio 驱动） |
| `goworker -version` | 版本信息 |

### 运行模式

daemon 模式只装配后台服务（frontend ACP、dispatcher ingress、status socket），启动时
stdout 打印一段 banner（版本 / 各 socket 端点 / config / 日志路径）后保持安静，等
SIGINT/SIGTERM 收尾；详细运行日志按配置走 stderr 与日志文件。`--tui` 额外起 stdin REPL；
`--daemon` 与 `--tui` 同时给出直接报错。

### status

`goworker status` 拨号 `<config-dir>/status.sock` 读取运行快照：版本、PID、uptime、模式
（daemon/tui）、config/日志路径，并对 frontend/dispatch socket 做 100ms 拨号探活；
`--json` 输出原始快照。daemon 未运行 / socket 残留时给出对应提示（残留文件下次启动自动清理）。

### service（macOS launchd 常驻）

```bash
goworker service install [--web]                       # 注册 daemon（--web 连网关）为 LaunchAgent
goworker service start|stop|restart [daemon|web|all]   # 生命周期（默认目标 daemon）
goworker service status                                # agent 状态 + daemon 运行快照
goworker service uninstall                             # 卸载全部
```

- plist：`~/Library/LaunchAgents/com.goworker.{daemon,web}.plist`；`RunAtLoad` 登录自启 +
  `KeepAlive` 崩溃自动拉起（ThrottleInterval 10s）；日志在 `~/Library/Logs/goworker/`
- launchd 不继承 shell env：install 时把当前 `PATH` 与 `GOWORKER_*`（如
  `GOWORKER_CONFIG_DIR`、`GOWORKER_WEB_PORT`）烘焙进 plist —— 先设好环境变量再 install
- web 服务的 ProgramArguments 是 `[goworker, web]`（入口解析逻辑单源），不是直接跑 node
- 已在跑但不受 launchd 托管的 daemon / 端口已被占用会拒绝 install（防 socket 冲突崩溃
  循环）；`go run` 的临时二进制拒装，先构建到固定路径

典型部署：

```bash
cmake -B build && cmake --build build
./build/bin/goworker service install --web    # daemon + 网关双双常驻
./build/bin/goworker service status           # 验证
```

### web（Node 网关托管）

```bash
goworker web [--entry <gateway.js>] [--port N] [--host H] [--workspace DIR]
```

入口解析优先级：flag > `GOWORKER_WEB_ENTRY` > config `frontend.web.entry` > 自动探测
`~/Work/*/apps/web-host/dist/gateway.js`（`src/gateway.ts` 兜底，npx tsx 运行）。workspace
未显式指定时从 `apps/web-host` 布局派生仓库根注入 `GOWORKER_WEB_WORKSPACE`（托管启动时
网关 cwd 不可信）；端口默认 5177、host 默认 127.0.0.1。子进程跑在独立进程组，轮询
`/api/health` 报就绪，随信号收停整组不留孤儿 node；daemon 未运行只提示不阻塞（网关自带
socket 重连），端口已被占用时拒绝启动。

config.yaml 对应段（均可省略走默认 / 自动探测）：

```yaml
frontend:
  web:
    entry: ""          # 网关入口路径；空 = 自动探测
    port: 5177
    host: 127.0.0.1
    workspace: ""      # 空 = 从 entry 派生（apps/web-host 布局命中时）
```

多运行时：`GOWORKER_CONFIG_DIR=<dir>` 隔离每个实例的 config/sessions/memory/dispatch 与 acp.sock，
dispatcher 在 `<dir>/dispatch/acp.sock` 接受外部 ACP 任务提交（见 docs/dispatcher.md）。

Build with the version stamped in:

```bash
cd daemon
go build -ldflags "-X github.com/tinguo/goworker/daemon/internal/version.version=v0.1.5" \
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

**Plugin selection at build time**: plugins are compiled in by default — the
same as a bare `go build`, which is what CI and release use. `GOWORKER_NO_PLUGINS`
lists the plugins to leave out; each name becomes a `goworker_no_<name>` build
tag that removes it at compile time:

```bash
cmake -B build                                        # default: every plugin compiled in
cmake --build build --target goworker-full             # full explicit artifact
cmake --build build --target goworker-agent            # Agent-only artifact
cmake --build build --target goworker-dispatcher       # Dispatcher-only artifact
cmake --build build --target goworker-minimal          # no-plugin artifact
cmake -B build -DGOWORKER_NO_PLUGINS=agent            # legacy goworker target: no agent plugin
cmake -B build -DGOWORKER_NO_PLUGINS=dispatcher       # legacy goworker target: no dispatcher plugin
```

The wiring lives in `daemon/internal/builtin` and `daemon/internal/pluginregistry`: each
compiled-in plugin self-registers its descriptor, while the Application constructs
factories in deterministic dependency order. CMake's legacy `GOWORKER_NO_PLUGINS`
option remains supported. CMake's cache is sticky: after changing `GOWORKER_NO_PLUGINS`,
pass `-D` explicitly or delete `build/` when switching back to the default.

Android Agent-only AAR:

```bash
daemon/mobile/build-aar.sh                         # -> build/goworker-agent.aar
GOWORKER_AAR=/tmp/goworker-agent.aar daemon/mobile/build-aar.sh
```

The script passes `goworker_no_dispatcher`, verifies that the Agent implementation is
in the mobile dependency graph, and rejects Dispatcher-linked builds. Android-side
file, permission, MCP/Tool, and Activity lifecycle adapters use the ACP client-ward
contract described in [`daemon/mobile/PROTOCOL.md`](daemon/mobile/PROTOCOL.md).

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
