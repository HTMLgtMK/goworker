# P0+P1 代码级实施 Plan:多 Session 执行内核 + 多 Session 持久化

> 依据 `docs/multi-session-runtime-plan.md`(总方案)第十节改造清单 P0/P1、第十一节验收测试、第十二节开发顺序,以及 2026-10-26 对分支 `feature/multi-session-p0p1`(commit 7ff1e6b)的代码勘察。
> 勘察发现与总方案不符之处已按实际代码修正,全部标注「勘察修正」。

## 0. 勘察事实基线

### 0.1 modules(go.work 全部模块)

```
go.work(go 1.27.1, use 7 模块):
  ai-acp      → github.com/tinguo/goworker/ai-acp     (go 1.27.1)
  ai-core     → github.com/tinguo/goworker/ai-core    (go 1.26.1)
  ai-dispatch → github.com/tinguo/goworker/ai-dispatch(go 1.26.1)
  ai-memory   → github.com/tinguo/goworker/ai-memory  (go 1.26.1)
  ai-runtime  → github.com/tinguo/goworker/ai-runtime (go 1.26.1)
  ai-sandbox  → github.com/tinguo/goworker/ai-sandbox (go 1.26.1)
  daemon      → github.com/tinguo/goworker/daemon     (go 1.26.1)
```
CI 以 go.work 为版本唯一来源(`.github/workflows/ci.yml:27` `go-version-file: go.work`)。

### 0.2 buildTags

daemon 四个 CI 变体(`.github/workflows/ci.yml:82-113`):`full`(无 tag)/ `agent`(`goworker_no_dispatcher`)/ `dispatcher`(`goworker_no_agent`)/ `minimal`(两者)。源码 tag 面:`plugins_<name>.go` 与 `plugins_<name>_off.go` 成对(`goworker_no_<name>`,`daemon/internal/app/plugins.go:4-5`)。
mobile:`daemon/mobile/build-aar.sh:8,38` → `gomobile bind -target=android -tags=goworker_no_dispatcher -o build/goworker-agent.aar ./mobile`;`GOWORKER_MOBILE_GO_TAGS` 可覆盖但脚本强制「必须含 plugins/agent、不得含 plugins/dispatcher」(build-aar.sh:30-37);`daemon/mobile/build_tags_test.go:1` 要求 `goworker_no_dispatcher && !goworker_no_agent`。

### 0.3 testCommands(CI 实际命令)

1. `gofmt -l .` 必须为空(ci.yml:47-54);
2. 对 7 个模块逐个执行 `(cd $m && go vet ./...)` / `(cd $m && go test -race ./...)` / `(cd $m && go build ./...)`(ci.yml:56-80);
3. daemon 变体矩阵:每个变体 `(cd daemon && go test -race -tags $tags ./...)` + `(cd daemon && go build -tags $tags -o /tmp/goworker-$variant ./cmd/goworker)`,dispatcher/minimal 变体再 `go list -deps` 断言不含 `daemon/internal/plugins/agent`(ci.yml:99-110);
4. mobile 验证命令(等价 CI agent 变体):`cd daemon && go test -race -tags goworker_no_dispatcher ./...` 与 `go test -race -tags goworker_no_dispatcher ./mobile`。

本文各 step 的验收命令统一用 `go -C <模块目录>` 形式(等价 CI 的 `cd`,go1.27.1 已验证:`go vet -C ai-runtime ./session/` 与 `go test -C daemon -race -tags goworker_no_dispatcher ./mobile/ -count=1` 本次实际运行通过,后者输出 `ok github.com/tinguo/goworker/daemon/mobile 3.344s`)。

### 0.4 本 Plan 依赖的勘察结论(与总方案的差异)

| # | 勘察结论 | 对本 Plan 的影响 |
|---|---|---|
| A | **runGate 并非只 gate handleAgent**:handleAgent/handleNew/handleUsage/handleCompact/handleHistory/handleTask/handleRewind 共 7 个 handler 全部经 `withSession`(`daemon/internal/plugins/agent/commands.go:40-49`)排队获取 runGate;死锁靠 `sessionGateKey` ctx 可重入标记(commands.go:38-49)规避,契约测试 `gate_test.go:18,41,81,112` 已固化。plugin.go:43-46 的「只 gate handleAgent」注释过时。 | P0-2 移除 runGate 时必须保留 ctx 重入语义;P0-2 顺带修正注释 |
| B | 存在总方案未提及的**第二把门禁** `SessionGate`(`daemon/internal/core/service/acp/coordinator.go:21-52`),FrontendHandler.Prompt 持有它从 superseded 检查贯穿到 evaluate 结束(handler.go:184-190),与 session/new、load-current 互斥(handler.go:115,260)。 | P0/P1 **不动** SessionGate(单活动 TOCTOU 守卫仍必要);多前端接入是 P5 |
| C | acp worker 已是 `map[transportSessionID]*Session`(`daemon/internal/core/acpworker/acp_worker.go:39,90`),但 runMu 全局串行(168)且 `Store: nil`(97)。 | P1 每会话独立 Store 明确**不含** stdio worker(worker 生命周期归调用方管理,见 acp_worker.go:97 注释);范围裁剪写进 P1-3 |
| D | `Session.Run` 恒返回 nil;Commit/Compact/Checkpoint 失败全部 slog.Warn 后继续(`ai-runtime/agent/session.go:169-190`),provider 错误也吞掉返 nil(122-128,147-153)。 | P0-3 是本 Plan 唯一改 `Session.Run` 签名的 step |
| E | `Store.Open` 固定打开 `dir/current.jsonl`(`ai-runtime/session/store.go:62`),无 per-id 布局/List/Delete/文件锁;`/new` = `Archive()` 轮转 + `startSession` 重建(`commands.go:118-127`、`plugin.go:299-359`)。 | P1-1 的 Repository 兼容挂载 legacy 布局为硬性要求 |
| F | 默认 `cfg.Session.Dir = <DefaultDir()>/sessions`(`daemon/internal/app/config/config.go:190-192`);`session.enabled` 关闭时 store 为 nil 走纯内存(plugin.go:301-309, spec.go:68)。 | Repository root 即 `cfg.Session.Dir`,nil Store 纯内存路径必须保留 |
| G | 工具收集签名无 ctx:`SessionDeps.CollectTools func(*sandbox.Config)`(spec.go:65)在 `session.go:104` 调用;连接级设备工具走另一入口 `req.ToolSource(ctx)`(session.go:107-109)。 | 本 Plan 不改这两个签名(属 P4 Tool Proxy 范围) |
| H | ai-core 把 ctx 传进工具 Execute:`toolCtx = WithTimeout(ctx, 60s)`(`ai-core/agent/agent.go:147-149`),sessionGateKey 值由此流入嵌套 `hub.Eval`(`tools.go:36`)。 | P0-2 的重入标记继续依赖此链路 |
| I | 测试桩已存在:`captureProvider`(`ai-runtime/agent/test_helpers_test.go:11-28`)非流式返回固定 assistant 消息,`ChatStream` 返回错误(agent 据此回退非流式,`ai-core/agent/agent.go:183-187`)。 | TestConcurrentSessions / 失败注入直接复用 |
| J | CI 只在 master 触发(ci.yml:4-7);仓库根 `.claude/worktrees/agent-afeac443f18b2f382/...` 有未格式化文件会被 `gofmt -l .` 卡住。 | 提交首个 PR 前清理(见 §5 风险) |

---

## 1. 目标形态(P0+P1 完成后)

```
ai-runtime/agent/
  session.go        # 保留;Run 签名改 (RunResult, error)(P0-3)
  manager.go        # 新增 P0-1:SessionManager
  runtime.go        # 新增 P0-1:SessionRuntime(每会话独占 gate)
  run.go            # 新增 P0-1:AgentRun / RunState / RunResult
ai-runtime/session/
  store.go          # 保留;P1-2 加文件锁 + 尾记录截断修复
  repository.go     # 新增 P1-1:Create/Open/List/Delete + 实例唯一缓存
daemon/internal/plugins/agent/
  plugin.go         # runGate/store/session atomic 三个字段退役,改持 manager+repo(P0-2/P0-4/P1-3)
  commands.go       # withSession 委托 SessionRuntime;handleAgent 记账 Run
```

**用户可见行为在 P0+P1 全程不变**(总方案第十二节:「保持现有单 Session CLI/ACP 路径可用」):

- CLI 的 `/agent` `/new` `/compact` `/usage` `/history` `/rewind` `/task` 全部作用于「当前会话」;
- ACP 的单活动会话语义(superseded / busy,PROTOCOL.md:40-47)、SessionGate、`activeTransportSession` 守卫(handler.go:192-199)原样保留;
- 多会话并行的能力在 ai-runtime 的 manager/runtime 层达成并由测试证明;前端按 SessionID 路由是 P5(`x-goworker/*` 协议)的事。

### 1.1 CLI 命令按 SessionID 路由与兼容适配器

**路由规则(P0-4 落地,本 Plan 全程有效):**

1. `AgentPlugin` 持有 `manager *runtimeagent.SessionManager` 与 `current atomic.Pointer[runtimeagent.SessionRuntime]` 两个解析点;
2. 所有命令 handler(`/agent` `/new` `/compact` `/usage` `/history` `/rewind` `/task`,注册点 plugin.go:100-168)解析会话的方式统一为 `p.currentSession() *runtimeagent.SessionRuntime`(内部 `p.current.Load()`,nil 时报「no active session」)——这就是「按 SessionID 路由」的兼容适配器:显式 SessionID 到来时(P5),每个 handler 只需把 `p.currentSession()` 换成 `p.manager.Lookup(sid)`,命令签名与互斥结构不变;
3. `model.Command` / `model.Context` 不加 SessionID 字段(`model.Context.Values map[string]any` 已存在,`daemon/internal/core/model/context.go:21`,P5 若需要可用它携带,本 Plan 不动);
4. **嵌套命令**(agent 工具执行期间 `hub.Eval` 跑 `/new` 等,tools.go:36)继承 `sessionGateKey` 重入标记(commands.go:38-49),作用于发起时刻的 current;嵌套 `/new` 更换 current 的行为与现状一致(commands.go:127 在闭包内 `startSession`),不改;
5. `/new` 的兼容语义(commands.go:93-138)逐步保留:pending 快照 → `Archive()`(轮转归档)→ `startSession()` 以 **Repository.Create 生成新会话**(P1-3 起)→ `CheckpointSync` 固化旧会话。旧 CLI 看到的仍是「结束当前会话、开新会话」,只是新会话从轮转产生变为独立目录产生。

**旧 CLI 单会话行为保持清单**(每个 step 的回归底线):

| 命令 | 保持的行为 | 现状代码 |
|---|---|---|
| `/agent`(及自然语言 fallback) | 经 withSession 排队执行;流式输出;HITL 同步决策 | plugin.go:100-105,184;commands.go:57-70 |
| `/new` | 归档旧会话 → 开新会话 → 固化旧 pending | commands.go:93-138 |
| `/compact` | 压缩当前会话(store 路径) | commands.go:386-391;session.go:378-444 |
| `/usage` `/history` | 读当前会话快照 | commands.go:320-382,409-424 |
| `/rewind` | 当前会话 store head 回移 + reload | rewind.go:29-120 |
| 纯内存模式 | `session.enabled=false` 时全链路 store=nil | plugin.go:301-309;session.go:162-167 |

---

## 2. P0 steps

### P0-1 ai-runtime:SessionManager / SessionRuntime / AgentRun 骨架(纯增量)

**目标**:新增多会话执行内核的三个类型与每会话独占执行权,不接线任何现有代码,零回归风险。

**新增文件**(全部在 `ai-runtime/agent/`):

1. `run.go`:
   - `type RunState string`;常量 `RunQueued/RunRunning/RunCompleted/RunFailed/RunCancelled`(对齐总方案第三节枚举;`RunWaiting` 属 P4 ApprovalManager,本步不定义);
   - `type RunResult struct { State RunState; Err error; Persisted bool }`(`Persisted` 语义见 P0-3);
   - `type AgentRun struct { ID, SessionID string; mu sync.RWMutex; state RunState; startedAt time.Time; done chan struct{}; result RunResult }`;内部方法 `transition(RunState)` 与 `snapshot() RunResult`。
2. `runtime.go`:
   - `type SessionRuntime struct { ID string; mu sync.Mutex; session *Session; gate chan struct{}; active *AgentRun }`;
   - `func NewSessionRuntime(id string, s *Session) *SessionRuntime`(`s` 允许 nil,由 `BindSession(*Session)` 后置绑定——P0-2 的 daemon 在 startSession 前就要有 gate);
   - `func (r *SessionRuntime) AcquireRun(ctx context.Context) (release func(), err error)`:容量 1 channel + `select ctx.Done()`(形态对齐 SessionGate,coordinator.go:29-36,**锁不可被卡死客户端攥住**的同一论证);`release` 必须恰好调用一次;
   - `func (r *SessionRuntime) Session() *Session` / `BindSession(*Session)` / `Active() *AgentRun`。
3. `manager.go`:
   - `type SessionManager struct { mu sync.RWMutex; runtimes map[string]*SessionRuntime }`;
   - `NewSessionManager()` / `Register(*SessionRuntime) error`(id 冲突报错)/ `Get(id)` / `Remove(id)` / `List() []*SessionRuntime` / `Close()`;
   - 注释明确:全局并发配额(`runSlots`)属 P2「有界全局并发调度」,本结构预留 `Register/Remove` 即可,**不在 P0 实现**。
4. `manager_test.go` + `runtime_test.go`:
   - `TestSessionRuntimeExclusiveRun`:A 持 gate 时 B `AcquireRun` 阻塞,ctx 取消后 B 返回 `ctx.Err()`,A release 后 B 成功(对应现有 `gate_test.go:112` 的语义,搬到 SDK 层);
   - `TestManagerIndependentSessions`:两个 runtime 同时持有各自 gate 互不阻塞(TestConcurrentSessions 的 manager 层雏形)。

**涉及现有函数/字段**:无(纯增量)。

**验收命令**:
```bash
go vet -C ai-runtime ./agent/...
go test -C ai-runtime -race ./agent/... -count=1
go build -C ai-runtime ./...
```

**依赖**:无(起点)。

### P0-2 daemon:runGate 退役,每会话独占权移交 SessionRuntime(行为等价重构)

**目标**:完成总方案 P0 第 2、3 条——移除全局 runGate 的串行执行职责,把互斥点挪到「每 Session 一个 gate」的 SessionRuntime 上。本步只有单个 runtime,外部行为与现状逐一等价。

**涉及现有函数/字段与改动**:

| 位置 | 改动 |
|---|---|
| `plugin.go:47` | 字段 `runGate chan struct{}` **移除**,替换为 `runSession *runtimeagent.SessionRuntime`(注释:当前会话执行独占权持有者;P0-4 起由 manager 注册) |
| `plugin.go:79-81` | `NewPlugin` 不再 `make(chan struct{}, 1)`,改 `runSession: runtimeagent.NewSessionRuntime("current", nil)` |
| `plugin.go:43-46` | 注释重写(勘察修正 A:gate 由 7 个 handler 经 withSession 共用,重入靠 ctx 标记) |
| `commands.go:25-36` | `acquireRun`/`releaseRun` **移除**(去向:逻辑并入 `SessionRuntime.AcquireRun`,P0-1) |
| `commands.go:40-49` | `withSession` 重写:`sessionGateKey` 重入检查不变;互斥改 `p.runSession.AcquireRun(ctx)` / `defer release()`;`context.WithValue(ctx, sessionGateKey{}, p)` 不变 |
| `daemon/internal/plugins/agent/gate_test.go:81,112` | `TestRunGate_SecondAcquireBlocksUntilRelease` / `TestRunGate_AcquireReturnsContextErrorOnCancel` 改为对 `p.runSession`(SessionRuntime)的等价断言(或直接删除,P0-1 已在 SDK 层覆盖);`:18,41` 的 `TestWithSession_*` 原样保留——它们是重入语义的守卫 |
| 其他 | `/new /compact /usage /history /task /rewind` 的 handler **一行不改**(它们本就经 withSession,勘察修正 A) |

**明确不做**:`SessionGate`(service/acp)不动(勘察修正 B);`Session.Run` 签名不动(P0-3);`/new` 语义不动(P1-3)。

**验收命令**:
```bash
go vet -C daemon ./internal/plugins/agent/...
go test -C daemon -race ./internal/plugins/agent/... ./internal/core/service/acp/... ./internal/core/acpworker/... -count=1
go build -C daemon -tags goworker_no_dispatcher ./cmd/goworker
```

**依赖**:P0-1。

### P0-3 Run 生命周期记账 + 「模型成功 ≠ 持久化成功」

**目标**:完成总方案 P0 第 4 条的 Run 侧(Compact/Rewind 的并发约束已由 P0-2 的统一 gate 覆盖,本步用测试钉死);补上方案第九/十一节点名的硬门槛:**Conversation Commit 失败时不得记录 RunCompleted**。

**签名与类型改动**:

1. `ai-runtime/agent/spec.go:68`:`SessionDeps.Store *session.Store` → `SessionDeps.Store ConversationStore`。新增接口(放 `spec.go`):
   ```go
   // ConversationStore 是 Session.Run/Compact 所需的持久化最小面;*session.Store 天然满足。
   type ConversationStore interface {
       ActiveView() []core.Message
       Commit(msgs []core.Message) ([]core.Message, error)
       Compact(coveredFrom, coveredTo, summary string) error
       Checkpoint(preview string) error
   }
   ```
   (接口成员取自 session.go 实际调用点:ActiveView 104 之外的 383/468,Commit 175/180,Compact 171/422,Checkpoint 185。`Archive/SetHead/AdvanceCursor/PendingAfterCursor` 只被 plugin/rewind 使用,不进接口——daemon 侧继续持具体 `*session.Store`,plugin.go:337 传参零改动。)
2. `ai-runtime/agent/session.go:85`:`func (s *Session) Run(ctx, req, cb) error` → `func (s *Session) Run(ctx, req, cb) (RunResult, error)`:
   - 空输入(87-92):返回 `(RunResult{State: RunCancelled}, nil)`,注释「未进入 ReAct,不创建 AgentRun」;
   - provider 错误(122-128):仍写 `✘` 给 `cb.Write`(行为不变),返回 `(RunResult{State: RunFailed, Err: err, Persisted: false}, nil)`;
   - `agent.Run` 失败(147-153):同上 `RunFailed`;
   - 提交块(159-192):`Store.Compact`(171)/`Store.Commit`(175,180)失败 → `(RunResult{State: RunFailed, Err: 持久化错误, Persisted: false}, nil)`,Checkpoint 失败(185-187)保持 warn 不改 State;全部成功 → `(RunResult{State: RunCompleted, Persisted: true}, nil)`;纯内存模式(162-167)`Persisted = true`(内存 conversation 即事实源);
   - 第二个返回值 `error` 仅保留给 ctx 取消等调用层错误(现状恒 nil),CLI/ACP 错误面不变——用户看到的输出与今天逐字相同。
3. `ai-runtime/agent/runtime.go`(P0-1 文件扩展):
   - `func (r *SessionRuntime) RecordRun(fn func(*Session) (RunResult, error)) RunResult`:**假定调用方已持 gate**(withSession 临界区内调用,避免二次 Acquire 死锁),创建 `AgentRun{state: RunRunning}` 存入 `r.active` → 执行 `fn(r.session)` → 按 result/ctx 终态化 `RunCompleted/RunFailed/RunCancelled` → 清 `r.active` → 返回 result。注释注明:P2 的 RunSupervisor 将提供「独占+记账+宿主 ctx」合一的 `StartRun`,`RecordRun` 是 P0 阶段 daemon 同步调用形态的适配。
4. 调用点修复(编译器指路,共 2 处产品代码):
   - `daemon/internal/plugins/agent/commands.go:63-69` `handleAgent`:`withSession` 闭包内改 `res := p.runSession.RecordRun(func(s *runtimeagent.Session) (runtimeagent.RunResult, error) { return s.Run(runCtx, req, cb) })`,`res` 用于一条 debug 日志(可选);`handleAgent` 返回值仍为闭包返回的 error(现状 nil);
   - `daemon/internal/core/acpworker/acp_worker.go:181`:`if _, err := session.Run(...)` 适配新签名(stdio worker 无 runtime,记账留 P2,见勘察修正 C)。
5. 测试改动:`ai-runtime/agent/session_test.go`、`session_integration_test.go`、`rewind_test.go` 等对 `Run` 的断言适配新签名。

**新增测试**(复用 `captureProvider`,test_helpers_test.go:11-28):

- `TestConcurrentSessions`(总方案第十一节 → 本步):manager 注册 A/B 两个 runtime,各自以独立 `SessionDeps`(fake `ConversationStore` 记账 + captureProvider)并行 `RecordRun`,断言总耗时 ≈ 单轮、两个 fake store 各自只收到自己的提交、互不阻塞;
- `TestSameSessionBusy`(第十一节 → 本步):同一 runtime 第二个 Run 在 gate 上排队,第一个完成后执行;两轮提交串行、历史无覆盖(fake store 断言两次 Commit 内容完整);
- `TestCommitFailureNotCompleted`(第十一节失败注入 → 本步):fake `ConversationStore.Commit` 返回错误 → `RunResult.State == RunFailed && Persisted == false`,绝不出现 RunCompleted;Compact 失败注入同理。

**验收命令**:
```bash
go vet -C ai-runtime ./... && go vet -C daemon ./...
go test -C ai-runtime -race ./agent/... -count=1
go test -C daemon -race ./internal/plugins/agent/... ./internal/core/acpworker/... -count=1
```

**依赖**:P0-2(gate 语义就位);P1 依赖本步的 RunResult。

### P0-4 daemon:AgentPlugin 接线 SessionManager(单 current,多会话内核就绪)

**目标**:plugin 的会话持有从「裸 Session 指针」升级为「manager 注册的 SessionRuntime」,命令解析统一走 §1.1 的 `p.currentSession()`;至此多会话内核(注册/互斥/记账)全部就位,只差持久化(P1)与前端(P5)。

**涉及现有函数/字段与改动**:

| 位置 | 改动 |
|---|---|
| `plugin.go:50` | `deps runtimeagent.SessionDeps` **保留**(仍由 startSession 全量重建,per-session Store 拆分属 P1-3) |
| `plugin.go:56` | `session atomic.Pointer[runtimeagent.Session]` **移除**;去向:`manager *runtimeagent.SessionManager` + `current atomic.Pointer[runtimeagent.SessionRuntime]`(新增字段);Session 实例从 `runtime.Session()` 取 |
| `plugin.go:79-81` | `NewPlugin` 增建 `manager: runtimeagent.NewSessionManager()` |
| `plugin.go:209` | `Stop` 的 `p.session.Load().Consolidate(ctx)` 增加 nil 判空(勘察风险:Init 失败回滚路径下 `p.session.Load()` 可能为 nil → panic) |
| `plugin.go:299-359` `startSession` | 构造 `Session` 后 `rt := runtimeagent.NewSessionRuntime(id, s)` → `p.manager.Register(rt)` → `p.runSession = rt`(过渡字段,P0-4 末尾删除)→ `p.current.Store(rt)`;id 本步用 `"current"`(稳定单例),P1-3 换 store 的真实 SessionID |
| `plugin.go:358,391-393` | `p.session.Load().SetCWD(cwd)` → `p.currentSession().Session().SetCWD(cwd)`(nil 防护) |
| `commands.go:65`(P0-3 后形态)、`323,371,389,412` | `p.session.Load()` → `p.currentSession().Session()` |
| `rewind.go:85,97,106` | 同上替换 |
| `plugin.go:47` | P0-2 引入的过渡字段 `runSession` 删除,`withSession` 改从 `p.currentSession().AcquireRun` 取 gate(解析与互斥同源,杜绝「锁在 A 会话、写在 B 会话」) |

**新增测试**:`daemon/internal/plugins/agent/manager_wiring_test.go`:
- `TestCurrentSessionSwapsOnNew`:模拟 `/new` 后 `current` 指向新 runtime、manager 内旧 runtime 仍可 `Get`(归档固化窗口);
- `TestGateFollowsCurrent`:current 切换后 `withSession` 的互斥落在新 runtime 上。

**验收命令**(本步起跑完整变体矩阵 + mobile,对齐 CI):
```bash
gofmt -l .                     # 必须为空(注意 §5 风险 7 的 worktree 文件)
go test -C daemon -race ./... -count=1
go test -C daemon -race -tags goworker_no_agent ./... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./... -count=1
go test -C daemon -race -tags "goworker_no_agent,goworker_no_dispatcher" ./... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./mobile/ -count=1   # mobile build_tags_test.go
go build -C daemon -tags goworker_no_dispatcher ./cmd/goworker
```

**依赖**:P0-1、P0-2、P0-3。P0 完成线。

---

## 3. P1 steps

### P1-1 ai-runtime/session:SessionRepository(Create/Open/List/Delete)+ 实例唯一

**目标**:完成总方案 P1 第 1、2 条的 SDK 层:每会话独立目录、仓库四操作、同 id 唯一 Store 实例。纯新增,不接线 daemon。

**新增文件** `ai-runtime/session/repository.go`:

```go
// 目录布局:
//   <root>/<sessionID>/current.jsonl        新布局(P1 起创建)
//   <root>/current.jsonl + <root>/archive/  legacy 布局(现有用户数据,只读兼容挂载)
type Repository struct {
    root string        // 即 cfg.Session.Dir(config.go:190 默认 <config>/sessions)
    mu   sync.Mutex
    open map[string]*Store  // 实例唯一缓存:同 id 重复 Open 返回同一 *Store
}

func OpenRepository(root string) (*Repository, error)   // root 不存在则创建(对齐 store.Open 的 MkdirAll,store.go:52)
func (r *Repository) Create(ctx context.Context, opts CreateOptions) (*Store, error)
    // opts.Title/CWD 仅记录于返回值(P1 的 metadata.json 不做,见下「决策」);
    // id = session.NewSessionID()(id.go),确认 <root>/<id>/ 不存在后 Open(该 dir)
func (r *Repository) Open(id string) (*Store, error)
    // 先查缓存;未命中则 <root>/<id>/current.jsonl 存在 → session.Open(<root>/<id>)
    //;否则若 root 自身是 legacy 槽(root/current.jsonl 存在且其 metadata.SessionID == id)
    // → session.Open(root)(只读兼容挂载,Archive 仍可在该槽内轮转)
    //;否则 ErrSessionNotFound
func (r *Repository) List(ctx context.Context) ([]Metadata, error)
    // 扫 <root>/*/current.jsonl 的 metadata(store.go:80-114 机制复用)+ legacy root 槽
    //;归档文件不计入(与 sessions.go:42-64 的现行为对齐,归档清单仍是 plugin 层扫描)
func (r *Repository) Delete(ctx context.Context, id string) error
    // open 缓存中存在 → ErrSessionInUse;存在 <root>/<id> → os.RemoveAll;legacy 槽 → 拒绝(ErrLegacyImmutable)
func (r *Repository) Close() error   // 关闭所有 open 中的 Store(store.Close,store.go:134-148)
```

**关键决策**(写入代码注释,评审点):

1. **不新增 metadata.json**:总方案第九节的 `SessionMetadata{ID,Title,CWD,...}` 中 ID/CreatedAt 已是不可变 metadata 记录(store.go:30-33,108-114),Title 可由现有 `firstUserTitle`(sessions.go:436-461)扫描得出,CWD 属会话运行态(见 P1-4)。P1 拒绝引入第二个身份事实源,目录发现即索引——这比总方案的 metadata.json 更保守,写明理由;
2. **不移动 legacy 文件**:`<root>/current.jsonl` 原地挂载为 legacy 会话,`/new` 在其上仍走 `Archive()` 轮转直到该槽被自然淘汰;新会话一律进 `<root>/<id>/`;
3. **实例唯一**:`Repository.Open` 对同 id 恒返回同一 `*Store`(总方案第九节:「Repository 对同一 SessionID 保证唯一的 Store 实例」),跨进程互斥由 P1-2 的文件锁兜底。

**新增测试** `repository_test.go`:
- `TestRepositoryCreateOpenListDelete` 四操作往返;Create 后 List 含新 id;
- `TestConcurrentStoreOpen`(总方案第十一节 → 本步):并发 `Open(same id)` N 次,断言返回同一 `*Store` 指针且无双重打开文件句柄;
- `TestLegacyRootMount`:预置 legacy 布局(root/current.jsonl + archive/*.jsonl,用 `session.Open`+`Commit`+`Archive` 造)→ Open(legacy id) 成功、List 含它、Delete 拒绝。

**验收命令**:
```bash
go vet -C ai-runtime ./session/...
go test -C ai-runtime -race ./session/... -count=1
```

**依赖**:无(与 P0 并行可行;P1-3 依赖它)。

### P1-2 Store 加固:跨进程文件锁 + 尾记录截断修复

**目标**:完成总方案 P1 第 4 条(store 层):「处理文件锁、重复 Open、关闭与崩溃恢复」。纯 store 层,不接线。

**涉及现有函数/字段与改动**(`ai-runtime/session/store.go`):

| 位置 | 改动 |
|---|---|
| `store.go:36-48` `Store` 结构 | 新增 `lock *os.File`(flock 句柄);`path/dir` 不变 |
| `store.go:51-131` `Open` | MkdirAll 之后追加:`os.OpenFile(<dir>/lock, O_CREATE\|O_RDWR, 0600)` + `syscall.Flock(fd, LOCK_EX\|LOCK_NB)`;失败 → `ErrStoreLocked`(第二写者拒绝,总方案第九节「多进程同时打开同一 Session 时拒绝第二个写者」)。锁文件常驻不解锁删除(避免 unlink 竞态);`//go:build unix` 实现文件 `lock_unix.go` + 非 unix 返回无锁桩(仓库 CI 与目标平台 android/darwin/linux 均为 unix 族) |
| `store.go:708-738` `loadRecords` | 返回值增加「最后一条合法记录的字节偏移」;`Open` 在加载后若发现**文件末尾**存在无法解析的尾行 → `f.Truncate(offset)` + `Sync` + `slog.Warn("session: repaired partial tail")`(总方案第九节「遇到部分写入的尾记录,有明确的截断修复策略」)。注意与现有「坏行跳过」(728-730)的边界:**仅当坏行是文件最后一行**才截断,中间坏行维持跳过不改动(纯追加原则,store.go:2) |
| `store.go:134-148` `Close` | 追加 `syscall.Flock(fd, LOCK_UN)` + 关闭锁句柄;`Sync` 既有逻辑保留 |

**新增测试** `store_lock_test.go` / `store_recovery_test.go`:
- `TestStoreDoubleOpenRejected`:同 dir 第二次 `Open` → `ErrStoreLocked`;`Close` 后可再开(重复 Open 防护的跨进程版,与 P1-1 的进程内唯一互补);
- `TestPartialTailRecovery`:手工向 current.jsonl 追加半行 JSON → `Open` → 文件被截断回最后合法记录、`Commit` 继续可用、既有消息 `ActiveView` 完整(崩溃恢复的历史可读性,即 TestRestoreInterruptedRun 的存储层前置;完整版涉及 Run 状态持久化,属 P2,此处明确范围)。

**验收命令**:
```bash
go test -C ai-runtime -race ./session/... -count=1
go test -C daemon -race ./internal/plugins/agent/... -count=1   # 确认 lock 不影响单进程多 store 场景(P1-3 前仍单 store)
```

**依赖**:P1-1(同包,顺序可互换;P1-3 依赖两者)。

### P1-3 daemon 接线:每会话独立 Store + /new 换轨 Repository

**目标**:完成总方案 P1 第 1 条的 daemon 落地与 CLI 兼容:plugin 的全局单 Store 退役,持久化经 Repository;`/new` 的「归档+新建」在多 Store 世界里语义保持。

**涉及现有函数/字段与改动**:

| 位置 | 改动 |
|---|---|
| `plugin.go:57` | `store atomic.Pointer[session.Store]` **移除**;去向:`repo *session.Repository`(新增字段,startSession 打开;`cfg.Session.Enabled=false` 时 repo 仍为 nil,纯内存路径 F 保留) |
| `plugin.go:299-359` `startSession` | 重排:① `repo = session.OpenRepository(p.cfg.Session.Dir)`(nil 降级同现状 301-309);② legacy 槽判定:root 下有 current.jsonl → `st = repo.Open(legacyID)`(首个启动周期维持现行为);否则 `st = repo.Create()`;③ `deps.Store = st`(P0-3 起 deps.Store 是接口,直接传);④ runtime id 从 `"current"` 改为 `st.SessionID()`(store.go:161-165);⑤ `p.current.Store(rt)` |
| `plugin.go:331-340` | `deps` 组装中 `Store: p.store.Load()`(337)→ `Store: st`;其余字段不变(per-session 全量重建语义保持) |
| `plugin.go:218-222` `Stop` | `p.store.Load().Close()` → `p.current` runtime 的 store Close + `p.repo.Close()`(幂等,Store.Close 对 nil file 安全,store.go:138-147) |
| `commands.go:93-138` `handleNew` | 结构不变,数据源换轨:pending 快照(101-110)`p.store.Load().PendingAfterCursor()` → current store 的同名方法;`Archive()`(120-124)保留——legacy 槽内轮转;新槽(<root>/<id>/)下 Archive 同样在会话目录内轮转(store.go:450-504 以 s.dir 为基准,天然成立);`startSession()`(127)按上表新逻辑 Create 新会话 → CLI 看到的行为与今天一致(§1.1) |
| `sessions.go:42-64` `ListSessions` | 数据源换轨:current 会话 = current store(`store.Head()`+`SessionID()`,50-56 逻辑保持);归档 = current 会话目录 `archive/` 扫描(legacy root 或 per-id 目录,路径由 repo 提供);后续步骤(P5)再并 per-id 列表,本步保证输出与今天逐字段一致 |
| `sessions.go:100-105` `CurrentSessionID` | `p.store.Load().SessionID()` → current store 的 SessionID(返回值语义不变:稳定逻辑 id) |
| `sessions.go:110-137` `ReloadCurrentSession`/`CurrentSessionHistory` | `p.store.Load()` → current store;`ActiveView`/`Checkpoints` 调用不变 |
| `sessions.go:144-157` `ArchivedSessionHistory` | 归档路径解析:`archivePathForSession(p.cfg.Session.Dir, id)`(148)改为先查 current 会话目录的 archive/,legacy root 次之(逻辑保持:OpenArchiveView 只读,不触碰 store) |
| `rewind.go:37,52,75,92` | `p.store.Load()` → current store(经 helper `p.currentStore() *session.Store`,新增于 plugin.go;nil 时 /rewind 输出「持久化未启用」与现状 rewind.go:38-41 一致) |

**明确不做**(勘察修正 C):`daemon/internal/core/acpworker` 不接入 Repository(worker 会话 Store=nil 由调用方管理生命周期,acp_worker.go:97 注释即边界);ACP FrontendHandler 与 SessionGate 不动。

**新增测试** `daemon/internal/plugins/agent/repo_wiring_test.go`:
- `TestNewCreatesIndependentStore`:临时 Session.Dir 造 legacy 槽 → `/new`(经 handler)→ 断言:① 新会话目录 `<root>/<新id>/current.jsonl` 存在且 id ≠ 旧 id;② 旧会话归档文件仍在;③ 新 store `ActiveView` 为空(旧历史不串入);
- `TestCurrentStoreTracksSession`:`ReloadCurrentSession`/`CurrentSessionHistory` 在 /new 前后返回各自会话的数据。

**验收命令**:
```bash
go test -C daemon -race ./internal/plugins/agent/... ./internal/core/service/acp/... -count=1
go test -C ai-runtime -race ./session/... -count=1
go build -C daemon -tags goworker_no_dispatcher ./cmd/goworker
```

**依赖**:P0-4、P1-1、P1-2。**本步是 P1 最高风险接线点**(回滚见 §5)。

### P1-4 状态绑定收口:CWD/Usage 随会话 + 全量验收

**目标**:完成总方案 P1 第 3 条「将 CWD、Usage 等状态与 Session 绑定」,并跑完全部验收。

**涉及现有函数/字段与改动**:

| 位置 | 改动 |
|---|---|
| `plugin.go:68-69` | `sessionCwd`/`cwdErr`(mu 保护的全局单值)**移除**;去向:`SessionRuntime` 新增字段 `cwd string; cwdErr error`(runtime.go,复用 runtime.mu 或独立小锁)+ 访问器 `DeclaredCWD() (string, error)` / `declareCWD(cwd string, err error)`。每 runtime 一份声明,`Register` 时不继承(新会话无声明),legacy 槽挂载时由 `startSession` 从旧 runtime 迁移有效声明(对齐现状 plugin.go:349-358「有效声明跨会话保留、无效标记作废」的取舍) |
| `plugin.go:369-394` `SetSession` | 校验逻辑(validateSessionCWD,408-432)不变;落点从 `p.sessionCwd/p.cwdErr` 改为 `p.currentSession().declareCWD(...)`;`activeTransportSession`(74)**不动**——它是 transport 层单活动守卫(handler.go:192-199 依赖),P5 才随多前端改造迁移(勘察修正 B) |
| `plugin.go:436-440` `sessionCWDError` | 改读 current runtime 的 cwdErr(handleAgent:60 调用方不变) |
| `sessions.go:93-97` `currentSessionCwd` | 改读 current runtime 的 DeclaredCWD |
| `plugin.go:354-358` `startSession` 尾部 | cwd 重放改走 runtime 级迁移(见上) |

**Usage**:已在 Session 内(`spec.go:50` `usage *core.UsageTracker`,跨轮累计、/new 清零),经 P0-4 的 current 解析后天然 per-session;本步仅补测试证明,不改代码。

**新增测试**:
- `TestSessionStateIsolation`(P1-4):manager 里 A/B 两个 runtime,分别 declareCWD 不同目录、各跑一轮 → 双方 `Session().cwd()` 互不污染、usage 各自累计;
- `TestCWDDeclarationSurvivesNew`:legacy 槽上有效声明 → `/new` → 新 runtime 继承声明;无效声明 → 作废(对齐 plugin.go:349-357 现状取舍)。

**验收命令**(P0+P1 全量收口,与 CI 等价):
```bash
gofmt -l .
go test -C ai-runtime -race ./... -count=1
go test -C daemon -race ./... -count=1
go test -C daemon -race -tags goworker_no_agent ./... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./... -count=1
go test -C daemon -race -tags "goworker_no_agent,goworker_no_dispatcher" ./... -count=1
go build -C daemon -tags goworker_no_dispatcher -o /tmp/goworker-agent ./cmd/goworker
go test -C daemon -race -tags goworker_no_dispatcher ./mobile/ -count=1
bash daemon/mobile/build-aar.sh    # 需 gomobile 工具链;无 Android 环境时报告为未运行
```

**依赖**:P1-3。P1 完成线。

---

## 4. 总方案第十一节测试 → step 分配

| 测试 | 分配 | 说明 |
|---|---|---|
| `TestConcurrentSessions` | **P0-3** | manager 层两 runtime 并行,captureProvider 模拟 Provider,互不阻塞 |
| `TestSameSessionBusy` | **P0-3** | 同会话第二个 Run 排队(行为等价于现状),提交串行无历史覆盖 |
| Commit 失败注入(不能记 RunCompleted) | **P0-3** | `TestCommitFailureNotCompleted`,fake ConversationStore |
| `TestConcurrentStoreOpen` | **P1-1** | 进程内实例唯一;跨进程版 `TestStoreDoubleOpenRejected` 在 P1-2 |
| 文件锁 / 重复 Open | **P1-2** | flock + `ErrStoreLocked` |
| 尾记录截断修复 / 崩溃恢复(存储层) | **P1-2** | `TestPartialTailRecovery`;`TestRestoreInterruptedRun` 完整版需 Run 状态持久化,显式划归 **P2** |
| CWD/Usage 会话隔离 | **P1-4** | `TestSessionStateIsolation` |
| `/new` 独立 Store | **P1-3** | `TestNewCreatesIndependentStore`(daemon 集成) |
| `TestRunSurvivesDisconnect` / `TestCancelIsolation` | P2 | 依赖 RunSupervisor 与宿主 ctx |
| `TestAttachReplayNoGap` / `TestSlowSubscriber` | P3 | EventHub/Journal |
| `TestApprovalAfterReconnect` / `TestToolBindingDisconnect` | P4 | ApprovalManager / CapabilityBinding |

---

## 5. 风险与回滚点

1. **`Session.Run` 签名变更(P0-3)**波及 ai-runtime 全部 Run 测试与 acp_worker 调用点:编译器指路,机械改动;第二返回值恒 nil 保持错误面不变,CLI/ACP 用户可见输出逐字不变。回滚:revert 单提交,P0-2 状态不受影响。
2. **ConversationStore 接口化(P0-3)**:接口成员取自实际调用点(session.go:171/175/180/185/383/422/468),遗漏会在编译期暴露;`*session.Store` 天然满足,plugin 传参零改动。回滚:revert 恢复具体类型。
3. **runGate 退役(P0-2)的重入回归**:最大隐性风险是嵌套 `hub.Eval` 死锁。守卫 = `gate_test.go:18,41` 的 `TestWithSession_*` 必须原样保留且全绿;严禁在本步顺带改 `/new` 语义或动 SessionGate。回滚:revert 单提交恢复 runGate(字段级小 diff)。
4. **P1-3 数据迁移**:Repository 兼容挂载 legacy 布局(`root/current.jsonl` 原地挂载、不移动文件)是硬性要求;任何「启动时搬运文件」的方案都被本 Plan 排除。若兼容挂载有缺陷,回滚 = revert P1-3 恢复 `store atomic.Pointer` 单 Store(该字段的恢复 diff 集中在 plugin.go/commands.go/sessions.go/rewind.go 的取数点)。
5. **flock 可用性(P1-2)**:网络文件系统(NFS/SMB)上 flock 可能失败或为本地语义——失败即拒绝打开(宁拒勿双写,总方案第九节),文档注明;非 unix 平台为无锁桩,仓库 CI(ubuntu)与目标平台(android/darwin/linux)均不受影响。
6. **行为边界重申**:P0+P1 完成后用户可见行为不变——多会话并行只发生在 ai-runtime 层并被测试证明;前端仍单 current/单活动会话(SessionGate、activeTransportSession、PROTOCOL.md:40-47 语义全部保留)。若在 P0/P1 期间发现前端需要多会话,属 P5 范围,不在本 Plan 放开。
7. **CI/gofmt 前置清理**:仓库根 `.claude/worktrees/agent-afeac443f18b2f382/daemon/internal/plugins/agent/sessions_test.go` 未格式化(勘察实测 `gofmt -l .` 报出),会被 CI 第一步卡住;首个 PR 前删除该 worktree 残留或纳入忽略。CI 仅 master 触发,feature 分支需 PR 合入后验证。
8. **提交与回滚链**:每 step 一个独立提交,可独立 revert:P1-4 → P1-3 → P1-2 → P1-1 → P0-4 → P0-3 → P0-2 → P0-1 依次回退均保持可构建。P1-3 是唯一动持久化接线语义的提交,review 时重点核对 §3 P1-3 表格中的每个取数点。
