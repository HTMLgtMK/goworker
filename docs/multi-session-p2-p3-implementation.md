# P2+P3 代码级实施 Plan:后台执行(RunSupervisor)+ 事件回放与 Attach(EventHub/EventJournal)

> 依据 `docs/multi-session-runtime-plan.md`(总方案)第十节改造清单 P2/P3、第十一节验收测试、第五节事件语义、第九节存储布局,以及 2026-10-26 对分支 `feature/multi-session-p0p1`(commit c3def23)的 P2/P3 代码勘察(纪要见附录 A)。
> 前置:P0+P1 已落地并全绿(本分支,`docs/multi-session-p0-p1-implementation.md` 为同构前置计划)。勘察发现与总方案不符之处按实际代码修正,全部标注「勘察修正」;需主 agent 拍板的设计决策在 §0.3 记录采用的默认,附录 B 附勘察 openQuestions 原文。

## 0. 勘察事实基线

### 0.1 P0+P1 落地现状(本次勘察实测)

```bash
# 本次实际运行,全部通过:
go build -C ai-runtime ./...            # BUILD_OK
go test -C ai-runtime ./agent/... ./session/... -count=1
#   ok  github.com/tinguo/goworker/ai-runtime/agent   8.152s
#   ok  github.com/tinguo/goworker/ai-runtime/session 17.030s
go build -C daemon ./...
go test -C daemon ./internal/plugins/agent/... ./internal/core/service/acp/... -count=1
#   ok  github.com/tinguo/goworker/daemon/internal/plugins/agent    8.220s
#   ok  github.com/tinguo/goworker/daemon/internal/core/service/acp 3.664s
```

已就位的 P0+P1 内核(行号以 commit c3def23 为准):

| 符号 | 现状 | P2/P3 关切 |
|---|---|---|
| `SessionManager`(ai-runtime/agent/manager.go:17-25) | 纯注册表 `runtimes map[string]*SessionRuntime`;Register/Get/Remove/List/Close;文件头注释(10-13)声明 runSlots 与宿主 ctx/cancel 属 P2 | P2-4/P2-5 在其上扩 ctx、closed、runSlots、pending |
| `SessionRuntime`(runtime.go:17-32) | gate(容量 1 channel,:46-53)+ active 记账 + cwd/cwdErr 声明;**无 events/closing 字段** | P3-4/P3-5 增 `events *EventHub`(惰性) |
| `RecordRun`(runtime.go:110-138) | 同步记账形态:「调用方已持 gate」,创建 run→RunRunning→fn→终态化→清 active;终态映射按 call-layer err 的 ctx 语义(:120-125) | P2-3 接 Run 状态持久化;P2-5 抽共享 finalize 供 StartRun 复用 |
| `AgentRun`(run.go:37-46) | done chan + transition/finish/snapshot;`RunQueued` 仅作初始态,无调度;**runSeq 进程内自增 `run-N`**(:102-107,重启归零);无 cancel 字段 | P2-1 改 RunID 格式;P2-5 增 cancel;RunQueued 由 StartRun 启用 |
| `Session.Run`(session.go:94-209) | `agentCtx = WithCancel(调用方 ctx)`(:149);RunResult 语义「模型成功≠持久化成功」(:181-207);第二返回值恒 nil(:93);agent.Run 失败一律记 RunFailed(:157-163) | P2-1 增取消态归一;P2-5 成为 StartRun 的执行体 |
| `Repository`(session/repository.go:68-73) | `<root>/<id>/current.jsonl` 新布局 + legacy 根槽原地挂载;实例唯一 open 缓存(:72);Delete=RemoveAll(:239);**对 runs/ 零感知** | P2-2 增 RunJournal custody;P3-1 增事件读写 |
| `Store`(session/store.go:43-56) | 纯追加+flock(`<dir>/lock`,lock_unix.go:26-37)+坏尾行截断恢复 loadRecords lastGoodEnd(:777-816);`dir` 未导出(:47);Archive 后同目录换新 SessionID(:556) | journal.go 放 session 包内直取 dir;尾恢复模式被 Journal 复用 |
| `AgentPlugin`(daemon/internal/plugins/agent/plugin.go:39-81) | manager+current(atomic)+repo 三件套;startSession(:362-458)是会话边界;Stop 收尾 currentStore+repo.Close(:276-286) | P2-7 接线 run 配额、RunJournal、启动恢复 |
| `withSession`/`handleAgent`(commands.go:37-51,59-84) | 7 个命令 handler 统一互斥入口;sessionGateKey 重入标记(:28,:38,:50);handleAgent=RecordRun 同步路径 | **不改**;StartRun 不经它(P2 边界) |
| ACP 链路 | jsonrpc.go:280 每请求 ctx(连接 Close 全取消:505-514)→ server.go:317 runCtx(session/cancel:368-376 与 cancelAll:352-362 触发)→ handler.go:214 `model.Context{Ctx: 请求 ctx}` → engine.go:264 → session.go:149 | **全部不动**;ctx 分离发生在 agent 包内(StartRun 用 manager.ctx) |

### 0.2 本 Plan 依赖的勘察结论

| # | 勘察结论 | 对本 Plan 的影响 |
|---|---|---|
| A | **RunID `run-N` 进程内自增(run.go:102-107)重启归零**,而 Journal 按 `runs/<runID>.jsonl` 落盘(总方案第九节)——同会话第二次进程生命的 run-1 会撞旧文件。 | P2-1 必须先改 RunID 格式,否则 P2-2 的存储布局不成立 |
| B | **取消语义缺口**:`Session.Run` 对 agent.Run 失败一律记 RunFailed(session.go:157-163),ctx.Canceled 也如此;RecordRun 的 RunCancelled 映射(runtime.go:120-125)只对 call-layer error 生效,而该 error 现状恒 nil(session.go:93)。 | P2-1 在 Session.Run 内归一取消态,否则 CancelRun/断连的 Run 状态持久化会把取消记成失败 |
| C | **Journal 无锁可借**:`<dir>/lock` flock(lock_unix.go)只保护 Store 写者;Journal 若独立持锁需新增锁面。但 Repository 实例唯一缓存(repository.go:72)+ flock 已保证「Store 已 Open ⇒ 独占写者」。 | P2-2 的 custody 设计:Journal 依附 Store in-cache,不新增锁 |
| D | **`SessionDeps` 是 agent→session 的既有注入口**(spec.go:75-86,含 PersistAllowRule 等函数字段);agent 已 import session 包(session.go:16),反向不可(session 不能 import agent,RunState 是 agent 类型)。 | P2-3 的 RunJournal 接口放 agent 包、记录类型放 session 包(State 用 string),避免循环依赖 |
| E | **测试基建齐全**:gatedProvider(可挂起/放行,run_lifecycle_test.go:101-136)、fakeConversationStore(:35-98)、newFakeStoreSession(:139)、captureProvider(test_helpers_test.go:11-28);daemon 侧 newAgentPluginP+fixedProvider(plugin_test.go:196-218)。 | P2/P3 全部验收测试用模拟 Provider,零真实 LLM(总方案第十一节) |
| F | **daemon config 直通 ai-runtime**:`daemon/internal/app/config/config.go:31` 直接内嵌 `runtimeconfig.SessionConfig yaml:"session"`,默认值在 :188-192。 | P2-7 给 SessionConfig 增字段自动贯通 yaml;构造点对 0 值回落默认作双保险 |
| G | **SessionGate/FrontendHandler/ai-acp/acpworker 与 P2/P3 无接缝**:同步 prompt 链路(ctx 链见 §0.1 末行)不经过 manager;stdio worker 无 SessionRuntime(acp_worker.go:182 注释「记账留 P2 RunSupervisor」)且无 repo。 | 本 Plan 不触碰这四处;边界见 §6 |
| H | **`/new` 后旧 store 滞留 repo 缓存是实例唯一不变量的代价**(plugin.go:271-276 注释),Stop 统一回收。 | P2/P3 不解决该边角(见 §6);RunJournal 句柄随 run 终态关闭,不新增滞留资源 |
| I | `Archive()` 轮转后同目录换新 SessionID(store.go:556),`runs/` 若按目录挂靠会「跟随」新身份。 | 记录带 SessionID 字段供诊断;归属歧义记为已知边角(§6 风险 7),V1 读取不过滤 |
| J | 事件面现状:streamTokens 是 token 流唯一出口(session.go:213-249),HITL 决策在 :229-244 同步等待 cb.Decide;usage/iteration 经 cb.Publish(session.go:254-259)。 | P3-4 的 teeRunCallbacks 挂在 RunCallbacks 三入口(emitToken/decide/publish)上,不进 ai-core |

### 0.3 openQuestions 拍板记录(采用勘察建议默认,主 agent 可推翻)

| # | 决策 | 本 Plan 采用 | 推翻时的波及面 |
|---|---|---|---|
| ① | P2 是否把 daemon handleAgent 切到 StartRun | **不切**。RecordRun 同步路径原样保留;StartRun 仅由 ai-runtime 测试验证,前端迁移属 P5。依据:session/prompt 是阻塞请求且 SessionGate 的 busy/superseded 语义依赖 prompt 在飞(PROTOCOL.md:40-47、coordinator.go:5-8);总方案第七节明令「不要直接改变现有 session/prompt 的阻塞响应语义」 | 若拍板切换:P2-7 改为 handleAgent 走 StartRun+阻塞 Wait,SessionGate 语义与 PROTOCOL.md 需同步修订,本 Plan 步序不变但 §6 行为边界作废 |
| ② | EventJournal 与 Repository 的目录/生命周期整合 | 落点 `<root>/<id>/runs/<runID>.jsonl`;**Journal 依附 Store**(经 `Repository.OpenRunJournal` API 形状强制:store 不在 open 缓存即拒),不新增独立锁文件;Archive 不轮转/不清理 runs/(记录带 SessionID 供诊断);Journal 句柄随 run 终态关闭,不存在跨 /new 滞留句柄;`Store.dir` 不加访问器(journal.go 与 repository.go 同包直取,store.go:47) | 若要求 Journal 独立锁:改 P2-2(自持 flock 文件),custody 论证作废 |
| ③ | runSlots 配置落点 | `SessionConfig` 增 `max_concurrent_runs`/`max_pending_runs`(默认 4/16,总方案第四节建议值);agent 侧 `WithRunLimits` 选项接收,`<=0` 回落默认(不依赖 yaml 载入路径,双保险) | 若拍板 V1 硬编码:P2-7 缩为只接 RunJournal+恢复扫描 |
| ④ | P3 后 daemon 行为变化 | **零变化**。EventHub/事件 Journal 仅 SDK 层(daemon 不接线 `deps.RunEvents`,P5 再接);session/load 重放不切 Journal(仍走 store ActiveView+checkpoint 锚点,sessions.go:147-203);usage/iteration 事件不并入 SessionEvent 流水 | 若 P3 提前接 daemon:P3-5 增接线步,§6 边界相应放宽 |

---

## 1. 目标形态(P2+P3 完成后)

```
ai-runtime/agent/
  run.go            # P2-1:RunID v2(run-<unixnano>-<seq>);P2-5:+cancel 字段
  session.go        # P2-1:取消态归一(RunCancelled)
  spec.go           # P2-3:+RunStateJournal 接口与 SessionDeps.RunJournal;P3-4:+EventJournal 接口与 SessionDeps.RunEvents
  runtime.go        # P2-3:RecordRun 接 journal;P2-5:抽 finalizeRun;P3-4/P3-5:+events *EventHub(惰性)与生命周期
  manager.go        # P2-4:+ctx/cancel/closed;P2-5:+runSlots/pending/StartRun/WithRunLimits;P2-6:+CancelRun
  supervisor.go     # P2-5 新增:RunHandle/execute;P3-4:+teeRunCallbacks(cb→SessionEvent 映射)
  event_hub.go      # P3-2/P3-3 新增:EventHub(Publish/Subscribe/背压/ErrCursorExpired)
ai-runtime/session/
  journal.go        # P2-2 新增:RunStateRecord/RunJournal/MarkInterruptedRuns;P3-1:+EventRecord/ReadEvents/SeqRange
  repository.go     # P2-2:+OpenRunJournal(custody);P3-1 复用
daemon/internal/plugins/agent/
  plugin.go         # P2-7:WithRunLimits(config)、deps.RunJournal 接线、startSession 恢复扫描
ai-runtime/config/
  spec.go/config.go # P2-7:SessionConfig 增两字段 + Default
```

**用户可见行为在 P2+P3 全程不变**(openQuestion ①④ 默认):CLI 与 ACP 的 prompt 仍是阻塞同步路径;`SessionGate`、`activeTransportSession`、superseded/busy、PROTOCOL.md:40-47 语义逐字保留;`ai-acp`、`acp service`、`acpworker`、`engine`、`model` 五处零改动。daemon 侧唯一新增 I/O:`runs/<runID>.jsonl` 每 run 2-3 条记录(fsync)+ 启动时恢复扫描的 slog——均不进任何前端通道。

后台执行与事件回放能力在 ai-runtime 层达成并由测试证明;`x-goworker/*` 协议、多观察者 Attach 的 daemon 接线是 P5 的事。

---

## 2. P2 steps(后台执行)

### P2-1 RunID 跨重启唯一 + 取消态归一(run 语义修正)

**目标**:完成总方案 P2 第 4 条「Run 状态持久化」的两个前置:①Journal 文件名所依赖的 RunID 必须跨重启唯一;②持久化的取消语义必须正确(勘察修正 A/B)。纯 agent 包小改,行为面不变。

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/agent/run.go:100-107` | `newRunID()` 改 `fmt.Sprintf("run-%d-%d", time.Now().UnixNano(), runSeq.Add(1))`;注释更新(原文「RunID 只需进程内唯一」→「进程内 seq 保唯一;跨重启同会话目录同 nano+同 seq 概率可忽略,且由 P2-2 Journal 建文件 O_EXCL 兜底——碰撞变成可观测错误而非静默损坏」)。文件名字符序=创建序(nano 定宽至 2262 年),P3-1 的跨文件 seq 拼接依赖此序 |
| `ai-runtime/agent/session.go:157-163` | agent.Run 失败分支:`errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)` → `RunResult{State: RunCancelled, Err: err, Persisted: false}`,其余维持 RunFailed。**用户可见输出不变**(`cb.Write("✘ …")` 照旧,仅 RunResult.State 变,它只进 debug 日志 commands.go:81) |
| `ai-runtime/agent/runtime.go:120-125` | call-layer 映射保留(冗余防御,注释注明主映射已前移至 session.go) |

**新增文件**:无。

**新增测试**(`runtime_test.go`/`run_test.go` 追加):
- `TestRunIDUniqueAcrossNewInstances`:两次 `newRunID()` 不等;格式含两个数字段;
- `TestSessionRun_CancelledProviderErrorMapsToCancelled`:stub provider 的 Chat 返回 `context.Canceled` → `Session.Run` 返回 `State==RunCancelled`;返回普通错误 → RunFailed(复用 test_helpers_test.go:11-28 captureProvider 改造)。

**并发与锁论证**:无新锁;UnixNano 单调性不做强假设(seq 后缀兜底进程内唯一)。

**验收命令**:
```bash
go vet -C ai-runtime ./agent/...
go test -C ai-runtime -race ./agent/... -count=1
go build -C ai-runtime ./... && go build -C daemon ./...
```

**依赖**:无(起点)。后续:P2-2(custody 用 RunID 校验)、P2-3(记录 RunID)。

### P2-2 session:Run 状态 Journal(存储层)+ 恢复扫描 —— TestRestoreInterruptedRun 完整版落点

**目标**:完成总方案 P2 第 4 条「Run 状态持久化」的存储面与总方案第九节「Daemon 重启后」恢复语义:runs/ 目录、durable-first 追加、尾记录截断修复、非终态标记 interrupted。纯 session 包,不接线 agent/daemon。

**新增文件** `ai-runtime/session/journal.go`:

```go
// 记录信封:kind 自立门户,P3 事件记录(同文件混存)零格式迁移。
type RunStateRecord struct {
    Kind      string    `json:"kind"`                  // 恒 "state"
    SessionID string    `json:"session_id"`            // 诊断字段(勘察修正 I:Archive 换 id 后的归属可辨)
    RunID     string    `json:"run_id"`
    State     string    `json:"state"`                 // agent.RunState 字符串化;另有 StateInterrupted
    Err       string    `json:"err,omitempty"`
    Persisted bool      `json:"persisted,omitempty"`
    At        time.Time `json:"at"`
}
const StateInterrupted = "interrupted"   // 总方案第九节「Run Running → 标记 Interrupted」;
                                         // 属 journal 语义,不进 agent.RunState 枚举(P2 无产生方)

type RunJournal struct { mu sync.Mutex; f *os.File; path string }   // 每 run 一文件
func (j *RunJournal) Append(rec RunStateRecord) error
    // marshal 单行+'\n'(字节格式对齐 appendRecord,store.go:680-693)+ file.Sync —— durable-first
func (j *RunJournal) Close() error

// Repository 增(custody 入口):
func (r *Repository) OpenRunJournal(ctx context.Context, sessionID, runID string) (*RunJournal, error)
    // ① validSessionID(sessionID)(repository.go:324-333)+ 新 validRunID(拒绝空/路径分隔符/非
    //    "run-<十进制>-<十进制>" 形——拼路径前卡死,防 traversal 同 validSessionID 论证);
    // ② r.mu 内查 open 缓存:无 → ErrSessionNotOpen(新 sentinel);
    // ③ dir := store.dir(同包直取,store.go:47);MkdirAll(dir/runs, 0700);
    // ④ os.OpenFile(dir/runs/<runID>.jsonl, O_CREATE|O_EXCL|O_APPEND|O_WRONLY, 0600)
    //    —— O_EXCL:P2-1 的碰撞兜底,EEXIST → ErrRunExists
func (r *Repository) MarkInterruptedRuns(ctx context.Context, sessionID string) ([]string, error)
    // store 不在缓存 → ErrSessionNotOpen;扫 <dir>/runs/*.jsonl(文件名序):
    // 逐文件按 loadRecords 同策略解析(store.go:777-816 复用/抽公共 helper),末尾坏行截断回
    // lastGoodEnd(总方案第九节「部分写入的尾记录有明确截断修复策略」);
    // 末条 state 记录非终态(completed/failed/cancelled/interrupted 之外)→ O_APPEND 追加
    // {kind:"state", state:"interrupted", run_id:文件名 runID};幂等(已终态跳过);返回被标记 runID
```

**涉及现有符号与改动**:`repository.go:49-57` 增 sentinel `ErrSessionNotOpen`/`ErrRunExists`;`repository.go` 文件头目录布局注释补 runs/ 一行。

**并发与锁论证(custody,勘察修正 C)**:
1. **不新增锁**:Journal 写权的前提是「该会话 Store 在本 Repository open 缓存」(OpenRunJournal ①-② 强制),而 store 进缓存必经 `Store.Open` 持 `<dir>/lock` flock(lock_unix.go:26-37)——跨进程第二写者开不了 Store ⇒ 开不了 Journal(宁拒勿双写);同进程重复写同一 run 文件被 O_EXCL 拒绝。
2. **Delete 安全**:open 缓存命中 ⇒ Delete 必拒(repository.go:231-235 ErrSessionInUse)⇒ Journal(依附在缓存 store)存在期间会话目录不可能被 RemoveAll——「删目录而 Journal 仍写」不可达,无需给 Journal 建独立占用登记。
3. **Append 串行**:j.mu 护单文件句柄;每 run 至多一个 RunJournal 实例(O_EXCL 保证全局唯一创建权),无多写者。

**新增测试** `journal_test.go`:
- `TestRunJournalRoundTrip`:Open→Append×2→Close→重读,记录逐字段一致;
- `TestRunJournalRejectsLockedStore`:未 Open 的 id → ErrSessionNotOpen;跨进程模拟:另一 Repository(同 root)Open 同 id → ErrStoreLocked(store_lock_test.go 语义延伸);
- `TestRunJournalDoubleCreateRejected`:同 runID 二次 OpenRunJournal → ErrRunExists;
- **`TestRestoreInterruptedRun` 完整版(总方案第十一节 → 本步,依赖即本步)**:造 store(legacy 布局,Commit 数轮)+ runs/ 两文件(run-A 终态 completed、run-B 末记录 running)→ `repo.Close()` 全释放 → 重新 `OpenRepository`+`Open(id)` → `MarkInterruptedRuns` → 断言:run-B 被追加 interrupted、run-A 不动、`current.jsonl` 的 `ActiveView()` 历史完整可读、除 interrupted 标记外**无任何新增记录**(恢复绝不执行工具/不重跑——「不盲目重新执行」由恢复路径纯标记的构造保证,测试用记录数断言钉死)。

**验收命令**:
```bash
go vet -C ai-runtime ./session/...
go test -C ai-runtime -race ./session/... -count=1
```

**依赖**:P2-1(validRunID 匹配 RunID 格式)。后续:P2-3(接口)、P2-7(daemon 恢复接线)。

### P2-3 agent:SessionDeps.RunJournal + RecordRun 接线(同步路径获得 Run 状态持久化)

**目标**:agent 包定义持久化最小面(消费侧接口,同 ConversationStore 先例 spec.go:64-71),RecordRun 在既有记账点写 journal——daemon 同步路径由此免费获得 Run 状态持久化,StartRun(P2-5)与它共用同一接口。

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/agent/spec.go`(新增,紧跟 ConversationStore) | `type RunStateJournal interface { AppendState(rec session.RunStateRecord) error }`;`SessionDeps`(:75-86)增字段 `RunJournal RunStateJournal // nil = 不持久化(纯内存);接口字段必须真 nil(typed-nil 陷阱同 Store,spec.go:83)`。State 用 string 字符串化(`string(res.State)`),session 包不得反向 import agent(勘察修正 D) |
| `ai-runtime/agent/runtime.go:110-138` `RecordRun` | 抽内部 helper `journalRunState(s *Session, rec session.RunStateRecord)`(nil sink/nil session 跳过;失败 slog.Warn 不阻塞——journal 可用性低于执行可用性);接线两点:①:113 `transition(RunRunning)` 后写 `{state:"running"}`;②:131 `run.finish(res)` **之前**写终态记录——次序即不变量:s.Run 返回时 Conversation Commit 已定(session.go:181-207),「Completed 状态必须在 Conversation Commit 成功之后记录」(总方案第九节)由「fn 返回后才写终态」的既有结构天然满足,本步用测试钉死 |

**新增文件**:无。编译期断言(`run_journal_test.go`):`var _ RunStateJournal = (*session.RunJournals)(nil)`?——否,P2-3 时 session 侧尚未有聚合类型;断言改为 daemon 接线步(P2-7)引入 `*session.RunJournalStore` 后补。本步测试用本地 stub。

**并发与锁论证**:journalRunState 在 rt.mu **外**调用(锁内不做文件 I/O);RecordRun 的调用方已持 gate,同一会话的 journal 写天然串行;跨会话互不共享句柄(每 run 一文件)。

**新增测试** `run_journal_test.go`:
- `TestRecordRunJournalsStartAndFinish`:stub RunStateJournal 收 2 条:running + 终态 completed;
- `TestRecordRunJournalCommitFailureInjectsFailed`(总方案第十一节失败注入的 journal 对偶):fakeConversationStore.commitErr(run_lifecycle_test.go:35-98)→ 终态记录 `state=="failed" && !persisted`,记录流中**绝不出现 completed**;
- `TestRecordRunJournalAppendErrorNonFatal`:stub 返回错误 → Run 照常完成(warn 路径)。

**验收命令**:
```bash
go vet -C ai-runtime ./agent/...
go test -C ai-runtime -race ./agent/... ./session/... -count=1
```

**依赖**:P2-1、P2-2。后续:P2-5(execute 复用 journalRunState)、P2-7(daemon 接线)。

### P2-4 manager:生命周期 Context + closed(宿主停机语义)

**目标**:完成总方案 P2 第 1 条的 Manager 半边:宿主生命周期 ctx 归 Manager(总方案第四节「runCtx, cancel := context.WithCancel(manager.ctx)」的父),Close = 取消 + 等待全部在途 Run 终态 + 拒绝后续 Register。

**涉及现有符号与改动**(`ai-runtime/agent/manager.go`):

| 位置 | 改动 |
|---|---|
| `manager.go:17-20` struct | 增 `ctx context.Context; cancel context.CancelFunc; closed bool`(mu 保护) |
| `manager.go:23-25` `NewSessionManager` | 内建 `ctx, cancel := context.WithCancel(context.Background())`(签名不变,plugin.go:89 等调用点零改动) |
| `manager.go:28-39` `Register` | `closed → ErrManagerClosed`(新 sentinel);注释更新(manager.go:67-68 「宿主级统一停机属 P2」兑现) |
| `manager.go:69-73` `Close` | 重写:`mu` 内置 closed+快照 runtimes → 解锁 → `cancel()` → 逐 runtime 等 `Active().done` 关闭(nil 跳过;**不持 m.mu 也不持 rt.mu 等待**)→ `mu` 内清表。幂等;注释:Close 即总方案 P2 第 4 条的 Shutdown;P2-7 起 daemon 在 **Stop 时不调用它**(同步路径无 manager 持有的 run,等待在途 prompt 反而拖慢停机,P5 StartRun 接入后再接) |
| 新增 | `func (m *SessionManager) contextOf() context.Context`(包内访问器,供 supervisor 派生 runCtx;不导出——P2 无外部消费者) |

**新增测试** `manager_test.go` 追加:
- `TestManagerCloseCancelsContextAndRejectsRegister`:Close 后 `contextOf().Done()` 关闭、Register → ErrManagerClosed、幂等;
- `TestManagerCloseWaitsActiveRun`:gatedProvider(run_lifecycle_test.go:101-136)挂起一轮 RecordRun goroutine → 另一 goroutine Close → release provider → Close 返回且 run 已终态(Close 阻塞到 done 的语义钉死)。

**并发与锁论证**:`closed` 只在 m.mu 内读写;Close 的等待在锁外,等待对象是 run.done(由 transition 在 run.mu 内关闭,run.go:72-79——done 关闭本身无锁竞争)。同步路径 RecordRun 的 fn 用调用方 ctx,与 m.ctx 无父子关系 ⇒ Close 取消不影响在途同步 prompt(行为不变论证,写进 Close 注释)。

**验收命令**:
```bash
go test -C ai-runtime -race ./agent/... -count=1
```

**依赖**:无硬依赖(与 P2-2/P2-3 并行可行)。后续:P2-5(execute 的 runCtx 父)。

### P2-5 supervisor:StartRun / RunHandle / runSlots 有界全局并发

**目标**:完成总方案 P2 第 2、3 条:有界全局并发(gate→runSlots 两级)+ StartRun 立即返回 RunHandle + queued 状态启用 + queued 记录 durable-first 落盘。

**新增文件** `ai-runtime/agent/supervisor.go`;**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/agent/manager.go:17-25` | 增 `runSlots chan struct{}`(容量=MaxConcurrentRuns)、`pending chan struct{}`(容量=MaxPendingRuns,排队计数令牌);构造:`NewSessionManager(opts ...ManagerOption)` 变参化(现有调用点零改动),`WithRunLimits(maxConcurrent, maxPending int)`,`<=0` 回落 `defaultMaxConcurrentRuns=4 / defaultMaxPendingRuns=16`(总方案第四节;P2-7 接 config) |
| `ai-runtime/agent/run.go:37-46` | AgentRun 增 `cancel context.CancelFunc`(mu 保护,execute 在派生 runCtx 后写入;RecordRun 路径恒 nil);新增访问器 `cancelFunc() context.CancelFunc` |
| `ai-runtime/agent/runtime.go` | 抽 `finalizeRun(r *SessionRuntime, run *AgentRun, res RunResult, jerr error)`:终态归一(P2-1 防御分支原样)→ journalRunState 终态(P2-3)→ `run.finish(res)` → rt.mu 内 `if r.active == run { r.active = nil }`(:132-136 防御原样)。RecordRun 改调它(行为逐一等价);sentinel:`ErrRunActive/ErrTooManyPending/ErrSessionNotBound/ErrRunNotFound/ErrRunNotCancellable`(manager.go) |
| `supervisor.go` 新增 | `type RunHandle struct { SessionID, RunID string; run *AgentRun }` + `State() RunState` / `Wait(ctx) (RunResult, error)`(select run.done/ctx.Done;返回 snapshot,run.go:83-89)。总方案第三节的 `{SessionID, RunID}` 之上**加法**扩展(不删字段)。<br>`func (m *SessionManager) StartRun(sessionID string, req RunRequest, cb RunCallbacks) (RunHandle, error)`,同步段:<br>① `m.mu.RLock`:closed→ErrManagerClosed;Get nil→ErrSessionNotFound;<br>② `rt.mu` 内:`active != nil → ErrRunActive`(同会话第二 Run 拒绝——总方案第四节「Session 在排队期间已经被占用,不能继续提交第二个 Run」;排队只发生在**全局槽**一级)、`Session()==nil → ErrSessionNotBound`;记下 sink 与 pending 预留之外的快照后**先释放 rt.mu**;<br>③ pending 非阻塞预留:`select { case m.pending <- struct{}{}: default: return ErrTooManyPending }`(有界排队,满即拒——锁/通道都不等);<br>④ `run := newAgentRun(...)`(state 保持 RunQueued,**不 transition**);`rt.mu` 内 `r.active = run`;<br>⑤ journal `{state:"queued"}`(**durable-first**:总方案第七节「run/start 响应在 Run 被接受并持久化登记后立即返回」——先落盘再返回;失败 warn 继续);<br>⑥ `go m.execute(rt, run, req, cb)` 并返回 RunHandle。<br>`execute`(单 goroutine,cb 所有权自 StartRun 返回起归它):<br>`runCtx, cancel := context.WithCancel(m.contextOf())`;run.mu 写 run.cancel;defer cancel()<br>→ ① `rt.AcquireRun(runCtx)`(:46-53,select gate/runCtx.Done;失败→释放 pending 预留→finalizeRun(RunCancelled)→return)<br>→ ② `select { case m.runSlots <- struct{}{}: ; case <-runCtx.Done(): }`(失败→**释放 gate**→释放 pending→finalizeRun(RunCancelled)→return)<br>→ ③ `<-m.pending`(离开队列,释放预留令牌)<br>→ ④ `transition(RunRunning)` + journal `{state:"running"}`<br>→ ⑤ `res, _ := rt.Session().Run(runCtx, req, cb)`<br>→ ⑥ journal 终态(先落盘)→ `finalizeRun`(后关闭 done——等待者看到终态时记录已持久)<br>→ ⑦ 释放 runSlots;gate 由 defer 释放 |

**并发与锁论证(本步核心)**:
- **锁/channel 次序**:`m.mu`(map+closed)只护查找与布尔,任何持有期间不做 channel 等待(总方案第四节「不建议在 Manager.mu 下等待执行槽」);`rt.mu` 只护 active/session 指针,同理;通道获取次序固定 **gate → runSlots**,释放逆序(runSlots → gate)——两把「锁」全序,无环;
- **pending 是计数令牌非锁**:StartRun 同步段非阻塞获取(满即拒),execute 两个出口各恰好释放一次(③ 或失败分支),无阻塞、无泄漏;
- **同会话互斥**:active 在同步段占位 ⇒ 第二 StartRun 被拒;execute 的 AcquireRun 与 daemon 同步路径的 withSession(commands.go:45)在 gate 上天然互斥——RecordRun(P2-3 后)若撞上在途后台 run,由 P2-5 的 finalizeRun 防御与 gate 串行保证不覆盖记账(active 相等才清,runtime.go:132-136 语义);
- **不丢事件/不丢记账**:run 状态迁移只经 transition(幂等,run.go:62-80);execute 各失败分支都收敛到 finalizeRun(active 必清、终态必写、令牌必释放)。

**新增测试** `supervisor_test.go`(全部模拟 Provider,复用 gatedProvider/newFakeStoreSession/fakeConversationStore):
- **`TestRunSurvivesDisconnect`(总方案第十一节 → 本步)**:A StartRun(gatedProvider 挂起)→ **提交方 goroutine 直接返回、不 Wait**(模拟连接断开——StartRun 无 ctx 参数,API 形状即保证调用方 ctx 不成 run 父)→ release provider → `handle.Wait()` 得 `RunCompleted && Persisted`;对照断言:期间 `m.contextOf()` 未取消;
- `TestStartRunQueuedOnGlobalSlot`:`WithRunLimits(1, 8)`;A StartRun 挂起中,B StartRun **立即**返回且 `State()==RunQueued`(同步段零阻塞钉死)→ release A → B 完成;两 fake store 各自提交完整;
- `TestStartRunPendingBounded`:槽 1、pending 2:A running + B/C queued → D StartRun → ErrTooManyPending;
- `TestStartRunSecondRunRejected`:同会话第二 StartRun → ErrRunActive;
- `TestStartRunJournalsQueuedBeforeReturn`:sink stub 断言 StartRun 返回时 queued 记录已在。

**验收命令**:
```bash
go vet -C ai-runtime ./agent/...
go test -C ai-runtime -race ./agent/... -count=1
```

**依赖**:P2-1、P2-3、P2-4。后续:P2-6(CancelRun)、P3-4(hub 接线)。

### P2-6 supervisor:CancelRun + 排队取消路径

**目标**:完成总方案 P2 第 4 条的 CancelRun:运行中与排队中的 Run 都可取消;取消隔离(TestCancelIsolation)。Shutdown 即 P2-4 的 Manager.Close,无新代码。

**涉及现有符号与改动**(`ai-runtime/agent/manager.go` 新增方法):

```go
func (m *SessionManager) CancelRun(sessionID, runID string) error
// m.mu.RLock Get;rt.mu 内快照 Active();nil 或 ID 不匹配 → ErrRunNotFound;
// run.cancelFunc() == nil → ErrRunNotCancellable(同步 RecordRun 路径无 cancel——取消其唯一
// 入口是请求 ctx,P5 前不经 CancelRun);否则 cancel() 并返回 nil。
// 取消的落点全在 execute 已有分支:P2-5 的 ①/② select 命中 runCtx.Done → 释放已持有资源 → 
// finalizeRun(RunCancelled)(P2-1 归一保证状态正确)。
```

**新增文件**:无。

**并发与锁论证**:cancel() 在 rt.mu **外**调用(快照后释放);run.cancel 一次写入多个读者(cancelFunc 读在 run.mu 内);取消与完成竞争:谁先到 finalizeRun 都幂等(transition 防重入,run.go:65-67;done 幂等关闭 :73-79);排队取消释放占用权 = execute 失败分支的 gate/pending 释放(总方案第四节「排队取消必须直接释放占用权」)。

**新增测试** `supervisor_test.go` 追加:
- **`TestCancelIsolation`(总方案第十一节 → 本步)**:`WithRunLimits(1, 8)`;A StartRun 挂起(占槽)+ B StartRun(queued)→ `CancelRun("session-B", B.RunID)` → B.Wait = RunCancelled、A release 后 RunCompleted、两 store 各自提交完整(A 不受 B 取消影响);
- `TestCancelRunningRun`:A 挂起中 Cancel → RunCancelled,journal 终态记录 state=="cancelled";
- `TestCancelUnknownRun` / `TestCancelSyncRunRejected`:ErrRunNotFound / ErrRunNotCancellable(RecordRun 路径)。

**验收命令**:
```bash
go test -C ai-runtime -race ./agent/... -count=1
```

**依赖**:P2-5。**P2 完成线(SDK 层)**。

### P2-7 daemon+config:run 配额、RunJournal 接线、启动恢复扫描(行为不变)

**目标**:完成总方案 P2 第 1/4 条的 daemon 落地:并发配额可配(openQuestion ③)、同步路径写 Run 状态 journal、启动时标记 interrupted 的 Run。**daemon 用户可见行为不变**:handleAgent 仍走 withSession+RecordRun(openQuestion ①),新增仅 journal I/O 与 slog。

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/config/spec.go:95-99` | `SessionConfig` 增 `MaxConcurrentRuns int yaml:"max_concurrent_runs,omitempty"`、`MaxPendingRuns int yaml:"max_pending_runs,omitempty"`(勘察修正 F:daemon/internal/app/config/config.go:31 直通内嵌,yaml 自动贯通;0 值省略,旧二进制读新配置安全) |
| `ai-runtime/config/config.go:189-198` `Default` | `Session: SessionConfig{Enabled: true, MaxConcurrentRuns: 4, MaxPendingRuns: 16}`(总方案第四节建议值;daemon app config 默认块 config.go:188-192 未设的字段为 0,由下一行兜底) |
| `daemon/internal/plugins/agent/plugin.go:88-90` `NewPlugin` | `runtimeagent.NewSessionManager(runtimeagent.WithRunLimits(p.cfg.Session.MaxConcurrentRuns, p.cfg.Session.MaxPendingRuns))`——WithRunLimits 对 `<=0` 回落默认(双保险,不依赖载入路径);cfg 注入先于 NewPlugin 现状成立(plugin.go:88 签名已收 cfg) |
| `daemon/internal/plugins/agent/plugin.go:405-419` startSession deps 组装 | `st != nil` 分支增 `p.deps.RunJournal = session.NewRunJournalStore(st)`(新增轻量类型:持 store 引用,按 runID 委派 `repo.OpenRunJournal`/AppendState;编译期断言 `var _ runtimeagent.RunStateJournal = (*session.RunJournalStore)(nil)`);`st == nil`(纯内存)不设——与 deps.Store 的 typed-nil 纪律一致(plugin.go:414-418) |
| `daemon/internal/plugins/agent/plugin.go:380-383` startSession(store 解析后) | `if marked := p.repo.MarkInterruptedRuns(context.Background(), st.SessionID()); len(marked) > 0 { slog.Warn("session: marked interrupted runs", "count", len(marked)) }`——失败仅 warn 不阻塞启动;每个会话边界执行(幂等,P2-2 保证) |
| **明确不动** | `commands.go`(withSession/handleAgent 逐字不变)、`coordinator.go`/`handler.go`(SessionGate/FrontendHandler)、`ai-acp` 全部、`acpworker/acp_worker.go:174-190`(stdio worker 无 repo,journal 为 nil,行为不变)、`PROTOCOL.md`、`plugin.Stop`(manager.Close 不接,理由见 P2-4) |

**新增文件** `ai-runtime/session/journal_store.go`(若 P2-2 未顺带):`RunJournalStore` 聚合类型 + `NewRunJournalStore(*Store)`——按 `store.SessionID()`/`store.dir` 委派;放 P2-2 亦可,此处只为控制 P2-2 步幅。

**新增测试** `daemon/internal/plugins/agent/runjournal_wiring_test.go`:
- `TestStartSessionMarksInterruptedRuns`:临时 Session.Dir 造 legacy 槽 + `runs/run-<nano>-1.jsonl`(末记录 running)→ `NewPlugin`+`Init` → 断言该文件追加 interrupted 记录、其余内容不动;
- `TestAgentRunJournalsThroughDaemon`:`newAgentPluginP`+fixedProvider(plugin_test.go:196-218)跑一轮 `/agent` → 断言当前会话 `runs/` 下出现 `running`+`completed` 两条记录(持久化启用路径);`session.enabled=false` 变体 → 无 runs/ 目录(纯内存)。

**验收命令**(对齐 CI 变体矩阵):
```bash
gofmt -l .   # 必须为空
go test -C ai-runtime -race ./... -count=1
go test -C daemon -race ./... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./mobile/ -count=1
go build -C daemon -tags goworker_no_dispatcher -o /tmp/goworker-agent ./cmd/goworker
```

**依赖**:P2-2、P2-3、P2-5(WithRunLimits)。**P2 完成线(含 daemon)**。

---

## 3. P3 steps(事件回放与 Attach)

### P3-0 事件语义契约(总方案第五节 → 本 Plan 各步的对照表,随 P3-1 落码)

| 总方案第五节要求 | 落实点 |
|---|---|
| Seq 是 Session 维度严格递增游标,跨 Run 不重复 | EventHub 单调 seq(hub.mu 内分配,P3-2);播种自 journal 既有 max seq(P3-2 构造);gate 串行 ⇒ 各 run 文件 seq 区间单调不重叠(P3-1 不变量) |
| 关键事件必须持久化,durable-first | 9 类 key 事件(P3-1 常量)mu 内 journal fsync 成功后才广播;失败不广播(P3-2) |
| 高频 delta 合并落盘(50~100ms 或字节阈值) | delta 3 类进合并缓冲,50ms/4KB 窗合并为一条 `[FromSeq..Seq]` 记录(P3-2);实时广播仍逐事件(per-event seq) |
| 实时与重放「相同事件序号语义」 | 合并记录 Seq=ToSeq、FromSeq 声明范围;回放产出 `SessionEvent{Seq: Seq}`,客户端推进游标到 Seq——范围声明消除「缺口歧义」(P3-1) |
| Race A(回放与订阅间丢事件) | ReadEvents+注册+watermark 同一 hub.mu 临界区,P3-3 |
| Race B(回放与实时重复/乱序) | watermark 过滤(只广播 seq>注册点 latest)+ pump 单线程转发 + lastSeq 去重,P3-3 |
| 持久化失败不得当作可恢复事件广播 | P3-2 key 事件路径 |
| 慢订阅者背压:不阻塞执行/他人,断开,重连从游标补齐 | fan-out 非阻塞投递,缓冲满即断开该订阅者(关闭通道);重连=新 Subscribe(fromSeq=已确认游标),P3-3/P3-4 |
| 游标落在保留历史之前 → ErrCursorExpired,不静默跳过 | Subscribe 窗口校验(fromSeq+1<earliest 或 >latest),P3-3 |

### P3-1 session:EventRecord/SessionEvent 契约 + 跨文件事件读写

**目标**:完成总方案 P3 第 1 条(SessionEvent 与 Seq 游标的存储面)+ 第 2 条(JSONL EventJournal 的读写)。纯 session 包。

**涉及现有符号与改动**(`ai-runtime/session/journal.go` 扩展;`repository.go` 不动):

```go
// 记录信封扩展:与 P2-2 的 "state" 同文件混存(runs/<runID>.jsonl)。
type EventRecord struct {
    Kind      string          `json:"kind"`        // 恒 "event"
    SessionID string          `json:"session_id"`
    RunID     string          `json:"run_id"`
    Type      string          `json:"type"`
    Seq       uint64          `json:"seq"`         // 记录末事件序号(合并记录=ToSeq)
    FromSeq   uint64          `json:"from_seq,omitempty"` // >0 = 合并 [FromSeq..Seq]
    Time      time.Time       `json:"time"`
    Payload   json.RawMessage `json:"payload,omitempty"`
}
// 对齐总方案第五节的传输形状(agent 侧消费):
type SessionEvent struct {
    SessionID string          `json:"session_id"`
    RunID     string          `json:"run_id"`
    Seq       uint64          `json:"seq"`
    Type      string          `json:"type"`
    Time      time.Time       `json:"time"`
    Payload   json.RawMessage `json:"payload"`
}
// 事件类型常量与分类(P3-0 表的常量化):9 类 key(tool_call/tool_result/permission.*/
// assistant.message_finalized/run.*)+ 3 类 delta(text.delta/thinking.delta/usage.progress);
// func IsKeyEvent(typ string) bool
// RunJournalStore 扩展(P2-7 类型):
func (s *RunJournalStore) AppendEvent(rec EventRecord) error
    // 路由到「当前 run」文件:runID 变化时关闭旧句柄再 O_APPEND 打开新文件(首次同 O_EXCL,
    // 复用 OpenRunJournal 的 collision 语义);单写者下无并发 run 文件切换(勘察不变量:gate 串行)
func (s *RunJournalStore) ReadEvents(afterSeq uint64) ([]EventRecord, error)
    // 扫 <dir>/runs/*.jsonl 文件名序拼接(=创建序=nano 序),过滤 kind=="event" && Seq>afterSeq;
    // 坏行跳过(与 loadRecords 同策略);**不**按 SessionID 过滤(勘察修正 I:V1 不过滤,P5 处理)
func (s *RunJournalStore) SeqRange() (earliest, latest uint64, ok bool)
    // 单遍扫描取 min/max(空 runs/ → ok=false);hub 构造播种用
```

**并发与锁论证**:RunJournalStore 自持小锁护「当前 run 文件句柄」;AppendEvent/切换在锁内做文件 I/O(P2-2 同款取舍:每 run 事件量经合并后有限);ReadEvents/SeqRange 只读扫描,与追加并存时按坏行跳过语义安全(读方容忍半行,写方单 Write 整行——store.go:686 同款)。**文件名序=seq 序不变量**:单 Session 单写者 + gate 串行 ⇒ 后启动的 run 的 seq 区间整体大于先前的;文件名 `run-<nano>-<seq>` 的 nano 定宽 ⇒ 字符序=创建序。测试钉死。

**新增测试** `journal_test.go` 追加:`TestEventRecordsCrossFileRoundTrip`(两个 run 文件、Seq 跨文件连续,ReadEvents 全量有序);`TestReadEventsFiltersAfterSeq`;`TestSeqRangeSeedsFromDisk`;`TestEventTailRecovery`(事件文件坏尾行 → ReadEvents 跳过坏行返回合法前缀)。

**验收命令**:
```bash
go test -C ai-runtime -race ./session/... -count=1
```

**依赖**:P2-2、P2-7(RunJournalStore)。后续:P3-2。

### P3-2 agent:EventHub 核心(Publish/Seq/durable-first/delta 合并)

**目标**:完成总方案 P3 第 2 条的写侧与广播核心:Seq 分配、关键事件 durable-first、高频 delta 合并落盘、fan-out(非阻塞)。

**新增文件** `ai-runtime/agent/event_hub.go`:

```go
type EventJournal interface {   // agent 侧消费面(P3-4 进 SessionDeps;session.RunJournalStore 实现)
    AppendEvent(rec session.EventRecord) error
    ReadEvents(afterSeq uint64) ([]session.EventRecord, error)
    SeqRange() (earliest, latest uint64, ok bool)
}
const (
    deltaFlushInterval = 50 * time.Millisecond   // 总方案第五节「50~100ms 或一定字节数」取下限
    deltaFlushBytes    = 4 << 10
    subscriberBuffer   = 256
)
type EventHub struct {
    sessionID string
    mu        sync.Mutex          // 保护以下全部字段
    seq, earliest, latest uint64  // seq=下一分配;播种自 journal.SeqRange(P3-1)
    subs      map[uint64]*hubSub
    nextSubID uint64
    journal   EventJournal
    curRunID  string              // AppendEvent 目标(BeginRun/EndRun 切换)
    deltaBuf  map[string]*pendingDelta  // key=type(runID 内同型合并),攒 50ms/4KB
    closed    bool
}
type IngressEvent struct { RunID, Type string; Payload json.RawMessage }
func NewEventHub(sessionID string, j EventJournal) *EventHub   // 播种;不touch磁盘者: j==nil 合法(纯内存 hub,测试用)
func (h *EventHub) BeginRun(runID string) / EndRun()           // EndRun:flush delta+关当前 run 文件
func (h *EventHub) Publish(ev IngressEvent) error
    // mu 内:closed→ErrHubClosed;seq++;包 SessionEvent;
    // key 事件:j.AppendEvent(fsync)——失败→**不广播**、返回 err(总方案第五节 durable-first,
    // 「持久化写入失败不能继续把事件当作可恢复事件正常广播」);成功→fan-out;
    // delta:先 fan-out(实时逐事件,per-event seq),后进合并缓冲(落盘失败仅 warn——重放缺
    // delta 段可接受,assistant.message_finalized 兜底定稿,注释写明);
    // fan-out:遍历 subs **非阻塞** send;满 → delete+close(sub.live)(断开该订阅者,不阻塞
    // AgentRun/他人——总方案第五节背压;P3-4 测试钉死)
func (h *EventHub) LatestSeq() uint64 / EarliestSeq() uint64
func (h *EventHub) Close() error   // flush+closed 置位+关 journal 当前句柄;Close 后 Publish/Subscribe → ErrHubClosed
```

**并发与锁论证**:hub.mu 全程保护 seq/subs/journal 目标/deltaBuf;Publish 的 fsync 在 mu 内(V1 取舍:正确性优先,delta 合并把写频率压到 ≤20 次/s,key 事件低频;若生产不达标,P5 优化为「mu 内定槽、mu 外写、写完补广播」——注释记录,本 Plan 不做)。fan-out 的 close(sub.live) 在 mu 内合法(Go channel close 无锁竞争)。无嵌套获取:hub.mu 内不触 gate/runSlots/manager.mu/rt.mu → 无锁环。

**新增测试** `event_hub_test.go`:
- `TestEventHubSeqMonotonicAcrossRuns`:BeginRun 切换后 seq 连续不重复(跨 Run 游标);
- `TestKeyEventDurableFirst`:stub journal AppendEvent 注错 → Publish 返回 err 且**无订阅者收到**;成功路径:journal 先于广播(以 stub 里的顺序钩子断言);
- `TestDeltaMergeWindow`:50ms 窗内 100 条 text.delta → journal 恰 1 条合并记录(Seq=最后一条、FromSeq=第一条)、订阅者仍逐条收到 100 条(实时不合并)。

**验收命令**:
```bash
go vet -C ai-runtime ./agent/...
go test -C ai-runtime -race ./agent/... -count=1
```

**依赖**:P3-1。后续:P3-3、P3-4。

### P3-3 EventHub:Subscribe Replay+Live 原子切换(防两个 Race)+ ErrCursorExpired

**目标**:完成总方案 P3 第 3 条(EventHub Replay+Live 原子切换)与第 4 条的 CursorExpired/断线清理;验收测试 TestAttachReplayNoGap。

**涉及现有符号与改动**(`ai-runtime/agent/event_hub.go` 扩展):

```go
var ErrCursorExpired = errors.New("session: event cursor expired")
type hubSub struct {
    live      chan SessionEvent   // fan-out 直投(容量 subscriberBuffer,非阻塞)
    watermark uint64              // 注册点 latest:live 广播只含 seq>watermark 的事件(时序上天然成立)
}
func (h *EventHub) Subscribe(ctx context.Context, fromSeq uint64) (<-chan session.SessionEvent, error)
// mu 内一口气完成(总方案第五节「订阅注册与确定回放边界组合成一个原子逻辑操作」= 防 Race A):
//   ① closed→ErrHubClosed;窗口校验:fromSeq>latest 或 (fromSeq+1<earliest 且 fromSeq!=0)
//      → ErrCursorExpired(fromSeq=0 = 从保留最早开始;游标超前=客户端声称收到未发事件,同样明确报错);
//   ② replay := j.ReadEvents(fromSeq)(mu 内文件读——V1 取舍:阻塞 Publish 数 ms,注释记录);
//   ③ 注册 sub(watermark=latest)+ nextSubID++;
//   ④ 起 pump goroutine,返回只读 chan(容量 subscriberBuffer):
//      阶段一:逐条投递 replay(select ch<-ev / <-ctx.Done() / <-h.done);
//      阶段二:range sub.live 转发;lastSeq 哨兵去重(skip ≤,防 Race B 的 belt-and-braces);
//      任一阶段 ctx 取消/Close → mu 内 removeSub + 退出(断线清理)。
// 顺序论证:replay 快照止于注册点 latest;此后 Publish 的事件 seq 恒 >latest 且 fan-out 只投
// sub.live ⇒ pump 先发完 ≤latest 的 replay 再转发 >latest 的 live:无缝、无重、无乱序。
```

**新增文件**:无。

**并发与锁论证**:①-③ 同一 mu 临界区 ⇒ 「回放边界」与「订阅可见性」原子(Race A:查询历史到 103 后才注册、104 已产生的丢失窗口不存在);pump 单线程消费两源(replay 快照 + live 通道)且去重 ⇒ 无重复无乱序(Race B);replay 投递只阻塞 pump 自身(缓冲 256,慢消费不反压 hub——满的是 fan-out 侧才会断开,pump 侧 ctx 感知)。

**新增测试** `event_hub_test.go` 追加:
- **`TestAttachReplayNoGap`(总方案第十一节 → 本步)**:单 hub,N 轮 BeginRun/Publish(混合 key+delta,含合并窗)/EndRun;M 个订阅者在随机时刻以随机 fromSeq(含 0、含 latest)Subscribe;各收集到 Close;断言:每个订阅者收到的 seq 严格覆盖 (fromSeq, latest] 无缺口无重复(合并记录按 Seq 记账、FromSeq 只作范围声明);`-race` 下跑;
- `TestSubscribeCursorExpired`:fromSeq 超前 latest / 落于 earliest 之前 → ErrCursorExpired;0 与区间内 → 成功;
- `TestSubscribeAfterClose` → ErrHubClosed;`TestSubscribeCtxCancelCleansUp`:取消后 hub.subs 不残留(经 LatestSeq 之外的内部断言/计数)。

**验收命令**:
```bash
go test -C ai-runtime -race ./agent/... -count=1
```

**依赖**:P3-2。后续:P3-4。

### P3-4 supervisor 接线:RunCallbacks→SessionEvent tee + 每会话 EventHub

**目标**:完成总方案 P3 第 3/4 条与运行面打通:StartRun 的执行流产出事件进 hub(journal+广播),背压断开在真实链路生效;验收 TestSlowSubscriber。

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/agent/spec.go` | `SessionDeps` 增 `RunEvents EventJournal // nil = 不启用(P2+P3 的 daemon 不接线,openQuestion ④;P5 随 x-goworker 协议接入)` |
| `ai-runtime/agent/runtime.go:17-32` | SessionRuntime 增 `events *EventHub`(惰性:首次 StartRun 且 `Session().deps.RunEvents != nil` 时创建;RecordRun **不**创建/不写事件——同步路径行为零变化,边界见 §6) |
| `ai-runtime/agent/supervisor.go` | StartRun 同步段:hub 惰性创建后 `hub.BeginRun(run.ID)`;cb 包装 `cb = teeRunCallbacks(cb, hub, run.ID)`(下表);execute 尾部 defer `hub.EndRun()`(flush delta+关文件);execute 的 runCtx 取消/失败路径照旧(journal 状态记录与事件记录互不替代) |
| `supervisor.go` 新增 `teeRunCallbacks` | 事件映射表(P3-0,源自 streamTokens 实际流 session.go:213-249):`EmitToken`:TokenTypeToolCall→`tool_call`(key)、TokenTypeToolResult→`tool_result`(key)、文本 Done token→`assistant.message_finalized`(key)、TokenTypeThinking 增量→`thinking.delta`、其余文本增量→`text.delta`(payload=token JSON);`Decide`:调用前发 `permission.requested`(payload=InterruptRequest)、返回前发 `permission.resolved`(payload=Decision)——hitl 决策等待本身不包进 hub 锁(:229-244 的 decisions 通道链路不动);`Publish`:runtimeconfig.EventUsage→`usage.progress`、EventIteration→`usage.progress` 同型高频;`Write`:**不**进 journal(终端文本,由 message_finalized 覆盖) |

**新增文件**:无。

**并发与锁论证**:tee 在 supervisor goroutine 上同步调用(Publish 的调用方即 execute),与 StartRun 返回后的调用方无共享可变状态(**cb 所有权转移**:StartRun 返回后调用方不得再触碰 cb——写进 StartRun 注释);hub.mu 内只做非阻塞 send/文件写,execute 持有的 gate/runSlots 与 hub.mu 无嵌套(hub 不回触 runtime)⇒ 慢订阅者满缓冲的断开路径不可能卡住 execute(P3-2 机制在真实链路成立)。

**新增测试** `supervisor_test.go`/`event_hub_test.go` 追加:
- **`TestSlowSubscriber`(总方案第十一节 → 本步)**:StartRun 全链(gatedProvider 挂起中灌大量 EmitToken)+ 订阅者 Subscribe 后**不读** → 断言:①该订阅者通道最终被关闭(断开);②Publish/execute 不阻塞(run 正常 RunCompleted,耗时上界断言);③其他正常消费的订阅者收全事件;
- `TestStartRunEmitsJournalEvents`:RunEvents=真 journal(RunJournalStore)→ 跑一轮 → `ReadEvents(0)` 断言:tool_call/tool_result/assistant.message_finalized 落盘(key 事件 durable-first)、delta 呈合并记录、Seq 全局连续。

**验收命令**:
```bash
go vet -C ai-runtime ./agent/...
go test -C ai-runtime -race ./agent/... -count=1
```

**依赖**:P3-2、P3-3、P2-5。后续:P3-5。

### P3-5 生命周期收口 + 全量验收(零 daemon 改动确认)

**目标**:完成总方案 P3 第 4 条的断线/关闭语义收口:EventHub 随 runtime/manager 生命周期关闭;确认 P3 全程 daemon 零改动(openQuestion ④);P3 完成线。

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/agent/runtime.go` | 新增 `func (r *SessionRuntime) closeEvents()`:events 非 nil → `EndRun()+Close()`(幂等) |
| `ai-runtime/agent/manager.go` `Close`(P2-4 形态) | 「等全部 active run 终态」之后、「清表」之前:逐 runtime `closeEvents()`——次序即论证:**先等 run done 再关 hub**,执行中的 Publish 不可能写已关闭的 journal;Close 后 hub 拒绝新 Subscribe(ErrHubClosed) |
| **明确不动** | daemon 全部(含 plugin.go——P2-7 之后无新改动)、ai-acp、acp service、acpworker、PROTOCOL.md;session/load 重放仍走 store 面(sessions.go:147-203);`x-goworker/*` 协议不出现 |

**新增测试**:
- `TestManagerCloseClosesEventHubs`:两个 runtime 各带 hub(一个在途 run)→ Close → 等待 run 终态后 hub Closed(Publish→ErrHubClosed)、journal 当前句柄已关(重读文件完整);
- 回归确认:`go test -C daemon -race ./...` 全绿即「P3 未触碰 daemon」的机器证明。

**验收命令**(P2+P3 全量收口,与 CI 等价):
```bash
gofmt -l .                                  # 必须为空
go test -C ai-runtime -race ./... -count=1
go test -C daemon -race ./... -count=1
go test -C daemon -race -tags goworker_no_agent ./... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./... -count=1
go test -C daemon -race -tags "goworker_no_agent,goworker_no_dispatcher" ./... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./mobile/ -count=1
go build -C daemon -tags goworker_no_dispatcher -o /tmp/goworker-agent ./cmd/goworker
```

**依赖**:P3-4。**P3 完成线**。

---

## 4. 总方案第十一节测试 → step 分配(本 Plan 范围)

| 测试 | 分配 | 模拟 Provider 依赖 |
|---|---|---|
| `TestRunSurvivesDisconnect` | **P2-5** | gatedProvider 挂起 + 提交方提前返回(API 形状保证 runCtx 源自 manager.ctx,非请求 ctx) |
| `TestCancelIsolation` | **P2-6** | gatedProvider 双会话编排,槽=1 制造 queued |
| `TestRestoreInterruptedRun` 完整版 | **P2-2**(存储层完整语义:尾恢复+interrupted 标记+历史可读+零自动重跑);P2-7 附 daemon 启动接线薄测试 | 无 Provider 参与恢复(纯标记,记录数断言钉死「不自动重跑」) |
| `TestAttachReplayNoGap` | **P3-3** | 无 Provider(直接灌 hub);-race 并发压测 |
| `TestSlowSubscriber` | **P3-4** | gatedProvider 全链,订阅者不读 |
| Commit 失败注入(journal 对偶,不记 completed) | **P2-3** | fakeConversationStore.commitErr |
| (P0 已有)TestConcurrentSessions/TestSameSessionBusy/TestCommitFailureNotCompleted | run_lifecycle_test.go:207/278/344,本 Plan 保持全绿 | gatedProvider |
| `TestApprovalAfterReconnect` / `TestToolBindingDisconnect` | P4(总方案范围,本 Plan 不含) | — |

---

## 5. 风险与回滚链

1. **StartRun 是 P2 新增公共 API 但 daemon 无调用方**:API 形状(无 ctx 参数、cb 所有权转移、RunHandle 观察面)只经测试校验;P5 接前端时可能调整——已知的调整点:runCtx 需注入 sessionGateKey 重入标记(commands.go:28-50,否则后台 Run 内嵌套 hub.Eval 死锁,勘察风险),该注入属 P5 集成步,本 Plan 在 StartRun 注释中预留契约说明。回滚:revert P2-5/P2-6,RecordRun 路径完全不受影响。
2. **fsync 在 hub.mu 内(P3-2)**:正确性优先的 V1 取舍;delta 合并把写频率压到 ≤20/s,但 key 事件密集的工具循环(每轮 tool_call/tool_result 各一条)仍受 fsync 延迟约束。已知限制,注释+本条留档;P5 优化路径(mu 外写+序号槽)已写注释。
3. **RunID 依赖 UnixNano(P2-1)**:NTP 回拨理论碰撞面;O_EXCL(P2-2)把碰撞变成可观测的 ErrRunExists 而非静默覆盖,run 以无 journal 状态继续(warn)。可接受偏差,记录于 run.go 注释。
4. **Journal/事件不可用时的降级语义**:Run 状态与 key 事件写失败均 warn/拒广播,不阻断执行——与「宁拒勿双写」不冲突(拒的是写,不是执行);重放视角下缺口显式(状态文件缺终态→恢复标记 interrupted;事件缺记录→客户端游标停在最后成功处)。总方案第十一节「Conversation Commit 失败不能记录 RunCompleted」由 P2-3 测试钉死。
5. **P2-7 config 载入路径的残余不确定**:daemon app config 直通 runtimeconfig.SessionConfig(config.go:31)已核实,但 `config.yaml` 反序列化对新字段的容忍仅由 yaml omitempty+构造点 `<=0` 回落双保险保证;若 daemon 另有白名单式字段校验(勘察未发现),实现时对齐 `session.dir` 先例(config.go:311)。
6. **ReadEvents/SeqRange 全文件扫描(P3-1/3-2)**:Subscribe 成本随 run 文件数线性增长;V1 以测试规模验收,生产规模(长会话高频事件)需 retention/快照——总方案第五节末段「Run 完成后将高频 Delta 压缩成最终内容记录」显式延期 P5+,本 Plan 不做修剪,ErrCursorExpired 的产生方暂只有「游标超前」。
7. **已知边角保留(勘察修正 H/I)**:`/new` 后旧 store 滞留 repo 缓存(plugin.go:271-276)P2/P3 **不解决**——且 P2-2 的 custody 与 P5 的「旧 runtime 留在 manager」语义一致依赖该滞留;`Archive` 换 SessionID 后 `runs/` 归属歧义同样保留(记录带 SessionID 供诊断,V1 读取不过滤)。两者均在 P5 清单。
8. **提交与回滚链**:每 step 一个独立提交,可独立 revert;任意前缀回退保持可构建:`P3-5 → P3-4 → P3-3 → P3-2 → P3-1 → P2-7 → P2-6 → P2-5 → P2-4 → P2-3 → P2-2 → P2-1`。落盘新格式(runs/ 文件)对旧代码不可见(旧代码不扫 runs/),回滚无迁移负担;P2-7 的 config 字段 omitempty,回滚后新配置文件仍可被旧二进制读取。review 重点:P2-5(execute 的资源释放矩阵:pending/gate/slot/active 四项在三条出口路径——成功/排队取消/门禁失败——各恰好一次)与 P3-3(mu 临界区的 ①-③ 次序)。

---

## 附录 A:P2/P3 勘察纪要(2026-10-26,commit c3def23)

### A.1 ctx 分离点(逐跳核实,非文档推断)

`ai-acp/protocol/jsonrpc.go:280` 每请求 ctx(Background 派生,连接 Close 全部取消,:505-514)→ `ai-acp/server.go:317` runCtx 注册 s.cancels(session/cancel :368-376、server 关闭 cancelAll :352-362 均触发)→ `daemon/internal/core/service/acp/handler.go:214` `model.Context{Ctx: 请求 ctx}` → `engine/engine.go:264` 透传 → `commands.go:65` `withSession(ctx.Ctx)`(排队也监听请求 ctx,runtime.go:46-53)→ `session.go:149` `agentCtx=WithCancel(ctx)`。**结论**:连接断开/session/cancel/daemon 停机三事件今天都直接取消 Agent 执行;P2 的分离动作全部落在 agent 包内(StartRun 用 manager.ctx),ai-acp 与 acp service 层无需改动。另一条平行链:stdio worker `acpworker/acp_worker.go:174-190` 直接用 dispatcher 请求 ctx、无 runtime 记账(:182 注释),P2/P3 不触碰。

### A.2 接缝清单

1. **RecordRun→StartRun**:RecordRun(runtime.go:110-138)已是「记账骨架」,StartRun=同步段(占 active+pending+queued journal)+execute(gate→runSlots→Run→finalize);二者经 finalizeRun 共享终态化;withSession 服务 7 个命令但只有 /agent 是 Run,StartRun 不经 withSession。
2. **manager 预留**:Register/Remove 预留即用;Close 的「取消宿主 ctx+拒绝 Register」由 P2-4 兑现(manager.go:67-68 注释即计划);runSlots/pending 为 P2-5 新字段。
3. **Run Journal 落点**:`<root>/<id>/runs/<runID>.jsonl` 与 P1 布局天然契合;List 只认 `*/current.jsonl`(repository.go:202-206)零冲突;Delete=RemoveAll 会连带 runs/(custody 论证保证安全);store 的 lastGoodEnd 尾恢复(store.go:777-816)是 Journal 恢复的直接先例。
4. **EventHub 生命周期**:SessionRuntime 无 events 字段(现状);hub 惰性创建+BeginRun/EndRun 随 run 切换 journal 文件;Close 链=Manager.Close→closeEvents(先等 run done 再关 hub);/new 后旧 runtime 留在 manager,其 hub 随 Manager.Close 统一收口——「旧 runtime 无人 Close」的现状语义(P1-3)被 P3 复用而非放大。
5. **P3 Attach 与现有守卫**:SessionGate/coordinator+superseded(handler.go:192-199)+单活动语义是 P5 前的秩序;EventHub.Subscribe 在 P2+P3 只有 SDK 测试消费;x-goworker 协议不出现;session/load 重放保持 store 面不变。

### A.3 勘察引用的验证命令与输出

见 §0.1(4 条命令全部实际运行,全部通过);`grep` 核验:全仓无 EventHub/Journal/Supervisor/StartRun/runSlots 的任何实现(grep 仅命中 P0/P1 注释里的 P2 预留,ai-runtime/agent/manager.go:11-12、runtime.go:109、run.go:12、run.go:101、acp_worker.go:182);`gatedProvider`/`fakeConversationStore`/`newAgentPluginP` 均实读确认。未运行:`-race` 全仓矩阵与 `build-aar.sh`(无 Android 工具链时报告未运行,验收步含命令)。

## 附录 B:勘察 openQuestions 原文(P2/P3 勘察产出,拍板记录见 §0.3)

1. ① P2 阶段 daemon 的 handleAgent 是否切换到 StartRun(还是保持 RecordRun 同步路径、StartRun 仅测试验证,前端迁移属 P5);
2. ② EventJournal 与 Repository 的目录/生命周期整合方式(锁归属、Archive 轮转、句柄托管、Store.dir 访问);
3. ③ runSlots 配置的落点(runtimeconfig 新增段?排队语义:排队发生在 runSlots 等待而 gate 已持有,还是 gate 等待即排队);
4. ④ P3 完成后 daemon 是否有任何行为变化(session/load 重放是否切 Journal、usage/iteration 是否并入 SessionEvent 流水)。
