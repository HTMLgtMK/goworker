# P4+P5 代码级实施 Plan:HITL 与设备工具 + 前端协议(含 ai-core 错误通道修正与遗留收口)

> 依据 `docs/multi-session-runtime-plan.md`(总方案)第六节(HITL)、第七节(ACP 扩展)、第八节(工具能力)、第四节(并发)、第五节(事件语义)、第十节 P4/P5 检查表、第十一节验收测试,以及 2026-10-26 对分支 `feature/multi-session-p0p1`(commit a9e4b0d)的 P4/P5 代码勘察(纪要已并入本文 §0 与附录 A)。
> 前置:P0+P1(`docs/multi-session-p0-p1-implementation.md`)与 P2+P3(`docs/multi-session-p2-p3-implementation.md`)已落地并全绿。勘察发现与总方案不符之处按实际代码修正,全部标注「勘察修正」;需主 agent 拍板的设计决策在 §0.3 记录采用的默认(勘察建议),附录 B 附 openQuestions 原文。
> 范围边界:Android App(/Users/tinguo/Work/goworker-android,独立仓库,头 commit c761cd6)全部改动列为**仓库外依赖**;本 Plan 只覆盖 daemon 侧协议 + `daemon/mobile/PROTOCOL.md` + Go 集成测试。标准 ACP 方法语义逐字不动(总方案第七节明令)。

## 0. 勘察事实基线

### 0.1 P0–P3 落地现状(本次勘察实测)

```bash
# 本次实际运行,全部通过:
go version                     # go1.27.1 darwin/amd64
gofmt -l .                     # 空(P0-P1 plan §5 风险 7 的 worktree 残留已消失)
go build -C ai-runtime ./...   # BUILD_OK
go build -C daemon ./...       # BUILD_OK
go build -C ai-core ./...      # BUILD_OK
go test -C ai-runtime -race ./agent/... ./session/... -count=1
#   ok  github.com/tinguo/goworker/ai-runtime/agent   10.923s
#   ok  github.com/tinguo/goworker/ai-runtime/session 16.713s
go test -C daemon -race ./internal/plugins/agent/... ./internal/core/service/acp/... ./mobile/ -count=1
#   ok  github.com/tinguo/goworker/daemon/internal/plugins/agent    8.110s
#   ok  github.com/tinguo/goworker/daemon/internal/core/service/acp 2.945s
#   ok  github.com/tinguo/goworker/daemon/mobile                    4.557s
```

`x-goworker` 全仓 grep 仅命中三份 plan 文档,零代码——P5 是纯增量协议面。P2+P3 内核(StartRun/EventHub/Journal)就位且 daemon 未接线 RunEvents(`ai-runtime/agent/spec.go:96`、`daemon/internal/plugins/agent/plugin.go:437-440` 仅设 RunJournal)。

### 0.2 本 Plan 依赖的勘察结论(与总方案的差异/接缝)

| # | 勘察结论 | 对本 Plan 的影响 |
|---|---|---|
| A | **ai-core 错误通道缺口(P2-1 发现,本 Plan 纳入修正)**:`ai-core/agent/agent.go:43` 的 Run 第三返回值恒 nil(:175);provider 错误 4 处入口(:82,88,93,107)经 `emitAgentNote`(:309-312)转 note token(TokenTypeText+Done=true)并 append 进 messages 历史;`runErr` 仅经 `AfterAgentEvent{Err}`(:55)漏出,且 defer 内 fire 先于 `msgCh <- messages`(:53-58)。`Session.Run`(session.go:159-217)收到含 note 的历史照常 Commit → 返回 **RunCompleted+Persisted=true**;journal 终态记 completed、`commands.go:81` debug 日志失真;execute 层取消归一(supervisor.go:235-241)只兜 runCtx,兜不住 provider 失败。`run_test.go:75-80` 已有 skip 子测试预约本修正(「ai-core 错误通道修正后改为真实断言」)。 | **AC-1**:capture 中间件方案,ai-core 零改动;note 仍 commit(零可见变化);skip 子测试替换为真实断言 |
| B | **note token 渲染面(逐一核实)**:CLI 只注入 WriteToken(stdin.go:300-303,handleToken :128-144)→ KindText 普通文本;ACP 主前端 EmitToken→`TokenToUpdate` default→agent_message_chunk(mapping.go:90-95;handler.go:219-221);stdio worker 同映射(acp_worker.go:174-176);mobile=Kotlin 消费同一 session/update 流(PROTOCOL.md:99-102),Go 侧无独立渲染代码;事件 tee 把 note 的 Done token 映射 `assistant.message_finalized` key 事件(supervisor.go:349-354)——错误与正常定稿在事件流不可区分;`EventRunError` 常量已备无产生方(journal.go:111) | AC-1 行为面=零可见变化;run.error 事件归 P5-2 |
| C | **HITL 决策链全貌**:中间件 confirm() 发 interrupt token 后**同步阻塞** GetDecision(middlewares/hitl.go:365-384),ExpiresAt 为唯一超时事实源、超时/取消/通道关闭一律 reject(hitl/channel_decision_provider.go:12-38,DefaultTimeout=2min,hitl/spec.go:19);session.go streamTokens :238-252 解码→`cb.Decide`→decisions chan(:141,:248-251);决策面三来源:CLI stdin.go:202-231、ACP `model.DecideViaACP`(permission.go:102-122,出错一律 reject;handler.go:224)、stdio acp_worker.go:179。`RunWaiting` 未定义(run.go:24 注释预约 P4);interrupt id=`req-N` 进程级 atomic(hitl.go:68,重启归零) | P4-1/P4-2:cb.Decide 换 ApprovalManager.Wait 即完成总方案 §6 适配,ai-core/中间件零改动;requestID 沿用 req.ID,V1 审批不持久化故无跨重启碰撞 |
| D | **x-device 工具面**:方法常量 `x-device/tools|call`(acpworker/sys_relay.go:24-26)、能力 key `x-device`(:38)、探测失败即收缩(:72-90,ProbeTimeout 2s :33)、`RelayDeviceTools` Execute→`caller.Call`(:121-159);调用时机=每轮 prompt 工具收集(session.go:119-121,总方案 §8 对 ToolSource「只调一次」的批评与代码相符);执行包在 ai-core **toolCtx 60s 硬超时**(agent.go:147-153);断连现状=Call 报错进 tool result(agent.go:151-153),无 waiting、无重绑校验。应用层扩展不进 ai-acp/protocol(sys_relay.go:5-8 先例) | P4-3/P4-4 代理面 SDK 化;P5-9 daemon 切换(显式声明可见变化);等待预算必须 <60s |
| E | **P5 守卫与解析点**:SessionGate 全 daemon 一份(app/service.go:127-134),Prompt 持有贯穿 evaluate(handler.go:184-190),CreateSession/load-current tryAcquire+busy(:115-131,:251-270),superseded 守卫(:192-199)读 `activeTransportSession`(plugin.go:546-552,:568-572);current 解析三件套 currentSession/currentStore/boundSession(plugin.go:101/:122/:107),plugin.go:96-100 注释预留「P5 换 manager.Get(sid)」;model.Context 无 SessionID 字段(Values map 可携,types.go:113) | 标准 ACP 面逐字保留;x-goworker 走可选能力接口(type-assert,SessionSource 先例 capability.go:61-78) |
| F | **协议承载面**:`Peer.Handle/HandleNotification/Notify/Call` 全部导出(peer.go:86/:91/:101/:96),Server 内嵌 *Peer(server.go:101)——x-goworker 方法可在 daemon 侧经 `server.Handle(...)` 注册,**ai-acp 零改动**;固定 6 方法注册(server.go:166-171);每请求独立 ctx(jsonrpc.go:281-283)、连接 Close 全取消(:505-510);`aiacp.Server.Notify` 已可发任意方法通知(server.go:383-395) | P5-7/P5-8 的注册与通知面零标准面侵入 |
| G | **连接生命周期**:主会话 ACP 每 accept 一连接一 FrontendHandler(service.go:180-203),断开→`server.Close`→cancelAll+prompts.Wait(server.go:350-366);**mobile 一个 Host=一个 app.Application,Host.Close→appl.Stop→整个 Engine/插件停机**(host.go:269-295、application.go:182-197)——Android「断连」即后端销毁,后台 Run 存活依赖 Android 侧 RuntimeService(仓库外);handler 无连接退出钩子(需新增可选 Close 接口) | P4/P5 的 TestRunSurvivesDisconnect 语义在 Android 形态受限于进程死亡;P5-7 补 handler.Close 钩子 |
| H | **P2+P3 遗留边角(勘察评估)**:①`/new` 后旧 runtime 留 manager(plugin.go:473-475,daemon 无任何 manager.Remove 调用点)且旧 store 滞留 repo 缓存(实例唯一不变量,repository.go:236-239 Delete 必拒);②runs/ 归属歧义(Archive 同目录换 SessionID store.go:556;记录带 SessionID journal.go:44/:75;ReadEvents 明确不过滤 journal_store.go:167-169);③重启连续性 SDK 已闭环(RunID run.go:114-124、恢复扫描 plugin.go:394-400、seq 播种 event_hub.go:186-192),缺口仅「重启后 attach 时 hub 未建」(bindEvents 仅 StartRun 触发,runtime.go:132-153);④retention 无修剪(EarliestSeq 恒播种值 event_hub.go:309-314),Subscribe/attach 全扫描随 run 数线性涨 | ①②纳入 P5(P5-5/P5-3);③协议面解决(attach 即建 hub,P5-6 EnsureHub);④P5 不做(拍板④,§6 风险 8 留档) |
| I | **测试基建**:gatedProvider(run_lifecycle_test.go:104-125)、fakeConversationStore、newFakeStoreSession(:139)、captureProvider 含 chatErr 注入点(test_helpers_test.go:11-33)、newAgentPluginP+fixedProvider(plugin_test.go:197-222);supervisor_test.go 11 个测试、event_hub_test.go 7 个测试(grep -c 实测);handler_test.go 有 io.Pipe 全栈与 fake caller 模式(:376-472) | 全部验收测试用模拟 Provider,零真实 LLM;P5 协议测试复用 io.Pipe 模式 |
| J | **config 直通**:daemon app config 内嵌 `runtimeconfig.SessionConfig`(app/config/config.go:31,ApplyRuntime :164)——SessionConfig 新字段 yaml 自动贯通;既有字段 MaxConcurrentRuns/MaxPendingRuns(ai-runtime/config/spec.go:104-105,Default config.go:197-199)是同型先例 | P4-4 的 capability_wait_timeout 沿用该模式 |
| K | **manager API 现状**:daemon 对 manager 只用 Register/Get(隐式经 plugin),List/Remove 无调用点;StartRun/CancelRun 无 daemon 调用方(P2P3 plan openQuestion①:前端迁移属 P5——即本 Plan) | P5-5 的 Remove 收口与 P5-6 的 StartRun 接线都是首用 |

### 0.3 openQuestions 拍板记录(采用勘察建议默认,主 agent 可推翻)

| # | 决策 | 本 Plan 采用 | 推翻时的波及面 |
|---|---|---|---|
| ① | ai-core 修正形态 | **capture 中间件**(读 AfterAgentEvent.Err,ai-core 零改动);错误 note 仍 commit 进对话历史(三渲染面零可见变化);`run.error` 事件随 P5-2 落码(常量 journal.go:111 已备) | 改 Agent.Run 签名/新增 token 类型 → tee 映射、三渲染面、ai-core 全部 Run 测试联动重做 |
| ② | Control Lease 宿主授权 | **V1 简化形态**:本机信任(单用户 daemon,无账号体系)、每会话至多一个 control 持有者、断开即释放(handler.Close)、后到 control 显式接管(tookOver=true+slog 审计);approval/respond 与 run/start|cancel 需 holder。依据总方案 §7 :612「第一版可以采用较简单的宿主授权策略」 | 完整 AttachMode 过期/授权协商 → P5-7/P5-8/P5-9 的 lease 面重设计,PROTOCOL.md 相应章节作废 |
| ③ | capability 等待默认时长 | `capability_wait_timeout` 默认 **45s**(SDK 常量兜底 + SessionConfig 字段),硬约束:必须 < ai-core toolCtx 60s(agent.go:147-153),否则等待被掐断呈现为普通超时 | 若拍板 >60s → 必须同步放宽 ai-core 工具执行超时或把等待移出 Execute,属设计变更非参数变更 |
| ④ | EventJournal retention | **P5 不做修剪**:本期「游标不过期」是 attach 的卖点之一;ErrCursorExpired 产生方仍仅「游标超前」;增长风险 §6 风险 8 留档,P6+ 再议 | P5-3 顺带加修剪步(终态 run journal 按期限/条数),需新增 retention 配置 |
| ⑤ | V1 路由面 | **仅 current 会话**:run/start|attach|approval 限 `CurrentSessionID()`;归档历史走既有 session/load 只读重放(sessions.go:147-203);/new 收口 Remove 旧 runtime+Close 旧 store(遗留①)。依据:V1 单活动 Run 会话模型(P2P3 §6),多 live 会话需 Session 重建/cwd/授权全链扩面 | repo.Open 任意会话 + Session 重建路由 → P5-5/6/8 扩面,attach 需 per-session cwd/授权快照 |
| ⑥ | x-goworker 能力通告 | **不做 agentCapabilities 通告**(V1):客户端按 -32601/method-not-found 降级(JSON-RPC 标准行为,x-device 同款,PROTOCOL.md:167-168) | ai-acp protocol InitializeResponse 扩展字段 → 标准面触碰,需谨慎评估 VS Code 兼容 |
| ⑦ | 同步 /agent 是否切 StartRun | **不切**(P2P3 openQuestion① 延续到期维持原判):session/prompt 保持阻塞同步(总方案 §7 明令),x-goworker/run/start 是增量入口,两入口靠 runtime gate 互斥 | 切换 → SessionGate busy 语义与 PROTOCOL.md:40-47 同步修订,P2P3 §6 行为边界作废 |

---

## 1. 目标形态(AC+P4+P5 完成后)

```
ai-runtime/agent/
  errcapture.go     # AC-1 新增:AfterAgentEvent.Err 捕获中间件
  approval.go       # P4-1/P4-2 新增:ApprovalManager(Pending/Wait/Resolve)+ decideFunc
  capability.go     # P4-3/P4-4 新增:CapabilityRegistry/ToolCapabilityBinding/ClientToolProxy
  run.go            # P4-1:+RunWaiting;P4-4:+RunWaitingCapability
  manager.go        # P4-1/P4-3:+approvals/capabilities 字段与访问器;P5-1:+WithRunContext/runCtxValues;P4-2:+PendingApprovals/ResolveApproval
  supervisor.go     # P4-2:审批回退+waiting 迁移;P4-4:观察者注入;P5-1:runCtx values;P5-2:run.* 事件
  runtime.go        # P5-6:+Hub()/EnsureHub() 导出访问器
ai-runtime/config/
  spec.go/config.go # P4-4:+CapabilityWaitTimeout(默认 45)
ai-runtime/session/
  journal_store.go  # P5-3:ReadEvents/SeqRange 按 SessionID 过滤;P5-6:+LastRunState
daemon/internal/core/model/
  capability.go     # P5-6:+RunControlSource;P5-9:+ClientToolBinder/DeviceCaller
daemon/internal/plugins/agent/
  plugin.go         # P5-1:WithRunContext;P5-4:deps.RunEvents;P5-9:实现 ClientToolBinder
  runcontrol.go     # P5-6 新增:RunControlSource 实现(requestId 幂等表/control lease/StartRun 包装)
  commands.go       # P5-5:handleNew 收口 manager.Remove+旧 store Close
daemon/internal/core/service/acp/
  xproto.go         # P5-7 新增:x-goworker 方法常量与 wire 类型
  xgw.go            # P5-7/P5-8 新增:FrontendHandler x-goworker handlers(连接注册/泵/lease 检查)
  handler.go        # P5-7:工厂 connectionID;P5-9:deviceToolSource 走 binder
  service.go        # P5-7:accept 注册扩展方法 + handler.Close 钩子
acpworker/sys_relay.go # P5-9:+SpecDeviceTools(descriptor→ClientToolSpec,与 RelayDeviceTools 共用 metadata 映射)
daemon/mobile/PROTOCOL.md # P5-10:x-goworker 扩展协议节 + 行为变化声明
```

**行为变化总览**(P4 全程为零;P5 逐项):

| step | 用户可见变化 |
|---|---|
| AC-1 | 零(note token 三渲染面逐字节不变;变化仅 RunResult/journal 终态/debug 日志) |
| P4-1..P4-4 | 零(daemon 零改动,SDK 能力由测试证明) |
| P5-1..P5-6 | 零(WithRunContext 无调用方;RunEvents 接线后同步路径仍不建 hub;ReadEvents 过滤对现网数据流不可见;/new 收口不改输出;RunControlSource 无协议暴露) |
| P5-7/P5-8 | 新协议面(增量能力,非破坏;标准 6 方法与 session/update/request_permission 逐字不动) |
| P5-9 | **P5 显式变化 #1**:声明 x-device 的客户端断连后,设备工具从「立即报错给模型」变为「等待重绑(默认 45s)→ 明确失败或重绑成功」;VS Code 等未声明方零变化 |
| P5-10 | 文档+集成测试 |

---

## 2. AC steps(ai-core 错误通道修正)

### AC-1 错误捕获中间件:provider 错误经流式出口不再记 RunCompleted

**目标**:补齐 P0-3「模型成功 ≠ 持久化成功」的最后缺口(勘察结论 A):provider 失败的 run 终态必须记 RunFailed/RunCancelled 而非 RunCompleted。ai-core 零改动(错误通道的修正落在消费侧,保持 ai-core 公共面不动)。

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/agent/session.go:263-309` `buildMiddlewareChain` | 签名追加参数 `cap *runErrorCapture`(Run 构造后传入);链尾(mws 最终 append 前)追加 cap——AfterAgent hooks 全部触发、顺序无语义(fireMiddlewareEvent 逐个分发,agent.go:213-241) |
| `ai-runtime/agent/session.go:178` | `messages := <-msgCh` 之后读 `runErr := cap.captured()`——可见性由 channel happens-before 保证(agent goroutine defer 内 fireMiddlewareEvent :55 先于 `msgCh <- messages` :57),c.mu 仅为防御与 -race 明示 |
| `ai-runtime/agent/session.go:217` | 最终 return 改:`runErr != nil` → 复用 :168-171 的 ctx 规则映射 RunFailed/RunCancelled,`Persisted: true`(note 已随 messages 提交成功——「持久化成功」与「模型成功」独立记账);否则 RunCompleted 照旧。commit 块(:179-216)结构不动:Commit/Compact 失败照旧提前返回 RunFailed+Persisted=false(持久化错误优先,绝不 RunCompleted) |
| `ai-runtime/agent/session.go:89-95` | RunResult 注释更新:provider 失败主映射改走 capture;:168-171 call-layer 分支保留为防御(runtime.go RecordRun 的 fn-error 合并路径仍可达,run_test.go:44-72 既有测试不变) |

**新增文件** `ai-runtime/agent/errcapture.go`:

```go
// runErrorCapture 捕获本轮 ReAct 的运行期错误:ai-core 把 provider 错误漏斗成
// note token(agent.go:82-107,309-312),同步 err 恒 nil(:175);唯一结构化出口
// 是 AfterAgentEvent.Err(:55)。capture 是每 Run 一实例的中间件,Session.Run 在
// <-msgCh 后读取(core.AfterAgent = Name()+OnAfterAgent(*AfterAgentEvent)
// *MiddlewareResponse,ai-core/core/spec.go:286-289)。
type runErrorCapture struct {
    mu  sync.Mutex
    err error
}
func (c *runErrorCapture) Name() string { return "runErrorCapture" }
func (c *runErrorCapture) OnAfterAgent(ev *core.AfterAgentEvent) *core.MiddlewareResponse {
    if ev.Err != nil {
        c.mu.Lock()
        c.err = ev.Err
        c.mu.Unlock()
    }
    return &core.MiddlewareResponse{}
}
func (c *runErrorCapture) captured() error { /* mu 内快照 */ }
```

**新增测试**(改 `run_test.go` 与 `run_journal_test.go`):

- run_test.go:77-80 的 **skip 子测试替换为真实断言**(P2-1 留下的修正预约,run_test.go:75-76 注释即本步):
  - `captureProvider{chatErr: context.Canceled}`(test_helpers_test.go:11-24 注入点)→ `Session.Run` 返回 `State==RunCancelled`;
  - `chatErr: errors.New("boom")` → `State==RunFailed`;
  - 两例同时断言:note 仍进 `ActiveView`(历史含 `⚠ [harness] ✘ agent error`)、`cb.EmitToken` 仍收到 Done note token、`Persisted==true`(note 已提交)——**用户可见面不变的机器证明**;
- `TestSessionRun_CommitFailureWithProviderErrorStaysFailed`:chatErr 注入 + fakeConversationStore.commitErr → RunFailed+Persisted=false(持久化分支优先);
- `TestRecordRunJournalsProviderFailureAsFailed`:chatErr 注入跑 RecordRun → 终态 journal 记录 `state=="failed"`(P2-3 journal 对偶;此前 provider 失败被记 completed 的缺口由此关闭)。

**并发与锁论证**:capture 写在 agent goroutine(fireMiddlewareEvent 串行分发,单写者),读在 Session goroutine `<-msgCh` 之后——channel 收发建立 happens-before,锁仅防御;capture 不触 s.mu/gate/hub.mu/rt.mu,无锁环。

**行为变化声明**:**用户可见=零**(勘察结论 B:三渲染面 note 流逐字节不变);变化仅 `RunResult.State`、`runs/<runID>.jsonl` 终态记录(provider 失败由 completed 纠正为 failed/cancelled——`TestRestoreInterruptedRun` 的恢复语义因此更准确)与 `commands.go:81` debug 日志。

**验收命令**:
```bash
go vet -C ai-runtime ./agent/...
go test -C ai-runtime -race ./agent/... -count=1
go build -C daemon ./... && go test -C daemon -race ./internal/plugins/agent/... -count=1
```

**依赖**:无(起点)。

---

## 3. P4 steps(HITL 与设备工具;全程 daemon 零改动)

### P4-1 RunWaiting 状态 + ApprovalManager 骨架

**目标**:总方案 P4 第 1 条 + §6:独立决策管理——Pending Approval 独立 requestID、属 SessionID+RunID(非连接)、相同 requestID 只接受一次有效决策、**超时默认拒绝**、V1 不做重启续跑。纯 SDK 增量。

**新增文件** `ai-runtime/agent/approval.go`:

```go
type PendingApproval struct {
    RequestID          string                // = hitl.InterruptRequest.ID(middlewares/hitl.go:129 等 "req-N")
    SessionID, RunID   string
    Request            hitl.InterruptRequest // 原始中断(含 ExpiresAt/Command/RiskLevel/Effects)
}
type ApprovalManager struct {
    mu      sync.Mutex
    pending map[string]*approvalEntry // requestID → entry(V1 进程内存量;审批不持久化→重启清空,无跨重启 id 碰撞)
}
type approvalEntry struct {
    pa       PendingApproval
    resolved bool
    decide   chan hitl.Decision // 容量 1:Resolve 恰好投递一次
}
func NewApprovalManager() *ApprovalManager
// Wait 阻塞至决策/超时/取消(总方案 §6 的 ApprovalManager.Request 适配形):
//   runCtx 取消 → reject;ExpiresAt 到点(含已过期)→ reject(「超时后默认拒绝,
//   不允许隐式通过」);Resolve 第一个有效决策获胜,迟到/重复 → ErrApprovalResolved
func (m *ApprovalManager) Wait(runCtx context.Context, sessionID, runID string, req *hitl.InterruptRequest) hitl.Decision
func (m *ApprovalManager) Resolve(requestID string, decision hitl.Decision) error
// Pending 返回会话未过期审批快照(ExpiresAt 未到且未 resolved;读时顺带清理过期条目)
func (m *ApprovalManager) Pending(sessionID string) []PendingApproval
```
sentinel:`ErrApprovalUnknown` / `ErrApprovalResolved`(approval.go)。

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/agent/run.go:24-25` | `RunWaiting RunState = "waiting_input"` 落码(注释从「P4 ApprovalManager,本步不定义」更新为「P4-1 定义,产生方 P4-2 审批等待」) |
| `ai-runtime/session/journal.go:143-150` `isTerminalRunState` | **不动**:waiting_input 非终态 → 重启恢复扫描照旧标 interrupted(总方案 §9「Run Waiting Approval → 恢复审批记录,但旧 Run 标记 Interrupted」;V1 审批在内存、不跨重启,自动续跑不做) |
| `ai-runtime/agent/manager.go:42-59,84-97` | SessionManager 增 `approvals *ApprovalManager`(NewSessionManager 构造)+ `func (m *SessionManager) Approvals() *ApprovalManager` |

**新增测试** `approval_test.go`:
- `TestApprovalWaitResolves`:Wait 挂起 → Resolve(approve) → 返回该决策;二次 Resolve → ErrApprovalResolved(单次生效);
- `TestApprovalWaitTimeoutRejects`:ExpiresAt 已过期 / 短窗超时 → reject(默认拒绝);
- `TestApprovalWaitCtxCancelRejects`:runCtx 取消 → reject;
- `TestApprovalPendingFiltersExpired`:Pending 只回未过期条目;
- `TestApprovalLateResolveRejected`:超时后 Resolve → ErrApprovalResolved(迟到决策无效,对齐 channel_decision_provider.go:20-21 语义)。

**并发与锁论证**:m.mu 只护 pending map 与 entry 状态;等待在 decide channel 上,绝不在锁内;entry.decide 容量 1 + resolved 标志保证恰好一次投递;超时用 time.Timer + runCtx.Done select(形态对齐 hitl/channel_decision_provider.go:12-38);不触 manager.mu/rt.mu/gate/hub.mu,无锁环。

**行为变化声明**:零(RunWaiting 无产生方,daemon 无改动)。

**验收命令**:
```bash
go vet -C ai-runtime ./agent/...
go test -C ai-runtime -race ./agent/... -count=1
```

**依赖**:无。后续:P4-2。

### P4-2 StartRun 审批回退接线 + waiting 迁移 —— TestApprovalAfterReconnect

**目标**:总方案 §6 的 Decide 适配在 SDK 层落地:`Decide → approvalManager.Wait(runCtx, runID, req)`(§6 :496-500)。后台 Run 的 HITL 决策由此跨连接化。daemon 零改动(StartRun 现无调用方,勘察结论 K)。

**涉及现有符号与改动**(supervisor.go):

| 位置 | 改动 |
|---|---|
| StartRun ⑥(:140-144)与 ⑦(:153-156)之间 | 审批回退挪到 **⑦ runCtx 派生之后、⑧ go execute 之前**(decideFunc 闭包需 runCtx):`if cb.Decide == nil { cb.Decide = m.approvals.decideFunc(runCtx, sess, run, sessionID, hub) }`;调用方自带 Decide(CLI/测试形态)不受影响。契约注释(:66-79)同步更新 |
| `approval.go` 新增 `decideFunc` | 返回闭包:`run.transition(RunWaiting)`(§6 状态机 Running→WFA)→ `journalRunState(sess, {state:"waiting_input", RunID:run.ID, ...})`(durable-first 观测面)→ `m.approvals.Wait(runCtx, sessionID, run.ID, req)` → `run.transition(RunRunning)` → 返回决策。终态仍由 finalizeRun 归一,等待期间的取消(CancelRun/宿主 Close)经 runCtx.Done → Wait reject → 中间件 reject → 工具中止 → 既有取消路径收敛 |
| 事件面 | 审批包装**先于** teeRunCallbacks(:144)——tee 检测 cb.Decide 非 nil 自动包 permission.requested/resolved(supervisor.go:296-307),回放面免费获得审批事件对 |
| `manager.go` 追加 | `func (m *SessionManager) PendingApprovals(sessionID string) []PendingApproval`、`func (m *SessionManager) ResolveApproval(requestID string, decision hitl.Decision) error`(P5-6/P5-7 协议消费的直转包装) |

**新增测试** `approval_test.go` 追加(模拟 Provider,零真实 LLM):

- **`TestApprovalAfterReconnect`(总方案第十一节 → 本步)**:Session 工具含 DefaultTools 的 bash(`runtimeagent.DefaultTools`,default_tools.go:31-34;deps.Config.Sandbox.Mode=normal,危险命令触发 DecisionHitl——middlewares/hitl.go:269-286)→ StartRun(cb.Decide 为 nil)→ 轮询至 `Approvals().Pending(sessionID)` 非空且 `handle.State()==RunWaiting` → 「原连接断开」零操作(StartRun 无 ctx 参数,API 形状即保证连接无关)→ 「新控制端」`ResolveApproval(requestID, approve)` → run RunCompleted、工具结果非拒绝文案。P2P3 §6 的「StartRun 无 ctx 参数 = 调用方 ctx 不参与」在此被复用为「断开不影响等待」。
- `TestApprovalTimeoutFailsTool`:短 ExpiresAt → Wait 到点 reject → 工具结果为 `⛔ rejected by user`(hitl.go:443)→ run 继续 RunCompleted(超时=安全拒绝,不挂死、不隐式放行);
- `TestStartRunJournalsWaiting`:journal 断言 `waiting_input` 记录存在且先于终态记录。

**并发与锁论证**:decideFunc 在 execute goroutine(Session.Run→streamTokens)上阻塞——与 P3-4 tee 论证同构(等待在 hub 锁外,supervisor.go:277-279);transition 在 run.mu 内(幂等,run.go:68-86);journalRunState 在 rt.mu 外;ApprovalManager.mu 与 run.mu/rt.mu/hub.mu 无嵌套。

**行为变化声明**:零(daemon 零改动;同步 RecordRun 路径不经此处,CLI/ACP 的 Decide 面不变)。

**验收命令**:
```bash
go test -C ai-runtime -race ./agent/... -count=1
go test -C daemon -race ./... -count=1   # 机器证明 daemon 零改动
```

**依赖**:P4-1。后续:P5-2(decideFunc 增 state_changed 事件)、P5-6。

### P4-3 CapabilityRegistry / ToolCapabilityBinding / ClientToolProxy 构造面

**目标**:总方案 P4 第 3 条 + §8 的 SDK 面:绑定注册表与稳定工具代理(Proxy 在执行时解析当前绑定,替换「ToolSource 只调一次」的结构限制,§8 :679-681)。纯新增。

**新增文件** `ai-runtime/agent/capability.go`:

```go
// ToolCapabilityBinding(总方案 §8 :655-661 原形)。
type ToolCapabilityBinding struct {
    SessionID, ConnectionID string
    ToolNames               []string
    ExpiresAt               time.Time
}
// ClientToolExecutor 是连接级执行通道(daemon 适配 x-device/call;SDK 测试用 fake)。
type ClientToolExecutor interface {
    Invoke(ctx context.Context, toolName string, argsJSON []byte) (string, error)
}
// ClientToolSpec 是代理工具声明(daemon 从 acpworker.DeviceToolDescriptor 转换,P5-9)。
type ClientToolSpec struct {
    Name, Description, Schema string
    Metadata                  map[string]string // risk_level/requires_permission/allow_always_key 原样透传
}
type CapabilityRegistry struct {
    mu       sync.Mutex
    bindings map[string]map[string]*capabilityBinding // sessionID → connectionID → binding
}
func NewCapabilityRegistry() *CapabilityRegistry
func (r *CapabilityRegistry) Bind(sessionID, connectionID string, toolNames []string, ttl time.Duration, exec ClientToolExecutor) (refreshed bool)
    // 幂等:同 (session,conn) 重绑 = 刷新 TTL + 工具集(每次 prompt 探测都会 Bind);
    // ExpiresAt = now+ttl(TTL 兜底「连接死而无人 Unbind」)。
func (r *CapabilityRegistry) UnbindConnection(connectionID string)                     // 连接退出(handler.Close)
func (r *CapabilityRegistry) Invalidate(sessionID, connectionID string)               // Invoke 失败即时失效
func (r *CapabilityRegistry) Lookup(sessionID, toolName string) (ClientToolExecutor, bool) // 过期条目视同无绑定
// NewClientToolProxy 按声明产出稳定 core.Tool 代理(P4-4 补齐等待语义):
//   - Name/Description/Parameters(schema 解析)/Metadata 透传——HITL 门控
//     (hitl.go:113-122 resolveToolPolicy + :201-249 checkSys)在每次调用照常触发,
//     「危险操作重绑后仍需确认」由中间件 per-call 裁决天然保证(总方案 §6 :524);
//   - Metadata 增内部标记 goworker.capability_wait(P4-4 supervisor 观察者注入的识别键)。
func NewClientToolProxy(reg *CapabilityRegistry, sessionID string, specs []ClientToolSpec, waitTimeout time.Duration) []core.Tool
```

**涉及现有符号与改动**:`manager.go` 增 `capabilities *CapabilityRegistry` 字段(NewSessionManager 构造)+ `func (m *SessionManager) Capabilities() *CapabilityRegistry`。

**新增测试** `capability_test.go`:Bind/Lookup/TTL 过期/UnbindConnection/Invalidate 幂等往返;重绑刷新 TTL;跨 connection 隔离(A 的绑定对 B 不可见——Lookup 只按 Bind 登记返回,不存在隐式路由面)。

**并发与锁论证**:r.mu 只护 map 快照;Invoke 在锁外(执行 I/O 不持锁);过期判定读时进行(lazy expiry,无后台 goroutine);与 manager.mu/rt.mu/gate/hub.mu 无嵌套。

**行为变化声明**:零(纯新增,无接线)。

**验收命令**:
```bash
go vet -C ai-runtime ./agent/...
go test -C ai-runtime -race ./agent/... -count=1
```

**依赖**:无(与 P4-1 并行可行)。后续:P4-4。

### P4-4 等待/安全超时/失效语义 + waiting_capability + 观察者注入 —— TestToolBindingDisconnect(P4 完成线)

**目标**:总方案 P4 第 4 条 + §8 策略落码:断连 → `waiting_capability` → 等待具备相同工具能力、获宿主授权的客户端显式重绑 → 超时把明确失败交给 Agent;**绝不跨设备静默执行**;V1 不做重启续跑。

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/agent/run.go` | `RunWaitingCapability RunState = "waiting_capability"`(§8 :673「waiting_input 或 waiting_capability」取后者——与 HITL 等待在 run/get 上可区分);isTerminalRunState 不含 → 重启标 interrupted |
| `ai-runtime/config/spec.go:95-106` + `config.go:197-199` | SessionConfig 增 `CapabilityWaitTimeout int yaml:"capability_wait_timeout,omitempty"`(秒);Default 增 45。**硬约束**:45 < ai-core toolCtx 60s(agent.go:147-153)——等待发生在工具 Execute 内,预算必须留余量,否则被 60s 掐断呈现为普通超时(勘察 risk);SDK 侧 `capability.go` 常量 `defaultCapabilityWaitTimeout = 45 * time.Second`,`NewClientToolProxy` 对 `<=0` 回落(双保险,P2-7 同款) |
| `capability.go` proxy Execute 完整语义 | ①`Lookup` 命中 → `Invoke`;②无绑定 / Invoke 失败(先 `Invalidate` 该绑定)→ 进入等待:`onWait(true)` → select{绑定就绪通知 / ctx.Done / waitTimer};重绑校验=registry 内出现含本工具名的 Bind(「具备相同工具能力」由 Bind 的 ToolNames 校验、「获宿主授权」由 V1 本机信任模型承担——**拍板②**;不匹配继续等);→ `onWait(false)` → 重绑成功则 Invoke(**重走 HITL per-call 裁决 = 危险操作重绑后仍需确认**);超时/取消 → 返回明确失败文本(`device tool X unavailable: connection lost, waited Ns`)交 Agent 决定下一步(§8 :677)。就绪通知:registry 每 (sessionID,toolName) 一条 `notify chan struct{}`(容量 1,Bind 成功时非阻塞投递) |
| `ai-runtime/agent/supervisor.go` execute(⑤ 之前) | 观察者注入:`orig := req.ToolSource; req.ToolSource = func(ctx) []core.Tool { tools := orig(ctx); for i := range tools { if tools[i].Metadata[capabilityWaitMetaKey] != "" { 注入 onWait 闭包(onWait(true)→run.transition(RunWaitingCapability)+journalRunState waiting_capability;false→RunRunning) } }; return tools }`;`capabilityWaitMetaKey = "goworker.capability_wait"`(capability.go 常量,同包共享)。**RecordRun 同步路径不注入**(不包装 ToolSource)→ waiting_capability 仅后台 Run 可观测,同步路径记 running——limitation 写进注释 |

**新增测试** `capability_test.go` 追加:

- **`TestToolBindingDisconnect`(总方案第十一节 → 本步)**:StartRun + `RunRequest.ToolSource` 返回 proxy(绑 conn-A fake executor)+ stub provider 先发 tool_call 再 final → run 到工具调用 → `UnbindConnection("conn-A")`(dead executor Invoke 置失败)→ 断言 `run.State()==RunWaitingCapability` → `Bind("conn-B", 含同名工具, 新 executor)` → 工具在 conn-B 执行完成 → RunCompleted;**dead executor 的 Invoke 计数==0**(不静默路由,断言钉死 §8 :650「不能将工具请求默默路由到 B」);
- `TestToolBindingTimeoutFails`:无重绑 → 短窗等待 → 工具结果含明确失败文本 → run 正常完成(失败结果交给 Agent,journal 终态 completed 但工具结果可辨);
- `TestRebindRequiresCapabilityMatch`:重绑连接的 ToolNames 不含该工具 → 等待不解除直至超时;
- `TestRebindReconfirmViaHITL`:重绑后的执行再次触发 interrupt(metadata risk_level 透传 → 第二次 permission.requested)。

**并发与锁论证**:等待在 proxy Execute 内(工具 goroutine 即 run goroutine,ai-core toolCtx 60s 预算内);notify chan 非阻塞投递(Bind 侧)/select 接收(等待侧);registry.mu 只护 map 快照;onWait 闭包调 run.transition(run.mu)与 journalRunState(锁外 I/O)——与 supervisor 既有锁序无环;慢等待不阻塞 AgentRun 之外的任何组件(等待本身就占着 run)。

**行为变化声明**:零(daemon 零改动;代理尚无 daemon 生产者——ACP 既有原始 relay 路径 handler.go:152-156 与 RunRequest.ToolSource 消费面不变)。

**验收命令**(P4 全量收口,对齐 CI):
```bash
gofmt -l .
go test -C ai-runtime -race ./... -count=1
go test -C daemon -race ./... -count=1
go build -C daemon -tags goworker_no_dispatcher ./cmd/goworker
```

**依赖**:P4-3(必需);P4-1/P4-2(状态与 journal 复用,弱依赖)。**P4 完成线(SDK 层;daemon 接线在 P5-9)**。

---

## 4. P5 steps(前端协议;逐项声明可见变化)

### P5-1 WithRunContext 注入机制(sessionGateKey 契约兑现)

**目标**:兑现 P2-5 契约(supervisor.go:70-75):StartRun 的 runCtx 补 sessionGateKey 重入标记,消除「后台 Run 内 hub 工具嵌套 Eval(tools.go:34-42 `p.hub.Eval`)→ 重入 withSession(commands.go:37-51)→ 同 gate 二次 Acquire → 死锁」。

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `ai-runtime/agent/manager.go` | 新增 `type RunContextValue struct{ Key, Value any }` 与 `func WithRunContext(values ...RunContextValue) ManagerOption`(存入新字段 `runCtxValues []RunContextValue`,构造期一次写入此后只读) |
| `ai-runtime/agent/supervisor.go:153` | `runCtx := m.contextOf()` → 逐项 `context.WithValue(runCtx, kv.Key, kv.Value)` → 再 `context.WithCancel(runCtx)`(WithCancel 链不变) |
| `daemon/internal/plugins/agent/plugin.go:93` NewPlugin | `NewSessionManager(WithRunLimits(...), runtimeagent.WithRunContext(runtimeagent.RunContextValue{Key: sessionGateKey{}, Value: p}))`——key 保持 daemon 包内非导出(commands.go:28),值携带 *AgentPlugin,与 withSession :38 判等(`owner == p`)精确匹配 |

**新增测试**:supervisor_test.go 追加 `TestStartRunInjectsRunContextValues`(经 Session.Run 内 ctx.Value 断言透传);daemon `manager_wiring_test.go` 追加 `TestStartRunCarriesGateReentryMarker`(plugin 构造 → StartRun(gatedProvider 挂起)→ fake hub 工具内 `hub.Eval("/usage")` 不死锁、正常返回)。

**并发与锁论证**:runCtxValues 构造期写定、运行期只读,无锁;ctx 链派生不改变既有取消传播。

**行为变化声明**:零(daemon 现无 StartRun 调用方;标记只在 P5-6/7 起 run/start 生效)。

**验收命令**:
```bash
go test -C ai-runtime -race ./agent/... -count=1
go test -C daemon -race ./internal/plugins/agent/... -count=1
```

**依赖**:无。**后续 P5-6/P5-7 的顺序硬前置**(死锁风险,§6 风险 2)。

### P5-2 run.* 生命周期事件产出(仅 hub 非 nil 路径)

**目标**:P3-0 契约表预留的 run.* 四类落码(journal.go:108-111 EventRunStarted/StateChanged/Finished/Error 现无产生方,grep 核实),供 x-goworker run/get 与 attach 回放表达 run 边界。

**涉及现有符号与改动**(supervisor.go):

| 位置 | 事件 |
|---|---|
| execute ④(transition(RunRunning) :216 处) | hub 非 nil → `run.started {state:"running"}` |
| P4-2 decideFunc 的 waiting 迁移两处 | hub 非 nil → `run.state_changed {from,to}` |
| execute ⑤ 与 ⑤.5 之间(**次序铁律**:事件先于终态 state 记录,journal_store.go:125-130) | `res.State==RunFailed` → `run.error {err}`;随后(无条件)`run.finished {state, persisted}` |
| execute ①/② 早退出口(:188-205) | hub 非 nil(StartRun ⑥ 已 BeginRun)→ `run.finished {state:"cancelled"}`(同一次序约束,先于 defer EndRun) |

payload 均为小 JSON 对象;全部走 teePublish(hub, runID, …)(supervisor.go:378-389,durable-first/降级语义复用)。

**新增测试**:supervisor_test.go 追加 `TestStartRunEmitsRunLifecycleEvents`(RunEvents=真 RunJournalStore → ReadEvents 断言 started/finished 有序、failed run 含 error 事件、事件 Seq 先于终态 state 记录)。

**并发与锁论证**:Publish 持 hub.mu(durable-first fsync)——P3-2 论证原样;新调用点全在 execute goroutine / decideFunc,无新共享状态。

**行为变化声明**:零(daemon 未接 deps.RunEvents,P5-4 才接线;SDK 测试外无消费者)。

**验收命令**:`go test -C ai-runtime -race ./agent/... -count=1`

**依赖**:P4-2(decideFunc 位置);与 P5-1 无依赖。

### P5-3 ReadEvents 按 SessionID 过滤(遗留②)

**目标**:P2P3 勘察修正 I 的收口:runs/ 归属歧义(Archive 同目录换 SessionID,store.go:556)下回放不串他 incarnation 的事件。

**涉及现有符号与改动**:`ai-runtime/session/journal_store.go:172-182` ReadEvents 增 `rec.SessionID == s.sessionID` 过滤(构造期快照 :60-67 即挂载键);`SeqRange`(:190-210)同滤(播种游标与回放同源);`walkEventRecords`(:217-240)不动,过滤在 visit 回调内做(注释说明)。测试:journal_test.go 追加 `TestReadEventsFiltersBySessionID`(同目录混入异 SessionID 记录 → ReadEvents/SeqRange 均不含)。

**行为变化声明**:SDK 收严。现网数据流无同目录多 incarnation(P2P3 勘察:/new 一律 Create 新目录,legacy 根槽仅首轮挂载,plugin.go:382-383),用户可见=零;对历史上已存在的混存文件,attach 回放不再含他 incarnation 段——如实声明。

**验收命令**:`go test -C ai-runtime -race ./session/... -count=1`

**依赖**:无。

### P5-4 daemon deps.RunEvents 接线(零行为)

**涉及现有符号与改动**:`plugin.go:437-440` startSession deps 组装——`p.deps.RunEvents = journalStore`(与 RunJournal **同一** *session.RunJournalStore 实例,该类型同时实现 AppendState/AppendEvent/ReadEvents/SeqRange,编译断言已在 event_hub.go:62);`st == nil`(纯内存)不设——typed-nil 纪律同 spec.go:96。

**行为变化声明**:零——bindEvents 仅 StartRun 触发(runtime.go:116),daemon 无 StartRun 调用方;同步 RecordRun 路径不建 hub、不写事件(P2P3 openQuestion④ 语义延续,由 P5-6/7 起 x-goworker 路径改变)。测试:runjournal_wiring_test.go 追加 deps.RunEvents 非 nil 断言(持久化启用变体)+ nil 断言(纯内存变体)。

**验收命令**:`go test -C daemon -race ./internal/plugins/agent/... -count=1`

**依赖**:无(P3 已就绪)。

### P5-5 /new 旧 runtime + 旧 store 收口(遗留①)

**涉及现有符号与改动**:`commands.go:107-158` handleNew——闭包内捕获 `oldRT := p.currentSession()`(:112 前)与 `oldStore`(:115);`CheckpointSync` 完成后、return 前:

```go
if oldRT != nil { p.manager.Remove(oldRT.ID) }        // 首用 manager.Remove(勘察 K)
if oldStore != nil { if err := oldStore.Close(); err != nil { slog.Warn("session: old store close failed", "err", err) } }
```

依据:拍板⑤「V1 仅 current 可路由」成立后,旧 runtime 留册只余误路由面(此前 daemon 对 Remove 零调用,manager 随 /new 无界累积);`Store.Close` 幂等(store.go:177-199);repo 缓存条目保留(实例唯一不变量,Stop 统一清 plugin.go:275-289)。

**并发与锁论证**:handleNew 闭包持 gate(current 在临界区内稳定);在途 run 与 Remove/Close 被 gate 串行;其他连接对旧 id 的操作(P5-8 attach)在 Remove 后得 ErrSessionNotFound → 诚实报错(客户端语义写入 PROTOCOL.md)。

**行为变化声明**:用户可见=零(/new 输出逐字不变;旧 store 本就无写者);管理面变化=manager 注册表不再累积旧 runtime。

**新增测试**:manager_wiring_test.go 追加 `TestNewRemovesOldRuntimeAndClosesStore`(/new → manager.Get(旧id)==nil、旧 store 已 Close、新会话照常)。

**验收命令**:`go test -C daemon -race ./internal/plugins/agent/... -count=1`

**依赖**:无;P5-6 的 current-only 路由以本步为前提(顺序硬约束)。

### P5-6 model.RunControlSource + AgentPlugin 实现(后台 Run 的 daemon 面)

**新增**(daemon/internal/core/model/capability.go 追加,可选能力接口,SessionSource 先例 :61-78):

```go
// RunControlSource 是 x-goworker 扩展协议的消费面(可选能力,handler type-assert 获取)。
type RunControlSource interface {
    StartRun(sessionID, input, requestID string) (runID, state string, err error)
    CancelRun(sessionID, runID string) error
    RunStatus(sessionID, runID string) (state string, latestSeq uint64, err error)
    PendingApprovals(sessionID string) ([]PendingApprovalInfo, error) // daemon 侧扁平结构,避免 model→agent 类型泄漏
    ResolveApproval(requestID string, decision hitl.Decision) error
    Attach(sessionID string) (hub *runtimeagent.EventHub, latest uint64, err error) // P5-8
    // Control lease(V1 宿主授权,拍板②):本机信任 + 接管放行 + slog 审计
    AcquireControl(connectionID string) (tookOver bool)
    ReleaseControl(connectionID string)
    HoldControl(connectionID string) bool
    UnbindCapabilities(connectionID string) // P5-9:断连解绑
    CapabilityWaitTimeout() time.Duration
}
```

**新增文件** `daemon/internal/plugins/agent/runcontrol.go`(AgentPlugin 实现):
- `StartRun`:校验 `sessionID == p.CurrentSessionID()`(V1 仅 current,拍板⑤)→ `rt := p.currentSession()` → bg cb = `runtimeagent.RunCallbacks{}`(**全 nil**:tee 只包装非 nil 入口 supervisor.go:287-289、streamTokens nil 检查 session.go:228-234——后台 Run 的输出只进事件流/journal,不进任何终端;`Decide` 留 nil → P4-2 审批回退接管)→ `p.manager.StartRun(rt.ID, runtimeagent.RunRequest{Input: input}, cb)`;**requestId 幂等**:mu+map[requestID]runID,重试同 requestId → 返回原 runID/当前 state;V1 进程内存量、容量 4096 环形逐出(防无界);
- `RunStatus`:`rt.Active()` 命中 runID → snapshot;否则 currentStore 的 RunJournalStore 新增方法 `LastRunState(runID) (session.RunStateRecord, bool)`(journal_store.go:读该 run 文件末条 state 记录;文件不存在 → ErrRunNotFound 语义);
- `CancelRun` / `PendingApprovals` / `ResolveApproval`:直转 manager(经 RunStateJournal 访问:plugin 自持 p.repo+currentStore 可重建 `session.NewRunJournalStore`,P2-7 同款);
- Control lease:plugin 增 `leaseMu sync.Mutex; controlLease string`;Acquire:空→占(返回 false);他人→置新 holder(返回 true,slog.Info 审计——「权限检查必须在 Daemon 侧完成」§7.4 :610);Release:holder 相等才清;Hold:判等;
- `UnbindCapabilities` / `CapabilityWaitTimeout`(cfg.Session.CapabilityWaitTimeout 秒化,<=0 → 45s);
- `ai-runtime/agent/runtime.go` 访问器导出:`func (r *SessionRuntime) Hub() *EventHub`(eventHub() 导出形)与 `func (r *SessionRuntime) EnsureHub() *EventHub`(导出包装 bindEvents(r.ID, sess.deps.RunEvents)——遗留③「重启后 attach 时 hub 未建」的解法:attach 即建,含 SeqRange 磁盘播种)。

**新增测试** `runcontrol_test.go`:`TestStartRunThroughPlugin`(newAgentPluginP+fixedProvider → StartRun → handle 等价完成 → RunStatus 返回 completed;sessionID 不匹配 → 错误);`TestRequestIdempotency`(同 requestId 两次 → 同 runID);`TestControlLeaseTakeover`(A 占 → B 接管 tookOver=true → A Hold==false)。

**行为变化声明**:零(接口与实现新增,无协议暴露;StartRun 仍无前端调用方)。

**验收命令**:
```bash
go vet -C daemon ./... && go test -C daemon -race ./internal/plugins/agent/... -count=1
```

**依赖**:P4-2(Approvals)、P5-1(重入标记,顺序硬前置)、P5-5(current-only 前提)。后续:P5-7/P5-8/P5-9。

### P5-7 x-goworker 方法面 I:run/start|get|cancel + approval/respond

**新增文件** `daemon/internal/core/service/acp/xproto.go`:

```go
// x-goworker 扩展协议(总方案 §7 :554-562)。放置依据:x-device 先例——应用层
// 扩展不进 ai-acp/protocol 标准面(sys_relay.go:5-8);daemon 侧自持常量与 wire 类型。
XMethodRunStart       = "x-goworker/run/start"        // {sessionId,input,requestId} → {runId,state}
XMethodRunGet         = "x-goworker/run/get"          // {sessionId,runId} → {state,result?,latestSeq}
XMethodRunCancel      = "x-goworker/run/cancel"       // {sessionId,runId} → {accepted}
XMethodApprovalRespond = "x-goworker/approval/respond" // {requestId,decision} → {accepted}
XMethodSessionAttach  = "x-goworker/session/attach"   // P5-8
XMethodSessionDetach  = "x-goworker/session/detach"   // P5-8
XMethodSessionEvents  = "x-goworker/session/events"   // 服务端通知(P5-8,Send only)
```
错误约定:沿用 -32000 + 明确文案(不新增标准错误码);control 缺失 → `-32000 "x-goworker: control lease held by <connID>"`。

**新增文件** `daemon/internal/core/service/acp/xgw.go`(FrontendHandler 扩展):
- 工厂(handler.go:58-67)生成 `connectionID`(crypto/rand hex,对齐 ai-acp newSessionID server.go:430-432 形态);新字段 `runs model.RunControlSource`(工厂内 `sessions.(model.RunControlSource)` type-assert,nil → 全部 x-goworker 方法诚实报错,与 loader/lister 降级同型 server.go:267-269);
- handlers:`xgwRunStart`(require HoldControl → runs.StartRun;requestId 幂等在 source 层 P5-6)、`xgwRunGet`、`xgwRunCancel`(require control)、`xgwApprovalRespond`(require control;decision 按 approval/respond 的 wire 形状映射 hitl.Decision);
- **连接生命周期钩子**:FrontendHandler 实现可选接口 `Close()`(→ runs.ReleaseControl(h.connectionID) + 订阅泵清理(P5-8)+ runs.UnbindCapabilities(h.connectionID)(P5-9));`service.go:180-203` accept 在 NewServer 后 `registerXGWMethods(server, handler)`(`server.Handle(XMethod..., handler.xgwXxx)`——Peer.Handle 导出,peer.go:86),并在 `<-server.Done()` 返回路径调用 handler 的 Close(若实现)。

**新增测试** `xgw_test.go`(jsonrpc over io.Pipe 全栈,仿 handler_test.go 模式 :376-472):initialize → session/new → x-goworker/run/start 响应 `{runId,state}`;同 requestId 重试返回同 runId;未持 control → -32000;approval/respond 转发断言;标准 6 方法回归(session/prompt 响应形状逐字不变)。

**行为变化声明**:**新协议面**(增量能力,非破坏);标准方法与 session/update/request_permission 逐字不动;不调用 x- 方法的旧客户端零感知。

**验收命令**:
```bash
go test -C daemon -race ./internal/core/service/acp/... -count=1
go build -C daemon -tags goworker_no_dispatcher ./cmd/goworker
```

**依赖**:P5-6。后续:P5-8。

### P5-8 x-goworker 方法面 II:session/attach|detach + session/events + Observe/Control

**涉及**(xgw.go 追加):
- `attach {sessionId, afterSeq, mode}`:`mode=="control"` → `runs.AcquireControl(h.connectionID)`(tookOver 回显);observe 不动 lease。→ `runs.Attach(sessionID)`(P5-6:manager.Get → `rt.EnsureHub()`(遗留③解法:attach 即建,RunEvents 未接线/纯内存 → 诚实错误)→ `hub.Subscribe(ctx, afterSeq)`——窗口校验/ErrCursorExpired 原样上抛为 -32000,event_hub.go:316-340)。响应 `{currentRun:{runId,state}|null, latestSeq}`;**latestSeq 仅描述当前位置,客户端不得跳游标**——只有真正应用了对应事件才能推进本地 cursor(总方案 §7 :586-589,写进响应注释与 PROTOCOL.md);
- **订阅泵**:handler 持 `subs map[string]context.CancelFunc`(mu 护);每连接每会话至多一订阅(重复 attach → 先 cancel 旧泵再建新);pump goroutine range hub 通道 → type-assert reporter `interface{ Notify(method string, params any) error }`(ai-acp Server.Notify 导出,peer.go:101/server.go:384-395)→ `Notify("x-goworker/session/events", ev)`;hub 背压断开(通道被关闭,event_hub.go fan-out)→ pump 退出自清——慢客户端断开不影响 AgentRun(P3-2 机制在协议面成立);泵 ctx 为 handler 作用域 WithCancel,handler.Close(P5-7)统一回收;
- `detach {sessionId}`:cancel 泵;mode==control 时 ReleaseControl;
- 事件线上形状 = `session.SessionEvent` 原样 JSON(journal.go:88-95,与总方案 §7 :575 报文对齐)。

**新增测试**(xgw_integration 部分可先落本步,P5-10 汇总):attach 回放无缺口的协议透传断言(先 StartRun 产事件 → 后 attach afterSeq=0 → 收齐 (0, latest] 无重无缺;SDK 层 TestAttachReplayNoGap 已钉 hub 语义);ErrCursorExpired → -32000;control 接管后原 holder 的 approval/respond 被拒;observe 模式 respond → -32000。

**行为变化声明**:新协议面;**标准 session/load 重放不动**(sessions.go:147-203 store 面)——两者分工写入 PROTOCOL.md:load=历史对话重放,attach=事件游标(Replay+Live)。**拍板⑤**:attach 仅 current 会话,归档会话 → 诚实报错并指引 session/load。

**验收命令**:`go test -C daemon -race ./internal/core/service/acp/... -count=1`

**依赖**:P5-6(EnsureHub/Hub/Attach)、P5-7(注册面/lease/close 钩子)。

### P5-9 设备工具切 CapabilityRegistry + proxy(P5 显式可见变化 #1)

**涉及现有符号与改动**:

| 位置 | 改动 |
|---|---|
| `daemon/internal/core/model/capability.go` | 新增可选接口 `ClientToolBinder interface { ClientToolSource(connectionID string, caller DeviceCaller) func(ctx context.Context) []core.Tool }` 与 `DeviceCaller interface { Call(ctx context.Context, method string, params, result any) error }`(结构类型,PermissionAsker 先例 permission.go:30-34;acpworker.DeviceCaller sys_relay.go:42-44 天然满足——model 不 import acpworker,无环) |
| `daemon/internal/core/acpworker/sys_relay.go` | 新增 `SpecDeviceTools(caller DeviceCaller) ([]runtimeagent.ClientToolSpec, error)`(复用 ProbeDeviceTools+normalizeDeviceToolDescriptor :72-116;descriptor→metadata 映射抽公共 helper,`RelayDeviceTools` :121-159 改调它——stdio worker 路径行为不变) |
| `daemon/internal/plugins/agent/`(runcontrol.go/tools.go) | 实现 `ClientToolBinder`:closure 内 probe(2s)失败 → nil(现状收缩语义);成功 → `manager.Capabilities().Bind(currentStore 会话 id, connID, names, ttl=2*CapabilityWaitTimeout, executor{caller})` → `runtimeagent.NewClientToolProxy(reg, sessionID, specs, waitTimeout)`;executor 适配 x-device/call(caller.Call) |
| `daemon/internal/core/service/acp/handler.go:138-157` `deviceToolSource` | 优先 `h.sessions.(model.ClientToolBinder)`(工厂已有 sessions)走 proxy;type-assert 失败回落现有原始 relay 路径(测试桩兼容);Prompt 装配点 :228 不变 |
| handler.Close(P5-7) | 追加 `runs.UnbindCapabilities(h.connectionID)`——连接退出即解绑;在途等待由 P4-4 的「Invoke 失败→Invalidate→等待重绑」语义接管 |

**行为变化声明(P5 逐项 #1)**:**声明 x-device 能力的客户端(当前=Android app)在设备连接断开后,设备工具调用从「立即返回错误给模型」变为「等待重绑(默认 45s)后返回明确失败或重绑成功」**;未声明 x-device 的客户端(VS Code 等)零变化;沙箱/HITL 门控面零变化(spec.Metadata 透传,重绑后照常走 per-call 裁决)。Android 侧对等待语义/失败文案的适配为**仓库外依赖**。

**新增测试**:daemon 集成(newAgentPluginP + fake caller 编排:绑定→断连(Unbind+dead executor)→等待→重绑→完成;超时→明确失败文本);handler_test.go 既有 `TestPromptInjectsDeviceToolSourceWhenCapable`(:397-425)与无能力用例(:437-472)适配 binder 断言。

**验收命令**:
```bash
go test -C daemon -race ./internal/plugins/agent/... ./internal/core/service/acp/... ./internal/core/acpworker/... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./... -count=1
```

**依赖**:P4-3/P4-4(代理语义)、P5-6/7(connectionID/binder/Close 钩子)。

### P5-10 PROTOCOL.md 修订 + 协议级集成测试(P5 完成线)

**PROTOCOL.md 修订**(daemon/mobile/PROTOCOL.md):
- 方法面表(:28-38)后新增「**x-goworker 扩展协议**」节:7 方法表(对齐总方案 §7 :554-562,含各自 require control 标注)+ requestId 幂等键语义(与 JSON-RPC id 不同,§7 :564)+ attach 原子 Replay+Live / latestSeq 非游标(:586-589)+ Observe/Control(V1 本机信任、接管审计,拍板②)+ 交互示例(§7 :570-589 报文)+ 「标准方法语义逐字不变」声明 + method-not-found 降级指引(拍板⑥:无 agentCapabilities 通告);
- 线程模型节(:104-112):补 session/events 异步通知消费注意(独立于 request/response 表);
- 系统能力工具节(:131-168):补断连等待语义(P5-9 变化)与 `capability_wait_timeout` 配置;
- 演进规则(:195-200):补 x-goworker 归属(daemon 侧 agent-ward 扩展,不进 ai-dispatch/protocol);
- 仓库外依赖声明:Android App 需跟进的改动清单(attach/respond/events 消费、断连等待文案)。

**协议级集成测试** `daemon/internal/core/service/acp/xgw_integration_test.go`(JSON-RPC over io.Pipe 全栈,零真实 LLM):
1. initialize → session/new → attach(control) → x-goworker/run/start → 逐条收 session/events(tool_call/text.delta/assistant.message_finalized/run.finished,Seq 连续);
2. run 内触发 HITL → 第二连接 attach(control,接管)→ approval/respond → run 完成 → run/get 返回 completed + latestSeq(**TestApprovalAfterReconnect 的协议级对偶**);
3. requestId 重试幂等;afterSeq 断点续传无缺无重;ErrCursorExpired → -32000;标准方法(session/new|load|prompt|list)响应形状回归断言;
4. 设备工具断连等待/重绑/超时(协议级,配合 fake caller)。

**验收命令**(P5 全量收口,与 CI 等价):
```bash
gofmt -l .                                            # 必须为空
go test -C ai-runtime -race ./... -count=1
go test -C daemon -race ./... -count=1
go test -C daemon -race -tags goworker_no_agent ./... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./... -count=1
go test -C daemon -race -tags "goworker_no_agent,goworker_no_dispatcher" ./... -count=1
go test -C daemon -race -tags goworker_no_dispatcher ./mobile/ -count=1
go build -C daemon -tags goworker_no_dispatcher -o /tmp/goworker-agent ./cmd/goworker
bash daemon/mobile/build-aar.sh                       # 需 gomobile 工具链;无 Android 环境时报告为未运行
```

**依赖**:全部。**P5 完成线**。

---

## 5. 总方案第十一节测试 → step 分配(本 Plan 范围)

| 测试 | 分配 | 说明 |
|---|---|---|
| `TestApprovalAfterReconnect` | **P4-2**(SDK)+ P5-10(协议级对偶) | gatedProvider/真实 bash+HITL 链,新控制端 Resolve 同一 Pending |
| `TestToolBindingDisconnect` | **P4-4**(SDK)+ P5-10(协议级) | dead executor Invoke 计数==0 钉死「不静默路由」 |
| ai-core 修正对偶(失败不记 completed 的 journal 面) | **AC-1** | `TestRecordRunJournalsProviderFailureAsFailed`(P2-3 对偶的补全) |
| P5 协议集成(总方案 P5 第 4 条「集成测试」) | **P5-10** | 全栈 io.Pipe:attach/events/approve/idempotency/标准面回归 |
| 既有 P0–P3 测试(run_lifecycle/supervisor/event_hub/journal/…) | 每 step 回归底线 | 保持全绿即行为边界证明 |

---

## 6. 风险与回滚链

1. **AC-1 波及面**:run_test.go:77-80 skip 替换与 session 系测试断言适配(编译器/测试指路,机械改动);capture 可见性依赖 agent.go:53-58 defer 内「fire 先于 msgCh 发送」的既有结构——**AC-1 的测试必须钉死该次序**,未来重构把 msgCh 发送挪出 defer 会静默破坏修正(本条同时是 review 重点)。回滚:revert 单提交,P4 各步不受影响。
2. **sessionGateKey 死锁窗口**:P5-6/P5-7 的任何 StartRun 接线若先于 P5-1 落地,后台 Run 调 hub 工具(tools.go:34-42)即永久死锁——P5-1 是 P5 内的顺序硬前置,验收含嵌套 Eval 集成测试。回滚:P5-1 独立提交。
3. **跨面 busy 空洞**:x-goworker 后台 run 运行中,另一连接发标准 session/prompt 会阻塞在 runtime gate(withSession)直到 run 结束——无 busy 错误(SessionGate 只互斥 ACP 内部,coordinator.go:17-20;aiacp cancels 只挡同连接并发,server.go:324-328)。P5-10 在 PROTOCOL.md 显式声明该共存语义;根治属 P6。
4. **P5-9 用户可见变化的配套**:Android App 需同步适配等待语义与失败文案(仓库外依赖,goworker-android);daemon 侧集成测试以 fake caller 覆盖;VS Code 路径经 type-assert 保持原始行为。
5. **Android 断连=整后端停机**(host.go:269-295 → appl.Stop):后台 Run 存活在 Android 进程内形态依赖 Android 侧 RuntimeService(仓库外);waiting_capability 的等待期可被进程死亡截断——总方案 §9 :773-775 的边界,PROTOCOL.md 声明。
6. **requestID 双源**:HITL interrupt id=req-N(hitl.go:68,进程级)与 run/start 的 requestId 是两个命名空间;V1 审批不持久化 → 无跨重启碰撞;若未来持久化 Pending Approval,必须先做重启唯一化(如 run 前缀)。
7. **P5-5 的诚实失败面**:/new 收口后,对已归档会话的 run/start|attach 返回 ErrSessionNotFound——客户端必须处理并指引 session/load;PROTOCOL.md 声明(P5-10)。
8. **runs/ 全文件扫描 + 零 retention**(P2P3 风险 6 延续):attach/Subscribe 成本随 run 文件数线性涨;拍板④ 本期不做修剪,ErrCursorExpired 产生方仍仅「游标超前」(event_hub.go:319-325)——增长风险留档 P6。
9. **提交与回滚链**:每 step 一个独立提交,可独立 revert;依赖图无环,任意前缀回退保持可构建:
   `AC-1 | P4-1 → P4-2 → P4-3 → P4-4(P4 完成线)| P5-1 → P5-2 → P5-3 → P5-4 → P5-5 → P5-6 → P5-7 → P5-8 → P5-9 → P5-10(P5 完成线)`
   (P5-2/P5-3/P5-4 与 P5-1 无相互依赖,可并行开发但保持提交序;P5-5→P5-6、P5-1→P5-6、P5-6→P5-7→P5-8/P5-9 为硬序。)review 重点:P4-4 proxy 等待的出口矩阵(重绑/超时/ctx 取消各恰好一次 onWait(false) 与结果返回)与 P5-8 attach 泵的生命周期(建/换/断三路径均无泄漏)。

---

## 附录 A:勘察引用的验证命令与输出(2026-10-26,commit a9e4b0d)

见 §0.1(7 条命令全部实际运行,全部通过)。grep 核验:`x-goworker` 全仓零代码;`manager.Remove`/`List` 在 daemon 零调用点;`EventRunError` 常量无产生方;`RunWaiting` 仅 run.go:24 注释;`gofmt -l .` 为空。补充核实(Peer/接口面):`Peer.Handle/HandleNotification/Notify/Call` 导出(ai-acp/peer.go:86/:91/:101/:96);`core.Middleware`/`core.AfterAgent` 接口形状(ai-core/core/spec.go:195-197/:286-289);`model.SessionSource` 全貌(model/capability.go:61-78);`captureProvider.chatErr` 注入点(test_helpers_test.go:11-33);`run_test.go:77-80` 存在为 ai-core 修正预留的 skip 子测试(本 Plan AC-1 兑现)。未运行:`-race` 全仓矩阵(daemon 全变体)与 `build-aar.sh`(无 Android 工具链,验收步含命令)。

## 附录 B:勘察 openQuestions 原文(P4/P5 勘察产出,拍板记录见 §0.3)

1. ai-core 修正的范围与行为面(capture vs 签名/token;note 是否 commit;run.error 何时落);
2. Control Lease 宿主授权策略(V1 本机信任简化 vs 完整 AttachMode/过期/接管);
3. Android 断连等待时长默认值(受 toolCtx 60s 硬约束);
4. P2+P3 遗留边角哪些纳入(/new 滞留、runs/ 归属、重启连续性、retention);
5. sessionGateKey 注入机制(导出 key/StartRun 选项/decorator);
6. P5 是否把同步 /agent 切 StartRun(P2P3 openQuestion① 到期重判)。
