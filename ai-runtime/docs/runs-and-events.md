# Session Runtime、Run 与事件流

本文说明 `ai-runtime/agent` 当前的会话执行模型，依据 `agent/manager.go`、`runtime.go`、`supervisor.go`、`event_hub.go` 及其测试。daemon 的连接绑定与授权流程见 [`daemon/docs/lifecycle-and-control.md`](../../daemon/docs/lifecycle-and-control.md)；ACP wire contract 见 [`daemon/mobile/PROTOCOL.md`](../../daemon/mobile/PROTOCOL.md)。

## 运行时对象与生命周期

- `SessionManager` 是宿主级管理器：注册和查找 `SessionRuntime`，提供 manager 生命周期 context、全局并发槽和有界 pending 队列。默认全局并发为 4，pending 上限为 16；可由构造选项调整。
- `SessionRuntime` 是单会话执行边界：承载 Session、Run Journal/Event Journal、会话事件 hub、当前 active Run、执行准入和归档/退役状态。
- 一个会话同一时刻最多一个 active Run；不同会话可以并行，但共同受 manager 的全局并发与排队配额限制。队列已满或会话不可准入时提交失败，不会悄悄丢进无界队列。
- runtime 的 `Retire` 与 Run 准入共享线性化锁。Retire 成功后，即便调用方先前拿到了 runtime 指针，尚未完成占位的提交也会被拒绝；宿主随后才能安全摘除 runtime 并关闭 hub/store。
- `Archive` 是会话只读终态，不等于立即销毁观察面；daemon 对归档会话仍可提供只读查询和历史重放。

## Run 提交与状态

`StartRun` 接受请求后返回 `RunHandle`，执行由 manager 生命周期管理，而不是由提交连接的请求 context 管理。等待方通过 `Wait(ctx)` 观察结果；等待 context 取消只结束这次等待，不会取消已接受的后台 Run。需要停止执行时，宿主应显式调用取消 API。

状态沿 `queued`、`running`、`waiting_input`、`waiting_capability` 到 `completed`、`failed` 或 `cancelled` 变化。等待审批或外部能力期间，Run 保持 active，因此不能并发启动同一会话的第二个 Run。状态变化由 supervisor 统一记账；终态收敛会先完成 active 清理，再关闭 Run 的 done 信号并通知宿主生命周期 hook。

同步 prompt 的 `StartRunSync` **不是 `ai-runtime/agent` 的通用 API**，而是 daemon `agentcontrol.Service` 在受限的 `InternalRunner` 面提供的宿主入口，daemon 插件再委托它；该入口最终也调用 `SessionManager.StartRun`，因此共享同会话 admission、全局槽、pending、journal 和终态化。runtime 面本身提供后台 `SessionManager.StartRun`，`RecordRun` 仅是已持有执行权时保留的底层记账方法，不是 daemon 同步请求入口。

Run 事件和状态持久化由 `SessionDeps` 中的 `RunJournal` / `RunEvents` 接口注入。没有注入 journal 时，当前进程内执行仍可工作，但不可假定 Run 状态能够跨进程恢复。后台 `StartRun` 接受前先接通事件 hub，再严格追加 `queued` 状态记录；该写失败会回滚 active/pending 并拒绝提交。运行中状态和终态记录写失败只告警，重启扫描会把未观察到终态的 Run 标为 `interrupted`。

终态顺序是刻意安排的：后台 Run 先发 `run.error`（失败时）和 `run.finished` 事件，再 `EndRun` flush delta，之后 `finalizeRun` 追加终态 Run 状态记录，清除 active、关闭 `RunHandle` 的 done，最后通知 daemon 生命周期 hook。`RunJournalStore` 在终态状态记录写入时关闭并逐出该 Run 的 journal 句柄，所以事件和 delta 必须先写；done 通知等待者时，终态记录已尝试落盘，active 也已清除。运行中和终态状态写失败只告警、不阻断执行终态化；重启扫描若找不到终态记录，会把该 Run 标记为 `interrupted`。

## 停止、取消与错误语义

- `CancelRun(sessionID, runID)` 只对当前 active 的后台 `StartRun` 有效；queued Run 也可取消。未知会话、没有 active Run 或 RunID 不匹配返回 `ErrRunNotFound`。取消提交成功只表示取消信号已发出，不表示 Run 已经进入终态；最终结果仍由 supervisor 收敛为 `cancelled`，且取消与完成竞争时以实际完成结果为准。
- `StartRun` 的提交 context 不控制已接受的 Run。`RunHandle.Wait(ctx)` 的 context 只结束本次等待，返回 `ctx.Err()` 不会停止 Run；停止执行要显式调用 `CancelRun`，或由宿主生命周期 context 取消。
- `ErrRunNotCancellable` 只适用于没有后台 cancel 函数的底层 `RecordRun` 记账形态，不是 daemon 同步 prompt 的一般结果；daemon 同步入口现在也走 `StartRun`。取消请求与终态竞争时，daemon 适配层可能把已终结后的 `ErrRunNotFound` 当作取消已完成处理。
- 配置 Run Journal 时，`queued` 接受记录写失败会拒绝提交并返回可由 `errors.Is` 识别的 `ErrRunJournalWrite`，不产生已接受句柄。运行中状态与终态记录写失败只告警，不阻止执行收敛；重启时缺少终态的 Run 会被标为 `interrupted`，不自动重跑。事件写入是另一条账：关键事件 durable-first 失败会拒绝广播该事件，但不会把 Run 本身改成失败；delta flush 失败则告警并将 `ReplayIntegrity` 置为 `incomplete`。

## EventHub 的锁与持久化边界

EventHub 的 `hub.mu` 同时保护会话 Seq、订阅集合、delta 缓冲与 closed 状态。`Publish` 在持锁期间分配 Seq、执行关键事件的 journal append/fsync、广播，或将 delta 放入合并缓冲；定时/显式 delta flush 也在持锁下写 journal。`Subscribe` 持锁完成窗口校验、flush pending delta、读取回放、注册实时订阅者，保证 replay/live 边界连续。这样会让慢 journal I/O 暂时串行阻塞同会话的发布、订阅和 flush，这是用锁换取顺序正确性的当前实现取舍；fan-out 本身非阻塞，慢订阅者满缓冲会被断开，不会卡住 Agent 执行。

新建 hub 时 `EventJournal.SeqRange` 会扫描已存事件并播种 Seq；读取失败会显式返回错误，不能降级成空历史/从 Seq=1 重起。由 `StartRun` 创建 hub 失败时，提交被拒绝并清理 active/pending；由 `EnsureHub` 创建时也将错误原样上抛。空历史与扫描失败因此不会混为一谈。

每个 runtime 有自己的 EventHub。Seq 在会话范围内单调递增，跨 Run 连续。订阅会先得到 afterSeq 之后的回放，再无缝接收 live 事件；回放与实时订阅边界由 hub 原子化，避免空档和重复。

持久化与广播遵循事件类型的可靠性要求：

- 关键生命周期、工具、审批等事件采用 durable-first：先写事件 journal，成功后再广播。写失败时事件不会被伪装成已持久化的事件发送。
- 高频 `text.delta`、`thinking.delta`、`usage.progress` 实时逐条广播；journal 写侧按时间/字节窗口合并为批记录以减少写放大。回放会展开批记录，恢复为原始逐 Seq 事件。
- `ReplayIntegrity` 表达回放可信度：`complete`（当前观察范围无已知缺口）、`unknown`（存在无法验证的旧进程历史）或 `incomplete`（本进程发生过 journal 写失败）。`incomplete` 是粘滞状态；客户端不能把非 complete 历史当成完整事实。
- `latestSeq` 只是查询时的事件流位置，不是已消费游标。客户端只能在实际应用每条事件后推进游标。
- 慢订阅者不会反压 Agent 执行。订阅缓冲区耗尽时会断开该订阅，由客户端按已确认 Seq 重新 attach。

## 重要边界

Run 的执行 context 属于 manager/宿主，不属于发起请求的连接；但与连接绑定的外部能力（例如 Android 设备工具）可能在断连时失效。具体能力等待/重绑由宿主注入的执行器和 daemon 策略决定，不能把后台 Run 仍存活误解成连接能力仍可用。
