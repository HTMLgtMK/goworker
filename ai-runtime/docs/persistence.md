# 会话持久化与迁移

本文描述 `ai-runtime/session` 的当前持久化行为。运行时如何使用这些接口见 [`runs-and-events.md`](runs-and-events.md)；daemon 的启动迁移与连接生命周期见 [`daemon/docs/lifecycle-and-control.md`](../../daemon/docs/lifecycle-and-control.md)。

## 存储组件

- `Store` 管理单个会话的 JSONL 记录和内存视图；数据以追加方式写入，并在需要时同步到磁盘。消息视图、checkpoint、compact/head、workspace、metadata、archived 标记以及 Run/事件 journal 记录按各自 record 类型编码。
- `Repository` 按逻辑 SessionID 创建、打开、列出和删除 Store，并缓存唯一的写 Store 实例。关闭会话会释放实例和目录锁；调用方不得绕过 Repository 再为同一会话另开写句柄。
- 持久化目录中的单会话 Store 使用 Unix flock 防止多个进程同时写同一会话。该锁只保护 Store 写者，不是数据根目录级迁移锁，也不串行化启动期目录迁移；部署前提是同一数据目录由单个 daemon 独占使用。此锁不构成备份或跨设备同步机制。
- 对话历史 Store 与 Run Journal 分工不同：前者保存可恢复的对话视图；后者记录 Run 状态和事件序列，使后台执行状态可查询、事件可续放。

## 布局与兼容

新布局以 `<root>/<sessionID>/` 为会话目录，当前对话文件使用 `<sessionID>.jsonl`；仍兼容旧的 `current.jsonl` 文件。Run journal 位于会话目录的 `runs/<runID>.jsonl`。目录名、逻辑 SessionID 与历史文件名不总是相同，因此应经 Repository/迁移器解析，不要在宿主中自行拼路径假定它们等价。

`MigrateLegacyLayout` 在 daemon 启动时、正常打开会话 Store 之前执行；进程启动阶段尚未由 Repository 持有会话 Store 或写锁，迁移假设没有并发写者。迁移幂等但不是单纯 rename-only：它会搬迁 legacy 根槽、归档文件与 v1 文件名/目录错位；旧根归档重命名到新目录后，还会 `Open` 目标 Store 并调用 `MarkArchived` 追加归档终态记录。

迁移对源数据的处理按原因区分：无法解析稳定身份、身份不合法、或目标已存在的冲突/重复源，会按迁移规则删除源文件或源目录（避免把不可寻址数据或重复副本误当作权威会话）；普通 I/O 错误不会一概当成可丢弃条目，相关 rename/mkdir/remove 失败会告警并保留尚未移动/删除的现场，根目录读取失败则返回错误。归档文件若已成功 rename、但随后 `MarkArchived` 的写入失败，目标数据仍留在新布局，只是缺少 archived 标记，不能假定它已被识别为归档。迁移是逐条操作，不是全局事务；失败后可能处于部分完成状态，重跑用于幂等收敛，但不提供自动回滚或备份恢复保证。协议侧迁移行为见 [`daemon/mobile/PROTOCOL.md`](../../daemon/mobile/PROTOCOL.md)。

## 恢复与损坏处理

- JSONL 加载会识别尾部不完整/损坏记录，并在可恢复的尾部场景截断到最后一条有效记录；中间数据损坏不能简单视为完整历史。
- Run Journal 重启扫描会修复可恢复的尾部坏行，并将未到终态的 Run 标记为 `interrupted`。这是诊断状态，不代表自动重跑。
- 不自动重新执行中断 Run：工具操作可能已经产生外部副作用，盲目重放不安全。客户端应观察恢复后的状态，由用户或上层策略显式决定下一步。
- 事件流跨进程恢复时，对旧进程内的写失败记录并无可靠认知，因此完整性可能是 `unknown`；只有当前进程可证明的无缺口才报告 `complete`。
- Session 与 Run 持久化是否启用取决于宿主是否提供相应 store/journal。纯内存模式不能提供跨进程历史、Run 或事件恢复保证。

## 历史文档

`../../docs/multi-session-runtime-plan.md` 是多会话总体设计资料，三个 `multi-session-p*-implementation.md` 是分阶段实施记录，保留了当时的代码快照、分支和测试结果。它们不应作为今天的布局/API 契约；本文和当前源码、测试描述现状。
