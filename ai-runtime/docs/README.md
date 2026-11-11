# ai-runtime 模块文档

`ai-runtime` 是 goworker 的纯 Go Agent 运行时与会话数据层。这里描述的是当前实现；daemon 如何装配运行时、管理连接和暴露 ACP 扩展，见 [`daemon/docs/README.md`](../../daemon/docs/README.md)。

## 主题

- [Session Runtime、Run 与事件流](runs-and-events.md)：多会话执行、并发门禁、Run 状态、停止/错误语义，以及 EventHub 的订阅和回放语义。
- [会话持久化与迁移](persistence.md)：JSONL 数据、Repository 生命周期、布局兼容和重启恢复。

## 按问题查找

- [Run 取消、等待与错误处理](runs-and-events.md#停止取消与错误语义)：区分取消执行和取消等待，查找提交拒绝、journal 失败与终态结果的关系。
- [事件 Replay 与完整性](runs-and-events.md#eventhub-的锁与持久化边界)：查找 durable-first、delta 合并、游标失效、慢订阅者及 `replayIntegrity`。
- [迁移和恢复边界](persistence.md#布局与兼容)：查找布局迁移、部分失败、幂等重跑与 Run 恢复语义。

## 边界

- `agent` 包提供 `SessionManager`、`SessionRuntime`、`Session` 和后台 Run API；它不依赖 daemon 插件或 ACP wire 类型。
- `session` 包提供会话 Store、Repository 与 Run Journal。宿主可以通过 `SessionDeps` 注入对话存储、Run 状态 journal 和事件 journal。
- `hitl`、`middlewares`、`provider` 等运行时组件被组合进 Session；连接级身份、control lease、ACP 方法路由属于 daemon。
- 数据持久化是可选依赖。未注入 Store/Journal 时，runtime 仍可执行，但重启恢复和持久化事件回放能力相应缺失。

## 方案与现行实现

`../../docs/multi-session-runtime-plan.md` 与 `../../docs/multi-session-p0-p1-implementation.md`、`multi-session-p2-p3-implementation.md`、`multi-session-p4-p5-implementation.md` 是设计/实施阶段留下的历史资料，包含当时分支与时间点的勘察记录，不是当前 API 规范。发生冲突时，以当前源码、测试以及本目录主题文档为准。命令决策引擎与统一工具风险策略的批准计划分别见 [`specs/001-command-decision-engine/plan.md`](../../specs/001-command-decision-engine/plan.md) 和 [`specs/005-unified-tool-risk-policy/plan.md`](../../specs/005-unified-tool-risk-policy/plan.md)；它们讨论的是工具安全策略，不取代本目录的运行时契约。
