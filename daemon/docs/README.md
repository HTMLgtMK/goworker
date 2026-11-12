# daemon 模块文档

`daemon` 负责宿主装配、连接生命周期、授权控制和协议适配；Agent 会话执行与通用持久化原语属于 [`ai-runtime`](../../ai-runtime/docs/README.md)。本目录描述当前 daemon 实现，不替代 wire 协议定义。

## 主题

- [会话生命周期与控制面](lifecycle-and-control.md)：连接绑定、延迟收口、control lease、fencing 与 ACP 控制面边界。
- [Android ACP 协议](../mobile/PROTOCOL.md)：方法、字段、错误、事件游标、兼容策略和 Android Host 生命周期；这是客户端/daemon wire 行为的主要说明。

## 按问题查找

- [会话身份、绑定与收口](lifecycle-and-control.md#会话身份与连接绑定)：查找 `active`/`idle`/`archived` 区别、绑定引用源、runtime 退役与归档。
- [Control lease 与 fencing](lifecycle-and-control.md#control-lease-与-fencing)：查找 attach/接管、控制操作提交、断连释放及 `(sessionID, requestID)` 幂等键。
- [审批与设备能力](lifecycle-and-control.md#审批与客户端能力)：查找审批归属、默认拒绝、`x-device` 协商、能力等待/重绑及危险工具再次审批。
- [ACP 路由、Run 入口与事件 attach](lifecycle-and-control.md#acp-与扩展边界)：查找 `session/load` 与事件 replay 的区别、`model.RunControlSource`/`agentcontrol.EventSubscription` 的边界，以及 `SessionGate` 与 Run gate 的分工。
- [启动迁移和重启恢复](lifecycle-and-control.md#持久化和迁移的宿主职责)：查找 daemon 启动时的迁移职责，以及 interrupted Run 不自动重跑的约束。

## 职责分层

- `daemon/internal/plugins/agent` 是宿主适配层：装配 Session/runtime 依赖、解析会话路由、提供 workspace/设备能力，并把 ACP 能力面转换成 runtime 调用。
- `daemon/internal/agentcontrol` 是控制服务：集中处理会话绑定、每会话 control lease、Run 幂等提交、审批归属、事件订阅编排和生命周期引用关系。ACP handler 不直接管理 runtime 内部状态。
- `daemon/internal/core/service/acp` 是 ACP/JSON-RPC 协议适配层：解析标准 ACP 与 `x-goworker/*` 扩展、维护连接级 session binding，并把 wire 请求委托给控制面。
- `daemon/mobile` 是进程内字节管道 Host，不重新定义 Session/Run 语义。Android 客户端通过 ACP 通信，不接触 Go context、channel 或 runtime 对象。

## 方案资料的边界

`../../docs/multi-session-runtime-plan.md` 和 `../../docs/multi-session-p*-implementation.md` 是历史设计/实施记录，包含阶段性代码勘察、决策和测试输出。当前行为以源码、测试、本文档及 [`mobile/PROTOCOL.md`](../mobile/PROTOCOL.md) 为准。`../../specs/001-command-decision-engine/plan.md` 与 `../../specs/005-unified-tool-risk-policy/plan.md` 是各自功能的批准实施计划，不是 daemon 多会话生命周期契约。
