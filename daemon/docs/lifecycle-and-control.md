# daemon 会话生命周期与控制面

本文按当前 `daemon/internal/plugins/agent`、`daemon/internal/agentcontrol` 与 ACP handler 的实现说明宿主职责。Agent 执行内核及事件语义见 [`ai-runtime/docs/runs-and-events.md`](../../ai-runtime/docs/runs-and-events.md)，持久化见 [`ai-runtime/docs/persistence.md`](../../ai-runtime/docs/persistence.md)，客户端 wire contract 见 [`mobile/PROTOCOL.md`](../mobile/PROTOCOL.md)。

## 会话身份与连接绑定

持久化模式下，逻辑 SessionID 是跨连接定位会话的稳定身份；runtime 是宿主当前挂载的执行实例。`AgentPlugin` 通过 `SessionManager` 注册多个 runtime，路由时先解析当前便利 runtime，再查 manager 注册表。无持久化模式的 current 会话可使用宿主内部身份，不应把它当成跨进程稳定 ID。

ACP 连接和会话是多对多关系，但每个连接对某个会话的绑定是单值：连接 `session/new`、可写 `session/load` 或路由动作更新绑定；改绑会释放该连接对旧会话的引用。ACP 层的 `SessionBinding` 保存该连接当前声明/使用的会话路由状态；`agentcontrol.BindingRegistry` 则记录连接到会话的生命周期引用，是判断连接是否仍持有会话的事实源。前者的局部状态变化会通过 `ConnectionBinder` 同步到后者；二者职责相关但不能当作同一张表。`session/list` 的三态为：

- `active`：存在连接绑定或 Run 在飞；CLI/stdin 的 current 便利引用也会保持 current 活跃。
- `idle`：没有这些活引用；会话数据仍保留，可重新 load。
- `archived`：显式终态标记，只读保留，不可再提交新的 Run。

## 收口顺序

会话失去最后一个连接绑定后，daemon 会检查引用和执行状态：

- 仍有绑定，或是 CLI/stdin current 便利引用：保留 runtime。
- Run 仍在飞：登记延迟收口，Run 终态通知到达后再检查，不中止 Run。
- 引用归零且没有活动 Run：按 `Retire` → `SessionManager.Remove` → 关闭 EventHub → `Repository.CloseSession` 收口。Store 数据不会随收口删除；之后可通过 load 重新挂载。

`Retire` 和 runtime 的 Run admission 共用线性化锁。收口开始后，已取得旧 runtime 指针但尚未完成 Run 占位的请求会被拒绝，避免在 hub/store 关闭后写入。重开时会创建新的 runtime/hub，并从 journal Seq 范围播种事件序列。

显式归档会把会话标记为不可逆的只读终态，不会搬走数据或自动删除历史。若 Run 在飞，归档因 busy 拒绝；连接绑定可以继续存在，但写提交被拒绝。归档数据仍能通过只读历史重放和状态查询访问。

## Control Lease 与 fencing

Lease 由 `agentcontrol.Service` 按 SessionID 管理。一个会话至多一个 control 持有者，但可有多个 observe 订阅者；不同会话的 lease 相互独立。

- `attach` 的 observe 模式只建立观察，不获取或改变 lease。
- control attach 可获取空闲 lease，也可显式接管；接管只影响之后提交的控制操作，不会撤销已成功启动的后台 Run。
- `run/start`、`run/cancel` 与 `approval/respond` 的 lease 校验和对应提交在同一个受保护临界区完成。旧持有者在接管后不能凭先前通过的检查落地操作。
- 审批归属以 daemon 注册的待决审批记录为准；客户端声明的 sessionId 必须与该审批实际所属会话一致。
- 连接断开时释放该连接持有的 lease、会话绑定和设备能力绑定。已接受 Run 由宿主继续执行；依赖断开设备通道的工具会按能力等待策略等待重绑或明确失败。

`(sessionID, requestID)` 是 Run start 幂等键；相同键、相同输入返回已有 Run，输入冲突明确拒绝。审批 response 的 requestID 是审批请求 ID，和 Run start 的键空间分离。

## 锁序、绑定与生命周期串行化

ACP handler 的 `h.mu` 保护本连接的订阅表与 closed 屏障。attach/detach 的提交阶段会在持有 `h.mu` 时调用 source 侧的会话 lease 操作，唯一嵌套方向是 `h.mu → SessionLease.mu`；source 不回调 handler、不获取 `h.mu`。attach 在锁外准备 `SubscribeEvents`，准备失败不改旧订阅或 lease；随后在 `h.mu` 内原子提交订阅替换与 control lease 获取。`Close` 在 `h.mu` 内设置 closed 并摘出订阅表，解锁后才 stop 订阅、释放 connection leases/绑定并解绑设备能力。因此 Close 与 attach 提交有确定先后，Close 返回后不再有新订阅或 lease 提交。

`SessionGate` 是跨连接共享的 ACP 会话管理门禁，只用于需要串行化的会话创建、可写 load/重开和归档管理；占用时直接返回 busy，不排队，也不覆盖 prompt 执行。prompt 的同 runtime 串行由 runtime gate 负责，后台 Run 的接受、全局并发与 pending 配额由 `SessionManager` 负责；不要把这几种门禁混成一把锁。

LeaseRegistry 的 `mu` 只保护 sessionID→lease 表的查找/创建和快照；它在取到 lease 指针后先释放 registry 锁，再单独获取 `SessionLease.mu`，两把锁不嵌套。registry 临界区不做 I/O 或回调。`SessionLease.mu` 是该会话租约检查、接管和短控制提交的线性化点；它会覆盖同步提交动作（例如 StartRun 的 queued 记录 fsync、审批决策提交），但不包住后台 AgentRun 执行。不要把早期版本的跨层锁序图当成现行实现：当前约束是上述有限的嵌套方向和锁内短提交。

ACP 层 `SessionBinding` 是每连接的会话路由选择器；`BindingRegistry` 不是路由表，而是连接→会话生命周期引用的事实源。handler 更新 SessionBinding 时通过 binder 同步登记引用。plugin 的 `lifecycleMu` 串行化绑定改绑及 runtime 收口；收口会重新检查 BindingRegistry、当前会话引用和活动 Run，再决定保留、挂起或注销 runtime。run 在飞时登记挂起回收；终态 hook 再次检查。断连先释放该连接登记，再触发带锁复查；若重绑定已先发生，非零引用会撤销挂起收口并保留会话。

`current` 的保护不是以 SessionID 字符串判等：plugin 检查 current runtime 指针是否就是待收口 runtime，只有同一个实例才享有 CLI/stdin 便利引用保护。已退役的旧实例不因 ID 相同而复活；持久化重开会创建新 runtime。

## 审批与客户端能力

HITL 审批由 runtime 的进程内 `ApprovalManager` 管理，requestID 记录服务端权威的 SessionID/RunID 归属，不绑定某条连接。审批 Wait 在内存登记请求并等待一次性决策；取消、超时默认拒绝，重启后待决审批不恢复。daemon 响应审批时先按 session 路由，再在该会话 lease 临界区内校验持有者、检查 ApprovalManager 中的真实归属、确认目标会话一致并提交决策；接管前已失权的连接不能穿过 fencing。

ACP 设备工具只在 initialize 协商了 `x-device` 且 reporter 支持反向调用时启用。每轮 prompt 探测设备工具规格后，plugin 按执行中的 runtime/session 和 connection 将工具名及 `DeviceCaller` 执行器登记到 `CapabilityRegistry`，同时构造稳定的代理工具；同一连接重复探测会刷新该会话下的绑定和 TTL。代理每次调用按 session/tool 查当前执行器；无绑定或非取消类 Invoke 失败时会等待显式重绑，后台 Run 可观察为 `waiting_capability`。新连接为同一会话绑定相同工具能力后唤醒等待者，再按当前注册表重查并调用；超时/取消会给 Agent 明确失败结果，不会静默切换到别的连接或会话。连接 Close 会解绑该连接的全部能力，等待中的调用继续等待重绑或超时。危险工具的权限元数据仍在每次调用时由常规 HITL/工具策略检查，重绑不跳过审批。

## ACP 与扩展边界

标准 `session/load` 用于对话历史重放；`x-goworker/session/attach` 用于事件流 Replay+Live 订阅，两者不互相替代。标准同步 `session/prompt` 和后台 `x-goworker/run/start` 最终共用同一个会话执行准入，避免同一 Session 并发写入；`prompt` 等待结果，`run/start` 接受后立即返回。

ACP handler 负责 wire 验证与响应；生产面的控制依赖通过 `model.RunControlSource` 提供，事件订阅结果携带 `agentcontrol.EventSubscription`（`Events()`/`Close()`），ACP 侧不直接依赖 `ai-runtime/agent` 的 `SessionRuntime` 或 `EventHub` 具体类型。扩展方法列表、字段形状、JSON-RPC 错误、`latestSeq`/`replayIntegrity`、客户端降级和 Android Host 行为统一见 [`mobile/PROTOCOL.md`](../mobile/PROTOCOL.md)。`ai-acp` 是通用 REV_1 JSON-RPC 协议库；PR #10/#11 的多会话、后台 Run 与 lease 编排位于 daemon/runtime 层，当前没有额外的 `ai-acp` 模块主题文档需求。

## 持久化和迁移的宿主职责

daemon 启动时调用 session 布局迁移器，整理旧根槽、旧归档和 v1 命名布局。Repository 为每个逻辑会话管理 Store 生命周期；runtime 释放时先关闭事件 hub，再关闭 Store。重启时 Run journal 将未终结 Run 恢复为 `interrupted`，不会自动重跑可能产生外部副作用的操作。完整存储格式和恢复约束见 [`ai-runtime/docs/persistence.md`](../../ai-runtime/docs/persistence.md)。
