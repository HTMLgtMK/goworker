# Feature Specification: Task Detail ACP Observer

**Feature Branch**: `feat/task-detail-acp-observer`  
**Created**: 2026-09-11  
**Status**: Draft  
**Input**: User description: "task detail 按 event 重放 + acpRoute observer 做完整执行考古；本分支只做 ACP observer。"

## User Scenarios & Testing *(mandatory)*

### User Story 1 — 已完成任务的原生执行考古 (Priority: P1)

作为 ACP client 用户，我希望为已完成 task 新建一个 observer session 并发送 `--attach <task_id>`，收到该任务全部历史的原生 ACP `session/update`，从而由任意合规 renderer 展示真实的 thought、tool call、tool output、diff 与 agent message，而不是一堆包着 JSON 的文本日志。

**Why this priority**: 完整 trace 是 task detail 的主体验。没有可回放的原生 update，所谓 task detail 只是又一个低配日志页。

**Independent Test**: 预写入包含 message、thought、tool call、tool update 的 EventLog，attach 一个 completed task，断言收到顺序一致的 `session/update` 且每条使用 observer session ID，随后 prompt 以 `end_turn` 完成。

**Acceptance Scenarios**:

1. **Given** 一个有多个 `EventUpdate` 的 completed task，**When** observer 提示 `--attach task_xxx`，**Then** 每个历史 update 都按事件顺序成为标准 `session/update`
2. **Given** 回放中的 update，**When** outbound notification 发送，**Then** 外层 `sessionId` 是 observer 的 session ID，而不是原任务提交方或下游 worker 的 session ID
3. **Given** 历史包含 tool call 及其 update，**When** attach，**Then** tool call ID、内容和 update subtype 原样保留
4. **Given** 历史包含 `EventStatus`，**When** attach，**Then** status 不会伪造成 ACP `session/update` 或聊天文本
5. **Given** task 已完成，**When** history 重放完，**Then** prompt 返回 `end_turn`

---

### User Story 2 — 运行中任务的无缝观察 (Priority: P1)

作为 ACP client 用户，我希望 attach 一个运行中任务时，先得到已发生的完整 trace，再持续收到新 update；历史与实时之间不能漏一条，也不能重一条。

**Why this priority**: “后挂观察”只有在无缝连接 EventLog 历史与 live tail 时才可信。timestamp 拼接和先 replay 再挂实时 route 都有竞态空洞，不能用。

**Independent Test**: 在 `Subscribe` history snapshot 后追加 update，observer 必须只收到一次且保持完整事件顺序；任务结束后 observer 收到 `end_turn`。

**Acceptance Scenarios**:

1. **Given** task 正在 working，**When** observer attach，**Then** 先回放 Subscribe snapshot 的所有 EventUpdate
2. **Given** history snapshot 创建后出现新 update，**When** live signal 到达，**Then** observer 用 cursor 从 `EventsAfter` 取回全部未消费 update
3. **Given** subscriber signal 因缓冲已满而合并，**When** observer 醒来，**Then** cursor 仍追到所有 durable event，不丢更新
4. **Given** task 状态进入 `awaiting_review`，**When** trace 已重放到该状态，**Then** observer 结束本次 ACP prompt，不等待 review/merge 生命周期
5. **Given** task 正在 `merging`，**When** attach，**Then** observer 继续追尾，直到 terminal status

---

### User Story 3 — 原提交方与多路在线观察不互相影响 (Priority: P2)

作为任务原始提交 ACP client 或另一位观察者，我希望新的 observer 不覆盖现有实时 route，且 observer 取消不会中止正在执行的 task。

**Why this priority**: 当前 map 单 route 的行为会悄悄覆盖先前 session。多 route 必须是追加、独立移除、锁外广播，别把一个 observer 写成任务中断按钮。

**Independent Test**: 同 task 注册两个 route 并 attach observer；追加进度时每路恰好收一次；取消 observer 后原任务和原 route 仍收到后续进度。

**Acceptance Scenarios**:

1. **Given** 同 task 已有同步提交 route，**When** 新 observer attach，**Then** 原 route 保持可用
2. **Given** 同 task 有多个 route，**When** worker 产生 progress update，**Then** 每个当前 route 各收到一次
3. **Given** 一个 route 结束，**When** defer 移除它，**Then** 只删除自身 `{server, sessionID}`，不删除同 task 的其他 route
4. **Given** observer client 发出 `session/cancel` 或断开，**When** attach context 结束，**Then** EventLog subscriber 被移除，worker task 与其他 route 不受影响

---

### User Story 4 — 旧日志损坏不阻塞可用考古 (Priority: P3)

作为排障用户，我希望单条损坏或过期的持久 update 被跳过，并且后续有效 trace 仍可查看；一个坏 JSON 行不该让整段历史直接废掉。

**Why this priority**: EventLog 是长生命周期 JSONL。正常日志必须原样保存，但 attach 要能容忍已有损坏记录。

**Independent Test**: 写入一条无法解析的 EventUpdate 和一条有效 EventUpdate；attach 仍收到后者并可结束。

**Acceptance Scenarios**:

1. **Given** EventUpdate 的 payload 不是合法 `SessionUpdateBody`，**When** attach 回放，**Then** 记录 warning 并跳过该 event
2. **Given** 损坏 event 后还有有效 update，**When** attach，**Then** 有效 update 正常送出，cursor 继续推进
3. **Given** 一个协议允许的未知 update subtype，**When** payload 可解码，**Then** 原始 Raw payload 不被 attach 侧过度校验或丢弃

---

### Edge Cases

- `--attach` 缺 task ID、附带多余参数或引用不存在 task 时必须返回现有 RPC status error。
- 空历史 completed task 立即 `end_turn`，不得挂起。
- context cancel、连接关闭和 daemon Stop 都必须触发 unsubscribe。
- EventLog 的 status 可能在 history replay 中或 live tail 中出现；两处都必须判定终止语义。
- append EventLog 失败只记录错误，不能让原路由的实时 update 被阻断。

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: ingress MUST 接受格式严格为 `--attach <task_id>` 的 prompt directive。
- **FR-002**: 系统 MUST 在 attach 前验证 task 存在。
- **FR-003**: 系统 MUST 使用 `TaskEventLog.Subscribe` 获得历史 snapshot、cursor 与 live signal，并用 `EventsAfter` 按 cursor 追尾。
- **FR-004**: 系统 MUST 将每个有效 `EventUpdate` 作为标准 ACP `session/update` 发出，不得编码为 JSON 文本 chunk。
- **FR-005**: 系统 MUST 将回放 notification 的外层 session ID 映射为 observer session ID，并保持 update body 不变。
- **FR-006**: `EventStatus` MUST 仅驱动 observer completion，不得转写为 ACP update。
- **FR-007**: task 为 `awaiting_review` 或 terminal 时，系统 MUST 在所有可用 update 重放后返回 `end_turn`。
- **FR-008**: 系统 MUST 支持每个 task 多个 `acpRoute`，且 route remove MUST 只移除自身。
- **FR-009**: 广播 MUST 在 mutex 外执行；锁内只复制 route snapshot。
- **FR-010**: attach 取消 MUST unsubscribe 且 MUST NOT cancel worker task 或其他 route。
- **FR-011**: 无法解码的存量 EventUpdate MUST 记录 warning 并跳过，且 MUST NOT 阻断后续 event。
- **FR-012**: `--events` MUST 保持现有 JSON-text 调试协议；attach 不与其复用输出格式。

### Key Entities

- **Observer session**: ACP client 新建、仅用于观看一个 task trace 的 dispatcher session。
- **TaskEvent**: append-only EventLog 记录。V1 继续使用 `update` 和 `status` 两种类型，不改 schema。
- **ACP route**: 任务运行中可低延迟接收 worker update 的 `{server, sessionID}` 订阅目标。
- **Cursor**: observer 已消费 EventLog event 的下标；它是追尾的唯一进度边界。

## Success Criteria *(mandatory)*

- **SC-001**: 已完成 task attach 时，100% 的有效 EventUpdate 依序以原生 `session/update` 回放。
- **SC-002**: snapshot 和 live tail 边界测试中，事件重复数为 0、漏失数为 0。
- **SC-003**: 多 route 测试中，每个在线 route 对每次 progress update 恰好收到一次。
- **SC-004**: observer cancel 后 task 继续执行，已有 route 继续收 update。
- **SC-005**: 损坏 EventUpdate 后的有效 event 仍可回放，且目标 package 的 race 检查通过。

## Assumptions

- dispatcher 继续仅监听本地 Unix socket，不增加 TCP 暴露面。
- 下游 worker 产生的标准 ACP updates 已由现有 orchestrator callback 保存到 EventLog。
- task review、permission/HITL 与 UI renderer 各自是后续 feature，不在本分支扩张范围。
