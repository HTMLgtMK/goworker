# session/load 与会话续接：设计讨论记录

状态：讨论稿 v2（2026-09-17，两轮讨论后）。基于当前代码实况，逐条讨论三个
议题；每条给出现状核实 → 讨论 → 结论。标注【待定】的条目需要拍板后才进入实现。

背景：daemon 的 ACP 入口（`ai-dispatch`）实现 `session/list`、`session/new`、
`session/load`、`session/prompt`。会话持久化在 `ai-runtime/session`（jsonl
追加式 store，parent 链 + compact 树），记忆固化在 `ai-memory`。

---

## 议题 1：会话上下文的来源分层（models / cwd / mcpServers）

### 现状核实

| 上下文 | 来源 | 现状 |
|---|---|---|
| models（llm.providers 等） | daemon 进程配置（config.yaml） | `agent` 插件启动时装入 `SessionDeps.Config`，进程级 |
| skills / MCP | daemon 进程配置 | `startSession()` 统一加载，进程级 |
| cwd（session/new） | client 请求字段 | wire 上已传（vscode 前端把 workspace 路径放进 `session/new`），但 vscode ingress `SetSession(sessionID, _ string)` 直接丢弃 |
| cwd（session/load） | client 请求字段 | `LoadSessionRequest.Cwd` 解码后在 `dispatch.Server.handleSessionLoad` 就被丢掉，只转发 sessionId 给 handler——连 handler 都拿不到 |
| mcpServers（new/load） | client 请求字段 | 始终忽略，daemon 只用自己的 MCP 配置 |
| 工具执行目录 | `sandbox.allowed_work_dir`（全局配置） | bash 工具 `cmd.Dir = cfg.AllowedWorkDir`；checkpoint 记录的是 `os.Getwd()`（daemon 进程 cwd） |

对照组：dispatcher 的 `acpIngress` 已经实现了每会话 cwd——`SetSession` 把
sessionID→cwd 存 map，`Run` 时取用，`session/new` 不带 cwd 直接报错。即
「按会话 cwd」在 dispatcher 侧是既有事实，agent 插件侧没有。

另：ACP SDK 的 `SessionInfo` schema 把 `cwd` 定为必填字段，daemon 清单现在
不填（omitempty 缺省）；未来补 cwd 时顺带对齐。

### 讨论

职责分层应该是三层，互不越界：

1. **进程级（daemon 配置）**：models、skills、MCP server 定义、sandbox 策略。
   client 不该传，agent 也不该从 wire 取。daemon 有完整 models 配置，续接
   会话时用本地配置重建 provider 即可，无需 client 提供任何模型信息。
2. **会话级（wire 请求字段）**：cwd、mcpServers 引用。这是 client 对「这次
   会话在哪个环境里干活」的声明，应当被尊重而不是丢弃。
3. **消息级（store）**：对话内容。已有 parent 链模型，见议题 3。

支持每会话 cwd 的改动面（agent 插件侧）：

- `dispatch.Server.handleSessionLoad` 把 `req.Cwd` 连同 sessionId 一起传给
  `SessionLoader.LoadSession`（接口加参或改传 request struct）；
- vscode ingress 象 dispatcher 一样存 sessionID→cwd；
- `SessionDeps` 增加 `CWD`（会话工作目录），向下渗透：
  - bash 工具：`cmd.Dir` 优先用会话 cwd，`allowed_work_dir` 退化为边界校验；
  - read/write 等相对路径工具：相对会话 cwd 解析；
  - memory checkpoint：`CWD` 记会话 cwd 而非 `os.Getwd()`（`agent/checkpoint.go`）；
- worker 模式（`acpWorker.SetSession`）同样落地 cwd——orchestrator 在
  `session/new`/`session/load` 已经传了 `t.Worktree`/`t.Repo`，wire 层早就
  是每任务 cwd，只是 worker 端没收。

安全边界：sandbox 是 daemon 的安全策略，不能被 client cwd 绕过。规则建议——
`allowed_work_dir` 非空时，client 声明的 cwd 必须落在它之下（含符号链接解析），
否则拒绝该会话（`missing/invalid session cwd`）；为空时沿用现行为。

mcpServers：client 声明的 per-session MCP 集合是更大的改动（连接生命周期要
跟会话走）。先不做，daemon 维持自己的 MCP 配置；请求字段继续忽略但记录在案，
未来要做时从 `SetSessionServer` 的时机入手。【待定：是否列入 roadmap】

**models 显示（本轮决议）**：「models 会话覆盖」指同一 daemon 下不同会话
各自绑定 provider（A 会话 deepseek、B 会话 kimi），需要 per-session provider
状态 + `session/set_mode` 全链路，成本高、暂不做。用户拍板：**先只显示、
不做切换**。ACP 现成挂点：

- `session/new` / `session/load` 响应允许带 `modes: { currentModeId,
  availableModes[] }`（vscode 前端 bundle 的 ACP SDK 有 `SessionModeState`
  schema，webview 自动渲染模型选择器，见 dist extension.js 的
  `sessionModelStateToAcpUiSelection`）；
- daemon 侧新增可选接口（形如 `SessionModesProvider`，对齐 `SessionLister`
  模式），`handleSessionNew` / `handleSessionLoad` 探测并填充 modes；
  数据源 `cfg.LLM`：`currentModeId = DefaultProvider`，
  `availableModes = providers`（id=provider 名，name=人类可读，如
  `deepseek (deepseek-chat)`，description 可空）；
- `session/set_mode` 暂不实现：用户点切换 → method-not-found → 前端回退，
  诚实失败优于假成功。后续真要做切换时，最小可行路径是把选中 provider 写进
  会话级 deps（`SessionDeps.Config` 的副本）+ 重开 provider，不动全局配置。

### 结论

- 接受 wire cwd 为会话工作目录，daemon 配置做 fallback 与安全边界（上轮确认）；
- models：**只显示不切换**——modes 填充进 new/load 响应，`session/set_mode`
  暂不实现（本轮拍板）；
- mcpServers 维持忽略，标记为未来扩展。

---

## 议题 2：归档会话的清单排序、查看与 resume

### 现状核实

- daemon `ListSessions`（`daemon/internal/agent/sessions.go`）**已经**是：
  当前会话置顶（不参与排序）+ `archive/*.jsonl` 按文件 mtime 倒序，上限 50。
  排序诉求 daemon 侧已满足。
- vscode 前端 `toChatEntries`（`extensions/vscode-goworker/src/views/chatEntries.ts`）
  **故意只渲染当前会话**：注释明说「daemon 尚不支持归档重放（session/load 对
  归档 id 必报错），只保留置顶的当前会话，避免用户点开注定失败的条目」。
- `LoadSession`（`daemon/internal/frontend/vscode/frontend.go`）对归档 id 返回
  明确错误 `archived session resume is not supported yet`。

所以缺口不在排序，在**归档会话能不能打开**：清单给了但前端不敢展示，因为
load 一定失败。

### 讨论（两轮）

第一轮提出方案 A（load = 会话切换：先 Archive 当前、再 rename 归档文件为
current.jsonl、store 重开）与 A'（复制）。本轮用户拍板：**先做「仅查看」，
resume 后议**——即第一轮的方案 B 升级为选定方向。

方案 B（只读查看）落地形状（单活动会话模型下）：

- **只读归档视图**：session 包新增只读加载路径（形如
  `OpenArchiveView(path)`）：复用 `loadRecords` 解析归档 jsonl，构建
  ActiveView + checkpoint 锚点，**不持写句柄、不 rename、不触碰 current**；
- **load 流程**：`session/load(archiveId)` → 只读解析 → 复用现有重放映射
  （`CurrentSessionHistory` 同款：user→message_chunk、thinking、tool_call
  生命周期）推 session/update → 返回 `{}`。ingress 对归档 id **不调用
  `remember()`**（不进可 prompt 集合）；
- **续写防护（双保险）**：dispatch 层对只读会话的 `session/prompt` 诚实报错
  `archived session is read-only`；前端归档条目（`isCurrent=false`）直接禁用
  聊天输入框——用户从 UI 就看不出「能输入」的错觉；
- **前端清单恢复**：`toChatEntries` 恢复完整清单（遍历全部条目、仅 index 0
  标 `isCurrent`）——代码里已预留恢复点；
- 与方案 A 的关系：B 是 A 的前置子集。B 的只读解析、清单恢复、重放路径全部
  可被 A 复用；将来做 resume 时只需把「rename 切换 + 写句柄接管」叠上去，
  不会扔掉 B 的工作。

清单细节确认：

- sessionId 用归档文件名（UnixNano）稳定唯一；只读查看不改变文件位置，id
  不变（A 方案下 rename 后才由 `store.Head()` 接管新 id）；
- mtime 排序已实现且只读查看不改动 mtime，无需再动。

### 结论

- 排序：已实现，无需改；
- 本阶段做**只读查看**（方案 B）：只读解析归档 + load 重放 + prompt 诚实
  报错 + 前端完整清单与输入禁用；
- resume（切换/恢复续写）后议，B 的产出为其前置子集。

---

## 议题 3：续接会话时的 history 重建（parent 链回溯）

### 现状核实

`Store.ActiveView()`（`ai-runtime/session/store.go`）**正是**这个语义：

1. 从 `head` 沿 `parent` 指针倒序回溯（带 visited 防环）到根；
2. 反转得正向顺序；
3. `compact` 节点转成一条 `role=system` 的摘要消息。

每个 Session 构造时和每轮 Run 提交后都从 ActiveView 刷新 conversation
（`refreshConversation`），即续接/继续对话的 history 重建已经完全由 parent
链驱动，与文件行顺序无关。

一个需要澄清的语义：回溯**不是**「遇到 compact 就停」。compact 节点的
parent 指向被覆盖段之前的前驱（`Compact` 里 `compactParent = activeIDs[fromIdx-1]`），
链会穿过 compact 继续回溯到根。最终视图是：

```
[covered_from 之前的真实消息 ..., compact 摘要(system), covered_to 之后的副本...]
```

即 compact 只替换 `[covered_from..covered_to]` 这一段，**不截断更早的历史**
（store_test.go `TestCompactBasic` 断言 view = [m0, s_m3, m4']）。

「遇到 compact 即停、丢弃 compact 之前所有历史」是不对的：covered_from 之前
的消息不在压缩覆盖范围内，仍是有效上下文，截掉纯属丢信息。现语义更合理。

配套事实（重建的完整拼图）：

- system prompt 不落 store：commit 时剥离 `messages[0]`（system）和 memory
  注入块（`stripMemoryBlocks`，防自指污染）。续接时 system prompt 由当次 Run
  重新生成、记忆检索重新注入。**重建 = 新 system prompt + 新记忆注入 + ActiveView**。
- checkpoint 记录不参与重建，只作 `/rewind` 锚点和 `session/load` 重放的轮次
  切分边界（`segmentByCheckpoints`）。
- parent 链（而非文件顺序）作为重建依据，正好使 rewind 语义免费成立：
  `SetHead` 把 head 挪到早前节点后，被弃分支仍在文件里但天然不在回溯链上，
  续写自动挂到新 head 下。

### 结论

现设计与预期一致，**无需改代码**；本议题的价值是把语义钉死进文档：

- 重建算法：head 沿 parent 回溯到根，反转；compact 节点 = system 摘要消息，
  链穿过它继续回溯（compact 只替换覆盖段）；
- system prompt / 记忆注入永远是会话级的「活水」，不入 store、不重放；
- 文件行顺序、checkpoint、head 之外的分支都不是重建依据。

---

## 决议汇总

| 议题 | 结论 | 改动点 | 状态 |
|---|---|---|---|
| 1a. cwd 分层 | wire cwd 采纳为会话 cwd；sandbox 做边界 | dispatch server 传参、ingress 存 cwd、SessionDeps.CWD 渗透到工具与 checkpoint | 方向已确认，待开工 |
| 1b. models | 只显示不切换：modes 填充进 new/load 响应；set_mode 暂不实现 | protocol 增 SessionModeState；dispatch 探测 SessionModesProvider；agent 从 cfg.LLM 填充 | 已拍板 |
| 2. 归档 | 只读查看（方案 B）；resume 后议 | session 只读归档视图、load 分支重放、prompt 报错、`toChatEntries` 完整清单 + 输入禁用 | 已拍板 |
| 3. parent 重建 | 现设计即如此；穿过 compact 不截断早前历史 | 无 | 已确认 |
| — | mcpServers 维持忽略 | 无 | 记录在案，未来扩展 |

---

## 实施记录（2026-09-17 两批完成）

### Go 批次（1a cwd + 1b modes + 2 归档只读）
- `ai-dispatch`：SessionLoader 加 cwd 参数；SessionModesProvider 可选接口；SessionMode/SessionModeState 类型；SessionInfo.IsCurrent。
- `ai-runtime/agent`：SessionDeps.CWD → Run 时注入 sandbox 配置副本 AllowedWorkDir（bash cmd.Dir / read / write 全部锚定会话 cwd）；checkpoint CWD 会话级优先。
- `ai-runtime/session`：提取共用 buildActiveView/checkpointsFromRecords；新增 ArchiveView 只读视图（OpenArchiveView，含 compact 场景测试与字节级只读断言）。
- `daemon/agent`：SetSession cwd 归一化 + sandbox 边界校验（Abs+EvalSymlinks 前缀）；ArchivedSessionHistory 复用重放管线；SessionModes 从 cfg.LLM 填充；ListSessions 置 IsCurrent/回显 cwd。
- `daemon/frontend/vscode`：LoadSession 三分支（当前会话不变/归档只读重放/未知报错）；只读集合 prompt 返回 "archived session is read-only"。
- 验证：各模块 go build/vet 干净；session/dispatch/agent/vscode 包测试全绿（-race）。预存在 flake：TestLoadMCP_ConnectsFakeServer 全量并发下握手超时（基线复现，与本批无关）。

### TS 批次（extensions/vscode-goworker）
- sessionListClient：isCurrent 窄化（仅 true 保留，对齐 omitempty）。
- chatEntries：toChatEntries 恢复完整清单，isCurrent 优先 wire 标记、缺失回退 index 0 约定。
- extension.ts：移除归档确认弹窗，树点击直接 load 重放。
- chatsTree：归档行 tooltip 补 "Archived · read-only"。
- 错误透传确认：归档 prompt 的 -32000 经 SDK prompt() catch → postToWebview({type:'error'}) → webview errorText 渲染，无需新通道。
- 验证：npm test 35/35 通过；tsc --noEmit 干净。

### 已知取舍
- 会话 cwd 跨 /new：保留最近一次有效声明，无效标记随重建作废。
- read/write 相对路径在「未声明 cwd 但配置了 allowed_work_dir」时基准从进程 cwd 变为 allowed_work_dir（与 bash cmd.Dir 一致化，有意修正）。
- session/set_mode 未实现（modes 只显示不切换，切换走 method-not-found 诚实失败）。
- 归档 resume 后议；ArchiveView/重放管线/清单恢复均为其前置子集。
