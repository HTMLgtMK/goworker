package model

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
)

const SessionSourceCapability = "session.source"

// SessionSummary is the daemon's protocol-neutral view of one persisted
// session. 三态归属由 State 给出（Plan A3：active/idle/archived，mapping 透传
// wire；评审收尾删除了旧 Active 布尔位——它与 State=active 完全同源，全仓无
// 读者，双事实源是腐化面）。Archived 是 archived 终态标记的 domain 形（Plan A1
// 记录模型），清单据此分流只读语义。
type SessionSummary struct {
	SessionID string
	Cwd       string
	Title     string
	UpdatedAt string
	// Archived 报告会话是否已落 archived 终态标记（Plan A3；与 State=archived
	// 同源——终态是互斥三态的一支，字段冗余是为 wire 之外的 domain 消费方
	// （load 分流）提供不解析 State 字符串的直读面）。
	Archived bool
	// State 是三态归属（Plan A3/定案需求 #3）：active（连接绑定或 run 在飞）、
	// idle（无引用无在飞）、archived（终态标记）。空串 = 清单方未判定（仅降级
	// 形态可能产生）。
	State string
}

// 会话三态（Plan A3/定案需求 #3）：wire 的 SessionInfo.state 直接取这三值。
const (
	SessionStateActive   = "active"
	SessionStateIdle     = "idle"
	SessionStateArchived = "archived"
)

// SessionMode is a protocol-neutral model/provider choice.
type SessionMode struct {
	ID          string
	Name        string
	Description string
}

// SessionModeState is the daemon domain representation of available modes.
type SessionModeState struct {
	CurrentModeID  string
	AvailableModes []SessionMode
}

// SessionContent is the protocol-neutral text content used by replay events.
type SessionContent struct {
	Type string
	Text string
}

// SessionUpdate is a typed port between the daemon session domain and a wire
// adapter. Raw carries extension fields without exposing map[string]any as a
// capability API.
type SessionUpdate struct {
	Kind       string
	Content    *SessionContent
	ToolCallID string
	Title      string
	Status     string
	Raw        json.RawMessage
}

const (
	SessionUpdateUserMessageChunk  = "user_message_chunk"
	SessionUpdateAgentMessageChunk = "agent_message_chunk"
	SessionUpdateAgentThoughtChunk = "agent_thought_chunk"
	SessionUpdateToolCall          = "tool_call"
	SessionUpdateToolCallUpdate    = "tool_call_update"
	SessionUpdateAvailableCommands = "available_commands_update"
)

// SessionSource is the consumer-facing capability exposed by the agent plugin.
// It deliberately uses daemon domain types; the VS Code/ACP adapter maps them
// to ai-acp/protocol values at the transport boundary.
type SessionSource interface {
	ListSessions() []SessionSummary
	CurrentSessionID() string
	// ActiveSessionID 返回 transport 层的活动会话 id：最近一次经 session/new 或
	// session/load（当前会话）有效声明的 transport UUID。它与 CurrentSessionID
	// 返回的 store 稳定 id 不同源——前者是连接方自造的会话句柄，后者是持久化
	// store 的逻辑身份。空 = 尚无 ACP 连接声明过会话（守卫不参与）。
	// daemon 是单活动会话模型：prompt 一个已被其他连接声明顶掉的 transport 会话
	// 会在 FrontendHandler.Prompt 被 superseded 守卫拒绝，而非静默串线执行。
	ActiveSessionID() string
	SetSession(sessionID, cwd string)
	ReloadCurrentSession() error
	CurrentSessionHistory() []SessionUpdate
	ArchivedSessionHistory(sessionID string) ([]SessionUpdate, error)
	SessionModes() *SessionModeState
}

// ProviderCatalogEntry 是 provider 模型目录的一个条目：id = provider 名
// （/model use 的寻址面），Models 为该 provider 的模型目录（Models 配置非空
// 用之，否则 [Model]），Model 为该 provider 当前的生效模型（UI 一键切
// provider 时与 id 配对提交）。
type ProviderCatalogEntry struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Type   string   `json:"type,omitempty"`
	Model  string   `json:"model,omitempty"`
	Models []string `json:"models"`
}

// ProviderCatalog 是 provider 模型目录的只读快照（UI 弹框数据源）：current*
// 取 DefaultProvider 的当前字段（model / thinking effort）。无 LLM 配置时
// ProviderCatalog() 返回 nil。
type ProviderCatalog struct {
	CurrentProviderID string                 `json:"currentProviderId"`
	CurrentModel      string                 `json:"currentModel"`
	CurrentEffort     string                 `json:"currentEffort"`
	Providers         []ProviderCatalogEntry `json:"providers"`
}

// ProviderCatalogSource 是 provider 模型目录的可选能力（x-goworker/providers
// RPC 的数据面；工厂内对 SessionSource type-assert，与 RunControlSource 同款
// 先例）。nil = 后端无目录能力（简化壳/测试桩），方法诚实报错。
type ProviderCatalogSource interface {
	ProviderCatalog() *ProviderCatalog
}

// ProviderDetail 是单个 provider 的全量配置快照（管理面表单数据源）。
// APIKey 永不出域：只给 APIKeySet 布尔——密钥经 wire 回传 UI 是泄露面。
type ProviderDetail struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
	// Endpoint / APIKeySet / Models 是表单直接消费的三元组。
	Endpoint  string   `json:"endpoint"`
	APIKeySet bool     `json:"apiKeySet"`
	Models    []string `json:"models"`
	// 可选字段（openai/anthropic 各有取舍，零值 = 未配置）。
	ContextWindow int    `json:"contextWindow,omitempty"`
	MaxTokens     int    `json:"maxTokens,omitempty"`
	AuthType      string `json:"authType,omitempty"`
	RequestMode   string `json:"requestMode,omitempty"`
	Effort        string `json:"effort,omitempty"`
	// IsDefault = 该 provider 是 cfg.LLM.DefaultProvider（UI 据此禁用删除）。
	IsDefault bool `json:"isDefault"`
}

// ProviderUpsertRequest 是 provider 新建/更新的请求面（更新 = merge：
// Endpoint/Type/Models 覆盖；APIKey 空串 = 保留原值；ContextWindow/MaxTokens/
// AuthType 零值 = 保留原值；thinking 字段不在请求面——表单不触碰即保留）。
// ID 为空 = 新建（Name 即 map key，受 daemon 侧 validProviderName 约束）。
type ProviderUpsertRequest struct {
	ID            string   `json:"id,omitempty"`
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	Endpoint      string   `json:"endpoint"`
	APIKey        string   `json:"apiKey,omitempty"`
	Models        []string `json:"models"`
	ContextWindow int      `json:"contextWindow,omitempty"`
	MaxTokens     int      `json:"maxTokens,omitempty"`
	AuthType      string   `json:"authType,omitempty"`
}

// ProviderManageSource 是 provider 配置管理的可选能力（x-goworker/provider/
// detail|upsert|remove 的数据面；工厂内 type-assert，CatalogSource 同款先例）。
// 全部走 hub.SaveConfig（写盘 + 引擎内存同步）——外部编辑配置文件无热加载，
// RPC 经 SaveConfig 是唯一即时生效路径。ProviderUpsert 返回落盘后的 provider id。
type ProviderManageSource interface {
	ProviderDetail(id string) (*ProviderDetail, error)
	ProviderUpsert(req ProviderUpsertRequest) (string, error)
	ProviderRemove(id string) error
}

// PendingApprovalInfo 是一笔等待决策的审批快照的 daemon 侧扁平结构（P5-6）：
// 字段取 hitl.InterruptRequest 按 wire 需要的最小面（requestId 寻址 + 展示所需
// 的命令/风险/截止），不透传 agent 包的 PendingApproval 结构——model 是协议
// 中立层，避免 model→agent 类型泄漏（SessionSource 的 domain-type 先例同理，
// 方向相反：那处是 agent 类型不进 model，这处是 agent 包结构不进 model）。
// 完整原始中断（Effects/AllowAlways/Description 等）经 P5-8 attach 回放的
// permission.requested 事件 payload 携带，不在本清单面上重复。
type PendingApprovalInfo struct {
	RequestID string
	SessionID string
	RunID     string
	ToolName  string
	Command   string
	RiskLevel string
	ExpiresAt time.Time
}

// RunControlSource 是 x-goworker 扩展协议的消费面（P5-6，可选能力接口，handler
// 以 type-assert 获取——SessionSource 先例；无此能力的插件/测试桩零感知）。
// 路由面（C3 修正，推翻拍板⑤的 current-only）：run/start|status|cancel、attach
// 与 approval 列表按 sessionID 多会话寻址（runtime 注册表权威），未知会话返回
// ErrSessionNotFound 语义错误；同步 /agent 与标准 ACP 面保持 current 语义不动
// （总方案 §7）。Control lease 为 V1 宿主授权简化形（拍板②原文「每会话至多
// 一个 control 持有者」，I3 修正后按会话维度记账）：本机信任、每会话至多一个
// 持有者（两连接各控各会话互不影响）、断开即释放（handler.Close 经
// ReleaseConnection 释放该连接全部租约）、后到连接对同一会话显式接管
// （tookOver=true + slog 审计）。
//
// 方法集纪律(第四轮评审 R3):本接口刻意**不含** StartRunSync/CancelRunSync——
// 同步宿主入口(/agent、session/prompt、CLI prompt)的提交/取消是
// agentcontrol.InternalRunner 受限内部面,仅供宿主组装面注入可信的本地命令
// Adapter(CLI/嵌入命令);Transport Adapter(含未来新增)一律走本接口的 fencing
// 面(StartRun/CancelRun:Connection Lease 校验与提交同一临界区)。消费者
// (acp handler 的 h.runs)type-assert 到的是本接口 ⇒ 拿不到 InternalRunner,
// ACP 面结构性无法绕过 fencing。分离的钉死点:agentcontrol 与插件侧的编译期
// 断言(var _ agentcontrol.InternalRunner = (*Service)(nil) /
// (*AgentPlugin)(nil))正向钉住受限面;本接口不含 Sync 方法由 acp 包反射守卫
// (constraint_test.go)钉死——Go 无编译期负断言,反射守卫 + CI 是可落地的最强
// 钉法,改动本方法集时两处都会拦。
type RunControlSource interface {
	// StartRun 提交一次后台 Run 并立即返回 (runID, 提交时状态)。connectionID
	// 是调用方连接句柄(fencing,第三轮评审 I4):实现必须把「lease 校验
	// (lease[sessionID] == connectionID,未持有→明确错误)」与操作提交合并进
	// 服务端受保护临界区(与 AcquireControl 同锁)——校验通过后不可能被并发
	// 接管,接管后旧持有者的控制操作一律拒绝。requestID 是与 JSON-RPC id 不同的
	// 幂等键，按 (sessionID, requestID) 组合寻址（I5）：
	// 重试同键同 Input 返回原 runID 与其当前状态，不重复起跑；同键不同 Input
	// （含跨会话同 requestID 不同 Input）返回显式冲突错误。
	StartRun(sessionID, connectionID, input, requestID string) (runID, state string, err error)
	// CancelRun 取消一个后台 Run(connectionID fencing 同 StartRun:lease 校验与
	// 取消提交在同一受保护临界区内原子完成,接管后旧持有者一律拒绝)。
	CancelRun(sessionID, connectionID, runID string) error
	// RunStatus 返回 run 当前状态、会话事件流的 latest seq（游标描述，非游标
	// 授权——客户端只有真正应用了对应事件才能推进本地游标）与回放完整性三态
	// replayIntegrity（I4：eventsComplete 布尔的替代，取 runtimeagent 的
	// ReplayIntegrityComplete/Unknown/Incomplete——"complete"=本进程未观察到
	// 写侧失败且无可疑历史；"unknown"=journal 有跨进程纪元的既有历史，完整性
	// 不可验证，诚实优先；"incomplete"=本进程发生过 key 事件落盘失败或合并
	// delta flush 失败，粘滞永不复位。非 complete 都不得把收到的事件当完整
	// 历史；无事件流/无失败证据时为 complete）。
	RunStatus(sessionID, runID string) (state string, latestSeq uint64, replayIntegrity string, err error)
	// PendingApprovals 返回会话未过期审批快照（daemon 侧扁平结构，避免
	// model→agent 类型泄漏）。未知会话/无审批返回空切片。
	PendingApprovals(sessionID string) ([]PendingApprovalInfo, error)
	// ResolveApproval 投递一笔审批决策(C1 修复:归属三段校验;第三轮评审 I4:
	// fencing——connectionID 的 lease 校验、归属校验与 Resolve 提交在实现侧
	// 同一受保护临界区内原子完成,接管与决策提交不可交错):先以服务端
	// 记录的审批归属为准(ApprovalOwner)——归属不存在(未知 requestID)或归属
	// 会话与 sessionID 不一致(越权,即便调用方持有其他会话的 control lease)
	// 一律拒绝,绝不信任客户端声明;未持有该会话 lease 的连接(含被接管的旧
	// 持有者)在提交前即被拒绝;匹配才投递,首个有效决策获胜,迟到/重复
	// 决策返回错误。
	ResolveApproval(sessionID, connectionID, requestID string, decision hitl.Decision) error
	// SubscribeEvents 建立会话事件订阅并返回订阅契约(总方案 §七,PR3 收敛:
	// 原 Attach 返回具体 *runtimeagent.EventHub 的越界面随之移除——EventHub
	// 从协议面隐藏,ACP 只消费 agentcontrol.AttachResult:游标、回放完整性
	// 三态、活动 Run 快照与 EventSubscription 回收句柄,事件通道经
	// Subscription.Events() 供给)。afterSeq 是客户端已确认游标:回放
	// (afterSeq, 注册点 latest] 后无缝续 live;游标失效(超前/落于保留历史
	// 之前)→ ErrCursorExpired 语义错误。§七 边界限制:本方法只做数据面订阅,
	// **不**顺便 AcquireControl——数据面与授权面拆分,attach(mode=control)
	// 的两动作编排(先订阅成功、再取 lease、失败回滚不动 lease)由调用方
	// 显式保证。未知会话 → ErrSessionNotFound 语义(CR4 多会话路由);
	// RunEvents 未接线/纯内存 → 诚实错误(hub 不存在,无事件流可订阅)。
	SubscribeEvents(sessionID string, afterSeq uint64) (agentcontrol.AttachResult, error)
	// ActiveRun 返回会话当前活动 Run 的 (runID, state) 快照（P5-8 attach 响应
	// 的 currentRun 字段消费；RunControlSource 在 P5-6 未备该访问器，P5-8 按
	// 响应契约最小补齐——RunStatus 需 runID 寻址，无法充当）。无活动 Run 或无
	// runtime（未注册/已收口）→ ok=false（currentRun=null，非错误）。第二轮
	// 评审 I1 修正：按 sessionID 多会话路由（与其他路由入口同一身份模型），
	// 非 current 会话返回其自身的在途快照而非恒 null。
	ActiveRun(sessionID string) (runID, state string, ok bool)
	// AcquireControl 获取 (sessionID, connectionID) 的 control lease：该会话
	// 空闲→占用并返回 false；他人持有→显式接管（返回 true，slog.Info 审计）；
	// 同持有者重复→false。每会话独立记账（I3）：连接 A 控制会话 A、连接 B
	// 控制会话 B 互不影响，接管只发生在同一会话上。
	AcquireControl(sessionID, connectionID string) (tookOver bool)
	// ReleaseControl 释放 (sessionID, connectionID) 的 control lease：仅该会话
	// 的持有者本人有效，他人调用为空操作。
	ReleaseControl(sessionID, connectionID string)
	// HoldControl 报告 connectionID 当前是否持有 sessionID 的 control lease。
	HoldControl(sessionID, connectionID string) bool
	// ReleaseConnection 释放 connectionID 持有的全部会话的 control lease
	// （I3：断连收口——handler.Close 一次调用替代逐会话归还，审批不悬死在
	// 已死连接上）。连接不持有任何租约时幂等空操作。
	ReleaseConnection(connectionID string)
	// UnbindCapabilities 解绑某连接登记的全部客户端工具能力（P5-9 的
	// handler.Close 断连收口消费）。
	UnbindCapabilities(connectionID string)
	// CapabilityWaitTimeout 返回设备工具断连等待重绑的预算（拍板③默认 45s，
	// 硬约束 < ai-core 工具执行 60s）。
	CapabilityWaitTimeout() time.Duration
}

// DeviceCaller 是 client-ward 调用通道的最小面（P5-9，结构类型——PermissionAsker
// 先例 permission.go:32-34）：x-device 扩展的工具探测（x-device/tools）与执行
// 转发（x-device/call）都经它发往客户端。acpworker.DeviceCaller（dispatch.Server
// 实现）方法集相同、天然满足；model 不 import acpworker，无环。
type DeviceCaller interface {
	Call(ctx context.Context, method string, params, result any) error
}

// ClientToolBinder 是设备工具的能力代理面（P5-9，P5 显式可见变化 #1 的 daemon
// 生产者）：对端声明 x-device 能力时，ACP handler 优先经它取得绑定 SDK
// CapabilityRegistry 的稳定工具代理——断连后设备工具调用等待重绑（默认 45s，
// CapabilityWaitTimeout）而非立即报错给模型；未实现此能力的插件/测试桩由
// handler 以 type-assert 回落原始 relay 路径，行为不变。
//
//   - connectionID 是 x-goworker 控制面的连接句柄（handler 工厂生成）：绑定按
//     （会话，connection）二元组登记，断连经 RunControlSource.UnbindCapabilities
//     同键解绑（handler.Close 收口，连接退出即解绑）；
//   - caller 是本连接的反向调用通道；
//   - 返回 nil = 本连接无设备工具（能力未声明/探测失败：现状收缩语义「无能力
//     即无工具」，不登记任何绑定）；非 nil 闭包的调用时机 = 每轮 prompt 工具
//     收集（勘察结论 D），每次成功探测都幂等刷新绑定（重绑 = 刷新 TTL + 工具集）。
type ClientToolBinder interface {
	ClientToolSource(connectionID string, caller DeviceCaller) func(ctx context.Context) []core.Tool
}

// ErrSessionUnknown 是 BoundSessionSource 声明面的「会话不存在」sentinel
// （Plan A2b1）：handler 的 session/load 据此把未知会话映射回既有的 -32000
// 「unknown session」wire 契约。为什么放 model 而不直接判执行侧 sentinel：
// ACP 生产面不得 import ai-runtime/agent（service/acp constraint_test 钉死，
// EventHub 具体类型不回流协议层的同一道隔离），路由面的 ErrSessionNotFound
// 对 handler 不可见——协议中立层（model）的 sentinel 是依赖方向允许的唯一
// 判定点；实现侧用多 %w 同时包裹本 sentinel 与执行侧 sentinel，两个消费面
// 各取所需。
var ErrSessionUnknown = errors.New("session: unknown session")

// BoundSessionSource 是 session/new|load 多会话语义的可选能力面（Plan A2b1，
// handler 以 type-assert 获取——RunControlSource/ConnectionBinder 先例；无此
// 能力的 SessionSource 实现（简化壳/测试桩）零感知，handler 落既有单活动
// 降级路径）。
//
// 为什么是独立可选接口而不是扩 SessionSource 方法集：SessionSource 的既有
// 方法集是「current 语义」的元数据面（ListSessions/CurrentSessionID/
// ReloadCurrentSession/SetSession/...），A2b1 把标准 ACP 的 session/new|load
// 从「落 current 的单活动语义」切换到「连接绑定语义」（plan A2「current 收缩
// 为 CLI/stdin 便利指针」）——两组语义在过渡期并存于同一插件（A2b2 收缩 gate、
// A4 反转空 cwd 语义、A3 重写清单，各自继续消费 current 面），可选接口把边界
// 显式化，也让无多会话能力的实现保持可编译；方法集 opt-in 的纪律与
// RunControlSource 的 R3 注释同源（受限面必须显式声明，不做隐式膨胀）。
//
// 与 ConnectionBinder 的分工：本接口是「声明动作」的执行面（创建/重开/reload
// 的状态突变），ConnectionBinder 是「声明登记」的记账面（生命周期引用计数）；
// 旧实现由 handler 在声明成功点成对驱动两者；实现 BoundSessionLoader 的后端则
// 在连接级 load 临界区内完成历史快照与登记，避免 Runtime 在成功返回前被回收。
type BoundSessionSource interface {
	// CreateBoundSession 真新建一个会话（ACP session/new，定案需求 #2 的 ACP
	// 面）：持久化 Create（创建即声明 workspace，Plan A1）→ 新 runtime 注册进
	// manager。cwd 是新会话声明的工作目录（空 = 不落 workspace 记录，执行域
	// 维持既有回退语义）；cwd 落在 sandbox 边界外时拒绝创建（创建前失败优于
	// 产出一个每个 prompt 都会被拒的带标记会话）。返回 store 稳定 SessionID
	// （handler 据此设 Binding）。**不动 current**：新会话只是发起连接的路由
	// 目标，发起方此前绑定的会话由登记面的改绑评估走引用计数（A2a 机制）。
	// 不携带请求 ctx：实现内部（repo.Create）的 ctx 只做入口取消检查，管理
	// 操作是短临界区、无长等待可取消点——断连中途的创建照常完成且无害（响应
	// 随连接丢弃，孤儿会话由断连 ReleaseConnection 的引用计数收口），不为它
	// 扩签名（LoadBoundSession/SessionHistory 同理）。
	CreateBoundSession(cwd string) (string, error)
	// LoadBoundSession 把 sessionID 载入为「可 prompt 的已注册会话」，二分支：
	//   - 已注册 runtime（manager 注册表命中，含 current 与别连接绑着的）：
	//     reload 内存视图（共享会话可能有别连接的追加），cwd 非空时重申声明；
	//   - 持久化但不在册：重开（repo.Open，flock 空闲拿活实例——A2a 收口逐出
	//     缓存后的重开走 flock 慢路径，这正是重开需要的语义）→ 新 runtime
	//     注册 → cwd 恢复（实参非空优先，否则 workspace 记录）。
	// 未知会话返回包裹 ErrSessionUnknown 的错误；cwd 无效（sandbox 边界外）
	// 不拒绝 load——落无效标记（prompt 诚实报错），会话已存在、拒绝 load 会让
	// 历史不可达，与 SetSession 的既有无效声明语义一致。
	LoadBoundSession(sessionID, cwd string) error
	// SessionHistory 导出已注册会话的完整对话历史（session/load 响应前的重放
	// 面）。未知会话报错；纯内存（无持久化）返回空切片。
	SessionHistory(sessionID string) ([]SessionUpdate, error)
	// SessionArchived 只读探测：会话是否已落 archived 终态标记（Plan A1 记录
	// 模型）。handler 据此把 session/load 分流到只读重放（不进门禁、不改绑、
	// 不注册 runtime）；探测经 repo 层读持久化事实，重启后依然为真（A7 设计
	// 点 4 的只读执法数据源统一）。未知会话/持久化禁用返回 false——交由
	// LoadBoundSession 诚实报错，探测不吞掉「不存在」与「非终态」的差别。
	SessionArchived(sessionID string) bool
	// ArchiveSession 落 archived 终态标记（Plan A3 的 x-goworker/session/archive
	// 执行面，定案需求 #1：只读保留、不可逆、无 unarchive）。实现侧职责：
	// fencing（该会话 control lease，与 run/cancel 同款——全局管理门禁由调用方
	// handler 持有，本方法在门禁内被调）、run 在飞拒绝（定案需求 #8：在飞即
	// 活跃，终态会话与在飞 run 并存会让只读执法与事件面失真——瞬态 busy，等
	// run 终态后重试）、MarkArchived（幂等，重复归档 no-op）。绑定中的会话
	// **允许**归档：绑定保留、prompt 转为只读拒绝（A7④ store 执法）、生命周期
	// 收口等引用归零（A2a 机制自然处理）。已收口（不在册）的会话同样可归档
	// （repo 层打开落标记后释放）。
	ArchiveSession(sessionID, clientID string) error
}

// SessionDeleteSource 是物理删除能力的可选扩展面。单独拆出是为了让已有
// BoundSessionSource 测试桩/简化后端不被新增删除能力的接口变更打破；xgw
// handler 对 nil 诚实返回 method-not-supported。
type SessionDeleteSource interface {
	DeleteSession(sessionID, clientID string) error
}

// BoundSessionLoader lets a backend linearize load, history snapshot, and lifecycle binding.
type BoundSessionLoader interface {
	LoadBoundSessionForConnection(sessionID, cwd, connectionID string) ([]SessionUpdate, error)
}

// ConnectionBinder 是连接级会话绑定登记的可选能力面（Plan A2 生命周期半边，
// handler 以 type-assert 获取——RunControlSource/ClientToolBinder 先例；无此
// 能力的插件/测试桩零感知，行为与 PR2 一致）。
//
// 语义：连接在其 SessionBinding.Set 点（CreateSession/LoadSession 声明成功后）
// 同步登记 clientID → sessionID，供 agentcontrol.BindingRegistry 记账——它是
// 多 session 运行时生命周期引用计数的唯一事实源（「任一连接绑定即活；最后解绑
// → 关 store + 注销 manager」，Plan A2），也是 A3 state=active 判定的取数面。
// 与 SessionBinding 的分工：连接内寻址仍走 ctx 携带的 SessionBinding（路由/
// 解析不消费登记），跨连接计数走本登记（架构边界收窄论证见 agentcontrol
// bindings.go / sessions.go 文件头）。实现侧在改绑（返回的旧会话失去引用）时
// 自行评估生命周期收口，登记动作本身绝无副作用（不删会话、不取消 Run、不动
// control lease，§6.2 纪律）。
type ConnectionBinder interface {
	// BindConnectionSession 登记 clientID 当前绑定的会话（改绑即替换；空
	// sessionID = 解除绑定）。connectionID 与 RunControlSource 的连接句柄同源
	// （handler 工厂生成的 connectionID），断连收口经 ReleaseConnection 清账。
	BindConnectionSession(connectionID, sessionID string)
}
