package acp

import (
	"fmt"

	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// x-goworker 扩展协议(总方案 §7)。放置依据:x-device 先例——应用层扩展不进
// ai-acp/protocol 标准面(acpworker/sys_relay.go:5-8 先例),daemon 侧自持方法
// 常量与 wire 类型,经 server.Handle 注册(P5-7,xgw.go),ai-acp 零改动。
//
// 错误约定:沿用 -32000 + 明确文案(不新增标准错误码);control 缺失 →
// -32000 "x-goworker: control lease held by ..."。客户端按 -32601
// method-not-found 降级的既有约定不变(拍板⑥:无 agentCapabilities 通告)。
const (
	XMethodRunStart         = "x-goworker/run/start"        // {sessionId,input,requestId} → {runId,state}
	XMethodRunGet           = "x-goworker/run/get"          // {sessionId,runId} → {state,result?,latestSeq}
	XMethodRunCancel        = "x-goworker/run/cancel"       // {sessionId,runId} → {accepted}
	XMethodApprovalRespond  = "x-goworker/approval/respond" // {sessionId,requestId,decision} → {accepted}
	XMethodSessionAttach    = "x-goworker/session/attach"   // P5-8
	XMethodSessionDetach    = "x-goworker/session/detach"   // P5-8
	XMethodSessionEvents    = "x-goworker/session/events"   // 服务端通知(P5-8,Send only)
	XMethodSessionArchive   = "x-goworker/session/archive"  // Plan A3:{sessionId} → {archived}
	XMethodSessionModes     = "x-goworker/session/modes"    // Plan B 任务3:无 session 取 modes（草稿面板模型下拉）
	XMethodSessionProviders = "x-goworker/providers"        // provider 模型目录：无 session 取树 + 当前选择（UI 弹框）
	XMethodProviderDetail   = "x-goworker/provider/detail"  // 单 provider 全量配置（管理页表单；APIKey 不出域）
	XMethodProviderUpsert   = "x-goworker/provider/upsert"  // provider 新建/更新（SaveConfig 即时生效）
	XMethodProviderRemove   = "x-goworker/provider/remove"  // provider 删除（默认/最后一个拒绝）
)

// xgwErrorCode 是 x-goworker 方法面的统一 JSON-RPC 错误码:沿用既有 -32000
// 约定( FrontendHandler 的 sessionBusyError / read-only 守卫同码),不新增
// 标准错误码——客户端既有错误处理分支继续可用。
const xgwErrorCode = -32000

// XRunStartParams 是 x-goworker/run/start 的入参:requestId 是客户端提交 Run
// 的幂等键,与 JSON-RPC 消息 id 不同(总方案 §7)——连接断开后不知 Start 是否
// 成功时,重试相同 requestId 不会创建两个 Run。幂等记账在 source 层
// (model.RunControlSource.StartRun,P5-6);协议面强制 requestId 非空:空键
// 等于放弃重试保护,违背本方法的幂等契约。
type XRunStartParams struct {
	SessionID string `json:"sessionId"`
	Input     string `json:"input"`
	RequestID string `json:"requestId"`
}

// XRunStartResult 是 x-goworker/run/start 的响应:Run 被接受并持久化登记后
// 立即返回,不等 LLM 完成(总方案 §7);state 为提交时快照(P5-6 恒 queued)。
type XRunStartResult struct {
	RunID string `json:"runId"`
	State string `json:"state"`
}

// XRunGetParams 是 x-goworker/run/get 的入参。
type XRunGetParams struct {
	SessionID string `json:"sessionId"`
	RunID     string `json:"runId"`
}

// XRunGetResult 是 x-goworker/run/get 的响应。latestSeq 只描述会话事件流的
// 当前位置,不是游标授权——客户端只有真正应用了对应事件才能推进本地游标
// (总方案 §7)。result 字段对齐总方案 wire 形状(run 终态结果文本);P5-6
// 的 RunStatus 不产生 result,本步恒省略(omitempty),run 结果面在后续
// step 接通 RunResult 时补齐——诚实收缩,不发明第二事实源。
// replayIntegrity(I4)恒显式携带(无 omitempty,三态字符串都上 wire):取值
// complete/unknown/incomplete(runtimeagent.ReplayIntegrity* 常量)——
// complete = 本进程未观察到写侧失败且无可疑历史;unknown = journal 有跨进程
// 纪元的既有历史,完整性不可验证;incomplete = 本进程发生过事件落盘失败(key
// 事件 durable-first 拒绝/合并 delta flush 失败),粘滞永不复位。非 complete
// = 回放可能缺段,客户端不得把收到的事件当完整历史。
type XRunGetResult struct {
	State           string `json:"state"`
	Result          string `json:"result,omitempty"`
	LatestSeq       uint64 `json:"latestSeq"`
	ReplayIntegrity string `json:"replayIntegrity"`
}

// XRunCancelParams 是 x-goworker/run/cancel 的入参。
type XRunCancelParams struct {
	SessionID string `json:"sessionId"`
	RunID     string `json:"runId"`
}

// XRunCancelResult 是 x-goworker/run/cancel 的响应:accepted=true 表示取消
// 请求已被接受( Run 的实际终态经 run/get 或 session/events 观察,取消是
// 异步收敛,本响应不等待)。
type XRunCancelResult struct {
	Accepted bool `json:"accepted"`
}

// XApprovalDecision 是 x-goworker/approval/respond 的 decision wire 形状:
// 与 hitl.Decision 的 JSON 形状同形(interrupt_id/type/command/message),
// 独立成 daemon 侧 wire 类型以显式承载数据包络与安全校验(xproto 自持
// wire 类型,同本文件放置依据)。type 白名单校验见 xgwDecisionFromWire。
type XApprovalDecision struct {
	InterruptID string `json:"interrupt_id"`
	Type        string `json:"type"`
	Command     string `json:"command,omitempty"`
	Message     string `json:"message,omitempty"`
}

// XApprovalRespondParams 是 x-goworker/approval/respond 的入参。sessionId 是
// 审批归属会话的调用方声明(C1 修复,wire 变更——协议未发布可直接改,推翻总方
// 案 §7 的 {requestId, decision} 形状):handler 以它判 control lease(按会话
// 判,不再「持有任意会话 lease 即放行」),服务端再按 requestID 查询审批注册
// 表的真实归属校验两者一致——以服务端记录为准,不信客户端声明。requestId
// 寻址待决审批(PendingApprovalInfo.RequestID / PendingApproval 契约的
// req-N 命名空间),与 run/start 的 requestId 是两个命名空间(§6 风险 6)。
type XApprovalRespondParams struct {
	SessionID string            `json:"sessionId"`
	RequestID string            `json:"requestId"`
	Decision  XApprovalDecision `json:"decision"`
}

// XApprovalRespondResult 是 x-goworker/approval/respond 的响应:accepted=
// true 表示决策已被接受(首个有效决策获胜);重复/迟到/未知 requestID 经
// -32000 错误回绝(source 层 ErrApprovalResolved/ErrApprovalUnknown 语义)。
type XApprovalRespondResult struct {
	Accepted bool `json:"accepted"`
}

// attach/detach 的 mode 白名单(总方案 §7.4 的 AttachMode 二值枚举):control
// 显式获取 control lease(后到连接接管,tookOver 回显),observe 只观察不动
// lease。空串按 observe 处理(总方案 §7 交互示例的 attach 报文即不带 mode——
// 无特权默认;特权必须显式 opt-in)。未知值 fail-closed 拒绝:把 "control"
// 拼错的客户端绝不静默降级为 observe(拿不到 lease 的错误可见,好过以为
// 持有控制权实则没有)。
const (
	XAttachModeObserve = "observe"
	XAttachModeControl = "control"
)

// XSessionAttachParams 是 x-goworker/session/attach 的入参。afterSeq 是客户端
// 已确认游标:Subscribe 回放 (afterSeq, 注册点 latest] 后无缝续 live(Replay+
// Live 原子建立,总方案 §7);0 = 从保留最早开始。attach 仅 current 会话
// (拍板⑤):归档会话 source 层诚实报错并指引 session/load。
type XSessionAttachParams struct {
	SessionID string `json:"sessionId"`
	AfterSeq  uint64 `json:"afterSeq"`
	Mode      string `json:"mode,omitempty"`
}

// XSessionAttachRun 是 attach 响应 currentRun 字段的子形状:attach 时刻该
// 会话活动 Run 的快照(经 source 的 ActiveRun 访问器);无活动 Run 时
// currentRun 为 null(指针 nil,不是错误)。
type XSessionAttachRun struct {
	RunID string `json:"runId"`
	State string `json:"state"`
}

// XSessionAttachResult 是 x-goworker/session/attach 的响应。latestSeq 是
// attach 时刻会话事件流的位置快照,**仅描述当前位置,客户端不得跳游标**——
// 只有真正应用了对应事件才能推进本地 cursor(总方案 §7 :586-589)。快照取于
// 订阅注册之后(PR3:SubscribeEvents 的收敛序——先 Subscribe 后读游标与完整
// 性),故恒不低于订阅注册水位;注册点与响应之间发布的事件仍由 live 侧续传,
// 客户端按逐事件 seq 记账,不据此批量前移。replayIntegrity(I4)恒显式携带:attach
// 时刻事件流的回放完整性三态(complete/unknown/incomplete,语义见
// XRunGetResult 注释)——unknown = 跨进程纪元的既有历史不可验证,incomplete =
// 本进程发生过事件落盘失败(标记粘滞永不复位),两者都不得把收到的事件当
// 完整历史。tookOver 仅 mode=control 且接管了他连接的 lease 时为 true(其余
// 省略)。
type XSessionAttachResult struct {
	CurrentRun      *XSessionAttachRun `json:"currentRun"`
	LatestSeq       uint64             `json:"latestSeq"`
	ReplayIntegrity string             `json:"replayIntegrity"`
	TookOver        bool               `json:"tookOver,omitempty"`
}

// XSessionDetachParams 是 x-goworker/session/detach 的入参。mode 与 attach 同
// 白名单:mode=control 时本连接归还 control lease(仅持有者本人有效,他人
// 调用为空操作,P5-6 契约);observe/省略只收订阅泵,lease 不动——lease 生命
// 周期显式(attach(control) 获取,detach(control) 或断连归还)。
type XSessionDetachParams struct {
	SessionID string `json:"sessionId"`
	Mode      string `json:"mode,omitempty"`
}

// XSessionDetachResult 是 x-goworker/session/detach 的响应:detached=true 表示
// 本连接该会话的订阅(与 mode=control 时的 lease)已交还。对无订阅的会话
// detach 同样返回 detached=true(幂等,总方案 §7 响应形状)。
type XSessionDetachResult struct {
	Detached bool `json:"detached"`
}

// XSessionArchiveParams 是 x-goworker/session/archive 的入参（Plan A3）：
// 终态归档（只读保留、不可逆、无 unarchive——定案需求 #1）。fencing 双保险：
// 全局管理门禁（归档是管理操作，与 session/new|load 互斥——满即 busy）+ 该
// 会话 control lease（与 run/cancel 同款，实现侧 WithControl 同临界区）。
type XSessionArchiveParams struct {
	SessionID string `json:"sessionId"`
}

// XSessionModesResult 是 x-goworker/session/modes 的响应：与 session/new|load
// 响应里的 modes 同形（model.SessionModeState）。无配置 LLM → 空对象。供前端
// 草稿面板在 session/new 之前渲染模型下拉（Plan B 任务3）。
type XSessionModesResult struct {
	CurrentModeID  string              `json:"currentModeId"`
	AvailableModes []SessionModeChoice `json:"availableModes"`
}

// SessionModeChoice 是 availableModes 的条目形状（对齐 model.SessionModeState）。
type SessionModeChoice struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// XProviderCatalogResult 是 x-goworker/providers 的响应：provider→models 树
// + 当前选择（UI 弹框数据源；模型层 model.ProviderCatalog 的直译）。无 LLM
// 配置 → providers 空数组、current* 空。老扩展不认识本方法零感知；扩展侧对
// -32601 降级为平铺 modes（既有约定）。
type XProviderCatalogResult struct {
	CurrentProviderID string                  `json:"currentProviderId"`
	CurrentModel      string                  `json:"currentModel"`
	CurrentEffort     string                  `json:"currentEffort"`
	Providers         []XProviderCatalogEntry `json:"providers"`
}

// XProviderCatalogEntry 是 providers 的条目形状（id = provider 名，/model use
// 的寻址面）。
type XProviderCatalogEntry struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Type   string   `json:"type,omitempty"`
	Models []string `json:"models"`
}

// XProviderDetailParams 是 x-goworker/provider/detail 的入参。
type XProviderDetailParams struct {
	ID string `json:"id"`
}

// XProviderDetailResult 是 x-goworker/provider/detail 的响应（model.ProviderDetail
// 直译）。APIKey 永不出域——只有 apiKeySet 布尔，密钥经 wire 回传 UI 是泄露面。
type XProviderDetailResult struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	Endpoint      string   `json:"endpoint"`
	APIKeySet     bool     `json:"apiKeySet"`
	Models        []string `json:"models"`
	ContextWindow int      `json:"contextWindow,omitempty"`
	MaxTokens     int      `json:"maxTokens,omitempty"`
	AuthType      string   `json:"authType,omitempty"`
	RequestMode   string   `json:"requestMode,omitempty"`
	Effort        string   `json:"effort,omitempty"`
	IsDefault     bool     `json:"isDefault"`
}

// XProviderUpsertParams 是 x-goworker/provider/upsert 的入参（更新 = merge：
// apiKey 空串 = 保留原值；contextWindow/maxTokens/authType 零值 = 保留；thinking
// 不在请求面即保留）。id 空 = 新建（name 即 map key）。
type XProviderUpsertParams struct {
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

// XProviderUpsertResult 是 x-goworker/provider/upsert 的响应：saved=true 表示
// 已 Validate + SaveConfig 落盘（引擎内存同步，运行中即时生效）。
type XProviderUpsertResult struct {
	Saved bool   `json:"saved"`
	ID    string `json:"id"`
}

// XProviderRemoveParams 是 x-goworker/provider/remove 的入参。
type XProviderRemoveParams struct {
	ID string `json:"id"`
}

// XProviderRemoveResult 是 x-goworker/provider/remove 的响应：removed=true
// 表示已删除并落盘。默认 provider / 最后一个 provider → -32000 明确报错。
type XProviderRemoveResult struct {
	Removed bool `json:"removed"`
}

// XSessionArchiveResult 是 x-goworker/session/archive 的响应：archived=true
// 表示终态标记已持久化（幂等——重复归档 no-op 返回 true）；session/list 随即
// 反映 state=archived。绑定中的会话允许归档（绑定保留、prompt 转为只读拒绝、
// 生命周期收口等引用归零）；run 在飞 → -32000 busy（瞬态，终态后重试）。
type XSessionArchiveResult struct {
	Archived bool `json:"archived"`
}

// xgwError 按本协议错误约定构造 -32000 RPC 错误。
func xgwError(format string, args ...any) *protocol.RPCError {
	return &protocol.RPCError{Code: xgwErrorCode, Message: fmt.Sprintf(format, args...)}
}

// xgwNotAvailableError 是后端无 RunControlSource(纯 SessionSource 插件/测试
// 桩)时全部 x-goworker 方法的诚实报错,与 session load 降级同型
// ("acp frontend: session load not supported")。
func xgwNotAvailableError(method string) *protocol.RPCError {
	return xgwError("x-goworker: %s not supported: backend has no run control capability", method)
}

// xgwDecisionFromWire 把 approval/respond 的 decision wire 形状映射为
// hitl.Decision。type 白名单是安全面,不是风格:hitl 中间件对未知
// DecisionType 无 default 分支,零值("")会穿透全部 case 落到空的
// MiddlewareResponse,即放行——model.DecisionFromOption 的既有结论。未知/
// 缺失 type 一律 -32000 报错(失败方向是拒绝,绝不放行),列出合法值供
// 客户端修正。
func xgwDecisionFromWire(d XApprovalDecision) (hitl.Decision, error) {
	t := hitl.DecisionType(d.Type)
	switch t {
	case hitl.DecisionApprove,
		hitl.DecisionApproveAlways,
		hitl.DecisionReject,
		hitl.DecisionEdit,
		hitl.DecisionRespond:
		return hitl.Decision{
			InterruptID: d.InterruptID,
			Type:        t,
			Command:     d.Command,
			Message:     d.Message,
		}, nil
	}
	return hitl.Decision{}, xgwError(
		"x-goworker: unknown decision type %q (want approve, approve_always, reject, edit or respond)", d.Type)
}

// xgwAttachMode 校验 attach/detach 的 mode 白名单(见常量注释):空串与
// observe 同义(无特权默认),control 是显式 opt-in,未知值 fail-closed。
func xgwAttachMode(mode string) (string, error) {
	switch mode {
	case "", XAttachModeObserve, XAttachModeControl:
		return mode, nil
	}
	return "", xgwError("x-goworker: unknown attach mode %q (want %q or %q)", mode, XAttachModeObserve, XAttachModeControl)
}
