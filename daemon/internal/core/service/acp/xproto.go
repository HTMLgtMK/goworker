package acp

import (
	"fmt"

	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// x-goworker extensions are daemon-owned application methods, following x-device: wire types
// and method names live here and are registered by xgw.go without changing ai-acp's standard
// protocol. Errors use -32000; unsupported methods retain -32601 fallback behavior.
const (
	XMethodRunStart        = "x-goworker/run/start"        // {sessionId,input,requestId} → {runId,state}
	XMethodRunGet          = "x-goworker/run/get"          // {sessionId,runId} → {state,result?,latestSeq}
	XMethodRunCancel       = "x-goworker/run/cancel"       // {sessionId,runId} → {accepted}
	XMethodApprovalRespond = "x-goworker/approval/respond" // {sessionId,requestId,decision} → {accepted}
	XMethodSessionAttach   = "x-goworker/session/attach"
	XMethodSessionDetach   = "x-goworker/session/detach"
	XMethodSessionEvents   = "x-goworker/session/events" // server notification
	XMethodSessionArchive  = "x-goworker/session/archive"
	XMethodSessionModes    = "x-goworker/session/modes"
)

// xgwErrorCode 是 x-goworker 方法面的统一 JSON-RPC 错误码:沿用既有 -32000
// 约定( FrontendHandler 的 sessionBusyError / read-only 守卫同码),不新增
// 标准错误码——客户端既有错误处理分支继续可用。
const xgwErrorCode = -32000

// XRunStartParams carries a client idempotency key distinct from the JSON-RPC ID. Retries after
// disconnect reuse the same key; the source records it, and the protocol rejects empty keys.
type XRunStartParams struct {
	SessionID string `json:"sessionId"`
	Input     string `json:"input"`
	RequestID string `json:"requestId"`
}

// XRunStartResult 是 x-goworker/run/start 的响应:Run 被接受并持久化登记后
// 立即返回,不等 LLM 完成;state 为提交时快照(恒 queued)。
type XRunStartResult struct {
	RunID string `json:"runId"`
	State string `json:"state"`
}

// XRunGetParams 是 x-goworker/run/get 的入参。
type XRunGetParams struct {
	SessionID string `json:"sessionId"`
	RunID     string `json:"runId"`
}

// XRunGetResult reports run state and the latest event sequence (informational only), plus an
// explicit replay-integrity status. Unknown or incomplete history must not be treated as complete.
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

// XApprovalRespondParams includes the caller-declared session used for lease checks; the source
// verifies it against the server-recorded approval owner. requestId identifies a pending approval
// and is separate from run/start's idempotency key.
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

// attach/detach mode is fail-closed: control explicitly acquires or releases a lease; observe
// and an omitted mode leave it unchanged. Unknown values are rejected rather than silently
// degrading a misspelled control request to observe.
const (
	XAttachModeObserve = "observe"
	XAttachModeControl = "control"
)

// XSessionAttachParams 是 x-goworker/session/attach 的入参。afterSeq 是客户端
// 已确认游标:Subscribe 回放 (afterSeq, 注册点 latest] 后无缝续 live(Replay+
// Live 原子建立);0 = 从保留最早开始。attach 仅 current 会话:
// 归档会话 source 层诚实报错并指引 session/load。
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

// XSessionAttachResult reports the subscription cursor and replay-integrity snapshot taken
// after registration. latestSeq is informational, not an ack; clients advance only after applying
// events. unknown/incomplete integrity forbids treating replay as complete, and tookOver appears
// only when control mode displaced another holder.
type XSessionAttachResult struct {
	CurrentRun      *XSessionAttachRun `json:"currentRun"`
	LatestSeq       uint64             `json:"latestSeq"`
	ReplayIntegrity string             `json:"replayIntegrity"`
	TookOver        bool               `json:"tookOver,omitempty"`
}

// XSessionDetachParams 是 x-goworker/session/detach 的入参。mode 与 attach 同
// 白名单:mode=control 时本连接归还 control lease(仅持有者本人有效,他人
// 调用为空操作);observe/省略只收订阅泵,lease 不动——lease 生命
// 周期显式(attach(control) 获取,detach(control) 或断连归还)。
type XSessionDetachParams struct {
	SessionID string `json:"sessionId"`
	Mode      string `json:"mode,omitempty"`
}

// XSessionDetachResult 是 x-goworker/session/detach 的响应:detached=true 表示
// 本连接该会话的订阅(与 mode=control 时的 lease)已交还。对无订阅的会话
// detach 同样返回 detached=true(幂等响应形状)。
type XSessionDetachResult struct {
	Detached bool `json:"detached"`
}

// XSessionArchiveParams 是 x-goworker/session/archive 的入参：终态归档
// （只读保留、不可逆、无 unarchive）。fencing 双保险：
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

// xgwDecisionFromWire accepts only known decision types. This is a security boundary: unknown
// or empty types can otherwise fall through HITL handling as approval, so invalid values return
// -32000 instead of reaching the middleware.
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
