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
	XMethodRunStart        = "x-goworker/run/start"        // {sessionId,input,requestId} → {runId,state}
	XMethodRunGet          = "x-goworker/run/get"          // {sessionId,runId} → {state,result?,latestSeq}
	XMethodRunCancel       = "x-goworker/run/cancel"       // {sessionId,runId} → {accepted}
	XMethodApprovalRespond = "x-goworker/approval/respond" // {requestId,decision} → {accepted}
	XMethodSessionAttach   = "x-goworker/session/attach"   // P5-8
	XMethodSessionDetach   = "x-goworker/session/detach"   // P5-8
	XMethodSessionEvents   = "x-goworker/session/events"   // 服务端通知(P5-8,Send only)
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
type XRunGetResult struct {
	State     string `json:"state"`
	Result    string `json:"result,omitempty"`
	LatestSeq uint64 `json:"latestSeq"`
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

// XApprovalRespondParams 是 x-goworker/approval/respond 的入参。requestId
// 寻址待决审批(PendingApprovalInfo.RequestID / PendingApproval 契约的
// req-N 命名空间),与 run/start 的 requestId 是两个命名空间(§6 风险 6)。
type XApprovalRespondParams struct {
	RequestID string            `json:"requestId"`
	Decision  XApprovalDecision `json:"decision"`
}

// XApprovalRespondResult 是 x-goworker/approval/respond 的响应:accepted=
// true 表示决策已被接受(首个有效决策获胜);重复/迟到/未知 requestID 经
// -32000 错误回绝(source 层 ErrApprovalResolved/ErrApprovalUnknown 语义)。
type XApprovalRespondResult struct {
	Accepted bool `json:"accepted"`
}

// xgwError 按本协议错误约定构造 -32000 RPC 错误。
func xgwError(format string, args ...any) *protocol.RPCError {
	return &protocol.RPCError{Code: xgwErrorCode, Message: fmt.Sprintf(format, args...)}
}

// xgwControlLeaseError 是 control 缺失的统一拒绝:connectionID 回显请求方
// 自身 id,客户端可对照 attach(接管)回显确认持有者已易主。
func xgwControlLeaseError(connectionID string) *protocol.RPCError {
	return xgwError("x-goworker: control lease held by another connection (connection %s does not hold the lease)", connectionID)
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
