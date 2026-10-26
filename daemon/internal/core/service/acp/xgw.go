package acp

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"

	aiacp "github.com/tinguo/goworker/ai-acp"
)

// xgw.go 是 FrontendHandler 的 x-goworker 扩展协议面(P5-7):run/start|get|cancel
// 与 approval/respond 四个入站方法 + 连接生命周期钩子 Close。方法常量与 wire
// 类型在 xproto.go;消费面是工厂 type-assert 得到的 model.RunControlSource
// (h.runs,P5-6 实现=P5-6 的 AgentPlugin runcontrol.go)。
//
// control lease(V1 宿主授权简化形,拍板②):本机信任、每会话至多一个持有者。
// run/start|cancel 与 approval/respond 要求请求方连接当前持有 lease(HoldControl);
// run/get 是只读观察面,不要求。V1 没有入站的 lease 获取方法——attach(mode=
// control) 在 P5-8 才落地,本步 lease 由测试/装配面直接经 source 设置。
//
// 行为变化声明(P5-7):新协议面(增量能力,非破坏)。标准 6 方法与
// session/update/request_permission 逐字不动;不调用 x- 方法的旧客户端零感知。

// newConnectionID 生成连接级稳定句柄:conn_ 前缀 + crypto/rand 8 字节 hex,
// 形态对齐 ai-acp newSessionID 的 sess_%x(server.go:430-432)。熵不足是进程级
// 致命错误,panic 语义与 ai-acp 同型。
func newConnectionID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("acp: generate connection id: " + err.Error())
	}
	return fmt.Sprintf("conn_%x", b[:])
}

// registerXGWMethods 在 server 上注册 x-goworker 扩展方法(在 NewServer 之后、
// server.Start 之前调用——读循环未启动,注册表无并发面)。仅 *FrontendHandler
// 注册:扩展是 FrontendHandler 的 daemon 语义,其他 SessionHandler(测试桩/
// 简化壳)不携带,x- 方法保持 method-not-found(-32601,客户端既有降级路径)。
// Peer.Handle 导出(ai-acp peer.go:86),ai-acp 标准面零改动。
func registerXGWMethods(server *aiacp.Server, handler aiacp.SessionHandler) {
	fh, ok := handler.(*FrontendHandler)
	if !ok {
		return
	}
	server.Handle(XMethodRunStart, fh.xgwRunStart)
	server.Handle(XMethodRunGet, fh.xgwRunGet)
	server.Handle(XMethodRunCancel, fh.xgwRunCancel)
	server.Handle(XMethodApprovalRespond, fh.xgwApprovalRespond)
	// P5-8:XMethodSessionAttach / XMethodSessionDetach 入站注册与
	// XMethodSessionEvents 服务端通知泵在 attach 落地时接入本函数与 Close。
}

// requireControl 断言本连接当前持有 control lease。当前持有者身份不经接口
// 暴露(model.RunControlSource 只有 Hold 判等),错误文案回显请求方自身
// connectionID(见 xgwControlLeaseError)。调用方必须先确认 h.runs != nil。
func (h *FrontendHandler) requireControl() error {
	if h.runs.HoldControl(h.connectionID) {
		return nil
	}
	return xgwControlLeaseError(h.connectionID)
}

// xgwRunStart 处理 x-goworker/run/start:require control → runs.StartRun。
// requestId 幂等记账完全在 source 层(P5-6:同 requestID 重试返回原 runID 与
// 当前状态);协议面只负责 requestId 非空守卫(幂等契约的前提,见
// XRunStartParams)与错误码归一。ctx 不下发:StartRun 无 ctx 参数(后台 Run
// 与调用方生命周期解耦,P2P3 §6),连接断开不影响已提交的 Run。
func (h *FrontendHandler) xgwRunStart(_ context.Context, params json.RawMessage) (any, error) {
	if h.runs == nil {
		return nil, xgwNotAvailableError(XMethodRunStart)
	}
	var req XRunStartParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params: %v", XMethodRunStart, err)
	}
	if req.RequestID == "" {
		return nil, xgwError("%s: requestId is required (idempotency key)", XMethodRunStart)
	}
	if err := h.requireControl(); err != nil {
		return nil, err
	}
	runID, state, err := h.runs.StartRun(req.SessionID, req.Input, req.RequestID)
	if err != nil {
		return nil, xgwError("%s: %v", XMethodRunStart, err)
	}
	return XRunStartResult{RunID: runID, State: state}, nil
}

// xgwRunGet 处理 x-goworker/run/get:run 状态 + 会话事件流 latestSeq 的只读
// 观察面,不要求 control lease(观察者与控制者分离,总方案 §7.4;与 P5-8 的
// observe attach 模式同向)。未知 run → source 的 ErrRunNotFound 归一 -32000。
func (h *FrontendHandler) xgwRunGet(_ context.Context, params json.RawMessage) (any, error) {
	if h.runs == nil {
		return nil, xgwNotAvailableError(XMethodRunGet)
	}
	var req XRunGetParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params: %v", XMethodRunGet, err)
	}
	state, latestSeq, err := h.runs.RunStatus(req.SessionID, req.RunID)
	if err != nil {
		return nil, xgwError("%s: %v", XMethodRunGet, err)
	}
	return XRunGetResult{State: state, LatestSeq: latestSeq}, nil
}

// xgwRunCancel 处理 x-goworker/run/cancel:require control → runs.CancelRun。
// accepted=true 仅表示取消请求已被接受;Run 的实际终态异步收敛(经 run/get
// 或 session/events 观察),本响应不等待。
func (h *FrontendHandler) xgwRunCancel(_ context.Context, params json.RawMessage) (any, error) {
	if h.runs == nil {
		return nil, xgwNotAvailableError(XMethodRunCancel)
	}
	var req XRunCancelParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params: %v", XMethodRunCancel, err)
	}
	if err := h.requireControl(); err != nil {
		return nil, err
	}
	if err := h.runs.CancelRun(req.SessionID, req.RunID); err != nil {
		return nil, xgwError("%s: %v", XMethodRunCancel, err)
	}
	return XRunCancelResult{Accepted: true}, nil
}

// xgwApprovalRespond 处理 x-goworker/approval/respond:require control →
// decision wire 形状白名单映射(xgwDecisionFromWire,零值 type 严禁放行)→
// runs.ResolveApproval。requestID 寻址与「首个有效决策获胜」语义在 source 层
// (P4-1 ApprovalManager.Resolve);requestID 无需 current 会话校验(审批归属
// SessionID+RunID 非连接,P5-6 ResolveApproval 契约)。
func (h *FrontendHandler) xgwApprovalRespond(_ context.Context, params json.RawMessage) (any, error) {
	if h.runs == nil {
		return nil, xgwNotAvailableError(XMethodApprovalRespond)
	}
	var req XApprovalRespondParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params: %v", XMethodApprovalRespond, err)
	}
	if err := h.requireControl(); err != nil {
		return nil, err
	}
	decision, err := xgwDecisionFromWire(req.Decision)
	if err != nil {
		return nil, err
	}
	if err := h.runs.ResolveApproval(req.RequestID, decision); err != nil {
		return nil, xgwError("%s: %v", XMethodApprovalRespond, err)
	}
	return XApprovalRespondResult{Accepted: true}, nil
}

// Close 是连接生命周期钩子(daemon 侧扩展约定,ai-acp 无此接口):Service.accept
// 在 <-server.Done() 返回路径 type-assert 调用(service.go connectionCloser)。
// V1 语义:断开即释放 control lease——「审批不悬死在已死连接上」的宿主授权
// 收口;Release 仅持有者本人有效,重复/他人调用幂等空操作(P5-6 契约)。
//
// P5-8:订阅泵清理(handler 持 subs map 的 cancel 函,统一在此回收)。
// P5-9:runs.UnbindCapabilities(h.connectionID)——断连解绑客户端工具能力。
func (h *FrontendHandler) Close() {
	if h.runs != nil {
		h.runs.ReleaseControl(h.connectionID)
	}
}
