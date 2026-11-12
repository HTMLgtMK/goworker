package acp

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// xgw.go implements FrontendHandler's x-goworker methods and connection cleanup.
// Wire types and method names are defined in xproto.go. Control operations are
// fenced by the source: lease validation and mutation happen under the same
// per-session guard. run/get is read-only and needs no control lease.

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

// xgwEventSink 是订阅泵的下行通知面:窄接口 type-assert,不绑定 ai-acp
// 具体类型——*aiacp.Server 内嵌 *Peer 满足(Peer.Notify 导出,peer.go:101)。
// Notify 经 Conn.writeMu 串行化(protocol/jsonrpc.go:472),泵并发调用与服务端
// 自身的 session/update 写入互不踩踏;连接关闭后 Notify 返回错误(泵据此退出)。
type xgwEventSink interface {
	Notify(method string, params any) error
}

// xgwSubscription owns one event pump and its subscription contract. Pointer identity lets an
// old pump avoid deleting a replacement; stop closes done before cancelling the SDK subscription,
// so the pump exits promptly and drops buffered stale events. stop is idempotent.
type xgwSubscription struct {
	sub      agentcontrol.EventSubscription
	done     chan struct{}
	stopOnce sync.Once
}

// stop 收掉一条订阅:先关 done(协议泵就此退出,不再消费)、再 Close 订阅契约
// (SDK 订阅的 ctx cancel,hub 泵随之退出并关闭投递通道)。幂等:detach 与断连
// Close 对同一订阅的竞争收敛到恰好一次。
func (s *xgwSubscription) stop() {
	s.stopOnce.Do(func() {
		close(s.done)
		_ = s.sub.Close()
	})
}

// registerXGWMethods installs daemon extensions before server.Start, only on
// FrontendHandler instances; other handlers retain method-not-found fallback. It also injects
// the server as the event notification sink without coupling the pump to ai-acp types.
func registerXGWMethods(server *aiacp.Server, handler aiacp.SessionHandler) {
	fh, ok := handler.(*FrontendHandler)
	if !ok {
		return
	}
	fh.events = server
	server.Handle(XMethodRunStart, fh.xgwRunStart)
	server.Handle(XMethodRunGet, fh.xgwRunGet)
	server.Handle(XMethodRunCancel, fh.xgwRunCancel)
	server.Handle(XMethodApprovalRespond, fh.xgwApprovalRespond)
	server.Handle(XMethodSessionAttach, fh.xgwSessionAttach)
	server.Handle(XMethodSessionDetach, fh.xgwSessionDetach)
	server.Handle(XMethodSessionArchive, fh.xgwSessionArchive)
	server.Handle(XMethodSessionModes, fh.xgwSessionModes)
}

// xgwRunStart validates the request ID and delegates lease fencing, submission, and
// idempotency to RunControlSource. StartRun is intentionally detached from the request context:
// disconnecting the caller does not cancel an accepted background run.
func (h *FrontendHandler) xgwRunStart(ctx context.Context, params json.RawMessage) (any, error) {
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
	if !h.waitDeviceTools(ctx) {
		return nil, protocol.ErrClosed
	}
	h.bindDeviceTools(req.SessionID)
	var runID, state string
	var err error
	if runs, ok := h.runs.(model.NegotiatedRunControlSource); ok {
		runID, state, err = runs.StartRunWithTools(req.SessionID, h.connectionID, req.Input, req.RequestID, h.deviceToolSource(req.SessionID))
	} else {
		runID, state, err = h.runs.StartRun(req.SessionID, h.connectionID, req.Input, req.RequestID)
	}
	if err != nil {
		return nil, xgwError("%s: %v", XMethodRunStart, err)
	}
	return XRunStartResult{RunID: runID, State: state}, nil
}

// xgwRunGet returns run state, latest event sequence, and replay integrity without requiring
// a control lease. Unknown runs map to -32000; non-complete replay must not be treated as history.
func (h *FrontendHandler) xgwRunGet(_ context.Context, params json.RawMessage) (any, error) {
	if h.runs == nil {
		return nil, xgwNotAvailableError(XMethodRunGet)
	}
	var req XRunGetParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params: %v", XMethodRunGet, err)
	}
	state, latestSeq, replayIntegrity, err := h.runs.RunStatus(req.SessionID, req.RunID)
	if err != nil {
		return nil, xgwError("%s: %v", XMethodRunGet, err)
	}
	return XRunGetResult{State: state, LatestSeq: latestSeq, ReplayIntegrity: replayIntegrity}, nil
}

// xgwRunCancel 处理 x-goworker/run/cancel:runs.CancelRun(req.SessionID,
// h.connectionID, …)——fencing 同 run/start(校验内移 source)。
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
	if err := h.runs.CancelRun(req.SessionID, h.connectionID, req.RunID); err != nil {
		return nil, xgwError("%s: %v", XMethodRunCancel, err)
	}
	return XRunCancelResult{Accepted: true}, nil
}

// xgwSessionArchive archives a session under the management gate and its control lease.
// Active runs make the request transiently busy; bound sessions remain bound but become
// read-only. Archival is idempotent and irreversible.
func (h *FrontendHandler) xgwSessionArchive(_ context.Context, params json.RawMessage) (any, error) {
	if h.multi == nil {
		return nil, xgwNotAvailableError(XMethodSessionArchive)
	}
	var req XSessionArchiveParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params: %v", XMethodSessionArchive, err)
	}
	if req.SessionID == "" {
		return nil, xgwError("%s: sessionId is required", XMethodSessionArchive)
	}
	// 管理门禁：满即 busy（瞬态），绝不排队攥住门禁（tryAcquire 语义）。
	if !h.gate.tryAcquire() {
		return nil, sessionBusyError()
	}
	err := h.multi.ArchiveSession(req.SessionID, h.connectionID)
	h.gate.release()
	if err != nil {
		return nil, xgwError("%s: %v", XMethodSessionArchive, err)
	}
	return XSessionArchiveResult{Archived: true}, nil
}

// xgwSessionModes 处理 x-goworker/session/modes（Plan B 任务3）：无 session 即取
// daemon 的 modes 声明（数据源 cfg.LLM providers，与 session/new 响应同源同形）。
// 只读、无门禁——纯展示数据，草稿面板在 session/new 之前渲染模型下拉用。
func (h *FrontendHandler) xgwSessionModes(_ context.Context, _ json.RawMessage) (any, error) {
	if h.sessions == nil {
		return nil, xgwError("%s: session capabilities unavailable", XMethodSessionModes)
	}
	state := h.sessions.SessionModes()
	if state == nil {
		return XSessionModesResult{AvailableModes: []SessionModeChoice{}}, nil
	}
	choices := make([]SessionModeChoice, 0, len(state.AvailableModes))
	for _, mode := range state.AvailableModes {
		choices = append(choices, SessionModeChoice{ID: mode.ID, Name: mode.Name, Description: mode.Description})
	}
	return XSessionModesResult{CurrentModeID: state.CurrentModeID, AvailableModes: choices}, nil
}

// xgwApprovalRespond validates the decision wire value, then delegates atomic lease,
// ownership, and submission checks to ResolveApproval. The server-recorded approval owner
// must match the requested session; request IDs and first-valid-decision semantics are source-owned.
func (h *FrontendHandler) xgwApprovalRespond(_ context.Context, params json.RawMessage) (any, error) {
	if h.runs == nil {
		return nil, xgwNotAvailableError(XMethodApprovalRespond)
	}
	var req XApprovalRespondParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params: %v", XMethodApprovalRespond, err)
	}
	decision, err := xgwDecisionFromWire(req.Decision)
	if err != nil {
		return nil, err
	}
	if err := h.runs.ResolveApproval(req.SessionID, h.connectionID, req.RequestID, decision); err != nil {
		return nil, xgwError("%s: %v", XMethodApprovalRespond, err)
	}
	return XApprovalRespondResult{Accepted: true}, nil
}

// xgwSessionAttach prepares a subscription outside h.mu, then commits it under the lock.
// Control-mode subscription replacement and lease acquisition commit atomically; Close uses
// the same lock. Brief overlap may deliver duplicate sequence ranges, so clients deduplicate by seq.
func (h *FrontendHandler) xgwSessionAttach(ctx context.Context, params json.RawMessage) (any, error) {
	if h.runs == nil {
		return nil, xgwNotAvailableError(XMethodSessionAttach)
	}
	if h.events == nil {
		return nil, xgwError("%s: no event sink (handler not registered via registerXGWMethods)", XMethodSessionAttach)
	}
	var req XSessionAttachParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params: %v", XMethodSessionAttach, err)
	}
	mode, err := xgwAttachMode(req.Mode)
	if err != nil {
		return nil, err
	}
	if !h.waitDeviceTools(ctx) {
		return nil, protocol.ErrClosed
	}

	// Prepare the subscription before changing existing subscription or lease state.
	res, err := h.runs.SubscribeEvents(req.SessionID, req.AfterSeq)
	if err != nil {
		return nil, xgwError("%s: %v", XMethodSessionAttach, err)
	}
	// 测试钩子:挂在 SubscribeEvents 成功与提交临界区之间的
	// 真实锁外边界——此处 Close/detach 并发插入即生产可达交错;钩子内跑
	// Close/detach(它们取 h.mu,本 goroutine 未持锁,不会自锁)。生产恒 nil;
	// 提交临界区内的 closed 检查负责把钩子期间发生的状态变化收束为显式拒绝。
	if h.preCommitHook != nil {
		h.preCommitHook()
	}

	// 提交阶段(生命周期屏障):closed 检查 → 换入新句柄 → (control)取
	// lease,同一 h.mu 临界区内完成——对并发 detach/Close 是单事务:表内恒恰一
	// 「现任」句柄,且订阅提交与 lease 获取同生同死。临界区纯原子,无中途解锁。
	sub := &xgwSubscription{sub: res.Subscription, done: make(chan struct{})}
	h.mu.Lock()
	if h.closed {
		// closed 屏障:Close 已置位(其清表已发生过或正在进行),本请求拒绝,
		// 绝不复活订阅与 lease。刚建立的订阅契约经 Close 收掉(订阅未入表、泵
		// 未启动,无 handler 侧残留)——失败回滚只动本请求自建的订阅,不动
		// lease(回滚不夺原持有者的租约)。
		h.mu.Unlock()
		_ = res.Subscription.Close()
		return nil, xgwError("%s: connection closed", XMethodSessionAttach)
	}
	h.bindDeviceToolsLocked(req.SessionID)
	if h.subs == nil {
		h.subs = make(map[string]*xgwSubscription)
	}
	old := h.subs[req.SessionID]
	h.subs[req.SessionID] = sub
	// 控制权变更与订阅提交同一临界区(订阅准备成功后才 Acquire/接管,失败
	// 路径绝不夺走原持有者的 lease;closed 置位后一律拒绝,Close 返回后
	// 不会有新的 lease 获取——AcquireControl 内部取 source 侧本会话的 session
	// lease,锁序 h.mu→session lease 单向,与 Close 侧同向,无环)。
	tookOver := false
	if mode == XAttachModeControl {
		tookOver = h.runs.AcquireControl(req.SessionID, h.connectionID)
	}
	h.mu.Unlock()
	if old != nil {
		old.stop()
	}
	go h.pumpSessionEvents(req.SessionID, sub, res.Subscription.Events(), h.events)

	// Attach returns the subscription's active-run snapshot, latest sequence, and replay
	// integrity. latestSeq is descriptive only; clients advance cursors only after applying
	// events. Unknown prior history or sticky write failures mean replay is not complete.
	resp := XSessionAttachResult{LatestSeq: res.LatestSeq, ReplayIntegrity: res.ReplayIntegrity, TookOver: tookOver}
	if res.CurrentRun != nil {
		resp.CurrentRun = &XSessionAttachRun{RunID: res.CurrentRun.RunID, State: res.CurrentRun.State}
	}
	return resp, nil
}

// pumpSessionEvents forwards session events until the channel closes, cancellation arrives, or
// Notify fails. It checks stop before and after receiving to avoid sending stale buffered events;
// deferred cleanup removes only this subscription handle, not a replacement.
func (h *FrontendHandler) pumpSessionEvents(sessionID string, sub *xgwSubscription, ch <-chan session.SessionEvent, sink xgwEventSink) {
	defer h.clearSub(sessionID, sub)
	for {
		// 优先检查 cancel:done 已关就不碰通道(避免 select 双就绪时随机选中
		// 收事件,把换代后残存缓冲发出去)。
		select {
		case <-sub.done:
			return
		default:
		}
		select {
		case ev, ok := <-ch:
			if !ok {
				return
			}
			// 收后二次检查:事件入通道与 stop 竞争时宁丢不重(丢失面由换代
			// replay 覆盖,detach/Close 是客户端终事)。
			select {
			case <-sub.done:
				return
			default:
			}
			if err := sink.Notify(XMethodSessionEvents, ev); err != nil {
				return
			}
		case <-sub.done:
			return
		}
	}
}

// clearSub 是泵退出自清(调用方为 pumpSessionEvents 的 defer):仅当表内仍是
// 本句柄才删——换代后旧泵退出不误删新泵(detach/Close 已先行删表,正常路径
// 本就落空;判重是「detach 后同会话立即重 attach」窗口的 belt-and-braces)。
func (h *FrontendHandler) clearSub(sessionID string, sub *xgwSubscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.subs[sessionID]; ok && cur == sub {
		delete(h.subs, sessionID)
	}
}

// xgwSessionDetach removes the subscription and, in control mode, releases its lease under
// h.mu, serializing the pair with attach commits and Close. The lock prevents a concurrent
// attach from restoring a lease after detach; repeated detach is harmless.
func (h *FrontendHandler) xgwSessionDetach(_ context.Context, params json.RawMessage) (any, error) {
	if h.runs == nil {
		return nil, xgwNotAvailableError(XMethodSessionDetach)
	}
	var req XSessionDetachParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params: %v", XMethodSessionDetach, err)
	}
	mode, err := xgwAttachMode(req.Mode)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	sub := h.subs[req.SessionID]
	delete(h.subs, req.SessionID)
	if mode == XAttachModeControl {
		h.runs.ReleaseControl(req.SessionID, h.connectionID)
	}
	h.mu.Unlock()
	if sub != nil {
		// 先删后收,泵退出自清的判重必落空,无竞争删除;stop 关 done(协议泵
		// 立即退出)并 cancel ctx(SDK 泵退出关闭投递通道)。
		sub.stop()
	}
	return XSessionDetachResult{Detached: true}, nil
}

// Close marks the connection closed and removes subscriptions under h.mu, then stops their
// pumps and releases all leases, binding registrations, and tool capabilities. Lock order is
// h.mu → the source's per-session guard; attach commits share h.mu, so after Close returns no
// subscription or lease can be resurrected.
func (h *FrontendHandler) Close() {
	h.mu.Lock()
	h.closed = true
	h.closedForBinding.Store(true)
	probeCancel := h.deviceProbeCancel
	h.deviceProbeCancel = nil
	h.toolsReadyOnce.Do(func() {
		if h.toolsReady != nil {
			close(h.toolsReady)
		}
	})
	subs := h.subs
	h.subs = nil
	h.mu.Unlock()
	if probeCancel != nil {
		probeCancel()
	}
	for _, sub := range subs {
		sub.stop()
	}
	if h.runs != nil {
		h.runs.ReleaseConnection(h.connectionID)
		// 断连解绑客户端工具能力——连接退出即解绑 CapabilityRegistry 中
		// 本连接的全部绑定;在途的设备工具等待由「Invoke 失败→Invalidate
		// →等待重绑」语义接管(预算内重绑成功则继续执行,超时返回明确失败文本)。
		h.runs.UnbindCapabilities(h.connectionID)
	}
}
