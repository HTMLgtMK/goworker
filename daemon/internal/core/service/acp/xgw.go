package acp

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// xgw.go 是 FrontendHandler 的 x-goworker 扩展协议面(P5-7/P5-8):run/start|get|
// cancel 与 approval/respond 四个入站方法(P5-7)+ session/attach|detach 与
// session/events 通知泵(P5-8)+ 连接生命周期钩子 Close。方法常量与 wire
// 类型在 xproto.go;消费面是工厂 type-assert 得到的 model.RunControlSource
// (h.runs,P5-6 实现=P5-6 的 AgentPlugin runcontrol.go)。
//
// control lease(V1 宿主授权简化形,拍板②):本机信任、每会话至多一个持有者。
// run/start|cancel 与 approval/respond 要求请求方连接当前持有 lease(HoldControl);
// run/get 是只读观察面,不要求。lease 的获取/归还入口是 attach/detach
// (mode=control):attach(control) 获取或显式接管(tookOver 回显),detach
// (control) 归还,断连经 Close 强制归还。
//
// 行为变化声明(P5-7/P5-8):新协议面(增量能力,非破坏)。标准 6 方法与
// session/update/request_permission 逐字不动;不调用 x- 方法的旧客户端零感知;
// 标准 session/load 重放不动——load=历史对话重放,attach=事件游标(Replay+
// Live),分工在 P5-10 写入 PROTOCOL.md。

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

// xgwEventSink 是订阅泵的下行通知面(P5-8):窄接口 type-assert,不绑定 ai-acp
// 具体类型——*aiacp.Server 内嵌 *Peer 满足(Peer.Notify 导出,peer.go:101)。
// Notify 经 Conn.writeMu 串行化(protocol/jsonrpc.go:472),泵并发调用与服务端
// 自身的 session/update 写入互不踩踏;连接关闭后 Notify 返回错误(泵据此退出)。
type xgwEventSink interface {
	Notify(method string, params any) error
}

// xgwSubscription 是一条会话订阅的回收句柄(P5-8)。Go 函数值不可比较(func 只
// 能与 nil 比较相等),泵退出自清需要身份判定,故 CancelFunc 包一层取指针身份:
// 换代后的旧泵退出时不会误删新泵的表项。done 是 cancel 信号(close-only):
// 泵对它做优先检查——cancel 后立即退出并丢弃在途缓冲,不把旧订阅残存事件与新
// 订阅的 replay/live 叠成重复投递(换代路径的缺口由新泵按调用方游标 replay
// 补齐,detach/Close 本就是客户端终事);cancel 是异步收尾(唤醒 SDK 泵 → 关
// 闭投递通道),stop 用 Once 保证幂等。
type xgwSubscription struct {
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

// stop 收掉一条订阅:先关 done(协议泵就此退出,不再消费)、再 cancel(handler
// 作用域 ctx,SDK 泵随之退出并关闭投递通道)。幂等:detach 与断连 Close 对同
// 一订阅的竞争收敛到恰好一次。
func (s *xgwSubscription) stop() {
	s.stopOnce.Do(func() {
		close(s.done)
		s.cancel()
	})
}

// registerXGWMethods 在 server 上注册 x-goworker 扩展方法(在 NewServer 之后、
// server.Start 之前调用——读循环未启动,注册表无并发面)。仅 *FrontendHandler
// 注册:扩展是 FrontendHandler 的 daemon 语义,其他 SessionHandler(测试桩/
// 简化壳)不携带,x- 方法保持 method-not-found(-32601,客户端既有降级路径)。
// Peer.Handle 导出(ai-acp peer.go:86),ai-acp 标准面零改动。
//
// P5-8:同时把 server 本体作为订阅泵的下行通知面注入 handler(最简形态——
// Service.accept 持有 server 的唯一装配点就是本函数,工厂在 NewServer 之前
// 无从拿到;handler 持窄接口 xgwEventSink,不引入 ai-acp 类型到泵路径)。
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

// xgwSessionAttach 处理 x-goworker/session/attach(P5-8):原子建立 Replay+Live
// 订阅(总方案 §7)。序:mode 白名单校验 → mode=control 先取 lease(后到显式
// 接管,tookOver 回显;observe 不动 lease)→ runs.Attach(current 会话校验 +
// EnsureHub——遗留③「重启后 attach 时 hub 未建」的解法;RunEvents 未接线/纯
// 内存诚实报错)→ 先 cancel 本会话旧泵再 hub.Subscribe(ctx, afterSeq)(窗口
// 校验/ErrCursorExpired 原样上抛为 -32000)→ 起订阅泵 → 响应 currentRun +
// latestSeq。
//
// 泵 ctx 是 handler 作用域的 WithCancel(context.Background() 派生),不是每请求
// ctx——ai-acp 的 dispatchRequest 在响应返回时即 cancel 请求 ctx
// (protocol/jsonrpc.go:281-283 defer cancel),订阅必须活过本请求,直到 detach/
// 换代/断连(handler.Close)统一回收。
//
// 换代取「先 cancel 旧泵再建新」:每连接每会话至多一订阅,先 cancel 消除新旧
// 泵并存期的重复投递;换代到新泵启动之间的缺口由本次 attach 的 replay 补齐
// (客户端带自己的游标)。代价:新 Subscribe 失败(如 ErrCursorExpired)时旧泵
// 已被 cancel——attach 已报错,客户端修正游标后重新 attach 即可,重放不丢事件。
// 此前的失败(Acquire/Attach)不动既有订阅。
//
// 并发与锁:subs 表在 h.mu 内(叶锁);source 调用与 hub.Subscribe 均在锁外
// (各自内部锁序见 P5-6/event_hub.go,与 h.mu 无嵌套);泵退出自清仅短暂取
// h.mu 做指针身份判重。
func (h *FrontendHandler) xgwSessionAttach(_ context.Context, params json.RawMessage) (any, error) {
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

	tookOver := false
	if mode == XAttachModeControl {
		tookOver = h.runs.AcquireControl(h.connectionID)
	}
	hub, latest, err := h.runs.Attach(req.SessionID)
	if err != nil {
		return nil, xgwError("%s: %v", XMethodSessionAttach, err)
	}

	// 旧泵先收(P5-8 换代语义,见函数头);再以新 ctx 订阅。
	h.cancelSub(req.SessionID)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := hub.Subscribe(ctx, req.AfterSeq)
	if err != nil {
		cancel()
		return nil, xgwError("%s: %v", XMethodSessionAttach, err)
	}
	sub := &xgwSubscription{cancel: cancel, done: make(chan struct{})}
	h.mu.Lock()
	if h.subs == nil {
		h.subs = make(map[string]*xgwSubscription)
	}
	h.subs[req.SessionID] = sub
	h.mu.Unlock()
	go h.pumpSessionEvents(req.SessionID, sub, ch, h.events)

	// currentRun:attach 时刻的活动 Run 快照(ActiveRun,ok=false → null);
	// latestSeq 仅描述位置,客户端不得跳游标(XSessionAttachResult 注释)。
	res := XSessionAttachResult{LatestSeq: latest, TookOver: tookOver}
	if runID, state, ok := h.runs.ActiveRun(req.SessionID); ok {
		res.CurrentRun = &XSessionAttachRun{RunID: runID, State: state}
	}
	return res, nil
}

// pumpSessionEvents 是一条会话订阅的通知泵(P5-8):消费 hub 投递通道,逐事件
// 以 session.SessionEvent 原样 JSON(journal.go:88-95,总方案 §7 报文)经
// Notify(XMethodSessionEvents) 下行。退出路径全覆盖、自清不误删:
//   - 通道关闭:hub 侧订阅终结(慢客户端背压断开——fan-out 非阻塞投递,缓冲满
//     即断开该订阅者,不阻塞 AgentRun 与其他订阅者,P3-2 机制在协议面成立;或
//     ctx cancel 后 SDK 泵退出;或 hub Close)→ range 结束;
//   - done 关闭(stop):cancel 路径立即退出——先于通道关闭感知,丢弃在途缓冲
//     (见 xgwSubscription 注释);收到的未发事件同样丢弃(收后二次检查)。
//   - Notify 出错:连接死亡/写入失败,泵退出;
//   - 退出 defer clearSub:按句柄指针身份删表——换代后旧泵退出不会误删新泵。
//
// 泵阻塞在 Notify(客户端停摆)期间不再读通道:hub 侧 sub 缓冲满即被背压断开
// (P3-2),本 goroutine 随写入返回(完成/失败/连接关闭)退出,自清随之发生。
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

// cancelSub 收掉 sessionID 的当前订阅(换代/detach 路径):删表后 stop——先
// 删后收,泵退出自清的判重必落空,无竞争删除;stop 关 done(协议泵立即退出)
// 并 cancel ctx(SDK 泵退出关闭投递通道)。
func (h *FrontendHandler) cancelSub(sessionID string) {
	h.mu.Lock()
	sub := h.subs[sessionID]
	delete(h.subs, sessionID)
	h.mu.Unlock()
	if sub != nil {
		sub.stop()
	}
}

// clearSub 是泵退出自清(调用方为 pumpSessionEvents 的 defer):仅当表内仍是
// 本句柄才删——换代后旧泵退出不误删新泵(cancelSub 已先行删表,正常路径本就
// 落空;判重是「detach 后同会话立即重 attach」窗口的 belt-and-braces)。
func (h *FrontendHandler) clearSub(sessionID string, sub *xgwSubscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.subs[sessionID]; ok && cur == sub {
		delete(h.subs, sessionID)
	}
}

// xgwSessionDetach 处理 x-goworker/session/detach(P5-8):先收本连接该会话的
// 订阅泵(cancelSub;无订阅为幂等空操作);mode=control 再归还 control lease
// ——ReleaseControl 仅持有者本人有效(P5-6 契约),observe/省略不动 lease。
// 响应 detached=true(总方案 §7 形状;幂等,对无订阅会话同样成立)。
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
	h.cancelSub(req.SessionID)
	if mode == XAttachModeControl {
		h.runs.ReleaseControl(h.connectionID)
	}
	return XSessionDetachResult{Detached: true}, nil
}

// Close 是连接生命周期钩子(daemon 侧扩展约定,ai-acp 无此接口):Service.accept
// 在 <-server.Done() 返回路径 type-assert 调用(service.go connectionCloser)。
// V1 语义:断开即释放 control lease——「审批不悬死在已死连接上」的宿主授权
// 收口;Release 仅持有者本人有效,重复/他人调用幂等空操作(P5-6 契约)。
//
// P5-8:订阅泵统一回收——先清空 subs 表再逐个 stop,泵退出自清的判重落空;
// done 关闭使各协议泵立即退出,cancel 唤醒各 SDK 泵 → 投递通道关闭。断连即
// 断流,无人再消费的 hub 订阅由 ctx 取消路径收敛,不依赖 hub Close。
//
// P5-9:runs.UnbindCapabilities(h.connectionID)——断连解绑客户端工具能力。
func (h *FrontendHandler) Close() {
	if h.runs != nil {
		h.runs.ReleaseControl(h.connectionID)
		// P5-9:断连解绑客户端工具能力——连接退出即解绑 CapabilityRegistry 中
		// 本连接的全部绑定;在途的设备工具等待由 P4-4 的「Invoke 失败→Invalidate
		// →等待重绑」语义接管(预算内重绑成功则继续执行,超时返回明确失败文本)。
		h.runs.UnbindCapabilities(h.connectionID)
	}
	h.mu.Lock()
	subs := h.subs
	h.subs = nil
	h.mu.Unlock()
	for _, sub := range subs {
		sub.stop()
	}
}
