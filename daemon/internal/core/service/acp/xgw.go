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
// control lease(V1 宿主授权简化形,拍板②原文「每会话至多一个 control 持有
// 者」;I3 修正后按会话维度记账,两连接各控各会话互不影响):run/start|cancel
// 与 approval/respond 要求请求方连接持有**被操作会话**(请求 sessionId)的
// lease。第三轮评审 I4 起,该守卫不再是 handler 侧的先行检查(requireControl
// 已删除,校验内移 source)——四处调用点透传 h.connectionID,source 侧在受
// 保护临界区(本会话 session guard.mu,DE2-I1 后按会话分锁,与接管
// AcquireControl 同锁)内原子完成「lease 校验 + 操作提交」(fencing):校验通过
// 后到提交完成之间不可能被接管,接管后旧持有者
// 的控制操作一律明确失败。授权的最终事实源是服务端记录的审批归属(source 侧
// ResolveApproval 按 requestID 查归属会话并校验,不信客户端声明);run/get 是
// 只读观察面,不要求。lease 的获取/归还入口是 attach/detach(mode=control):
// attach(control) 获取或显式接管(tookOver 回显,I4:订阅建立成功后才变更控制
// 权),detach(control) 按会话归还,断连经 Close 的 ReleaseConnection 强制归还
// 该连接全部租约。
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

// xgwRunStart 处理 x-goworker/run/start:runs.StartRun(req.SessionID,
// h.connectionID, …)——第三轮评审 I4 起 lease 守卫内移 source,与提交在同一
// 受保护临界区内原子完成(fencing),未持有 lease / 被接管的旧持有者在提交前
// 即被明确拒绝。requestId 幂等记账完全在 source 层(P5-6:同 requestID 重试
// 返回原 runID 与当前状态);协议面只负责 requestId 非空守卫(幂等契约的前提,
// 见 XRunStartParams)与错误码归一。ctx 不下发:StartRun 无 ctx 参数(后台 Run
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
	runID, state, err := h.runs.StartRun(req.SessionID, h.connectionID, req.Input, req.RequestID)
	if err != nil {
		return nil, xgwError("%s: %v", XMethodRunStart, err)
	}
	return XRunStartResult{RunID: runID, State: state}, nil
}

// xgwRunGet 处理 x-goworker/run/get:run 状态 + 会话事件流 latestSeq + 回放
// 完整性三态(I4:replayIntegrity ∈ complete/unknown/incomplete,非 complete =
// 回放可能缺段或不可验证,客户端不得把收到的事件当完整历史)的只读观察面,
// 不要求 control lease(观察者与控制者分离,总方案 §7.4;与 P5-8 的 observe
// attach 模式同向)。未知 run → source 的 ErrRunNotFound 归一 -32000。
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
// h.connectionID, …)——fencing 同 run/start(第三轮评审 I4,校验内移 source)。
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

// xgwApprovalRespond 处理 x-goworker/approval/respond(C1 修复 + 第三轮评审 I4
// fencing 后的两段守卫):
//  1. decision wire 形状白名单映射(xgwDecisionFromWire,零值 type 严禁放行);
//  2. source 侧 ResolveApproval(req.SessionID, h.connectionID, req.RequestID,
//     decision)——lease 校验(fencing:与归属校验、决策提交在同一受保护临界区
//     内原子完成,接管后旧持有者一律拒绝)、审批归属校验(按 requestID 查服务
//     端记录的归属并校验归属会话一致——「服务端记录为准,不信客户端声明」:
//     连接若持有声明的 lease 但审批归属别的会话,同样被拒)三步原子。
//
// requestID 寻址与「首个有效决策获胜」语义在 source 层(P4-1 ApprovalManager.
// Resolve);归属会话校验的五步序(路由→lease→归属→越权→提交)见 runcontrol.go
// 的 ResolveApproval 注释。
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

// xgwSessionAttach 处理 x-goworker/session/attach(P5-8):原子建立 Replay+Live
// 订阅(总方案 §7)。I4 修正后是准备/提交两阶段:
//
//	准备(不触碰任何既有状态):mode 白名单校验 → runs.Attach(sessionID)(会话
//	校验 + EnsureHub——遗留③「重启后 attach 时 hub 未建」的解法;RunEvents 未
//	接线/纯内存诚实报错)→ hub.Subscribe(ctx, afterSeq)(窗口校验/
//	ErrCursorExpired 原样上抛为 -32000)。任一步失败 → 旧订阅与 control lease
//	原样保持,本请求零副作用。
//	提交(I2 生命周期屏障,单临界区事务):h.mu 内一次性完成 closed 检查 →
//	换入新订阅句柄(表内每会话恒一条,并发 attach 串行换入,被换下者由换入者
//	收泵)→ mode=control 才 AcquireControl(接管;同持有者重复 acquire 幂等)。
//	订阅表更新与 control lease 获取同临界区(评审 I2 建议四的事务语义):二者
//	对并发 detach/Close 要么整体先提交、要么整体被拒,不存在「订阅已死而
//	lease 复活」的中间态;提交临界区保持纯原子,无中途解锁。测试钩子
//	(preCommitHook,仅测试注入)挂在 Subscribe 成功与提交临界区之间的锁外
//	边界——该窗口生产真实存在(Close/detach 可并发插入),钩子复现的交错即
//	生产可达;生产恒 nil。锁外:收被换下的旧泵 → 起新泵。
//	C1 修复后 approval/respond 的会话归属授权在 source 侧按服务端记录校验
//	(第三轮评审 I4 起 lease 守卫同样内移 source,与提交原子);无需 handler
//	侧候选集簿记(旧 controlSessions 已清退)。
//
//	closed 屏障(I2,评审的 Close/Attach 竞态):Close 在 h.mu 内置位 closed 并
//	清空订阅表,本提交段与之同锁串行——要么提交段先入(其订阅与 lease 随后
//	被 Close 一并回收:表内句柄被 stop、ReleaseConnection 归还全部租约),要么
//	Close 先入(closed 已置位,提交段拒绝)。两序下 Close 返回后都不会有新的
//	订阅提交或 lease 获取。
//
// 控制权后置的理由(I4 评审点一):Acquire 在订阅准备成功之后——Attach/Subscribe
// 失败(如 ErrCursorExpired)绝不夺走原持有者的 lease;旧序(先 Acquire 后
// Attach)会在 attach 失败时把 lease 无谓易主。
//
// 换代语义的窗口论证:新 Subscribe 与旧泵 stop 之间存在 μs 级窗口,窗内发布的
// 事件可能被旧泵(其 done 检查先于 stop 完成)与新泵各投递一次——换取的是
// Subscribe 失败时旧订阅完整保持(I4 评审点二:旧序先 cancel 旧泵,Subscribe
// 失败即毁掉正常订阅)。detach/Close 是客户端终事,不换代,无此窗口。
//
// 泵 ctx 是 handler 作用域的 WithCancel(context.Background() 派生),不是每请求
// ctx——ai-acp 的 dispatchRequest 在响应返回时即 cancel 请求 ctx
// (protocol/jsonrpc.go:281-283 defer cancel),订阅必须活过本请求,直到 detach/
// 换代/断连(handler.Close)统一回收。
//
// 并发与锁:subs 表在 h.mu 内;runs.Attach 与 hub.Subscribe 在锁外(各自内部
// 锁序见 P5-6/event_hub.go,与 h.mu 无嵌套),提交段的 AcquireControl 在 h.mu
// 内嵌套取被操作会话的 source 侧 session guard(单向 h.mu→session guard,
// Close 侧同向,见 FrontendHandler subs 字段注释);泵退出自清仅短暂取 h.mu
// 做指针身份判重。
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

	// 准备阶段(零副作用):Attach(会话校验 + EnsureHub)→ Subscribe(游标
	// 校验 + Replay+Live 注册)。失败路径只回收本请求自建的 ctx。
	hub, latest, err := h.runs.Attach(req.SessionID)
	if err != nil {
		return nil, xgwError("%s: %v", XMethodSessionAttach, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := hub.Subscribe(ctx, req.AfterSeq)
	if err != nil {
		cancel()
		return nil, xgwError("%s: %v", XMethodSessionAttach, err)
	}
	// 测试钩子(第三轮评审 I2):挂在 Subscribe 成功与提交临界区之间的真实锁外
	// 边界——此处 Close/detach 并发插入即生产可达交错;钩子内跑 Close/detach
	//(它们取 h.mu,本 goroutine 未持锁,不会自锁)。生产恒 nil;提交临界区
	// 内的 closed 检查负责把钩子期间发生的状态变化收束为显式拒绝。
	if h.preCommitHook != nil {
		h.preCommitHook()
	}

	// 提交阶段(I2 生命周期屏障):closed 检查 → 换入新句柄 → (control)取
	// lease,同一 h.mu 临界区内完成——对并发 detach/Close 是单事务:表内恒恰一
	// 「现任」句柄,且订阅提交与 lease 获取同生同死。临界区纯原子,无中途解锁
	//(第三轮评审 I2:提交中途释放锁的测试插桩验证的是生产不存在的分支,钩子
	// 已迁移到 Subscribe 与本临界区之间的真实锁外边界)。
	sub := &xgwSubscription{cancel: cancel, done: make(chan struct{})}
	h.mu.Lock()
	if h.closed {
		// closed 屏障:Close 已置位(其清表已发生过或正在进行),本请求拒绝,
		// 绝不复活订阅与 lease。刚建的 SDK 订阅经已持有的 cancel 收掉(订阅未
		// 入表、泵未启动,无 handler 侧残留)。
		h.mu.Unlock()
		cancel()
		return nil, xgwError("%s: connection closed", XMethodSessionAttach)
	}
	if h.subs == nil {
		h.subs = make(map[string]*xgwSubscription)
	}
	old := h.subs[req.SessionID]
	h.subs[req.SessionID] = sub
	// 控制权变更与订阅提交同一临界区(I4:订阅准备成功后才 Acquire/接管,失败
	// 路径绝不夺走原持有者的 lease;I2:closed 置位后一律拒绝,Close 返回后
	// 不会有新的 lease 获取——AcquireControl 内部取 source 侧本会话的 session
	// guard,锁序 h.mu→session guard 单向,与 Close 侧同向,无环)。
	tookOver := false
	if mode == XAttachModeControl {
		tookOver = h.runs.AcquireControl(req.SessionID, h.connectionID)
	}
	h.mu.Unlock()
	if old != nil {
		old.stop()
	}
	go h.pumpSessionEvents(req.SessionID, sub, ch, h.events)

	// currentRun:attach 时刻的活动 Run 快照(ActiveRun,ok=false → null);
	// latestSeq 仅描述位置,客户端不得跳游标(XSessionAttachResult 注释);
	// replayIntegrity(I4):attach 时刻 hub 的回放完整性三态——本进程纪元内
	// 任何 key 事件落盘失败/合并 flush 失败 → incomplete(粘滞永不复位);
	// journal 播种发现既有历史(重启后 attach)→ unknown(跨纪元完整性不可
	// 验证);两者都提示客户端不得把收到的事件当完整历史。
	res := XSessionAttachResult{LatestSeq: latest, ReplayIntegrity: hub.ReplayIntegrity(), TookOver: tookOver}
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

// xgwSessionDetach 处理 x-goworker/session/detach(P5-8;I2 事务化):清表与
// (mode=control 时的)lease 归还在同一 h.mu 临界区内——与 attach 的提交段、
// Close 的 closed 置位同锁串行,「摘订阅」与「还 lease」对并发 attach 是一个
// 整体:attach 的提交段要么整体先入(其订阅随后被本 detach 摘除、其 lease 被
// 归还),要么整体后入(本 detach 先完成,attach 的二次确认看到被摘/顶替而
// 拒绝),不存在订阅已摘而 lease 复活的中间态。ReleaseControl 仅该会话持有者
// 本人有效(P5-6/I3 契约),observe/省略不动 lease。响应 detached=true(总方案
// §7 形状;幂等,对无订阅会话同样成立)。
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

// Close 是连接生命周期钩子(daemon 侧扩展约定,ai-acp 无此接口):Service.accept
// 在 <-server.Done() 返回路径 type-assert 调用(service.go connectionCloser)。
// V1 语义:断开即释放 control lease——「审批不悬死在已死连接上」的宿主授权
// 收口;I3 后 lease 按会话记账,ReleaseConnection 一次释放本连接持有的全部
// 会话租约,重复/他人调用幂等空操作(P5-6 契约)。
//
// P5-8:订阅泵统一回收——先清空 subs 表再逐个 stop,泵退出自清的判重落空;
// done 关闭使各协议泵立即退出,cancel 唤醒各 SDK 泵 → 投递通道关闭。断连即
// 断流,无人再消费的 hub 订阅由 ctx 取消路径收敛,不依赖 hub Close。
//
// P5-9:runs.UnbindCapabilities(h.connectionID)——断连解绑客户端工具能力。
//
// I2 生命周期屏障:closed 置位与清表在同一 h.mu 临界区先行,lease 归还
// (ReleaseConnection 扫 source 侧 session guard)在后——锁序 h.mu→session
// guard 单向,与 attach 提交段/detach 同向,无环。
// 二者与 attach 提交段以 h.mu 全序:attach 先提交 → 其订阅在表内被本函数
// stop、其 lease 被 ReleaseConnection 归还;Close 先入 → closed 置位,attach
// 的提交段一律拒绝。Close 返回之后不会再发生新的订阅提交或 lease 获取
// (不复活),这是评审 I2 的 Close/Attach 竞态收口。
func (h *FrontendHandler) Close() {
	h.mu.Lock()
	h.closed = true
	subs := h.subs
	h.subs = nil
	h.mu.Unlock()
	for _, sub := range subs {
		sub.stop()
	}
	if h.runs != nil {
		h.runs.ReleaseConnection(h.connectionID)
		// P5-9:断连解绑客户端工具能力——连接退出即解绑 CapabilityRegistry 中
		// 本连接的全部绑定;在途的设备工具等待由 P4-4 的「Invoke 失败→Invalidate
		// →等待重绑」语义接管(预算内重绑成功则继续执行,超时返回明确失败文本)。
		h.runs.UnbindCapabilities(h.connectionID)
	}
}
