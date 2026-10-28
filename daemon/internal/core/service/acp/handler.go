package acp

import (
	"context"
	"fmt"
	"strings"
	"sync"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// Evaluator 执行一条用户输入（装配层通常注入 Engine.Eval）。
type Evaluator = func(ctx *model.Context, input string) error

// FrontendHandler 是 daemon 主会话 ACP 入口的默认 handler：每连接一份，实现
// session/new、session/prompt、session/list、session/load 与 modes 协商的全部
// daemon 语义：
//
//   - CreateSession：登记本连接可 prompt 的 session，声明 cwd，下发命令补全，
//     并把连接级 SessionBinding 指向声明落地的已注册会话（PR2）；
//   - Prompt：known/read-only 守卫 → 连接级 superseded 守卫（PR2：只认本连接
//     自己的声明，其他连接的 session/new|load 不再顶掉本连接）→ 会话门禁（跨
//     连接互斥）→ 连接级 Binding 随 ctx 注入执行链（prompt 路由到 binding 指向
//     的会话，总方案 §六）→ 命令输出围栏 + token→update 映射、HITL 授权经
//     Reporter.RequestPermission（model.DecideViaACP）；
//   - LoadSession：当前会话 reload+重放（进可 prompt 集合，Binding 指向之），
//     归档只读重放（进只读集合，prompt 诚实报错，Binding 不变），未知会话报错；
//   - ListSessions/SessionModes：domain → wire 映射转发。
//
// 它放在本包而非装配层：session 语义是 daemon domain 逻辑，所有壳层共用；
// 装配层只接线。gate 由装配层创建、全 daemon 一份（见 coordinator.go）。
//
// PR2 语义变更声明（总方案 §六，重构方案明确要求的可见变化）：连接级
// SessionBinding 取代全局 CurrentSession 成为标准 ACP 方法的会话选择事实源
// （§二不变量 6：会话选择属于调用入口，不存在 Daemon 全局 CurrentSession 事实
// 源）。任何连接的 session/new|load 不再影响另一连接；被 Binding 放弃的会话
// 不删除、其后台 Run 不取消、不自动获得其 control lease（§6.2 四条纪律，
// agentcontrol.SessionBinding 注释）。x-goworker/* 显式携带 sessionId 的多会话
// 路由（CR4）不变；无 Binding 的调用方（CLI/stdio/测试）回落 current 语义。
type FrontendHandler struct {
	evaluate Evaluator
	commands func() []model.Command
	sessions model.SessionSource
	gate     *SessionGate

	// binding 是本连接的会话选择器(PR2,总方案 §6.1,agentcontrol.SessionBinding):
	// session/new|load-current 声明成功后 Set(声明落地的已注册 runtime id,即
	// store 稳定 SessionID;纯内存降级无稳定 id,保持空 = 回落 current 语义)。
	// Prompt 经 agentcontrol.WithBinding 把它挂上执行链 ctx,插件的 binding 形态
	// 解析点据此路由——这是「标准 ACP 会话解析优先读 Binding」的唯一通道。它只是
	// 选择器(§6.2):Set/清除不删除会话、不取消后台 Run、不动 control lease、
	// 不影响其他连接。零值可用,连接私有,无需额外加锁(类型自带 RWMutex)。
	binding agentcontrol.SessionBinding

	// connectionID 是本连接的稳定句柄(P5-7,工厂内 crypto/rand 生成):x-goworker
	// control lease 的持有者寻址面,连接退出时经 Close 归还(P5-8/P5-9 的泵清理
	// 与能力解绑同钩子)。仅在本连接内自洽,跨连接无意义。
	connectionID string
	// runs 是 x-goworker 扩展协议的消费面(P5-6 的 model.RunControlSource,工厂内
	// 对 sessions type-assert 获得)。nil = 后端无 run control 能力(纯
	// SessionSource 插件/测试桩):全部 x-goworker 方法诚实报错,与 loader/lister
	// 降级同型。
	runs model.RunControlSource
	// events 是 x-goworker 订阅泵的下行通知面(P5-8):registerXGWMethods 注入本
	// 连接的 *aiacp.Server(内嵌 *Peer,Notify 导出;注册先于 server.Start,无
	// 并发面,happens-before 经 Start 的 goroutine 创建成立)。nil = 未注册
	// (不经 wire 的直调形态):attach 诚实报错,泵无从建立。
	events xgwEventSink

	// subs 是本连接的会话订阅表(P5-8):sessionID → 订阅回收句柄,每连接每会话
	// 至多一订阅(重复 attach 先 cancel 旧泵再建新);泵退出自清按句柄指针身份
	// 判重,换代/断连(Close)统一回收。与 promptable/readOnly 共用 h.mu。
	//
	// 锁序(I2 生命周期屏障后;DE2-I1 后 source 侧是按会话 fencing guard):
	// h.mu 临界区内会嵌套调用 source 的 session guard——attach 提交段的
	// AcquireControl 与 detach 的 ReleaseControl 都与订阅表更新放在同一临界区
	// (订阅表更新与 control lease 操作的事务语义,评审 I2 建议四)。guard.mu
	// 是 source 侧叶锁(runcontrol.go,每会话一把),不回调 handler、不取
	// h.mu,嵌套单向 h.mu→session guard,Close 侧同向,无环;其余临界区仍互
	// 不嵌套。
	//
	// C1 修复清退了旧的 controlSessions 候选集:approval/respond 的会话归属
	// 守卫在 source 侧按服务端记录校验(第三轮评审 I4 起 run/start|cancel 的
	// lease 守卫同样内移 source,与操作提交同临界区原子完成),handler 无需
	// 本地簿记。
	subs map[string]*xgwSubscription

	// closed 是 I2 的连接生命周期屏障:Close 在 h.mu 内置位,此后 attach 的提交
	// 段一律拒绝——Close 返回之后不会再发生新的订阅提交或 control lease 获取
	// (不复活订阅与 lease;评审 I2 的 Close/Attach 竞态:attach 的提交段与
	// Close 的「closed 置位 + 清表」同以 h.mu 串行,二者必有一个全序)。
	closed bool

	// preCommitHook 是 attach 的测试钩子(第三轮评审 I2 验收,仅测试注入,
	// 生产恒 nil):在 hub.Subscribe 成功返回之后、进入提交临界区(h.mu)之前
	// 同步调用。该间隙是生产真实存在的锁外边界——Close/detach 可在此并发发生,
	// 钩子内跑 Close/detach(它们需要 h.mu,此时本 goroutine 未持锁,不会自锁)
	// 复现的正是生产可达的交错;提交临界区本身保持纯原子(无中途解锁)。钩子
	// 须在无并发 attach 运行时设置/清除(测试串行段使用),避免钩子字段自身的
	// 读写竞争。
	preCommitHook func()

	mu         sync.Mutex
	promptable map[string]struct{} // 可 prompt 集合：session/new 与当前会话 load 记入
	readOnly   map[string]struct{} // 只读集合：归档 load 记入，prompt 诚实报错
	// active 是本连接最近一次有效声明的 transport 会话 id(PR2 前是 daemon 全局
	// 单值——plugin 的 activeTransportSession;PR2 起声明归属连接,守卫只认本连接
	// 自己的声明,其他连接的 session/new|load 不再顶掉本连接)。空 = 本连接尚未
	// 声明:守卫回落全局 ActiveSessionID()(兼容不走 CreateSession 的测试桩与
	// 简化壳)。与 promptable/readOnly 共用 h.mu。
	active string
}

var (
	_ aiacp.SessionHandler       = (*FrontendHandler)(nil)
	_ aiacp.SessionCreator       = (*FrontendHandler)(nil)
	_ aiacp.SessionLoader        = (*FrontendHandler)(nil)
	_ aiacp.SessionLister        = (*FrontendHandler)(nil)
	_ aiacp.SessionModesProvider = (*FrontendHandler)(nil)
)

// NewFrontendHandlerFactory 返回每连接创建一个 FrontendHandler 的工厂。sessions
// 可为 nil（会话能力不可用时降级：list/load/modes 诚实报错或不支持，prompt 仍可路由）。
// commands 可为 nil（不下发命令补全）。evaluate 不可为 nil（Prompt 直接调用），
// 装配层须保证（如注入 Engine.Eval）。gate 不可为 nil：单活动会话模型的执行门禁，
// 由装配层创建一次、被本工厂闭包捕获，跨所有连接共享——每连接一份的话 TOCTOU
// 守卫等于没装。
//
// P5-7：工厂内生成 connectionID（crypto/rand hex），并对 sessions type-assert
// model.RunControlSource 得到 x-goworker 消费面（断言失败 → runs=nil，方法降级
// 为诚实报错）。type-assert 只对非 nil 接口进行：nil SessionSource 直接跳过。
func NewFrontendHandlerFactory(evaluate Evaluator, commands func() []model.Command, sessions model.SessionSource, gate *SessionGate) HandlerFactory {
	return func() aiacp.SessionHandler {
		handler := &FrontendHandler{
			evaluate:     evaluate,
			commands:     commands,
			sessions:     sessions,
			gate:         gate,
			connectionID: newConnectionID(),
		}
		if sessions != nil {
			if runs, ok := sessions.(model.RunControlSource); ok {
				handler.runs = runs
			}
		}
		return handler
	}
}

// remember 记入可 prompt 集合并登记为本连接的活动声明；rememberReadOnly 记入
// 只读集合。归档会话只读：load 成功记入只读集合，prompt 该 id 诚实报错而非掩盖
// 成未知会话。PR2:活动声明归连接(h.active)——superseded 守卫据此只拦本连接
// 自己的过期声明。
func (h *FrontendHandler) remember(sessionID string) {
	h.mu.Lock()
	if h.promptable == nil {
		h.promptable = map[string]struct{}{}
	}
	h.promptable[sessionID] = struct{}{}
	// 同一会话只能归属一个集合：若先以归档身份 load 进只读集合、后又被恢复为
	// 当前会话再 load，不清掉旧标记会让 Prompt 永远误判 read-only（评审 MEDIUM-2）。
	delete(h.readOnly, sessionID)
	h.active = sessionID
	h.mu.Unlock()
}

func (h *FrontendHandler) rememberReadOnly(sessionID string) {
	h.mu.Lock()
	if h.readOnly == nil {
		h.readOnly = map[string]struct{}{}
	}
	h.readOnly[sessionID] = struct{}{}
	h.mu.Unlock()
}

// membership 返回 sessionID 的集合归属（可 prompt / 只读）。
func (h *FrontendHandler) membership(sessionID string) (promptable, readOnly bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, promptable = h.promptable[sessionID]
	_, readOnly = h.readOnly[sessionID]
	return promptable, readOnly
}

// sessionBusyError 是 prompt 在飞时会话切换请求（session/new、load-current）收到的
// 诚实拒绝：busy 是瞬态错误，客户端取消在飞 prompt 或稍后重试即可。
func sessionBusyError() *protocol.RPCError {
	return &protocol.RPCError{
		Code:    -32000,
		Message: "acp frontend: session busy: a prompt is in flight on the active session; cancel it or retry",
	}
}

// CreateSession 实现 aiacp.SessionCreator：登记本连接的 session、向 daemon 声明
// cwd，并在响应返回前把命令清单作为 available_commands_update 下发（/ 补全）。
// PR2:声明成功后把连接级 Binding 指向声明落地的会话——id 取 CurrentSessionID
// (store 稳定 SessionID = 已注册 runtime 的 id,registerCurrentRuntime 不变量;
// 与 SetSession 落 cwd 的解析点同源,同在门禁临界区内读取以收窄 /new 换轨窗口;
// 纯内存降级返回空串,Binding 保持空 = 回落 current 语义)。
func (h *FrontendHandler) CreateSession(_ context.Context, sessionID string, req protocol.NewSessionRequest, reporter aiacp.Reporter) error {
	// SetSession 会把新 cwd 应用到当前会话（单活动会话模型里的共享状态突变），
	// prompt 在飞时诚实报 busy 而非排队：客户端取消在飞 prompt 或稍后重试即可。
	if !h.gate.tryAcquire() {
		return sessionBusyError()
	}
	h.remember(sessionID)
	boundTo := ""
	if h.sessions != nil {
		h.sessions.SetSession(sessionID, req.Cwd)
		boundTo = h.sessions.CurrentSessionID()
	}
	// 临界区只包共享状态突变，手动释放（不能用 defer：release 双还会偷走下一个
	// 持有者的 token）。Update 是连接 I/O，客户端停摆时管道写会无限阻塞——
	// 门禁绝不能被一个卡死的客户端攥住，否则全 daemon 的 prompt 都堵在这。
	h.gate.release()

	// Binding 是连接私有状态,门禁外 Set 即可(§6.2:不影响任何共享会话状态,
	// 也不影响其他连接)。
	h.binding.Set(boundTo)

	if h.commands != nil {
		if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
			reporter.Update(sessionID, body)
		}
	}
	return nil
}

// deviceToolSource 按 initialize 协商的 clientCapabilities 门控：
// 对端声明 x-device 且 reporter 提供反向 Call 通道时，返回惰性工具源；
// 否则 nil（无设备工具，零往返）。
//
// P5-9：后端实现 model.ClientToolBinder（工厂 type-assert h.sessions）时优先走
// 能力代理面——工具绑定 CapabilityRegistry 到本连接，断连后设备工具调用等待
// 重绑（默认 45s）而非立即报错给模型（P5 显式可见变化 #1）；type-assert 失败
// （简化壳/测试桩无 binder）回落原始 relay 路径，行为不变。能力门控（caps.Has）
// 在两种形态之前：未声明 x-device 的客户端（VS Code 等）零变化。DeviceCaller
// 是结构类型（model 不 import acpworker），acpworker.DeviceCaller 天然满足。
func (h *FrontendHandler) deviceToolSource(reporter aiacp.Reporter) func(ctx context.Context) []core.Tool {
	capable, ok := reporter.(interface {
		ClientCapabilities() protocol.ClientCapabilities
	})
	if !ok {
		return nil
	}
	caller, ok := reporter.(acpworker.DeviceCaller)
	if !ok {
		return nil
	}
	if !capable.ClientCapabilities().Has(acpworker.CapabilityDeviceTools) {
		return nil
	}
	// type-assert 对 nil 接口安全（zero, false）：sessions 为 nil 直接落回落路径。
	if binder, ok := h.sessions.(model.ClientToolBinder); ok {
		return binder.ClientToolSource(h.connectionID, caller)
	}
	return func(ctx context.Context) []core.Tool {
		pctx, cancel := context.WithTimeout(ctx, acpworker.ProbeTimeout)
		defer cancel()
		return acpworker.RelayDeviceTools(caller, acpworker.ProbeDeviceTools(pctx, caller))
	}
}

// Prompt 实现 aiacp.SessionHandler。流程：归档只读守卫 → 未知会话守卫
// → 会话门禁（跨连接互斥 prompt 与 session/new|load-current；ctx 取消即失败，
// 连接断开不悬挂）→ superseded 守卫（PR2 连接级:只认本连接自己的声明——其他
// 连接的 session/new|load 不再顶掉本连接,§六语义变更;本连接重声明后的旧 id
// 诚实报错。本连接从未声明时回落全局 ActiveSessionID,兼容不走 CreateSession
// 的测试桩/简化壳）→ 连接级 Binding 随 ctx 注入执行链(prompt 路由到 binding
// 指向的会话,总方案 §六;Binding 悬空 = 会话已被 /new 收口 → 插件侧诚实报错)
// → 命令输出围栏 + token 映射 + HITL 授权 → evaluate → 闭合围栏。守卫、Binding
// 读取与 evaluate 都在门禁内:检查与执行之间不再有 session/new|load 可钻的空档
// (TOCTOU,评审 P1——它们同样要取门禁,与持门禁的 prompt 全序)。
func (h *FrontendHandler) Prompt(ctx context.Context, sessionID string, prompt []protocol.ContentBlock, reporter aiacp.Reporter) (string, error) {
	promptable, readOnly := h.membership(sessionID)
	if readOnly {
		// 归档会话续写防护：诚实报错，优于报 unknown session 掩盖真相
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("acp frontend: archived session %q is read-only", sessionID),
		}
	}
	if !promptable {
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("acp frontend: unknown session %q", sessionID),
		}
	}

	// 会话门禁：与 session/new、load-current 互斥,封闭守卫到 evaluate 之间的
	// TOCTOU 空档。ctx 取消（连接断开）时排队失败，绝不悬挂。
	if err := h.gate.acquire(ctx); err != nil {
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("acp frontend: prompt canceled while waiting for the session gate: %v", err),
		}
	}
	defer h.gate.release()

	// superseded 守卫(PR2 连接级):本连接最近声明(h.active)优先——其他连接的
	// session/new|load 不再顶掉本连接(§六语义变更,声明归属连接);本连接重声明
	// 后的旧 id 仍诚实报错。空则回落全局 ActiveSessionID(评审 P1 原守卫,兜住
	// 不走 CreateSession 的测试桩/简化壳)。门禁内检查:本连接的重声明同样要取
	// 门禁,与之全序,检查后不会再被顶掉。
	h.mu.Lock()
	local := h.active
	h.mu.Unlock()
	if local != "" {
		if local != sessionID {
			return "", &protocol.RPCError{
				Code:    -32000,
				Message: fmt.Sprintf("acp frontend: session %q is no longer active on this connection (last declared %q); create a new session or load the session again", sessionID, local),
			}
		}
	} else if h.sessions != nil {
		if active := h.sessions.ActiveSessionID(); active != "" && active != sessionID {
			return "", &protocol.RPCError{
				Code:    -32000,
				Message: fmt.Sprintf("acp frontend: session %q is no longer active (current active session %q); single-session daemon — create a new session or load the current one", sessionID, active),
			}
		}
	}

	var input string
	for _, block := range prompt {
		if block.Type != "text" {
			continue
		}
		if input != "" {
			input += "\n"
		}
		input += block.Text
	}

	output := &commandOutput{reporter: reporter, sessionID: sessionID}
	commandContext := &model.Context{
		// PR2:连接级 Binding 挂上执行链 ctx——Engine.Eval 把 model.Context 直通
		// 命令 handler,插件的 binding 形态解析点(agentcontrol.BindingFromCtx)据此
		// 把 prompt 路由到本连接选择的会话(总方案 §六;Binding 悬空时插件诚实
		// 报错)。门禁内读取:声明也取门禁,读到的是与守卫一致的全序快照。
		Ctx: agentcontrol.WithBinding(ctx, &h.binding),
		FrontendContext: model.FrontendContext{
			// 命令输出统一包成代码块（见 commandOutput）：命令侧仍按等宽终端排版，
			// 由这里告诉渲染器「这段别重排」。
			Writer: output.Write,
			EmitToken: func(token core.Token) {
				reporter.Update(sessionID, TokenToUpdate(token))
			},
			// Decide 接上 ACP 授权请求：沙箱中间件的 HITL 决策经
			// Reporter.RequestPermission 到客户端，nil 的静默拒绝不再吞掉危险命令提示。
			Decide: model.DecideViaACP(ctx, reporter, sessionID),
			// Device tool source is gated on the initialize-time clientCapabilities
			// of this connection. Assigning nil directly is safe: Session.Run and
			// the plugin both treat a nil ToolSource as "no device tools".
			// P5-9 起优先 binder 能力代理面（断连等待重绑），装配点不变。
			ToolSource: h.deviceToolSource(reporter),
		},
		Values: make(map[string]any),
	}
	// evaluate 内部的报错也经 Writer 写出（命令自己格式化 ✘ 前缀），因此先闭合
	// 代码块再返回错误，否则已经流出去的内容会留一个未闭合的围栏。
	evalErr := h.evaluate(commandContext, input)
	output.Close()
	if evalErr != nil {
		return "", evalErr
	}
	return protocol.StopEndTurn, nil
}

// LoadSession 实现 aiacp.SessionLoader。daemon 的 agent 插件是单活动会话模型
// （store 是唯一真相，当前会话始终处于已加载状态），load 的语义是「确认会话上下文已在」：
//   - sessionId == 当前会话 → SetSession 声明 cwd，reload 后记入可 prompt 集合
//     并把连接级 Binding 指向该会话（PR2;req.SessionID 即分支判定命中的 store
//     稳定 id = 已注册 runtime id,registerCurrentRuntime 不变量），
//     下发命令补全，再逐条重放当前会话历史；
//   - sessionId 是归档会话 → 只读解析并重放（记入只读集合，不进可 prompt 集合；
//     归档 cwd 不改变当前会话,Binding 不变——只读视图不是会话选择），先命令补全
//     再归档历史；
//   - 其他 → 未知会话报错。
//
// 不为历史 sessionId 重建独立会话——daemon 模型不支持多活动会话。
func (h *FrontendHandler) LoadSession(_ context.Context, req protocol.LoadSessionRequest, reporter aiacp.Reporter) error {
	if h.sessions == nil {
		return &protocol.RPCError{Code: -32000, Message: "acp frontend: session load not supported"}
	}
	if req.SessionID == h.sessions.CurrentSessionID() {
		// 与 prompt 互斥：load-current 同样改写共享会话状态（SetSession + reload）。
		// prompt 在飞时诚实报 busy，归档分支只读、不碰共享状态，不需要门禁。
		// 临界区手动收窄到状态突变+历史快照：重放 Update 是连接 I/O，停摆客户端
		// 的阻塞写不能攥住门禁（同 CreateSession；release 不得双还）。
		if !h.gate.tryAcquire() {
			return sessionBusyError()
		}
		h.sessions.SetSession(req.SessionID, req.Cwd)
		if err := h.sessions.ReloadCurrentSession(); err != nil {
			h.gate.release()
			return fmt.Errorf("acp frontend: reload current session: %w", err)
		}
		history := h.sessions.CurrentSessionHistory()
		h.gate.release()

		h.remember(req.SessionID)
		// PR2:Binding 指向本连接刚加载的会话(连接私有状态,门禁外 Set;§6.2——
		// 重选不删除原会话、不取消其后台 Run、不动 control lease、不影响其他连接)。
		h.binding.Set(req.SessionID)
		if h.commands != nil {
			if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
				reporter.Update(req.SessionID, body)
			}
		}
		for _, body := range history {
			reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
		}
		return nil
	}
	// 归档会话：只读重放。先解析成功再入只读集合，解析失败保持未知会话语义。
	for _, info := range h.sessions.ListSessions() {
		if info.SessionID != req.SessionID {
			continue
		}
		history, err := h.sessions.ArchivedSessionHistory(req.SessionID)
		if err != nil {
			return fmt.Errorf("acp frontend: load archived session %q: %w", req.SessionID, err)
		}
		h.rememberReadOnly(req.SessionID)
		if h.commands != nil {
			if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
				reporter.Update(req.SessionID, body)
			}
		}
		for _, body := range history {
			reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
		}
		return nil
	}
	return &protocol.RPCError{
		Code:    -32000,
		Message: fmt.Sprintf("acp frontend: unknown session %q", req.SessionID),
	}
}

// ListSessions 实现 aiacp.SessionLister：转发给会话数据源；无数据源时返回空清单
// （非 nil，ACP 无 list 能力协商，空清单即「当前无可列会话」）。
func (h *FrontendHandler) ListSessions(_ context.Context, _ protocol.ListSessionsRequest) ([]protocol.SessionInfo, error) {
	if h.sessions == nil {
		return []protocol.SessionInfo{}, nil
	}
	summaries := h.sessions.ListSessions()
	infos := make([]protocol.SessionInfo, 0, len(summaries))
	for _, summary := range summaries {
		infos = append(infos, SessionInfoFromDomain(summary))
	}
	return infos, nil
}

// SessionModes 实现 aiacp.SessionModesProvider：转发给会话数据源（无数据源 =
// 无 modes，响应省略该字段）。
func (h *FrontendHandler) SessionModes() *protocol.SessionModeState {
	if h.sessions == nil {
		return nil
	}
	return SessionModeStateFromDomain(h.sessions.SessionModes())
}

// commandFence 是命令输出代码块的围栏。用 4 个反引号而非 3 个：命令输出会回显
// 用户内容（/history 回显对话、/rules 回显指令文件），其中出现 ``` 完全可能，
// 4 个反引号能让这些内容原样保留而不提前闭合代码块。
const commandFence = "````"

// commandOutput 把命令的预格式化输出包成一个 Markdown 代码块并流式发出。
//
// 为什么要包：命令输出是为等宽终端手工排版过的，而前端按 Markdown 渲染、比例
// 字体，不包的话对齐全塌。代码块在两端都是等宽字体，等于把「这段别重排」显式
// 告诉渲染器。规则与具体命令无关 —— 任何经 Writer 出来的输出一视同仁。
//
// 为什么能流式：CommonMark 规定未闭合的围栏代码块延伸到文档末尾，所以中途每次
// 增量渲染看到的都是一段合法的（尚未闭合的）代码块 —— 长命令的进度实时可见，
// 收尾补上闭合围栏定稿。不必先攒完再发。
//
// 并发约束：Writer/Close 只能在单 goroutine 上调用（engine 命令执行串行写）；
// EmitToken 走 reporter.Update，与 Writer 各走各的通道，不共享本结构状态。
//
// 围栏惰性开启：命令没有任何输出时不留空块。开启前只认「有实质内容」的写入，
// 开启后一律原样透传 —— 围栏内的空行是表格排版的一部分，不能吞。
type commandOutput struct {
	reporter  aiacp.Reporter
	sessionID string
	opened    bool
	trailing  byte // 已写出内容的最后一个字节，收尾时据此决定要不要补换行
}

// Write 实现 model.FrontendContext.Writer。
//
// 围栏开启前忽略纯空白：这条通道被 agent 路径共用（session.Run 每轮结束都会
// 无条件 Writer("\n")），不拦的话每次对话都会多出一个空代码块。
func (w *commandOutput) Write(text string) {
	if text == "" {
		return
	}
	if !w.opened {
		if strings.TrimSpace(text) == "" {
			return
		}
		w.reporter.MessageChunk(w.sessionID, commandFence+"\n")
		w.opened = true
	}
	w.reporter.MessageChunk(w.sessionID, text)
	w.trailing = text[len(text)-1]
}

// Close 闭合围栏；未写入过任何内容时不发，避免留下一个空代码块。
func (w *commandOutput) Close() {
	if !w.opened {
		return
	}
	if w.trailing != '\n' {
		w.reporter.MessageChunk(w.sessionID, "\n")
	}
	w.reporter.MessageChunk(w.sessionID, commandFence+"\n")
}
