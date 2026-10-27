package acp

import (
	"context"
	"fmt"
	"strings"
	"sync"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// Evaluator 执行一条用户输入（装配层通常注入 Engine.Eval）。
type Evaluator = func(ctx *model.Context, input string) error

// FrontendHandler 是 daemon 主会话 ACP 入口的默认 handler：每连接一份，实现
// session/new、session/prompt、session/list、session/load 与 modes 协商的全部
// daemon 语义：
//
//   - CreateSession：登记本连接可 prompt 的 session，声明 cwd，下发命令补全；
//   - Prompt：known/read-only 守卫 → 会话门禁（跨连接互斥）→ superseded 守卫
//     → 命令输出围栏 + token→update 映射、HITL 授权经 Reporter.RequestPermission
//     （model.DecideViaACP）；
//   - LoadSession：当前会话 reload+重放（进可 prompt 集合），归档只读重放
//     （进只读集合，prompt 诚实报错），未知会话报错；
//   - ListSessions/SessionModes：domain → wire 映射转发。
//
// 它放在本包而非装配层：session 语义是 daemon domain 逻辑，所有壳层共用；
// 装配层只接线。gate 由装配层创建、全 daemon 一份（见 coordinator.go）。
type FrontendHandler struct {
	evaluate Evaluator
	commands func() []model.Command
	sessions model.SessionSource
	gate     *SessionGate

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
	// 锁序(I2 生命周期屏障后):h.mu 临界区内会嵌套调用 source 的 leaseMu——
	// attach 提交段的 AcquireControl 与 detach 的 ReleaseControl 都与订阅表
	// 更新放在同一临界区(订阅表更新与 control lease 操作的事务语义,评审 I2
	// 建议四)。leaseMu 是 source 侧叶锁(runcontrol.go),不回调 handler、不取
	// h.mu,嵌套单向 h.mu→leaseMu,Close 侧同向,无环;其余临界区仍互不嵌套。
	//
	// C1 修复清退了旧的 controlSessions 候选集:approval/respond 的守卫按 wire
	// 携带的 sessionId 逐会话询问 source 的 HoldControl(requireControl),权威
	// 归属再由 source 按 requestID 校验,handler 无需本地簿记。
	subs map[string]*xgwSubscription

	// closed 是 I2 的连接生命周期屏障:Close 在 h.mu 内置位,此后 attach 的提交
	// 段一律拒绝——Close 返回之后不会再发生新的订阅提交或 control lease 获取
	// (不复活订阅与 lease;评审 I2 的 Close/Attach 竞态:attach 的提交段与
	// Close 的「closed 置位 + 清表」同以 h.mu 串行,二者必有一个全序)。
	closed bool

	// commitHook 是 attach 提交段的测试钩子(第二轮评审 I2 验收,仅测试注入,
	// 生产恒 nil):在换入新订阅句柄之后、closed 二次确认之前同步调用,此时
	// h.mu 已释放——钩子内可跑 Close/detach(它们需要取 h.mu,持锁调用会自锁)。
	// 测试用它在提交完成前确定性插入 Close/detach,复现评审的并发窗口;钩子
	// 返回后的二次确认把插桩期间发生的状态变化收束为显式拒绝。nil 时提交段是
	// 单临界区,无插桩窗口。
	commitHook func()

	mu         sync.Mutex
	promptable map[string]struct{} // 可 prompt 集合：session/new 与当前会话 load 记入
	readOnly   map[string]struct{} // 只读集合：归档 load 记入，prompt 诚实报错
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

// remember 记入可 prompt 集合；rememberReadOnly 记入只读集合。归档会话只读：
// load 成功记入只读集合，prompt 该 id 诚实报错而非掩盖成未知会话。
func (h *FrontendHandler) remember(sessionID string) {
	h.mu.Lock()
	if h.promptable == nil {
		h.promptable = map[string]struct{}{}
	}
	h.promptable[sessionID] = struct{}{}
	// 同一会话只能归属一个集合：若先以归档身份 load 进只读集合、后又被恢复为
	// 当前会话再 load，不清掉旧标记会让 Prompt 永远误判 read-only（评审 MEDIUM-2）。
	delete(h.readOnly, sessionID)
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
func (h *FrontendHandler) CreateSession(_ context.Context, sessionID string, req protocol.NewSessionRequest, reporter aiacp.Reporter) error {
	// SetSession 会把新 cwd 应用到当前会话（单活动会话模型里的共享状态突变），
	// prompt 在飞时诚实报 busy 而非排队：客户端取消在飞 prompt 或稍后重试即可。
	if !h.gate.tryAcquire() {
		return sessionBusyError()
	}
	h.remember(sessionID)
	if h.sessions != nil {
		h.sessions.SetSession(sessionID, req.Cwd)
	}
	// 临界区只包共享状态突变，手动释放（不能用 defer：release 双还会偷走下一个
	// 持有者的 token）。Update 是连接 I/O，客户端停摆时管道写会无限阻塞——
	// 门禁绝不能被一个卡死的客户端攥住，否则全 daemon 的 prompt 都堵在这。
	h.gate.release()

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
// 连接断开不悬挂）→ superseded 守卫（单活动会话模型：已被其他连接的 session/new|load
// 顶掉的会话诚实报错，而非让 prompt 静默串到新活动会话的工作区里执行）
// → 命令输出围栏 + token 映射 + HITL 授权 → evaluate → 闭合围栏。门禁在 superseded
// 检查之前获取并持有到 evaluate 返回：检查与执行之间不再有 session/new 可钻的
// 空档（TOCTOU，评审 P1）。
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

	// 会话门禁：与 session/new、load-current 互斥，封闭 superseded 检查到 evaluate
	// 之间的 TOCTOU 空档。ctx 取消（连接断开）时排队失败，绝不悬挂。
	if err := h.gate.acquire(ctx); err != nil {
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("acp frontend: prompt canceled while waiting for the session gate: %v", err),
		}
	}
	defer h.gate.release()

	if h.sessions != nil {
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
		Ctx: ctx,
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
//   - sessionId == 当前会话 → SetSession 声明 cwd，reload 后记入可 prompt 集合，
//     下发命令补全，再逐条重放当前会话历史；
//   - sessionId 是归档会话 → 只读解析并重放（记入只读集合，不进可 prompt 集合；
//     归档 cwd 不改变当前会话），先命令补全再归档历史；
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
