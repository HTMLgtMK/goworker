package acp

import (
	"context"
	"errors"
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

// FrontendHandler 实现 daemon 的 ACP 会话操作。每个连接使用独立 handler，
// 会话选择通过连接级 Binding 路由；BoundSessionSource 不可用时兼容单会话后端。
// SessionGate 由装配层创建并由所有 handler 共享，只保护会话管理操作；prompt
// 的同会话串行化由 runtime gate 负责。
type FrontendHandler struct {
	evaluate Evaluator
	commands func() []model.Command
	sessions model.SessionSource
	// gate serializes shared session-management mutations across all connections.
	gate *SessionGate

	// binding selects the session for this connection and is injected into the
	// evaluation context. Changing it does not mutate other connections or runs.
	binding agentcontrol.SessionBinding

	// binder 是连接级会话绑定登记面(工厂内对 sessions type-assert
	// model.ConnectionBinder 获得,RunControlSource 同款先例)。nil = 后端无
	// 生命周期登记能力(简化壳/测试桩):Set 照常、登记跳过,行为与单会话降级一致。
	binder model.ConnectionBinder

	// multi is the optional multi-session declaration capability. Without it, new keeps an
	// empty binding and load uses the single-session fallback; details are in BoundSessionSource.
	multi model.BoundSessionSource

	// connectionID 是本连接的稳定句柄(工厂内 crypto/rand 生成):x-goworker
	// control lease 的持有者寻址面,连接退出时经 Close 归还(订阅泵清理与能力
	// 解绑同钩子)。仅在本连接内自洽,跨连接无意义。
	connectionID string
	// runs 是 x-goworker 扩展协议的消费面(model.RunControlSource,工厂内
	// 对 sessions type-assert 获得)。nil = 后端无 run control 能力(纯
	// SessionSource 插件/测试桩):全部 x-goworker 方法诚实报错,与 loader/lister
	// 降级同型。
	runs model.RunControlSource
	// events 是 x-goworker 订阅泵的下行通知面:registerXGWMethods 注入本
	// 连接的 *aiacp.Server(内嵌 *Peer,Notify 导出;注册先于 server.Start,无
	// 并发面,happens-before 经 Start 的 goroutine 创建成立)。nil = 未注册
	// (不经 wire 的直调形态):attach 诚实报错,泵无从建立。
	events xgwEventSink

	// subs 保存本连接每个会话的当前订阅。替换、detach 和 Close 都在 h.mu 下
	// 更新表；泵退出按句柄身份清理，避免旧泵删除新订阅。
	// 锁序：attach/detach/Close 可在持有 h.mu 时获取 source 的会话 guard，方向
	// 仅为 h.mu → session guard。source 不回调 handler、不获取 h.mu，避免锁环。
	subs map[string]*xgwSubscription

	// closed 是连接生命周期屏障:Close 在 h.mu 内置位,此后 attach 的提交
	// 段一律拒绝——Close 返回之后不会再发生新的订阅提交或 control lease 获取
	// (不复活订阅与 lease;Close/Attach 竞态:attach 的提交段与
	// Close 的「closed 置位 + 清表」同以 h.mu 串行,二者必有一个全序)。
	closed bool

	// preCommitHook 仅供测试，在订阅准备完成、提交锁获取前注入 Close/detach
	// 竞态。生产环境保持 nil；测试须在无并发 attach 时设置或清除。
	preCommitHook func()

	mu         sync.Mutex
	promptable map[string]struct{} // 可 prompt 集合：session/new 与当前会话 load 记入
	readOnly   map[string]struct{} // 只读集合：归档 load 记入，prompt 诚实报错
	// active is this connection's latest declared session. Empty means no declaration, so the
	// membership guard rejects prompts rather than falling back to a process-wide session.
	// It stores transport IDs for new and stable store IDs for load; other connections cannot
	// overwrite it. promptable/readOnly and active are protected by h.mu.
	active string
}

var (
	_ aiacp.SessionHandler       = (*FrontendHandler)(nil)
	_ aiacp.SessionCreator       = (*FrontendHandler)(nil)
	_ aiacp.SessionLoader        = (*FrontendHandler)(nil)
	_ aiacp.SessionLister        = (*FrontendHandler)(nil)
	_ aiacp.SessionModesProvider = (*FrontendHandler)(nil)
)

// NewFrontendHandlerFactory creates one handler per connection. sessions and commands may be
// nil (session capabilities degrade honestly; command completion is omitted); evaluate and the
// shared SessionGate are required. Each handler gets a connection ID and optional RunControlSource
// capability; nil session sources skip the assertion.
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
			// RunControlSource deliberately excludes host-only synchronous methods, so ACP
			// run/start and run/cancel cannot bypass the connection-lease fencing path.
			if runs, ok := sessions.(model.RunControlSource); ok {
				handler.runs = runs
			}
			// 连接级会话绑定登记面(可选能力,缺省 nil 时 CreateSession/LoadSession
			// 只做 Binding Set、不登记)。
			if binder, ok := sessions.(model.ConnectionBinder); ok {
				handler.binder = binder
			}
			// 会话面多会话化:session/new 真新建与 session/load 的 reload/重开/归档
			// 只读判定面(可选能力,缺省 nil 时落单活动降级路径)。
			if multi, ok := sessions.(model.BoundSessionSource); ok {
				handler.multi = multi
			}
		}
		return handler
	}
}

// remember 记入可 prompt 集合并登记为本连接的活动声明；rememberReadOnly 记入
// 只读集合。归档会话只读：load 成功记入只读集合，prompt 该 id 诚实报错而非掩盖
// 成未知会话。活动声明归连接(h.active)——superseded 守卫据此只拦本连接
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

// sessionBusyError reports that another short session-management operation holds the shared
// gate. Prompts use per-runtime execution gates and do not block session creation/loading;
// callers can retry after the management operation completes.
func sessionBusyError() *protocol.RPCError {
	return &protocol.RPCError{
		Code:    -32000,
		Message: "acp frontend: session busy: a prompt is in flight on the active session; cancel it or retry",
	}
}

// CreateSession implements aiacp.SessionCreator. With BoundSessionSource, it creates
// a persistent session and updates this connection's Binding and lifecycle registration.
// That branch leaves the global current session unchanged; without it, the existing
// single-session fallback remains in effect.
func (h *FrontendHandler) CreateSession(_ context.Context, sessionID string, req protocol.NewSessionRequest, reporter aiacp.Reporter) error {
	// 真新建突变 manager 注册表与 repo（管理面共享状态），prompt 在飞时诚实报
	// busy 而非排队：客户端取消在飞 prompt 或稍后重试即可。SessionGate 本步
	// 不动（管理操作互斥；本使用点语义不变）。
	if !h.gate.tryAcquire() {
		return sessionBusyError()
	}
	boundTo := ""
	if h.multi != nil {
		id, err := h.multi.CreateBoundSession(req.Cwd)
		if err != nil {
			h.gate.release()
			return &protocol.RPCError{
				Code:    -32000,
				Message: fmt.Sprintf("acp frontend: create session: %v", err),
			}
		}
		boundTo = id
	}
	// 临界区只包共享状态突变，手动释放（不能用 defer：release 双还会偷走下一个
	// 持有者的 token）。Update 是连接 I/O，客户端停摆时管道写会无限阻塞——
	// 门禁绝不能被一个卡死的客户端攥住，否则全 daemon 的 prompt 都堵在这。
	h.gate.release()

	// Record the active ID and promptable membership only after declaration succeeds. Failed
	// creation must not leave a routable-looking ID that falls back to another session; this
	// state is connection-local and protected by h.mu.
	h.remember(sessionID)
	// Binding 是连接私有状态，门禁外 Set 即可（不影响任何共享会话状态,
	// 也不影响其他连接）。无多会话能力的后端 boundTo 为空 = Binding 保持空 =
	// 回落 current 语义（降级形态，与纯内存降级同形）。
	h.binding.Set(boundTo)
	// Set 处成对登记(clientID → boundTo,改绑即替换——本连接此前绑定的会话
	// 在此失去引用,由实现侧评估收口)。boundTo 为空(降级后端,无稳定 id)
	// 不登记,与 Binding 保持空一致。
	if h.binder != nil && boundTo != "" {
		h.binder.BindConnectionSession(h.connectionID, boundTo)
	}

	if h.commands != nil {
		if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
			reporter.Update(sessionID, body)
		}
	}
	return nil
}

// deviceToolSource enables lazy tools only when initialize declared x-device and the reporter
// provides reverse calls. A ClientToolBinder supplies connection-scoped proxies that wait for
// rebinding after disconnect; without it, the handler uses the legacy relay. Capability gating
// applies to both paths, so clients without x-device see no change.
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

// Prompt validates this connection's session, injects its Binding, and streams evaluation output.
// Execution uses the target runtime's gate, so management operations can proceed during a prompt.
// A redeclaration may race membership checking, but once resolved the runtime gate pins execution;
// the connection-local Binding prevents other clients from redirecting it.
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

	// Reject a prompt superseded by this connection's later declaration. An empty local
	// selection is allowed for compatibility shims and routes through the empty Binding;
	// a process-wide active-session fallback is intentionally not consulted.
	h.mu.Lock()
	local := h.active
	h.mu.Unlock()
	if local != "" && local != sessionID {
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("acp frontend: session %q is no longer active on this connection (last declared %q); create a new session or load the session again", sessionID, local),
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
		// Inject the connection's live Binding and client ID into the evaluation context. The
		// mutable Binding lets /new clear this connection's selection, while the client ID lets
		// lifecycle cleanup remove its binding registration; both use h.connectionID as identity.
		Ctx: agentcontrol.WithClientID(agentcontrol.WithBinding(ctx, &h.binding), h.connectionID),
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
			// 优先 binder 能力代理面（断连等待重绑），装配点不变。
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

// LoadSession handles connection-scoped load for multi-session backends: reload registered
// sessions, reopen persisted sessions, replay archived sessions read-only, and report unknown
// IDs as the existing -32000 error. Reopening and archive probing are serialized with session
// management; backends without BoundSessionSource retain the single-session fallback.
func (h *FrontendHandler) LoadSession(_ context.Context, req protocol.LoadSessionRequest, reporter aiacp.Reporter) error {
	if h.sessions == nil {
		return &protocol.RPCError{Code: -32000, Message: "acp frontend: session load not supported"}
	}
	if h.multi != nil {
		// 归档终态：只读重放。先探测（repo 层持久化事实，无门禁）——重放数据
		// 是 store 快照，解析失败保持未知会话语义之外的诚实报错。
		if h.multi.SessionArchived(req.SessionID) {
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
		// 可 prompt 分支（reload/重开）：门禁内完成状态突变（注册表突变、共享
		// 视图 reload、repo 打开），busy 语义与 CreateSession 同款。临界区手动
		// 收窄到突变 + 历史快照：重放 Update 是连接 I/O，停摆客户端的阻塞写不
		// 能攥住门禁（同 CreateSession；release 不得双还）。
		if !h.gate.tryAcquire() {
			return sessionBusyError()
		}
		var (
			history       []model.SessionUpdate
			loadErr       error
			historyErr    error
			boundByLoader bool
		)
		if loader, ok := h.multi.(model.BoundSessionLoader); ok {
			history, loadErr = loader.LoadBoundSessionForConnection(req.SessionID, req.Cwd, h.connectionID)
			boundByLoader = loadErr == nil
		} else {
			loadErr = h.multi.LoadBoundSession(req.SessionID, req.Cwd)
			if loadErr == nil {
				history, historyErr = h.multi.SessionHistory(req.SessionID)
			}
		}
		h.gate.release()
		if loadErr != nil {
			if errors.Is(loadErr, model.ErrSessionUnknown) {
				return &protocol.RPCError{
					Code:    -32000,
					Message: fmt.Sprintf("acp frontend: unknown session %q", req.SessionID),
				}
			}
			return fmt.Errorf("acp frontend: load session %q: %w", req.SessionID, loadErr)
		}
		if historyErr != nil {
			return fmt.Errorf("acp frontend: load session %q history: %w", req.SessionID, historyErr)
		}

		h.remember(req.SessionID)
		// Binding 指向本连接刚加载的会话(连接私有状态,门禁外 Set;
		// 重选不删除原会话、不取消其后台 Run、不动 control lease、不影响其他连接)。
		h.binding.Set(req.SessionID)
		if !boundByLoader && h.binder != nil {
			h.binder.BindConnectionSession(h.connectionID, req.SessionID)
		}
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

	// ── 降级路径（后端无 model.BoundSessionSource：简化壳/测试桩）──────────
	// 单活动语义：load 的语义是「确认会话上下文已在」。
	if req.SessionID == h.sessions.CurrentSessionID() {
		// 与 prompt 互斥：load-current 同样改写共享会话状态（reload）。
		// prompt 在飞时诚实报 busy，归档分支只读、不碰共享状态，不需要门禁。
		// 临界区手动收窄到状态突变+历史快照：重放 Update 是连接 I/O，停摆客户端
		// 的阻塞写不能攥住门禁（同 CreateSession；release 不得双还）。
		if !h.gate.tryAcquire() {
			return sessionBusyError()
		}
		if err := h.sessions.ReloadCurrentSession(); err != nil {
			h.gate.release()
			return fmt.Errorf("acp frontend: reload current session: %w", err)
		}
		history := h.sessions.CurrentSessionHistory()
		h.gate.release()

		h.remember(req.SessionID)
		// Binding 指向本连接刚加载的会话(连接私有状态,门禁外 Set;
		// 重选不删除原会话、不取消其后台 Run、不动 control lease、不影响其他连接)。
		h.binding.Set(req.SessionID)
		// Set 处成对登记(与 CreateSession 同款;改绑即替换,session/load 重开把
		// 引用落回重开的会话)。
		if h.binder != nil {
			h.binder.BindConnectionSession(h.connectionID, req.SessionID)
		}
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

// commandOutput wraps preformatted command output in a Markdown code block so proportional-font
// renderers preserve terminal alignment. The unclosed fence remains valid during streaming, and
// empty output does not open a block. Writer/Close run on the command goroutine; token emission
// uses a separate reporter path.
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
