package acp

import (
	"context"
	"fmt"
	"strings"
	"sync"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// Evaluator 执行一条用户输入（装配层通常注入 Engine.Eval）。
type Evaluator = func(ctx *model.Context, input string) error

// FrontendHandler 是 daemon 主会话 ACP 入口的默认 handler：每连接一份，实现
// session/new、session/prompt、session/list、session/load 与 modes 协商的全部
// daemon 语义：
//
//   - CreateSession：登记本连接可 prompt 的 session，声明 cwd，下发命令补全；
//   - Prompt：known/read-only 守卫 → 命令输出围栏 + token→update 映射 + HITL
//     授权经 Reporter.RequestPermission（model.DecideViaACP）；
//   - LoadSession：当前会话 reload+重放（进可 prompt 集合），归档只读重放
//     （进只读集合，prompt 诚实报错），未知会话报错；
//   - ListSessions/SessionModes：domain → wire 映射转发。
//
// 它放在本包而非装配层：session 语义是 daemon domain 逻辑，所有壳层共用；
// 装配层只接线。
type FrontendHandler struct {
	evaluate Evaluator
	commands func() []model.Command
	sessions model.SessionSource

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
// commands 可为 nil（不下发命令补全）。
func NewFrontendHandlerFactory(evaluate Evaluator, commands func() []model.Command, sessions model.SessionSource) HandlerFactory {
	return func() aiacp.SessionHandler {
		return &FrontendHandler{
			evaluate: evaluate,
			commands: commands,
			sessions: sessions,
		}
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

// CreateSession 实现 aiacp.SessionCreator：登记本连接的 session、向 daemon 声明
// cwd，并在响应返回前把命令清单作为 available_commands_update 下发（/ 补全）。
func (h *FrontendHandler) CreateSession(_ context.Context, sessionID string, req protocol.NewSessionRequest, reporter aiacp.Reporter) error {
	h.remember(sessionID)
	if h.sessions != nil {
		h.sessions.SetSession(sessionID, req.Cwd)
	}
	if h.commands != nil {
		if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
			reporter.Update(sessionID, body)
		}
	}
	return nil
}

// Prompt 实现 aiacp.SessionHandler。流程对齐 vscode ingress.Run：归档只读守卫
// → 未知会话守卫 → 命令输出围栏 + token 映射 + HITL 授权 → evaluate → 闭合围栏。
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
		h.sessions.SetSession(req.SessionID, req.Cwd)
		if err := h.sessions.ReloadCurrentSession(); err != nil {
			return fmt.Errorf("acp frontend: reload current session: %w", err)
		}
		h.remember(req.SessionID)
		if h.commands != nil {
			if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
				reporter.Update(req.SessionID, body)
			}
		}
		for _, body := range h.sessions.CurrentSessionHistory() {
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
