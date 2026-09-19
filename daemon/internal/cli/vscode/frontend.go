package vscode

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tinguo/goworker/ai-core/core"
	dispatch "github.com/tinguo/goworker/ai-dispatch"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// Evaluator delegates a raw frontend prompt to the daemon command engine.
type Evaluator func(ctx *model.Context, input string) error

// SessionSource 是 vscode frontend 对 daemon 会话元数据的最小消费接口（由 service
// 包的 agent 插件实现，结构化类型、service 包无需感知本接口）：
//   - ListSessions：session/list 数据源（当前会话置顶 + 归档，只读扫描）。
//   - CurrentSessionID：当前活动会话的 head id（空 = 无活动会话）。
//   - SetSession：把 ACP 提交方声明的会话 cwd 落到 daemon（校验与生效在实现方）。
//   - ReloadCurrentSession：load 命中当前会话时刷新会话视图（ReloadFromStore）。
//   - CurrentSessionHistory：当前会话对话历史 → session/update 序列，load 成功
//     路径上向客户端重放。
//   - ArchivedSessionHistory：归档会话只读解析 → 重放序列（归档 load 用）。
//   - SessionModes：session/new|load 响应的 models 状态（nil = 不带 modes）。
type SessionSource interface {
	ListSessions() []protocol.SessionInfo
	CurrentSessionID() string
	SetSession(sessionID, cwd string)
	ReloadCurrentSession() error
	CurrentSessionHistory() []protocol.SessionUpdateBody
	ArchivedSessionHistory(sessionID string) ([]protocol.SessionUpdateBody, error)
	SessionModes() *protocol.SessionModeState
}

// Frontend exposes the daemon's persistent main conversation over a local ACP socket.
type Frontend struct {
	path      string
	evaluate  Evaluator
	commands  func() []model.Command
	sessions  SessionSource
	listener  net.Listener
	mu        sync.Mutex
	lifecycle sync.Mutex
	ownedPath os.FileInfo
	// acceptMu 把 acceptLoop 的 Accept→注册→waitGroup.Add 变成对 Stop 原子的一段：
	// Stop 先关 listener 再取 acceptMu，保证 server 快照不漏掉"已 Accept 未注册"的
	// 连接（漏掉则无人 Close，awaitServer 永久阻塞，Stop 的 Wait 卡死）。
	acceptMu  sync.Mutex
	servers   map[*dispatch.Server]struct{}
	waitGroup sync.WaitGroup
}

// New creates a VS Code ACP frontend bound to one local Unix socket path.
// commands 提供 daemon 命令清单，session/new 时下发给客户端做 slash 补全。
// sessions 提供会话清单与 load 判定（nil = session/list 返回空、load 报不支持）。
func New(path string, evaluate Evaluator, commands func() []model.Command, sessions SessionSource) *Frontend {
	return &Frontend{
		path:     path,
		evaluate: evaluate,
		commands: commands,
		sessions: sessions,
		servers:  make(map[*dispatch.Server]struct{}),
	}
}

// Start listens on the configured Unix socket.
func (f *Frontend) Start() error {
	f.lifecycle.Lock()
	defer f.lifecycle.Unlock()

	if f.path == "" {
		return fmt.Errorf("vscode frontend: socket path is required")
	}
	if f.evaluate == nil {
		return fmt.Errorf("vscode frontend: evaluator is required")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listener != nil {
		return fmt.Errorf("vscode frontend: already started")
	}
	if err := os.MkdirAll(filepath.Dir(f.path), 0o700); err != nil {
		return fmt.Errorf("vscode frontend: create socket directory: %w", err)
	}
	if err := removeStaleSocket(f.path); err != nil {
		return err
	}
	listener, err := net.Listen("unix", f.path)
	if err != nil {
		return fmt.Errorf("vscode frontend: listen %s: %w", f.path, err)
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		return fmt.Errorf("vscode frontend: unexpected listener type %T", listener)
	}
	unixListener.SetUnlinkOnClose(false)
	info, err := os.Lstat(f.path)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("vscode frontend: inspect socket: %w", err)
	}
	if err := os.Chmod(f.path, 0o600); err != nil {
		_ = listener.Close()
		_ = removeSocket(f.path, info)
		return fmt.Errorf("vscode frontend: secure socket: %w", err)
	}
	current, err := os.Lstat(f.path)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("vscode frontend: inspect secured socket: %w", err)
	}
	if !os.SameFile(info, current) {
		_ = listener.Close()
		return fmt.Errorf("vscode frontend: socket path changed while starting")
	}

	f.listener = listener
	f.ownedPath = info
	f.waitGroup.Add(1)
	go f.acceptLoop(listener)
	return nil
}

// Stop closes all local connections and removes only this frontend's socket.
func (f *Frontend) Stop() error {
	f.lifecycle.Lock()
	defer f.lifecycle.Unlock()

	f.mu.Lock()
	listener := f.listener
	ownedPath := f.ownedPath
	f.listener = nil
	f.ownedPath = nil
	f.mu.Unlock()

	// 先关 listener：acceptLoop 里阻塞的 Accept 立刻报错返回。
	if listener != nil {
		_ = listener.Close()
	}
	// 再取 acceptMu：等 acceptLoop 完成"已 Accept 连接"的注册与入账，
	// 此后快照不漏 server（否则漏掉的 server 无人 Close，Wait 卡死）。
	// 快照后必须先释放 acceptMu：acceptLoop 可能正等它，才能看到已关闭的
	// listener 并退出；拿着它 Wait 会形成互等。
	f.acceptMu.Lock()
	f.mu.Lock()
	servers := make([]*dispatch.Server, 0, len(f.servers))
	for server := range f.servers {
		servers = append(servers, server)
	}
	f.mu.Unlock()
	f.acceptMu.Unlock()
	for _, server := range servers {
		_ = server.Close()
	}
	f.waitGroup.Wait()
	if err := removeSocket(f.path, ownedPath); err != nil {
		return err
	}
	return nil
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("vscode frontend: inspect socket path: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("vscode frontend: refuse to remove non-socket path %s", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("vscode frontend: remove stale socket: %w", err)
	}
	return nil
}

func removeSocket(path string, ownedPath os.FileInfo) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("vscode frontend: inspect socket path: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || (ownedPath != nil && !os.SameFile(info, ownedPath)) {
		return nil
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("vscode frontend: remove socket: %w", err)
	}
	return nil
}

func (f *Frontend) acceptLoop(listener net.Listener) {
	defer f.waitGroup.Done()
	for {
		f.acceptMu.Lock()
		conn, err := listener.Accept()
		if err != nil {
			f.acceptMu.Unlock()
			return
		}
		handler := &ingress{
			evaluate:      f.evaluate,
			commands:      f.commands,
			sessionSource: f.sessions,
			sessions:      make(map[string]struct{}),
			readOnly:      make(map[string]struct{}),
			cwds:          make(map[string]string),
		}
		server := dispatch.ServeConn(conn, handler)
		f.mu.Lock()
		f.servers[server] = struct{}{}
		f.mu.Unlock()
		f.waitGroup.Add(1)
		f.acceptMu.Unlock()
		go f.awaitServer(server)
	}
}

func (f *Frontend) awaitServer(server *dispatch.Server) {
	defer f.waitGroup.Done()
	<-server.Done()
	f.mu.Lock()
	delete(f.servers, server)
	f.mu.Unlock()
}

func tokenToUpdate(token core.Token) protocol.SessionUpdateBody {
	switch token.Type {
	case core.TokenTypeThinking:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateAgentThoughtChunk,
			Content:       &protocol.ContentBlock{Type: "text", Text: token.Content},
		}
	case core.TokenTypeToolCall:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateToolCall,
			ToolCallID:    token.ToolCall.ID,
			Title:         token.Content,
			Status:        "pending",
		}
	case core.TokenTypeToolResult:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateToolCallUpdate,
			ToolCallID:    token.ToolCallID,
			Title:         token.Content,
			Status:        "completed",
		}
	default:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateAgentMessageChunk,
			Content:       &protocol.ContentBlock{Type: "text", Text: token.Content},
		}
	}
}

// commandFence 是命令输出代码块的围栏。用 4 个反引号而非 3 个：命令输出会回显
// 用户内容（/history 回显对话、/rules 回显指令文件），其中出现 ``` 完全可能，
// 4 个反引号能让这些内容原样保留而不提前闭合代码块。
const commandFence = "````"

// commandOutput 把命令的预格式化输出包成一个 Markdown 代码块并流式发出。
//
// 为什么要包：命令输出是为等宽终端手工排版过的（`fmt.Sprintf("%-14s")` 那类
// 手工补的列），而 webview 按 Markdown 渲染、比例字体，不包的话对齐全塌。代码块
// 在两端都是等宽字体（webview 的 pre 用 --vscode-editor-font-family /
// ui-monospace），等于把「这段别重排」显式告诉渲染器。规则与具体命令无关 ——
// 任何经 Writer 出来的输出一视同仁。
//
// 为什么能流式：CommonMark 规定未闭合的围栏代码块延伸到文档末尾，所以中途每次
// 增量渲染看到的都是一段合法的（尚未闭合的）代码块 —— 长命令的进度实时可见，
// 收尾补上闭合围栏定稿。不必先攒完再发。
//
// 围栏惰性开启：命令没有任何输出时不留空块。开启前只认「有实质内容」的写入
// （见 Write），开启后一律原样透传 —— 围栏内的空行是表格排版的一部分，不能吞。
type commandOutput struct {
	rep       dispatch.Reporter
	sessionID string
	opened    bool
	trailing  byte // 已写出内容的最后一个字节，收尾时据此决定要不要补换行
}

// Write 实现 model.FrontendContext.Writer。
//
// 围栏开启前忽略纯空白：这条通道被 agent 路径共用（callbacksFromPlugin 的
// Write 就是 ctx.Writer），而 session.Run 每轮结束都会无条件 cb.Write("\n")。
// 不拦的话每次对话都会多出一个空代码块 —— /agent 和裸输入都走这条路。
func (w *commandOutput) Write(text string) {
	if text == "" {
		return
	}
	if !w.opened {
		if strings.TrimSpace(text) == "" {
			return
		}
		w.rep.MessageChunk(w.sessionID, commandFence+"\n")
		w.opened = true
	}
	w.rep.MessageChunk(w.sessionID, text)
	w.trailing = text[len(text)-1]
}

// Close 闭合围栏；未写入过任何内容时不发，避免留下一个空代码块。
func (w *commandOutput) Close() {
	if !w.opened {
		return
	}
	if w.trailing != '\n' {
		w.rep.MessageChunk(w.sessionID, "\n")
	}
	w.rep.MessageChunk(w.sessionID, commandFence+"\n")
}

// availableCommand 是 available_commands_update 里的一条命令补全项。
// name 不带前导 /（UI 侧补全统一展示为 /name）。
type availableCommand struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Source      string `json:"source,omitempty"` // 固定 "daemon"，标注命令来自 daemon 引擎
}

// availableCommandsUpdate 是 ACP available_commands_update 的透传载荷，
// 供 VS Code chat 面板做 slash command 补全。整体经 Raw 透传，不重组。
type availableCommandsUpdate struct {
	SessionUpdate     string             `json:"sessionUpdate"` // 固定 "available_commands_update"
	AvailableCommands []availableCommand `json:"availableCommands"`
}

// availableCommandsUpdateBody 从命令清单构建完整透传载荷。
// 别名展开为独立条目（description 标注 alias of /xxx），空清单也照发（清空旧状态）。
func availableCommandsUpdateBody(cmds []model.Command) protocol.SessionUpdateBody {
	out := make([]availableCommand, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, availableCommand{
			Name:        strings.TrimPrefix(cmd.Name, "/"),
			Description: cmd.Description,
			Source:      "daemon",
		})
		for _, alias := range cmd.Aliases {
			out = append(out, availableCommand{
				Name:        strings.TrimPrefix(alias, "/"),
				Description: fmt.Sprintf("(alias of %s)", cmd.Name),
				Source:      "daemon",
			})
		}
	}
	data, err := json.Marshal(availableCommandsUpdate{
		SessionUpdate:     protocol.UpdateAvailableCommands,
		AvailableCommands: out,
	})
	if err != nil {
		// 这些字段都是字符串/切片，json.Marshal 不会失败；真失败就跳过本次下发。
		return protocol.SessionUpdateBody{}
	}
	return protocol.SessionUpdateBody{Raw: data}
}

type ingress struct {
	evaluate      Evaluator
	commands      func() []model.Command
	sessionSource SessionSource
	mu            sync.Mutex
	sessions      map[string]struct{} // 可 prompt 集合：session/new 与当前会话 load 记入
	readOnly      map[string]struct{} // 只读集合：归档 load 记入，prompt 诚实报错
	cwds          map[string]string   // ACP sessionID → 提交方声明的工作目录（对齐 acpIngress）
}

func (h *ingress) remember(sessionID string) {
	h.mu.Lock()
	if h.sessions == nil {
		h.sessions = make(map[string]struct{})
	}
	h.sessions[sessionID] = struct{}{}
	h.mu.Unlock()
}

// rememberReadOnly 把归档会话 id 记入只读集合：与可 prompt 集合（h.sessions）分离，
// load 归档成功但 prompt 该 id 会被 Run 明确拒绝（归档只读，不进可 prompt 集合）。
func (h *ingress) rememberReadOnly(sessionID string) {
	h.mu.Lock()
	if h.readOnly == nil {
		h.readOnly = make(map[string]struct{})
	}
	h.readOnly[sessionID] = struct{}{}
	h.mu.Unlock()
}

// rememberCwd 记录 sessionID → cwd（ingress 侧的会话上下文台账，对齐 dispatcher
// 的 acpIngress 模式；对 daemon 的生效经 sessionSource.SetSession 落地）。
func (h *ingress) rememberCwd(sessionID, cwd string) {
	h.mu.Lock()
	if h.cwds == nil {
		h.cwds = make(map[string]string)
	}
	h.cwds[sessionID] = cwd
	h.mu.Unlock()
}

// knownSession 返回 sessionID 的集合归属（可 prompt / 只读）。
func (h *ingress) knownSession(sessionID string) (promptable, readOnly bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, promptable = h.sessions[sessionID]
	_, readOnly = h.readOnly[sessionID]
	return promptable, readOnly
}

// SetSession 实现 dispatch.SessionAware：session/new 时记住会话并声明 cwd 落地。
func (h *ingress) SetSession(sessionID, cwd string) {
	h.remember(sessionID)
	h.rememberCwd(sessionID, cwd)
	if h.sessionSource != nil {
		h.sessionSource.SetSession(sessionID, cwd)
	}
}

// SetSessionServer 在 session/new 时立即把 daemon 命令清单作为 ACP
// available_commands_update 下发给客户端，供 / 补全；随后才返回 session 响应。
func (h *ingress) SetSessionServer(sessionID, cwd string, server *dispatch.Server) {
	h.SetSession(sessionID, cwd)
	if h.commands == nil {
		return
	}
	if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
		server.Update(sessionID, body)
	}
}

// SessionModes 实现 dispatch.SessionModesProvider：转发给会话数据源
// （无数据源 = 无 modes，响应省略该字段）。
func (h *ingress) SessionModes() *protocol.SessionModeState {
	if h.sessionSource == nil {
		return nil
	}
	return h.sessionSource.SessionModes()
}

// ListSessions 实现 dispatch.SessionLister：转发给会话数据源；无数据源时返回
// 空清单（非 nil，ACP 无 list 能力协商，空清单即「当前无可列会话」）。
func (h *ingress) ListSessions() []protocol.SessionInfo {
	if h.sessionSource == nil {
		return []protocol.SessionInfo{}
	}
	return h.sessionSource.ListSessions()
}

// LoadSession 实现 dispatch.SessionLoader。daemon 的 agent 插件是单活动会话模型
// （store 是唯一真相，当前会话始终处于已加载状态），load 的语义是「确认会话上下文已在」：
//   - sessionId == 当前会话 head → 刷新会话视图后成功，并在响应前推送重放通知：
//     先把声明的 cwd 落到 daemon（SetSession），再记住该 id（load 不走 session/new，
//     不记住则后续 session/prompt 被本连接的 known-session 检查拒掉）；然后下发
//     available_commands_update（load 路径没有 SetSessionServer，斜杠补全只能在
//     此补齐，空清单也照发以清掉客户端旧状态）；最后逐条重放对话历史（user/
//     assistant 消息 → message chunk，映射规则见 agent.CurrentSessionHistory）。
//     这些通知经同一连接顺序写出，先于 load 响应到达，客户端可在响应返回前渲染。
//   - sessionId 是归档会话 → 只读查看（方案 B）：只读解析归档并重放（不进可
//     prompt 集合，记入只读集合；归档 cwd 不改变当前会话），响应前先推
//     available_commands_update（如命令清单可用）再推归档历史；
//   - 其他 → 未知会话报错。
//
// 不为历史 sessionId 重建独立会话——daemon 模型不支持多活动会话。
func (h *ingress) LoadSession(sessionID, cwd string, rep dispatch.Reporter) error {
	if h.sessionSource == nil {
		return &protocol.RPCError{Code: -32000, Message: "vscode frontend: session load not supported"}
	}
	if sessionID == h.sessionSource.CurrentSessionID() {
		h.sessionSource.SetSession(sessionID, cwd)
		if err := h.sessionSource.ReloadCurrentSession(); err != nil {
			return fmt.Errorf("vscode frontend: reload current session: %w", err)
		}
		h.remember(sessionID)
		if h.commands != nil {
			if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
				rep.Update(sessionID, body)
			}
		}
		for _, body := range h.sessionSource.CurrentSessionHistory() {
			rep.Update(sessionID, body)
		}
		return nil
	}
	// 归档会话：只读重放。先解析成功再入只读集合，解析失败保持未知会话语义。
	for _, info := range h.sessionSource.ListSessions() {
		if info.SessionID != sessionID {
			continue
		}
		history, err := h.sessionSource.ArchivedSessionHistory(sessionID)
		if err != nil {
			return fmt.Errorf("vscode frontend: load archived session %q: %w", sessionID, err)
		}
		h.rememberReadOnly(sessionID)
		if h.commands != nil {
			if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
				rep.Update(sessionID, body)
			}
		}
		for _, body := range history {
			rep.Update(sessionID, body)
		}
		return nil
	}
	return &protocol.RPCError{
		Code:    -32000,
		Message: fmt.Sprintf("vscode frontend: unknown session %q", sessionID),
	}
}

func (h *ingress) Run(ctx context.Context, sessionID, prompt string, rep dispatch.Reporter) (string, error) {
	promptable, readOnly := h.knownSession(sessionID)
	if readOnly {
		// 归档会话续写防护：诚实报错，优于报 unknown transport session 掩盖真相
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("vscode frontend: archived session %q is read-only", sessionID),
		}
	}
	if !promptable {
		return "", fmt.Errorf("vscode frontend: unknown transport session %q", sessionID)
	}
	output := &commandOutput{rep: rep, sessionID: sessionID}
	commandContext := &model.Context{
		Ctx: ctx,
		FrontendContext: model.FrontendContext{
			// 命令输出统一包成代码块（见 commandOutput）：命令侧仍按等宽终端排版，
			// 由这里告诉渲染器「这段别重排」。
			Writer: output.Write,
			EmitToken: func(token core.Token) {
				rep.Update(sessionID, tokenToUpdate(token))
			},
			// Decide 之前一直是 nil：沙箱中间件对 nil 的默认行为是静默拒绝，
			// 用户点不到任何东西、也看不到任何提示，危险命令就这么无声消失。
			// 接上 ACP 授权请求，vscode 侧 PermissionDialog 已有现成渲染。
			Decide: model.DecideViaACP(ctx, rep, sessionID),
		},
		Values: make(map[string]any),
	}
	// evaluate 内部的报错也经 Writer 写出（命令自己格式化 ✘ 前缀），因此先闭合
	// 代码块再返回错误，否则已经流出去的内容会留一个未闭合的围栏。
	evalErr := h.evaluate(commandContext, prompt)
	output.Close()
	if evalErr != nil {
		return "", evalErr
	}
	return protocol.StopEndTurn, nil
}
