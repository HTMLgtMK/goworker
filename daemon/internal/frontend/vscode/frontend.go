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
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// Evaluator delegates a raw frontend prompt to the daemon command engine.
type Evaluator func(ctx *plugin.Context, input string) error

// SessionSource 是 vscode frontend 对 daemon 会话元数据的最小消费接口（由 agent
// 插件实现，结构化类型、agent 包无需感知本接口）：
//   - ListSessions：session/list 数据源（当前会话置顶 + 归档，只读扫描）。
//   - CurrentSessionID：当前活动会话的 head id（空 = 无活动会话）。
//   - ReloadCurrentSession：load 命中当前会话时刷新会话视图（ReloadFromStore）。
//   - CurrentSessionHistory：当前会话对话历史 → session/update 序列，load 成功
//     路径上向客户端重放。
type SessionSource interface {
	ListSessions() []protocol.SessionInfo
	CurrentSessionID() string
	ReloadCurrentSession() error
	CurrentSessionHistory() []protocol.SessionUpdateBody
}

// Frontend exposes the daemon's persistent main conversation over a local ACP socket.
type Frontend struct {
	path      string
	evaluate  Evaluator
	commands  func() []plugin.Command
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
func New(path string, evaluate Evaluator, commands func() []plugin.Command, sessions SessionSource) *Frontend {
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
		handler := &ingress{evaluate: f.evaluate, commands: f.commands, sessionSource: f.sessions, sessions: make(map[string]struct{})}
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
func availableCommandsUpdateBody(cmds []plugin.Command) protocol.SessionUpdateBody {
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
	commands      func() []plugin.Command
	sessionSource SessionSource
	mu            sync.Mutex
	sessions      map[string]struct{}
}

func (h *ingress) remember(sessionID string) {
	h.mu.Lock()
	h.sessions[sessionID] = struct{}{}
	h.mu.Unlock()
}

func (h *ingress) SetSession(sessionID, _ string) {
	h.remember(sessionID)
}

// SetSessionServer 在 session/new 时立即把 daemon 命令清单作为 ACP
// available_commands_update 下发给客户端，供 / 补全；随后才返回 session 响应。
func (h *ingress) SetSessionServer(sessionID, _ string, server *dispatch.Server) {
	h.remember(sessionID)
	if h.commands == nil {
		return
	}
	if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
		server.Update(sessionID, body)
	}
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
//     先记住该 id（load 不走 session/new，不记住则后续 session/prompt 被本连接的
//     known-session 检查拒掉）；再下发 available_commands_update（load 路径没有
//     SetSessionServer，斜杠补全只能在此补齐，空清单也照发以清掉客户端旧状态）；
//     最后逐条重放对话历史（user/assistant 消息 → message chunk，映射规则见
//     agent.CurrentSessionHistory）。这些通知经同一连接顺序写出，先于 load 响应
//     到达，客户端可在响应返回前渲染。
//   - sessionId 是归档会话 → 明确报错（归档恢复为后续工作，诚实报错优于假成功）；
//   - 其他 → 未知会话报错。
//
// 不为历史 sessionId 重建独立会话——daemon 模型不支持多活动会话。
func (h *ingress) LoadSession(sessionID string, rep dispatch.Reporter) error {
	if h.sessionSource == nil {
		return &protocol.RPCError{Code: -32000, Message: "vscode frontend: session load not supported"}
	}
	if sessionID == h.sessionSource.CurrentSessionID() {
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
	for _, info := range h.sessionSource.ListSessions() {
		if info.SessionID == sessionID {
			return &protocol.RPCError{
				Code:    -32000,
				Message: fmt.Sprintf("vscode frontend: archived session resume is not supported yet (session %q)", sessionID),
			}
		}
	}
	return &protocol.RPCError{
		Code:    -32000,
		Message: fmt.Sprintf("vscode frontend: unknown session %q", sessionID),
	}
}

func (h *ingress) Run(ctx context.Context, sessionID, prompt string, rep dispatch.Reporter) (string, error) {
	h.mu.Lock()
	_, known := h.sessions[sessionID]
	h.mu.Unlock()
	if !known {
		return "", fmt.Errorf("vscode frontend: unknown transport session %q", sessionID)
	}
	commandContext := &plugin.Context{
		Ctx: ctx,
		FrontendContext: plugin.FrontendContext{
			Writer: func(text string) {
				rep.MessageChunk(sessionID, text)
			},
			EmitToken: func(token core.Token) {
				rep.Update(sessionID, tokenToUpdate(token))
			},
		},
		Values: make(map[string]any),
	}
	if err := h.evaluate(commandContext, prompt); err != nil {
		return "", err
	}
	return protocol.StopEndTurn, nil
}
