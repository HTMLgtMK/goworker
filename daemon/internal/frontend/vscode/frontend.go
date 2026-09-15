package vscode

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/tinguo/goworker/ai-core/core"
	dispatch "github.com/tinguo/goworker/ai-dispatch"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// Evaluator delegates a raw frontend prompt to the daemon command engine.
type Evaluator func(ctx *plugin.Context, input string) error

// Frontend exposes the daemon's persistent main conversation over a local ACP socket.
type Frontend struct {
	path      string
	evaluate  Evaluator
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
func New(path string, evaluate Evaluator) *Frontend {
	return &Frontend{
		path:     path,
		evaluate: evaluate,
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
		handler := &ingress{evaluate: f.evaluate, sessions: make(map[string]struct{})}
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

type ingress struct {
	evaluate Evaluator
	mu       sync.Mutex
	sessions map[string]struct{}
}

func (h *ingress) SetSession(sessionID, _ string) {
	h.mu.Lock()
	h.sessions[sessionID] = struct{}{}
	h.mu.Unlock()
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
