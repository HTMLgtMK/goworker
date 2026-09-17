package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
)

// TaskHandler 是 Server 对任务执行方（daemon 插件）的回调。
type TaskHandler interface {
	// Run 执行一回合任务（一回合 = 一个任务），阻塞到完成并返回 stop reason。
	// 执行期间用 rep 流式汇报进度；ctx 被 session/cancel 取消。
	Run(ctx context.Context, sessionID, prompt string, rep Reporter) (string, error)
}

// Reporter 供 handler 向 ACP 提交方流式汇报进度。
type Reporter interface {
	// Update 透传任意 session/update 子类型。
	Update(sessionID string, body protocol.SessionUpdateBody)
	// MessageChunk 快捷发送 agent_message_chunk 文本。
	MessageChunk(sessionID, text string)
}

// SessionAware 是 handler 的可选扩展：session/new 时收到提交方声明的 cwd，
// 供任务派发侧决定工作目录（git 仓库判型等）。
type SessionAware interface {
	SetSession(sessionID, cwd string)
}

// SessionServerAware 是 handler 的可选扩展：session/new 时除 cwd 外再拿到
// Server 引用，供 handler 在返回响应前立即向提交方下发 session/update
// （如 available_commands_update）。实现本接口的 handler 优先于 SessionAware；
// 未实现者保持原 SessionAware 行为不变。
type SessionServerAware interface {
	SetSessionServer(sessionID, cwd string, server *Server)
}

// SessionLoader 是 handler 的可选扩展：实现后 Server 在 initialize 能力协商中
// 上报 loadSession=true，session/load 请求转发给 handler；未实现者保持
// LoadSession:false（协议诚实：客户端只应对声明了能力的 agent 发 load）。
// cwd 是提交方声明的工作目录（会话级上下文，取值与落地在 handler 侧）；rep 供
// handler 在返回响应前向提交方推送 session/update 通知（历史重放、命令清单等）
// ——同一连接顺序写入保证这些通知先于 load 响应到达（对齐 @agentclientprotocol/sdk
// 语义：客户端可在 load 返回前就开始渲染重放内容）。
type SessionLoader interface {
	// LoadSession 确认会话上下文已在（daemon 单活动会话模型下即「当前会话」）。
	LoadSession(sessionID, cwd string, rep Reporter) error
}

// SessionLister 是 handler 的可选扩展：实现后 session/list 返回 handler 给出的
// 会话清单（当前 + 归档）。请求参数 cwd/cursor 不转发——清单方一次给全、不翻页。
type SessionLister interface {
	ListSessions() []protocol.SessionInfo
}

// SessionModesProvider 是 handler 的可选扩展：实现后 session/new 与 session/load
// 响应携带 modes 状态（ACP SessionModeState，客户端据此渲染模型选择器）。
// daemon 只显示不切换：session/set_mode 未注册，客户端切换请求以
// method-not-found 诚实失败。返回 nil 时响应省略 modes。
type SessionModesProvider interface {
	SessionModes() *protocol.SessionModeState
}

// Server 是 ACP Agent 角色：接受外部 ACP Client 的任务提交。
// 一条连接一个 Server；session/prompt 的内容即任务。
type Server struct {
	conn    *protocol.Conn
	closeFn func() error
	handler TaskHandler

	done chan struct{} // 读循环和已开始的 prompt 都退出后关闭

	mu      sync.Mutex
	closed  bool
	cancels map[string]context.CancelFunc // sessionID → prompt 执行的 cancel
	prompts sync.WaitGroup
}

// ServeConn 在现有连接上服务（测试或自定义 transport）。
func ServeConn(rwc io.ReadWriteCloser, handler TaskHandler) *Server {
	s := &Server{
		conn:    protocol.NewConn(rwc),
		handler: handler,
		cancels: make(map[string]context.CancelFunc),
	}
	s.conn.Handle(protocol.MethodInitialize, s.handleInitialize)
	s.conn.Handle(protocol.MethodSessionList, s.handleSessionList)
	s.conn.Handle(protocol.MethodSessionNew, s.handleSessionNew)
	s.conn.Handle(protocol.MethodSessionLoad, s.handleSessionLoad)
	s.conn.Handle(protocol.MethodSessionPrompt, s.handlePrompt)
	s.conn.HandleNotification(protocol.MethodSessionCancel, s.handleCancel)
	s.done = make(chan struct{})
	go func() {
		_ = s.conn.Serve()
		s.cancelAll()
		s.prompts.Wait()
		close(s.done)
	}()
	return s
}

// Done 在连接读循环退出（对端断开）后关闭。
func (s *Server) Done() <-chan struct{} { return s.done }

// Close 断开连接。
func (s *Server) Close() error {
	if s.closeFn != nil {
		return s.closeFn()
	}
	return s.conn.Close()
}

func (s *Server) handleInitialize(_ context.Context, params json.RawMessage) (any, error) {
	var req protocol.InitializeRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("dispatch: decode initialize: %w", err)
	}
	// 能力协商照实上报：只有 handler 真的实现 load 才声明 loadSession。
	caps := protocol.AgentCapabilities{}
	if _, ok := s.handler.(SessionLoader); ok {
		caps.LoadSession = true
	}
	return protocol.InitializeResponse{
		ProtocolVersion:   protocol.Version,
		AgentCapabilities: caps,
	}, nil
}

// handleSessionLoad 把 load 转发给实现了 SessionLoader 的 handler；cwd 随请求
// 交给 handler 落地。成功响应按 ACP 序列化为 {}（handler 未提供 modes 时）。
func (s *Server) handleSessionLoad(_ context.Context, params json.RawMessage) (any, error) {
	var req protocol.LoadSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("dispatch: decode session/load: %w", err)
	}
	if req.SessionID == "" {
		return nil, fmt.Errorf("dispatch: session/load requires sessionId")
	}
	loader, ok := s.handler.(SessionLoader)
	if !ok {
		return nil, &protocol.RPCError{Code: -32000, Message: "dispatch: session load not supported"}
	}
	if err := loader.LoadSession(req.SessionID, req.Cwd, s); err != nil {
		return nil, err
	}
	resp := protocol.LoadSessionResponse{}
	if provider, ok := s.handler.(SessionModesProvider); ok {
		resp.Modes = provider.SessionModes()
	}
	return resp, nil
}

// handleSessionList 把清单请求转发给实现了 SessionLister 的 handler；
// cwd/cursor 暂无消费方（清单一次给全），不解码。
func (s *Server) handleSessionList(_ context.Context, _ json.RawMessage) (any, error) {
	lister, ok := s.handler.(SessionLister)
	if !ok {
		return nil, &protocol.RPCError{Code: -32000, Message: "dispatch: session list not supported"}
	}
	sessions := lister.ListSessions()
	if sessions == nil {
		// 空清单必须是 [] 而非 null（对齐 availableCommandsUpdate 的空清单语义）
		sessions = []protocol.SessionInfo{}
	}
	return protocol.ListSessionsResponse{Sessions: sessions}, nil
}

func (s *Server) handleSessionNew(_ context.Context, params json.RawMessage) (any, error) {
	var req protocol.NewSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("dispatch: decode session/new: %w", err)
	}
	sessionID := newHexID("sess")
	if aware, ok := s.handler.(SessionServerAware); ok {
		aware.SetSessionServer(sessionID, req.Cwd, s)
	} else if aware, ok := s.handler.(SessionAware); ok {
		aware.SetSession(sessionID, req.Cwd)
	}
	resp := protocol.NewSessionResponse{SessionID: sessionID}
	if provider, ok := s.handler.(SessionModesProvider); ok {
		resp.Modes = provider.SessionModes()
	}
	return resp, nil
}

func (s *Server) handlePrompt(ctx context.Context, params json.RawMessage) (any, error) {
	var req protocol.PromptRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("dispatch: decode session/prompt: %w", err)
	}
	if req.SessionID == "" || len(req.Prompt) == 0 {
		return nil, fmt.Errorf("dispatch: session/prompt requires sessionId and prompt")
	}
	var text string
	for i, block := range req.Prompt {
		if block.Type == "text" {
			if i > 0 {
				text += "\n"
			}
			text += block.Text
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		return nil, protocol.ErrClosed
	}
	if _, active := s.cancels[req.SessionID]; active {
		s.mu.Unlock()
		cancel()
		return nil, &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("dispatch: session %q already has an active prompt", req.SessionID),
		}
	}
	s.cancels[req.SessionID] = cancel
	s.prompts.Add(1)
	s.mu.Unlock()
	defer s.prompts.Done()
	defer func() {
		s.mu.Lock()
		delete(s.cancels, req.SessionID)
		s.mu.Unlock()
		cancel()
	}()

	stop, err := s.handler.Run(runCtx, req.SessionID, text, s)
	if err != nil {
		return nil, err
	}
	if stop == "" {
		stop = protocol.StopEndTurn
	}
	return protocol.PromptResponse{StopReason: stop}, nil
}

func (s *Server) cancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for sessionID, cancel := range s.cancels {
		delete(s.cancels, sessionID)
		cancel()
	}
}

func (s *Server) handleCancel(params json.RawMessage) {
	var req protocol.CancelNotification
	if err := json.Unmarshal(params, &req); err != nil || req.SessionID == "" {
		return
	}
	s.mu.Lock()
	cancel := s.cancels[req.SessionID]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Update 实现 Reporter：session/update 通知转发给提交方。
func (s *Server) Update(sessionID string, body protocol.SessionUpdateBody) {
	if body.Raw != nil {
		// 未知/透传子类型：逐字转发，不重组结构
		var envelope struct {
			SessionID string          `json:"sessionId"`
			Update    json.RawMessage `json:"update"`
		}
		envelope.SessionID = sessionID
		envelope.Update = body.Raw
		_ = s.conn.Notify(protocol.MethodSessionUpdate, envelope)
		return
	}
	_ = s.conn.Notify(protocol.MethodSessionUpdate, protocol.SessionUpdate{
		SessionID: sessionID,
		Update:    body,
	})
}

// MessageChunk 实现 Reporter。
func (s *Server) MessageChunk(sessionID, text string) {
	s.Update(sessionID, protocol.SessionUpdateBody{
		SessionUpdate: protocol.UpdateAgentMessageChunk,
		Content:       &protocol.ContentBlock{Type: "text", Text: text},
	})
}
