package dispatch

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
)

// ---- fake agent：模拟一个 ACP worker ----

type fakeAgent struct {
	conn *protocol.Conn

	mu                sync.Mutex
	promptBody        string
	newSessionParams  json.RawMessage
	loadSessionParams json.RawMessage
	gotOptionID       string
}

func newFakeAgent(rwc io.ReadWriteCloser) *fakeAgent {
	a := &fakeAgent{conn: protocol.NewConn(rwc)}
	a.conn.Handle(protocol.MethodInitialize, a.handleInitialize)
	a.conn.Handle(protocol.MethodSessionNew, a.handleSessionNew)
	a.conn.Handle(protocol.MethodSessionLoad, a.handleSessionLoad)
	a.conn.Handle(protocol.MethodSessionPrompt, a.handlePrompt)
	go func() { _ = a.conn.Serve() }()
	return a
}

func (a *fakeAgent) handleInitialize(_ context.Context, _ json.RawMessage) (any, error) {
	return protocol.InitializeResponse{ProtocolVersion: protocol.Version}, nil
}

func (a *fakeAgent) handleSessionNew(_ context.Context, params json.RawMessage) (any, error) {
	a.mu.Lock()
	a.newSessionParams = append(a.newSessionParams[:0], params...)
	a.mu.Unlock()
	return protocol.NewSessionResponse{SessionID: "sess_fake"}, nil
}

func (a *fakeAgent) handleSessionLoad(_ context.Context, params json.RawMessage) (any, error) {
	a.mu.Lock()
	a.loadSessionParams = append(a.loadSessionParams[:0], params...)
	a.mu.Unlock()
	return protocol.NewSessionResponse{SessionID: "sess_fake"}, nil
}

func (a *fakeAgent) handlePrompt(ctx context.Context, params json.RawMessage) (any, error) {
	var req protocol.PromptRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, err
	}
	var text string
	for _, block := range req.Prompt {
		if block.Type == "text" {
			text += block.Text
		}
	}
	a.mu.Lock()
	a.promptBody = text
	a.mu.Unlock()

	// 进度：文本块 + 工具调用
	_ = a.conn.Notify(protocol.MethodSessionUpdate, protocol.SessionUpdate{
		SessionID: req.SessionID,
		Update: protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateAgentMessageChunk,
			Content:       &protocol.ContentBlock{Type: "text", Text: "working"},
		},
	})
	_ = a.conn.Notify(protocol.MethodSessionUpdate, protocol.SessionUpdate{
		SessionID: req.SessionID,
		Update: protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateToolCall,
			ToolCallID:    "tc_1",
			Title:         "bash",
			Kind:          "execute",
			Status:        "pending",
		},
	})

	// 权限请求走反方向 request
	if err := a.conn.Call(ctx, protocol.MethodSessionRequestPermission, protocol.PermissionRequest{
		SessionID: req.SessionID,
		ToolCall:  protocol.ToolCallInfo{ToolCallID: "tc_1", Title: "bash"},
		Options:   []protocol.PermissionOption{{OptionID: "allow", Kind: "allow_once"}},
	}, nil); err != nil {
		return protocol.PromptResponse{StopReason: protocol.StopRefusal}, nil
	}

	return protocol.PromptResponse{StopReason: protocol.StopEndTurn}, nil
}

func (a *fakeAgent) promptText() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.promptBody
}

// ---- helpers ----

func clientToAgent(t *testing.T, agent *fakeAgent) *Client {
	t.Helper()
	clientEnd, agentEnd := net.Pipe()
	agent.conn = protocol.NewConn(agentEnd)
	agent.conn.Handle(protocol.MethodInitialize, agent.handleInitialize)
	agent.conn.Handle(protocol.MethodSessionNew, agent.handleSessionNew)
	agent.conn.Handle(protocol.MethodSessionLoad, agent.handleSessionLoad)
	agent.conn.Handle(protocol.MethodSessionPrompt, agent.handlePrompt)
	go func() { _ = agent.conn.Serve() }()

	c := NewClient("fake", clientEnd, clientEnd.Close)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestClient_SessionRequestsIncludeEmptyMCPServers(t *testing.T) {
	agent := &fakeAgent{}
	client := clientToAgent(t, agent)

	if _, err := client.NewSession(context.Background(), "/repo"); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := client.SessionLoad(context.Background(), "sess_previous", "/repo"); err != nil {
		t.Fatalf("SessionLoad: %v", err)
	}

	agent.mu.Lock()
	newParams := append(json.RawMessage(nil), agent.newSessionParams...)
	loadParams := append(json.RawMessage(nil), agent.loadSessionParams...)
	agent.mu.Unlock()

	for _, tc := range []struct {
		name   string
		params json.RawMessage
	}{
		{name: "session/new", params: newParams},
		{name: "session/load", params: loadParams},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var request map[string]json.RawMessage
			if err := json.Unmarshal(tc.params, &request); err != nil {
				t.Fatalf("decode params: %v", err)
			}
			servers, ok := request["mcpServers"]
			if !ok {
				t.Fatal("mcpServers is missing")
			}
			if string(servers) == "null" {
				t.Fatal("mcpServers must be an array, got null")
			}
			var values []json.RawMessage
			if err := json.Unmarshal(servers, &values); err != nil {
				t.Fatalf("mcpServers must be an array: %v", err)
			}
			if len(values) != 0 {
				t.Errorf("mcpServers = %s, want []", servers)
			}
		})
	}
}

func TestClient_PromptFlow(t *testing.T) {
	agent := &fakeAgent{}
	client := clientToAgent(t, agent)

	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sessionID, err := client.NewSession(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if sessionID != "sess_fake" {
		t.Errorf("sessionId = %q", sessionID)
	}

	var mu sync.Mutex
	var updates []protocol.SessionUpdateBody
	client.SetCallbacks(ClientCallbacks{
		OnUpdate: func(u protocol.SessionUpdate) {
			mu.Lock()
			defer mu.Unlock()
			updates = append(updates, u.Update)
		},
		OnPermission: func(context.Context, protocol.PermissionRequest) (string, error) {
			return "allow", nil
		},
	})

	stop, err := client.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("do the thing")})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if stop != protocol.StopEndTurn {
		t.Errorf("stopReason = %q, want end_turn", stop)
	}
	if agent.promptText() != "do the thing" {
		t.Errorf("agent prompt = %q", agent.promptText())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(updates) != 2 ||
		updates[0].SessionUpdate != protocol.UpdateAgentMessageChunk ||
		updates[0].Content.Text != "working" ||
		updates[1].SessionUpdate != protocol.UpdateToolCall ||
		updates[1].ToolCallID != "tc_1" {
		t.Errorf("updates = %+v", updates)
	}
}

func TestClient_PermissionDeniedWithoutHandler(t *testing.T) {
	agent := &fakeAgent{}
	client := clientToAgent(t, agent)
	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	sessionID, err := client.NewSession(context.Background(), "/repo")
	if err != nil {
		t.Fatal(err)
	}
	client.SetCallbacks(ClientCallbacks{OnPermission: nil}) // 无人值守：拒绝

	// worker 在权限被拒后返回 refusal
	stop, err := client.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("x")})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if stop != protocol.StopRefusal {
		t.Errorf("stopReason = %q, want refusal", stop)
	}
}

func TestClient_CancelNotification(t *testing.T) {
	agent := &fakeAgent{}
	client := clientToAgent(t, agent)
	sessionID, err := client.NewSession(context.Background(), "/repo")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Cancel(sessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	// notification 无响应可等，只验证不 panic / 不阻塞
}

// ---- Server 侧：fake ACP client 提交任务 ----

type recordingHandler struct {
	mu       sync.Mutex
	prompts  []string
	reports  []string
	blockDur time.Duration
}

func (h *recordingHandler) Run(ctx context.Context, sessionID, prompt string, rep Reporter) (string, error) {
	h.mu.Lock()
	h.prompts = append(h.prompts, prompt)
	h.mu.Unlock()
	rep.MessageChunk(sessionID, "progress-1")
	if h.blockDur > 0 {
		select {
		case <-ctx.Done():
			return protocol.StopCancelled, nil
		case <-time.After(h.blockDur):
		}
	}
	return protocol.StopEndTurn, nil
}

func newServerWithFakeClient(t *testing.T, handler TaskHandler) (*Server, *fakeClient) {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, handler)
	t.Cleanup(func() { _ = server.Close() })
	fc := newFakeClient(clientEnd)
	t.Cleanup(func() { _ = fc.close() })
	return server, fc
}

// fakeClient 模拟一个提交任务的 ACP Client。
type fakeClient struct {
	conn    *protocol.Conn
	mu      sync.Mutex
	updates []protocol.SessionUpdate
}

func newFakeClient(rwc io.ReadWriteCloser) *fakeClient {
	fc := &fakeClient{conn: protocol.NewConn(rwc)}
	fc.conn.HandleNotification(protocol.MethodSessionUpdate, func(params json.RawMessage) {
		var u protocol.SessionUpdate
		if err := json.Unmarshal(params, &u); err == nil {
			fc.mu.Lock()
			fc.updates = append(fc.updates, u)
			fc.mu.Unlock()
		}
	})
	go func() { _ = fc.conn.Serve() }()
	return fc
}

func (fc *fakeClient) close() error { return fc.conn.Close() }

func (fc *fakeClient) collected() []protocol.SessionUpdate {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return append([]protocol.SessionUpdate(nil), fc.updates...)
}

func TestServer_TaskRoundtrip(t *testing.T) {
	handler := &recordingHandler{}
	_, fc := newServerWithFakeClient(t, handler)

	var initResp protocol.InitializeResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodInitialize,
		protocol.InitializeRequest{ProtocolVersion: protocol.Version}, &initResp); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if initResp.ProtocolVersion != protocol.Version {
		t.Errorf("protocolVersion = %d", initResp.ProtocolVersion)
	}

	var newResp protocol.NewSessionResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodSessionNew,
		protocol.NewSessionRequest{Cwd: "/repo"}, &newResp); err != nil {
		t.Fatalf("session/new: %v", err)
	}

	var promptResp protocol.PromptResponse
	err := fc.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: newResp.SessionID,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("build feature")},
	}, &promptResp)
	if err != nil {
		t.Fatalf("session/prompt: %v", err)
	}
	if promptResp.StopReason != protocol.StopEndTurn {
		t.Errorf("stopReason = %q", promptResp.StopReason)
	}

	handler.mu.Lock()
	prompt := handler.prompts[0]
	handler.mu.Unlock()
	if prompt != "build feature" {
		t.Errorf("handler prompt = %q", prompt)
	}

	deadline := time.After(time.Second)
	for {
		updates := fc.collected()
		if len(updates) >= 1 && updates[0].Update.Content != nil && updates[0].Update.Content.Text == "progress-1" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("progress update not received: %+v", fc.collected())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestServer_CancelInterruptsPrompt(t *testing.T) {
	handler := &recordingHandler{blockDur: 5 * time.Second}
	_, fc := newServerWithFakeClient(t, handler)

	var newResp protocol.NewSessionResponse
	if err := fc.conn.Call(context.Background(), protocol.MethodSessionNew,
		protocol.NewSessionRequest{Cwd: "/repo"}, &newResp); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		var resp protocol.PromptResponse
		err := fc.conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
			SessionID: newResp.SessionID,
			Prompt:    []protocol.ContentBlock{protocol.TextBlock("long task")},
		}, &resp)
		if err == nil {
			errCh <- errStopped(resp.StopReason)
			return
		}
		errCh <- err
	}()

	time.Sleep(100 * time.Millisecond) // 等 prompt 进入 handler
	_ = fc.conn.Notify(protocol.MethodSessionCancel, protocol.CancelNotification{SessionID: newResp.SessionID})

	select {
	case err := <-errCh:
		if se, ok := err.(stopError); !ok || se.reason != protocol.StopCancelled {
			t.Fatalf("prompt result = %v, want cancelled stop", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not interrupt prompt")
	}
}

type stopError struct{ reason string }

func (e stopError) Error() string { return "stop: " + e.reason }

func errStopped(reason string) error { return stopError{reason: reason} }
