package aiacp

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

type lifecycleHandler struct {
	mu       sync.Mutex
	created  string
	cwd      string
	prompt   string
	updates  int
	loaded   string
	blocking bool
	entered  chan struct{}
}

func (h *lifecycleHandler) CreateSession(_ context.Context, sessionID string, req protocol.NewSessionRequest, _ Reporter) error {
	h.mu.Lock()
	h.created, h.cwd = sessionID, req.Cwd
	h.mu.Unlock()
	return nil
}

func (h *lifecycleHandler) LoadSession(_ context.Context, req protocol.LoadSessionRequest, _ Reporter) error {
	h.mu.Lock()
	h.loaded = req.SessionID
	h.mu.Unlock()
	return nil
}

func (h *lifecycleHandler) ListSessions(context.Context, protocol.ListSessionsRequest) ([]protocol.SessionInfo, error) {
	return []protocol.SessionInfo{{SessionID: "archived", Cwd: "/repo"}}, nil
}

func (h *lifecycleHandler) SessionModes() *protocol.SessionModeState {
	return &protocol.SessionModeState{
		CurrentModeID:  "default",
		AvailableModes: []protocol.SessionMode{{ID: "default", Name: "Default"}},
	}
}

func (h *lifecycleHandler) sessionValues() (created, cwd, loaded, prompt string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.created, h.cwd, h.loaded, h.prompt
}

func (h *lifecycleHandler) Prompt(ctx context.Context, sessionID string, prompt []protocol.ContentBlock, reporter Reporter) (string, error) {
	if h.entered != nil {
		select {
		case <-h.entered:
		default:
			close(h.entered)
		}
	}
	for _, block := range prompt {
		if block.Type == "text" {
			h.mu.Lock()
			h.prompt = block.Text
			h.mu.Unlock()
		}
	}
	reporter.MessageChunk(sessionID, "working")
	if h.blocking {
		<-ctx.Done()
		return protocol.StopCancelled, nil
	}
	option, err := reporter.RequestPermission(ctx, sessionID, protocol.PermissionRequest{
		ToolCall: protocol.ToolCallInfo{ToolCallID: "tool-1", Title: "shell"},
		Options:  []protocol.PermissionOption{{OptionID: "allow", Kind: "allow_once"}},
	})
	if err != nil {
		return "", err
	}
	if option != "allow" {
		return protocol.StopRefusal, nil
	}
	return protocol.StopEndTurn, nil
}

func newTestClientServer(t *testing.T, handler *lifecycleHandler) *Client {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, handler)
	t.Cleanup(func() { _ = server.Close() })
	client := NewClient("test-agent", clientEnd, nil)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestClientServerSessionLifecycleAndPermission(t *testing.T) {
	handler := &lifecycleHandler{}
	client := newTestClientServer(t, handler)

	init, err := client.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if init.ProtocolVersion != protocol.Version || !init.AgentCapabilities.Has("loadSession") {
		t.Fatalf("initialize = %+v, want protocol %d and loadSession", init, protocol.Version)
	}

	sessionID, err := client.NewSession(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	created, cwd, _, _ := handler.sessionValues()
	if sessionID == "" || sessionID != created {
		t.Fatalf("session id = %q, handler saw %q", sessionID, created)
	}
	if cwd != "/repo" {
		t.Fatalf("session cwd = %q, want /repo", cwd)
	}

	listed, err := client.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "archived" {
		t.Fatalf("sessions = %+v", listed.Sessions)
	}

	loaded, err := client.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: sessionID, Cwd: "/repo"})
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	_, _, loadedID, _ := handler.sessionValues()
	if loaded != sessionID || loadedID != sessionID {
		t.Fatalf("loaded = %q, handler saw %q", loaded, loadedID)
	}

	var update protocol.SessionUpdate
	var updateMu sync.Mutex
	client.SetCallbacks(ClientCallbacks{
		OnUpdate: func(value protocol.SessionUpdate) {
			updateMu.Lock()
			update = value
			updateMu.Unlock()
		},
		OnPermission: func(context.Context, protocol.PermissionRequest) (string, error) {
			return "allow", nil
		},
	})
	stop, err := client.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("run it")})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if stop != protocol.StopEndTurn {
		t.Fatalf("stop reason = %q, want %q", stop, protocol.StopEndTurn)
	}
	updateMu.Lock()
	defer updateMu.Unlock()
	if update.SessionID != sessionID || update.Update.Content == nil || update.Update.Content.Text != "working" {
		t.Fatalf("update = %+v", update)
	}
}

func TestClientCancelStopsPrompt(t *testing.T) {
	handler := &lifecycleHandler{blocking: true, entered: make(chan struct{})}
	client := newTestClientServer(t, handler)
	sessionID, err := client.NewSession(context.Background(), "/repo")
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan struct {
		stop string
		err  error
	}, 1)
	go func() {
		stop, promptErr := client.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("wait")})
		result <- struct {
			stop string
			err  error
		}{stop: stop, err: promptErr}
	}()
	select {
	case <-handler.entered:
	case <-time.After(time.Second):
		t.Fatal("prompt did not enter handler")
	}
	if err := client.Cancel(sessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case outcome := <-result:
		if outcome.err != nil || outcome.stop != protocol.StopCancelled {
			t.Fatalf("prompt outcome = %+v", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not stop prompt")
	}
}

func TestSessionHandleNewAndPromptText(t *testing.T) {
	handler := &lifecycleHandler{}
	client := newTestClientServer(t, handler)
	client.SetCallbacks(ClientCallbacks{
		OnPermission: func(context.Context, protocol.PermissionRequest) (string, error) {
			return "allow", nil
		},
	})

	session, err := client.NewSessionHandle(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NewSessionHandle: %v", err)
	}
	created, cwd, _, _ := handler.sessionValues()
	if session.ID() == "" || session.ID() != created {
		t.Fatalf("session id = %q, handler saw %q", session.ID(), created)
	}
	if cwd != "/repo" {
		t.Fatalf("session cwd = %q, want /repo", cwd)
	}

	stop, err := session.PromptText(context.Background(), "run it")
	if err != nil {
		t.Fatalf("PromptText: %v", err)
	}
	if stop != protocol.StopEndTurn {
		t.Fatalf("stop reason = %q, want %q", stop, protocol.StopEndTurn)
	}
	_, _, _, prompt := handler.sessionValues()
	if prompt != "run it" {
		t.Fatalf("prompt = %q, want run it", prompt)
	}
}

func TestSessionHandleLoad(t *testing.T) {
	handler := &lifecycleHandler{}
	client := newTestClientServer(t, handler)

	session, err := client.LoadSessionHandle(context.Background(), protocol.LoadSessionRequest{
		SessionID: "archived",
		Cwd:       "/repo",
	})
	if err != nil {
		t.Fatalf("LoadSessionHandle: %v", err)
	}
	_, _, loaded, _ := handler.sessionValues()
	if session.ID() != "archived" || loaded != "archived" {
		t.Fatalf("session id = %q, handler loaded %q", session.ID(), loaded)
	}
}

func TestSessionHandleCancel(t *testing.T) {
	handler := &lifecycleHandler{blocking: true, entered: make(chan struct{})}
	client := newTestClientServer(t, handler)
	session, err := client.NewSessionHandle(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NewSessionHandle: %v", err)
	}

	result := make(chan struct {
		stop string
		err  error
	}, 1)
	go func() {
		stop, promptErr := session.PromptText(context.Background(), "wait")
		result <- struct {
			stop string
			err  error
		}{stop: stop, err: promptErr}
	}()
	select {
	case <-handler.entered:
	case <-time.After(time.Second):
		t.Fatal("prompt did not enter handler")
	}
	if err := session.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case outcome := <-result:
		if outcome.err != nil || outcome.stop != protocol.StopCancelled {
			t.Fatalf("prompt outcome = %+v", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not stop prompt")
	}
}
