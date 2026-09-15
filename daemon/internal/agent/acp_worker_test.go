package agent

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

// cannedProvider 返回固定回复的 LLM provider，用于无 LLM 端点的 worker 测试。
type cannedProvider struct {
	mu    sync.Mutex
	calls int
}

func (p *cannedProvider) Name() string  { return "canned" }
func (p *cannedProvider) Model() string { return "mock" }
func (p *cannedProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return &core.ChatResponse{Choices: []core.ResponseChoice{{
		Message: core.Message{Role: "assistant", Content: "worker reply"},
	}}}, nil
}
func (p *cannedProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errNotStreamed{}
}

type errNotStreamed struct{}

func (errNotStreamed) Error() string { return "not implemented" }

func TestACPWorker_ServesSessionOverACP(t *testing.T) {
	clientEnd, agentEnd := net.Pipe()

	runtimeCfg := &runtimeconfig.Config{
		Dispatch: runtimeconfig.DispatchConfig{},
		LLM:      runtimeconfig.DefaultLLM(),
	}
	server := ServeACPWorker(agentEnd, ACPWorkerDeps{Config: runtimeCfg})
	t.Cleanup(func() { _ = server.Close() })

	// fake ACP client
	conn := protocol.NewConn(clientEnd)
	var mu sync.Mutex
	var chunks []string
	updates := make(chan struct{}, 1)
	conn.HandleNotification(protocol.MethodSessionUpdate, func(params json.RawMessage) {
		var u protocol.SessionUpdate
		if err := json.Unmarshal(params, &u); err != nil {
			return
		}
		if u.Update.Content != nil && strings.Contains(u.Update.Content.Text, "agent error") {
			mu.Lock()
			chunks = append(chunks, u.Update.Content.Text)
			mu.Unlock()
			select {
			case updates <- struct{}{}:
			default:
			}
		}
	})
	go func() { _ = conn.Serve() }()
	t.Cleanup(func() { _ = conn.Close() })

	var initResp protocol.InitializeResponse
	if err := conn.Call(context.Background(), protocol.MethodInitialize,
		protocol.InitializeRequest{ProtocolVersion: protocol.Version}, &initResp); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	var newResp protocol.NewSessionResponse
	if err := conn.Call(context.Background(), protocol.MethodSessionNew,
		protocol.NewSessionRequest{Cwd: "/tmp"}, &newResp); err != nil {
		t.Fatalf("session/new: %v", err)
	}

	// 默认 LLM 端点不可达 → Session.Run 的错误经 cb.Write 回流为
	// agent_message_chunk（worker 协议行为验证点：错误不吞、不崩连接）
	var promptResp protocol.PromptResponse
	err := conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: newResp.SessionID,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("hello")},
	}, &promptResp)
	if err != nil {
		t.Fatalf("session/prompt: %v", err)
	}
	if promptResp.StopReason != protocol.StopEndTurn {
		t.Errorf("stopReason = %q", promptResp.StopReason)
	}
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for provider error update")
	}

	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(chunks, "")
	if !strings.Contains(joined, "agent error") {
		t.Errorf("chunks = %v, want provider error surfaced to client", chunks)
	}
}

// TestACPWorker_UnknownSession 测试未开 session 直接 prompt 的错误回传。
func TestACPWorker_UnknownSession(t *testing.T) {
	clientEnd, agentEnd := net.Pipe()
	server := ServeACPWorker(agentEnd, ACPWorkerDeps{Config: &runtimeconfig.Config{LLM: runtimeconfig.DefaultLLM()}})
	t.Cleanup(func() { _ = server.Close() })

	conn := protocol.NewConn(clientEnd)
	go func() { _ = conn.Serve() }()
	t.Cleanup(func() { _ = conn.Close() })

	err := conn.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: "sess_missing",
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("hi")},
	}, nil)
	var rpcErr protocol.RPCError
	if err == nil || !strings.Contains(err.Error(), "unknown session") {
		t.Fatalf("err = %v, want unknown session RPC error", err)
	}
	if !errorsAsRPC(err, &rpcErr) {
		t.Fatalf("err = %v, want *RPCError", err)
	}
}

func errorsAsRPC(err error, target *protocol.RPCError) bool {
	if e, ok := err.(*protocol.RPCError); ok {
		*target = *e
		return true
	}
	return false
}

func TestTokenToUpdate_PreservesToolCallCorrelation(t *testing.T) {
	call := TokenToUpdate(core.Token{
		Type:     core.TokenTypeToolCall,
		Content:  "bash(\"echo ok\")",
		ToolCall: core.ToolCall{ID: "call_1"},
	})
	result := TokenToUpdate(core.Token{
		Type: core.TokenTypeToolResult, Content: "ok", ToolCallID: "call_1",
	})

	if call.ToolCallID != "call_1" {
		t.Errorf("tool call ID = %q, want call_1", call.ToolCallID)
	}
	if result.ToolCallID != call.ToolCallID {
		t.Errorf("tool result ID = %q, want %q", result.ToolCallID, call.ToolCallID)
	}
}

// Compile-time alignment: every user-visible core token maps to an ACP update.
func TestTokenToUpdate_Types(t *testing.T) {
	cases := map[string]string{
		core.TokenTypeText:       protocol.UpdateAgentMessageChunk,
		core.TokenTypeThinking:   protocol.UpdateAgentThoughtChunk,
		core.TokenTypeToolCall:   protocol.UpdateToolCall,
		core.TokenTypeToolResult: protocol.UpdateToolCallUpdate,
	}
	for tokenType, want := range cases {
		body := TokenToUpdate(core.Token{Type: tokenType, Content: "x"})
		if body.SessionUpdate != want {
			t.Errorf("token type %s → %q, want %q", tokenType, body.SessionUpdate, want)
		}
	}
}
