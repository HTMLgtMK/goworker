package agent

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
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
	conn.HandleNotification(protocol.MethodSessionUpdate, func(params json.RawMessage) {
		var u protocol.SessionUpdate
		if err := json.Unmarshal(params, &u); err != nil {
			return
		}
		if u.Update.Content != nil && u.Update.Content.Text != "" {
			mu.Lock()
			chunks = append(chunks, u.Update.Content.Text)
			mu.Unlock()
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

// 编译期对齐：RunCallbacks 的 token 映射覆盖所有 RenderKind。
func TestTokenToUpdate_Kinds(t *testing.T) {
	cases := map[runtimeagent.RenderKind]string{
		runtimeagent.KindText:       protocol.UpdateAgentMessageChunk,
		runtimeagent.KindThinking:   protocol.UpdateAgentThoughtChunk,
		runtimeagent.KindToolCall:   protocol.UpdateToolCall,
		runtimeagent.KindToolResult: protocol.UpdateToolCallUpdate,
	}
	for kind, want := range cases {
		body := tokenToUpdate(kind, "x")
		if body.SessionUpdate != want {
			t.Errorf("kind %s → %q, want %q", kind, body.SessionUpdate, want)
		}
	}
}
