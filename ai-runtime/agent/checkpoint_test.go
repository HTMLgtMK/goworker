package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-memory"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	runtimeopenai "github.com/tinguo/goworker/ai-runtime/provider/openai"
	"github.com/tinguo/goworker/ai-sandbox"
)

// TestCheckpoint_FiltersToolMessages 回归测试：conversation 含 tool 消息时，
// 固化请求体不得泄漏 role="tool"（否则 OpenAI 400 missing field tool_call_id）。
func TestCheckpoint_FiltersToolMessages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []struct {
				Role string `json:"role"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		for _, m := range body.Messages {
			if m.Role == "tool" {
				t.Errorf("checkpoint request leaked tool message: %+v", body.Messages)
			}
		}
		json.NewEncoder(w).Encode(core.ChatResponse{
			Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: `{"tasks":[],"decisions":[]}`}}},
		})
	}))
	defer srv.Close()

	cfg := runtimeconfig.Default()
	provider := cfg.LLM.Providers["openai"]
	provider.Endpoint = srv.URL
	cfg.LLM.Providers["openai"] = provider
	ms, err := memory.NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { ms.Close() })

	s := NewSession(SessionDeps{
		Config:       cfg,
		AuditDir:     "",
		Memory:       ms,
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return runtimeopenai.NewProvider("mock", srv.URL, "", "mock", srv.Client()), nil
		},
	})
	s.conversation = []core.Message{
		{Role: "user", Content: "查下磁盘"},
		{Role: "assistant", Content: "", ToolCalls: []core.ToolCall{{ID: "call_1", Type: "function", Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"df -h"}`}}}},
		{Role: "tool", ToolCallID: "call_1", Content: "Filesystem 1.9T 60% used"},
		{Role: "assistant", Content: "磁盘用了 60%"},
	}
	if _, err := s.Consolidate(context.Background()); err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
}
