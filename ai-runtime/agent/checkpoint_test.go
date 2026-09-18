package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

// newCheckpointSession 构造一个指向 mock LLM 的会话，返回 (session, mock 响应 JSON 设置器)。
func newCheckpointSession(t *testing.T, response string) *Session {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(core.ChatResponse{
			Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: response}}},
		})
	}))
	t.Cleanup(srv.Close)

	cfg := runtimeconfig.Default()
	provider := cfg.LLM.Providers["openai"]
	provider.Endpoint = srv.URL
	cfg.LLM.Providers["openai"] = provider
	ms, err := memory.NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { ms.Close() })

	return NewSession(SessionDeps{
		Config:       cfg,
		AuditDir:     "",
		Memory:       ms,
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return runtimeopenai.NewProvider("mock", srv.URL, "", "mock", srv.Client()), nil
		},
	})
}

// phaseRecorder 收集 CheckpointSync 发出的阶段事件与 Write 输出。
type phaseRecorder struct {
	stages []string
	output string
}

func (r *phaseRecorder) callbacks() RunCallbacks {
	return RunCallbacks{
		Write: func(s string) { r.output += s },
		Publish: func(event string, data any) {
			if event != runtimeconfig.EventPhase {
				return
			}
			if ev, ok := data.(runtimeconfig.PhaseEvent); ok && ev.Kind == runtimeconfig.PhaseStage {
				r.stages = append(r.stages, ev.Label)
			}
		},
	}
}

// TestCheckpointSync_PublishesStageAndNotice 同步固化发阶段事件并回显固化结果。
func TestCheckpointSync_PublishesStageAndNotice(t *testing.T) {
	s := newCheckpointSession(t, `{"tasks":[{"title":"fix config","summary_delta":"moved to yaml"}],"decisions":[]}`)
	s.conversation = []core.Message{{Role: "user", Content: "把配置迁到 yaml"}}

	rec := &phaseRecorder{}
	s.CheckpointSync(context.Background(), s.conversation, rec.callbacks())

	if len(rec.stages) != 1 || rec.stages[0] != "固化旧会话记忆" {
		t.Errorf("stages = %v, want [固化旧会话记忆]", rec.stages)
	}
	if !strings.Contains(rec.output, "Memory updated: 1 task(s)") {
		t.Errorf("output = %q, want memory updated notice", rec.output)
	}
}

// TestCheckpointSync_NothingNew 有内容但 LLM 判定无可存时，仍要回完成信号。
func TestCheckpointSync_NothingNew(t *testing.T) {
	s := newCheckpointSession(t, `{"tasks":[],"decisions":[]}`)
	s.conversation = []core.Message{{Role: "user", Content: "hi"}}

	rec := &phaseRecorder{}
	s.CheckpointSync(context.Background(), s.conversation, rec.callbacks())

	if !strings.Contains(rec.output, "nothing new") {
		t.Errorf("output = %q, want nothing-new signal", rec.output)
	}
}

// TestCheckpointSync_SkipsWhenNoop memory 缺失 / 空会话直接返回，不发事件不输出。
func TestCheckpointSync_SkipsWhenNoop(t *testing.T) {
	s := newCheckpointSession(t, `{}`)
	for name, conv := range map[string][]core.Message{
		"empty conversation": nil,
	} {
		rec := &phaseRecorder{}
		s.CheckpointSync(context.Background(), conv, rec.callbacks())
		if rec.stages != nil || rec.output != "" {
			t.Errorf("%s: stages = %v, output = %q, want silent", name, rec.stages, rec.output)
		}
	}

	// memory 禁用：同样静默
	noMem := NewSession(SessionDeps{
		Config:       runtimeconfig.Default(),
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return nil, fmt.Errorf("should not be called")
		},
	})
	rec := &phaseRecorder{}
	noMem.CheckpointSync(context.Background(), []core.Message{{Role: "user", Content: "hi"}}, rec.callbacks())
	if rec.stages != nil || rec.output != "" {
		t.Errorf("memory disabled: stages = %v, output = %q, want silent", rec.stages, rec.output)
	}
}

// TestCheckpointSync_CanceledContext ctx 取消（用户 Esc）时回显警告，不 panic。
func TestCheckpointSync_CanceledContext(t *testing.T) {
	s := newCheckpointSession(t, `{}`)
	s.conversation = []core.Message{{Role: "user", Content: "hi"}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rec := &phaseRecorder{}
	s.CheckpointSync(ctx, s.conversation, rec.callbacks())

	if !strings.Contains(rec.output, "Consolidation failed") {
		t.Errorf("output = %q, want failure warning", rec.output)
	}
}
