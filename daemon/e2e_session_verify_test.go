package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	runtimeopenai "github.com/tinguo/goworker/ai-runtime/provider/openai"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/app/config"
)

// TestE2E_SessionPersistFullChain 端到端验证会话持久化完整链路：
// /agent 两轮 → jsonl 落盘 + 自动 checkpoint → 重启恢复 → rewind → /new 归档。
// mock LLM 返回固定回复，Memory 关闭以专注会话链路。
func TestE2E_SessionPersistFullChain(t *testing.T) {
	// mock LLM：返回固定 assistant 回复
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(core.ChatResponse{
			Choices: []core.ResponseChoice{{
				Message: core.Message{Role: "assistant", Content: "I am the mock reply."},
			}},
		})
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfg := config.Default()
	provider := cfg.LLM.Providers[cfg.LLM.DefaultProvider]
	provider.Endpoint = srv.URL
	cfg.LLM.Providers[cfg.LLM.DefaultProvider] = provider
	cfg.Session.Enabled = true
	cfg.Session.Dir = filepath.Join(dir, "sessions")
	cfg.Memory.Enabled = false // 专注会话链路

	runtimeCfg := cfg.ToRuntime()

	// 真实构造：Open store → 建 Session（与 startSession 一致的路径）
	st, err := session.Open(cfg.Session.Dir)
	if err != nil {
		t.Fatalf("session.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	s := agent.NewSession(agent.SessionDeps{
		Config:       runtimeCfg,
		AuditDir:     filepath.Join(dir, "audit"),
		Memory:       nil,
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return runtimeopenai.NewProvider("mock", srv.URL, "", "mock", srv.Client()), nil
		},
		Store: st,
	})

	jsonl := filepath.Join(cfg.Session.Dir, "current.jsonl")

	// 1. 第一轮 /agent
	var out strings.Builder
	if err := s.Run(context.Background(), agent.RunRequest{Input: "question one"}, e2eRunCallbacks(&out)); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	// jsonl 应有消息行 + 自动 checkpoint
	data, _ := os.ReadFile(jsonl)
	if !strings.Contains(string(data), `"kind":"msg"`) {
		t.Fatalf("jsonl missing msg records:\n%s", string(data))
	}
	if !strings.Contains(string(data), `"kind":"checkpoint"`) {
		t.Fatalf("jsonl missing checkpoint after Run (自动打 checkpoint):\n%s", string(data))
	}

	// 2. 第二轮（累积消息）
	if err := s.Run(context.Background(), agent.RunRequest{Input: "question two"}, e2eRunCallbacks(&out)); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	// 3. 重启恢复：重开 store，ActiveView 应含两轮消息
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	st2, err := session.Open(cfg.Session.Dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	view := st2.ActiveView()
	if len(view) < 2 {
		t.Fatalf("reopen ActiveView too short: %d", len(view))
	}

	// 4. /compact（mock 压缩可能无法安全切分，验证不 panic）
	if err := s.Compact(context.Background(), e2eRunCallbacks(&out)); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// 5. rewind：列出 checkpoint，回溯到第一个
	cks := st2.Checkpoints(10)
	if len(cks) == 0 {
		t.Fatal("expected checkpoints after runs")
	}
	if err := st2.SetHead(cks[0].At); err != nil {
		t.Fatalf("SetHead: %v", err)
	}
	if rewound := st2.ActiveView(); len(rewound) == 0 {
		t.Fatal("rewind restored empty view")
	}

	// 6. /new 归档：archive/ 应有旧文件
	if err := st2.Archive(); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(cfg.Session.Dir, "archive"))
	if err != nil {
		t.Fatalf("archive dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("archive/ should have old session file")
	}
}

func e2eRunCallbacks(buf *strings.Builder) agent.RunCallbacks {
	return agent.RunCallbacks{
		Write: func(s string) { buf.WriteString(s) },
		WriteToken: func(_ agent.RenderKind, content string, _ bool) {
			buf.WriteString(content)
		},
	}
}
