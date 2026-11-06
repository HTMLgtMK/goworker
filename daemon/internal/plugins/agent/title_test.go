package service

import (
	"context"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

func TestNormalizeGeneratedTitle(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"  \"Fix the login flow\"\nextra", "Fix the login fl"},
		{"Title:  处理登录问题  ", "处理登录问题"},
		{"一二三四五六七八九十一二三四五六七八九十", "一二三四五六七八九十一二三四五六"},
		{"   ", ""},
	}
	for _, tc := range cases {
		if got := normalizeGeneratedTitle(tc.input); got != tc.want {
			t.Errorf("normalizeGeneratedTitle(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// titleProvider 是标题 job 的固定响应 provider；不触真实网络。
type titleProvider struct{}

func (titleProvider) Name() string  { return "title-test" }
func (titleProvider) Model() string { return "title-model" }
func (titleProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{
		Role: "assistant", Content: "标题生成成功",
	}}}}, nil
}
func (titleProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, context.Canceled
}

func TestFirstCommitTitlePersistsAndNotifies(t *testing.T) {
	root := t.TempDir()
	repo, err := session.OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	st, err := repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer repo.CloseSession(st.SessionID())
	cfg := runtimeconfig.Default()
	hub := &model.Hub{Config: cfg}
	p := &AgentPlugin{cfg: cfg, hub: hub, deps: runtimeagent.SessionDeps{
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) { return titleProvider{}, nil },
	}}
	done := make(chan string, 1)
	p.makeFirstCommitTitle(st)("Fix the login flow", func(title string) { done <- title })
	select {
	case got := <-done:
		if got != "标题生成成功" {
			t.Fatalf("notify title = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("title generation did not notify")
	}
	if got := st.Title(); got != "标题生成成功" {
		t.Fatalf("stored title = %q", got)
	}
}
