package middlewares

import (
	"context"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/memory"
)

func memoryCfg() config.MemoryConfig {
	return config.Default().Memory
}

func seededClient(t *testing.T) *memory.Client {
	t.Helper()
	c, err := memory.NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.UpsertTask(&memory.Task{Title: "fix config parser", Status: "open", Summary: "yaml unmarshal errors"})
	c.AddFact(&memory.Fact{Content: "project uses yaml config", Topic: "config", Source: "user"})
	return c
}

func TestMemoryMiddleware_InjectsOnEveryQuery(t *testing.T) {
	// 每次新实例（= 每次用户 query）都注入，不再有会话边界概念
	client := seededClient(t)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "how config works?"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}

	mw1 := NewMemoryMiddleware(client, memoryCfg(), 32768)
	mw1.OnBeforeModel(ev)
	if len(ev.History) != len(history)+1 {
		t.Fatalf("first query: history = %d, want %d", len(ev.History), len(history)+1)
	}

	// 另一个 query（新实例）：仍然注入
	ev2 := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw2 := NewMemoryMiddleware(client, memoryCfg(), 32768)
	mw2.OnBeforeModel(ev2)
	if len(ev2.History) != len(history)+1 {
		t.Errorf("second query should inject too: %d", len(ev2.History))
	}
}

func TestMemoryMiddleware_InjectsOncePerRun(t *testing.T) {
	client := seededClient(t)
	mw := NewMemoryMiddleware(client, memoryCfg(), 32768)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "config"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw.OnBeforeModel(ev)
	before := len(ev.History)
	mw.OnBeforeModel(ev) // ReAct 第二轮：injected 已置位，no-op
	if len(ev.History) != before {
		t.Errorf("second inject changed history: %d → %d", before, len(ev.History))
	}
}

func TestMemoryMiddleware_InsertsBeforeLastUser(t *testing.T) {
	client := seededClient(t)
	mw := NewMemoryMiddleware(client, memoryCfg(), 32768)
	history := []core.Message{
		{Role: "system", Content: "agent prompt"},
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "hello!"},
		{Role: "user", Content: "current input"}, // 本轮输入（buildMessages 追加的最后一条 user）
	}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw.OnBeforeModel(ev)

	// 记忆块插在最后一个 user（本轮输入）之前：前面整段对话序列原样保留，
	// 前缀缓存不被记忆块打断（记忆块每次检索结果都变，放末尾只 miss 自己）
	if ev.History[0].Content != "agent prompt" || ev.History[1].Role != "user" || ev.History[2].Role != "assistant" {
		t.Errorf("original message sequence changed: %+v", ev.History[:3])
	}
	block := ev.History[3]
	if block.Role != "system" || !strings.Contains(block.Content, "[记忆]") {
		t.Errorf("History[3] = %+v, want memory block", block)
	}
	if !strings.Contains(block.Content, "fix config parser") {
		t.Errorf("memory block missing open task: %q", block.Content)
	}
	if !strings.Contains(block.Content, "project uses yaml config") {
		t.Errorf("memory block missing fact: %q", block.Content)
	}
	if ev.History[4].Role != "user" || ev.History[4].Content != "current input" {
		t.Errorf("History[4] = %+v, want current input user msg", ev.History[4])
	}
}

func TestMemoryMiddleware_DisabledOrNilStore(t *testing.T) {
	client := seededClient(t)

	cfg := memoryCfg()
	cfg.Enabled = false
	mw := NewMemoryMiddleware(client, cfg, 32768)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "config"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw.OnBeforeModel(ev)
	if len(ev.History) != len(history) {
		t.Error("disabled should not inject")
	}

	mw = NewMemoryMiddleware(nil, memoryCfg(), 32768)
	mw.OnBeforeModel(ev)
	if len(ev.History) != len(history) {
		t.Error("nil client should not inject")
	}
}

func TestMemoryMiddleware_EmptyStoreNoInject(t *testing.T) {
	client, _ := memory.NewClient(t.TempDir(), 10)
	defer client.Close()
	mw := NewMemoryMiddleware(client, memoryCfg(), 32768)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "anything"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "anything", History: history}
	mw.OnBeforeModel(ev)
	if len(ev.History) != len(history) {
		t.Error("empty store should not inject")
	}
}

func TestMemoryMiddleware_TinyBudgetDoesNotPanic(t *testing.T) {
	client := seededClient(t)
	// window=100 → budget=15，注入块必然超预算，走裁剪链直到放弃
	mw := NewMemoryMiddleware(client, memoryCfg(), 100)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "config"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw.OnBeforeModel(ev) // 不 panic 即可；放弃注入或尽力注入都算通过
}
