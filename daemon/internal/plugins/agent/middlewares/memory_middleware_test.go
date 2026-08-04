package middlewares

import (
	"context"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/memory"
)

func memoryCfg() config.MemoryConfig {
	return config.Default().Memory
}

func seededStore(t *testing.T) *memory.FileStore {
	t.Helper()
	s, err := memory.NewFileStore(t.TempDir(), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.UpsertTask(&memory.Task{Title: "fix config parser", Status: "open", Summary: "yaml unmarshal errors"})
	s.AddFact(&memory.Fact{Content: "project uses yaml config", Topic: "config", Source: "user"})
	return s
}

func TestMemoryMiddleware_InjectsOnlyOnSessionBoundary(t *testing.T) {
	store := seededStore(t)
	// 会话边界（inject=true）：注入
	mw := NewMemoryMiddleware(store, memoryCfg(), 32768, true)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "how config works?"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw.OnBeforeModel(ev)
	if len(ev.History) != len(history)+1 {
		t.Fatalf("session boundary: history = %d, want %d", len(ev.History), len(history)+1)
	}

	// 非边界（inject=false）：不注入
	mw2 := NewMemoryMiddleware(store, memoryCfg(), 32768, false)
	history2 := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "hi"}}
	ev2 := &core.BeforeModelEvent{Ctx: context.Background(), Input: "hi", History: history2}
	mw2.OnBeforeModel(ev2)
	if len(ev2.History) != len(history2) {
		t.Errorf("non-boundary run should not inject: %d", len(ev2.History))
	}
}

func TestMemoryMiddleware_InjectsOncePerRun(t *testing.T) {
	store := seededStore(t)
	mw := NewMemoryMiddleware(store, memoryCfg(), 32768, true)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "config"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw.OnBeforeModel(ev)
	before := len(ev.History)
	mw.OnBeforeModel(ev) // ReAct 第二轮：injected 已置位，no-op
	if len(ev.History) != before {
		t.Errorf("second inject changed history: %d → %d", before, len(ev.History))
	}
}

func TestMemoryMiddleware_InsertsAfterSystemPrompt(t *testing.T) {
	store := seededStore(t)
	mw := NewMemoryMiddleware(store, memoryCfg(), 32768, true)
	history := []core.Message{
		{Role: "system", Content: "agent prompt"},
		{Role: "system", Content: "another system block"},
		{Role: "user", Content: "hi"},
	}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw.OnBeforeModel(ev)

	if ev.History[0].Content != "agent prompt" || ev.History[1].Content != "another system block" {
		t.Errorf("leading system messages changed: %+v", ev.History[:2])
	}
	block := ev.History[2]
	if block.Role != "system" || !strings.Contains(block.Content, "[记忆]") {
		t.Errorf("History[2] = %+v, want memory block", block)
	}
	if !strings.Contains(block.Content, "fix config parser") {
		t.Errorf("memory block missing open task: %q", block.Content)
	}
	if !strings.Contains(block.Content, "project uses yaml config") {
		t.Errorf("memory block missing fact: %q", block.Content)
	}
	if ev.History[3].Role != "user" {
		t.Errorf("History[3] = %+v, want original user msg", ev.History[3])
	}
}

func TestMemoryMiddleware_DisabledOrNilStore(t *testing.T) {
	store := seededStore(t)

	cfg := memoryCfg()
	cfg.Enabled = false
	mw := NewMemoryMiddleware(store, cfg, 32768, true)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "config"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw.OnBeforeModel(ev)
	if len(ev.History) != len(history) {
		t.Error("disabled should not inject")
	}

	mw = NewMemoryMiddleware(nil, memoryCfg(), 32768, true)
	mw.OnBeforeModel(ev)
	if len(ev.History) != len(history) {
		t.Error("nil store should not inject")
	}
}

func TestMemoryMiddleware_EmptyStoreNoInject(t *testing.T) {
	store, _ := memory.NewFileStore(t.TempDir(), 10)
	defer store.Close()
	mw := NewMemoryMiddleware(store, memoryCfg(), 32768, true)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "anything"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "anything", History: history}
	mw.OnBeforeModel(ev)
	if len(ev.History) != len(history) {
		t.Error("empty store should not inject")
	}
}

func TestMemoryMiddleware_TinyBudgetDoesNotPanic(t *testing.T) {
	store := seededStore(t)
	// window=100 → budget=15，注入块必然超预算，走裁剪链直到放弃
	mw := NewMemoryMiddleware(store, memoryCfg(), 100, true)
	history := []core.Message{{Role: "system", Content: "agent prompt"}, {Role: "user", Content: "config"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), Input: "config", History: history}
	mw.OnBeforeModel(ev) // 不 panic 即可；放弃注入或尽力注入都算通过
}
