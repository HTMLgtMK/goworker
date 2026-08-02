package middlewares

import (
	"context"
	"errors"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
)

func TestUsageMiddleware_RecordsRealUsage(t *testing.T) {
	tracker := core.NewUsageTracker()
	var last core.Usage
	published := 0
	mw := NewUsageMiddleware(tracker, func(u core.Usage) { last = u; published++ })

	mw.OnAfterModel(&core.AfterModelEvent{
		Ctx:     context.Background(),
		History: []core.Message{{Role: "user", Content: "hi"}},
		Usage:   &core.UsageInfo{PromptTokens: 100, CompletionTokens: 20, TotalTokens: 120},
	})

	// 数据落到注入的 tracker，外部可读
	s := tracker.Snapshot()
	if s.PromptTokens != 100 || s.CompletionTokens != 20 || s.TotalTokens != 120 {
		t.Errorf("Snapshot = %+v, want prompt=100 completion=20 total=120", s)
	}
	if published != 1 {
		t.Errorf("publish called %d times, want 1", published)
	}
	if last.PromptTokens != 100 || last.TotalTokens != 120 {
		t.Errorf("published snapshot = %+v, want prompt=100 total=120", last)
	}
}

func TestUsageMiddleware_EstimatesWhenNoUsage(t *testing.T) {
	tracker := core.NewUsageTracker()
	mw := NewUsageMiddleware(tracker, nil)

	mw.OnAfterModel(&core.AfterModelEvent{
		Ctx:     context.Background(),
		History: []core.Message{{Role: "user", Content: "hello world"},
		},
	})

	s := tracker.Snapshot()
	if s.EstimateTokens == 0 {
		t.Error("expected estimate recorded when model omits usage")
	}
	// LastPromptTokens 无真实值，应退回估算
	if s.LastPromptTokens == 0 {
		t.Error("expected LastPromptTokens to fall back to estimate")
	}
	// 无 usage 时 prompt/completion/total 保持 0
	if s.PromptTokens != 0 || s.TotalTokens != 0 {
		t.Errorf("Snapshot = %+v, want zeros for unreturned usage", s)
	}
}

func TestUsageMiddleware_SkipsFailedCalls(t *testing.T) {
	tracker := core.NewUsageTracker()
	mw := NewUsageMiddleware(tracker, nil)
	mw.OnAfterModel(&core.AfterModelEvent{Ctx: context.Background(), Err: errors.New("boom")})

	if len(tracker.Calls()) != 0 {
		t.Errorf("failed call recorded, calls = %+v", tracker.Calls())
	}
}

func TestUsageMiddleware_IterNumbering(t *testing.T) {
	tracker := core.NewUsageTracker()
	mw := NewUsageMiddleware(tracker, nil)
	mw.OnAfterModel(&core.AfterModelEvent{Ctx: context.Background(), Usage: &core.UsageInfo{TotalTokens: 10}})
	mw.OnAfterModel(&core.AfterModelEvent{Ctx: context.Background(), Usage: &core.UsageInfo{TotalTokens: 20}})

	calls := tracker.Calls()
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	if calls[0].Iteration != 0 || calls[1].Iteration != 1 {
		t.Errorf("iterations = %d,%d want 0,1", calls[0].Iteration, calls[1].Iteration)
	}
}

// 每次 /agent 重建 middleware，iter 自然归零——统计从注册时刻重新开始。
func TestUsageMiddleware_FreshInstanceRestartsIter(t *testing.T) {
	tracker := core.NewUsageTracker()
	first := NewUsageMiddleware(tracker, nil)
	first.OnAfterModel(&core.AfterModelEvent{Ctx: context.Background(), Usage: &core.UsageInfo{TotalTokens: 10}})

	tracker.Reset() // 新一轮会话清零
	second := NewUsageMiddleware(tracker, nil)
	second.OnAfterModel(&core.AfterModelEvent{Ctx: context.Background(), Usage: &core.UsageInfo{TotalTokens: 5}})

	calls := tracker.Calls()
	if len(calls) != 1 || calls[0].Iteration != 0 {
		t.Errorf("after Reset + fresh middleware, calls = %+v, want single iter 0", calls)
	}
}

func TestUsageMiddleware_PublishDisabled(t *testing.T) {
	tracker := core.NewUsageTracker()
	mw := NewUsageMiddleware(tracker, nil) // nil publish
	mw.OnAfterModel(&core.AfterModelEvent{Ctx: context.Background(), Usage: &core.UsageInfo{TotalTokens: 10}})
	if len(tracker.Calls()) != 1 {
		t.Error("usage should still be recorded without publish callback")
	}
}
