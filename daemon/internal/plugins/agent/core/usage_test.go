package core

import (
	"sync"
	"testing"
)

func TestUsageTracker_RecordAndSnapshot(t *testing.T) {
	tracker := NewUsageTracker()

	// 第一次调用：模型返回 usage
	tracker.Record(0, 4000, &UsageInfo{PromptTokens: 1200, CompletionTokens: 500, TotalTokens: 1700})
	// 第二次调用：模型没返回 usage（本地模型常见）
	tracker.Record(1, 2300, nil)

	s := tracker.Snapshot()
	if s.PromptTokens != 1200 {
		t.Errorf("PromptTokens = %d, want 1200", s.PromptTokens)
	}
	if s.CompletionTokens != 500 {
		t.Errorf("CompletionTokens = %d, want 500", s.CompletionTokens)
	}
	if s.TotalTokens != 1700 {
		t.Errorf("TotalTokens = %d, want 1700", s.TotalTokens)
	}
	if s.EstimateTokens != 6300 {
		t.Errorf("EstimateTokens = %d, want 6300", s.EstimateTokens)
	}
	// LastPromptTokens 取最近一次：第二次无 usage → 退回估算
	if s.LastPromptTokens != 2300 {
		t.Errorf("LastPromptTokens = %d, want 2300", s.LastPromptTokens)
	}
}

func TestUsageTracker_SnapshotPrefersRealPrompt(t *testing.T) {
	tracker := NewUsageTracker()
	tracker.Record(0, 1000, &UsageInfo{PromptTokens: 900, CompletionTokens: 100, TotalTokens: 1000})
	s := tracker.Snapshot()
	if s.LastPromptTokens != 900 {
		t.Errorf("LastPromptTokens = %d, want 900 (real prompt)", s.LastPromptTokens)
	}
}

func TestUsageTracker_CallsDeepCopy(t *testing.T) {
	tracker := NewUsageTracker()
	tracker.Record(0, 100, &UsageInfo{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15})

	calls := tracker.Calls()
	calls[0].PromptTokens = 999 // 改返回的拷贝

	after := tracker.Calls()
	if after[0].PromptTokens != 10 {
		t.Errorf("Calls not a deep copy: internal mutated to %d", after[0].PromptTokens)
	}
}

func TestUsageTracker_CacheTokensAccumulate(t *testing.T) {
	// 回归：Record 曾漏拷 UsageInfo 的 cache 字段，导致总计里 hit/miss 恒为 0。
	tracker := NewUsageTracker()
	tracker.Record(0, 0, &UsageInfo{PromptTokens: 1000, PromptCacheHitTokens: 600, PromptCacheMissTokens: 400, TotalTokens: 1000})
	tracker.Record(1, 0, &UsageInfo{PromptTokens: 2000, PromptCacheHitTokens: 1500, PromptCacheMissTokens: 500, TotalTokens: 2000})

	s := tracker.Snapshot()
	if s.PromptCacheHitTokens != 2100 {
		t.Errorf("PromptCacheHitTokens = %d, want 2100", s.PromptCacheHitTokens)
	}
	if s.PromptCacheMissTokens != 900 {
		t.Errorf("PromptCacheMissTokens = %d, want 900", s.PromptCacheMissTokens)
	}
	// 明细同样带上 cache 字段
	calls := tracker.Calls()
	if calls[0].PromptCacheHitTokens != 600 || calls[1].PromptCacheMissTokens != 500 {
		t.Errorf("calls missing cache fields: %+v", calls)
	}
}

func TestUsage_CacheHitRate(t *testing.T) {
	tests := []struct {
		name   string
		hit    int
		miss   int
		want   float64
		hasVal bool
	}{
		{name: "model omits cache fields", hit: 0, miss: 0, want: 0, hasVal: false},
		{name: "fully cached", hit: 1000, miss: 0, want: 100, hasVal: true},
		{name: "mixed", hit: 600, miss: 400, want: 60, hasVal: true},
		{name: "low rate not rounded", hit: 1, miss: 999, want: 0.1, hasVal: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rate, ok := (Usage{PromptCacheHitTokens: tt.hit, PromptCacheMissTokens: tt.miss}).CacheHitRate()
			if ok != tt.hasVal {
				t.Fatalf("ok = %v, want %v", ok, tt.hasVal)
			}
			if ok && absDiff(rate, tt.want) > 1e-9 {
				t.Errorf("rate = %v, want %v", rate, tt.want)
			}
		})
	}
}

func absDiff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}

func TestUsageTracker_RecordCompaction(t *testing.T) {
	tracker := NewUsageTracker()
	tracker.RecordCompaction(Compaction{BeforeMsgs: 42, AfterMsgs: 13, Tokens: 1200})
	tracker.RecordCompaction(Compaction{BeforeMsgs: 30, AfterMsgs: 10, Tokens: 800})

	comps := tracker.Compactions()
	if len(comps) != 2 {
		t.Fatalf("Compactions = %d, want 2", len(comps))
	}
	if comps[0].BeforeMsgs != 42 || comps[0].AfterMsgs != 13 || comps[0].Tokens != 1200 {
		t.Errorf("comps[0] = %+v", comps[0])
	}
	// 深拷贝：改返回值不影响内部
	comps[0].BeforeMsgs = 999
	if got := tracker.Compactions()[0].BeforeMsgs; got != 42 {
		t.Errorf("Compactions not a deep copy: internal mutated to %d", got)
	}

	// Reset 清空压缩记录
	tracker.Reset()
	if len(tracker.Compactions()) != 0 {
		t.Error("Reset left compactions")
	}
}

func TestUsageTracker_Reset(t *testing.T) {
	tracker := NewUsageTracker()
	tracker.Record(0, 100, &UsageInfo{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15})
	tracker.Reset()

	s := tracker.Snapshot()
	if s.TotalTokens != 0 || s.PromptTokens != 0 || s.EstimateTokens != 0 {
		t.Errorf("Reset left residue: %+v", s)
	}
	if len(tracker.Calls()) != 0 {
		t.Errorf("Reset left calls: %d", len(tracker.Calls()))
	}
}

func TestUsageTracker_EmptySnapshot(t *testing.T) {
	tracker := NewUsageTracker()
	s := tracker.Snapshot()
	if s.LastPromptTokens != 0 {
		t.Errorf("empty LastPromptTokens = %d, want 0", s.LastPromptTokens)
	}
}

// 并发：Record（写）与 Snapshot/Calls（读）在不同 goroutine，-race 验证无竞争。
func TestUsageTracker_Concurrent(t *testing.T) {
	tracker := NewUsageTracker()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			tracker.Record(i, i*10, &UsageInfo{PromptTokens: i, CompletionTokens: i, TotalTokens: 2 * i})
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = tracker.Snapshot()
			_ = tracker.Calls()
		}
	}()

	wg.Wait()
	// 100 次调用，TotalTokens 累加 = 2*(0+1+...+99) = 9900
	s := tracker.Snapshot()
	if want := 2 * 99 * 100 / 2; s.TotalTokens != want {
		t.Errorf("TotalTokens = %d, want %d", s.TotalTokens, want)
	}
}
