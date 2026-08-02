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
