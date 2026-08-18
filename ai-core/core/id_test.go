package core

import (
	"sync"
	"testing"
)

func TestNewMsgID_Unique(t *testing.T) {
	// 并发生成不应重复。
	const n = 10000
	ids := make(map[string]struct{}, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(4)
	for range 4 {
		go func() {
			defer wg.Done()
			local := make([]string, n/4)
			for j := range local {
				local[j] = NewMsgID()
			}
			mu.Lock()
			for _, id := range local {
				ids[id] = struct{}{}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(ids) != n {
		t.Errorf("generated %d ids but only %d unique", n, len(ids))
	}
	// 格式校验：m_ + 8 hex
	for id := range ids {
		if len(id) != 2+8 {
			t.Errorf("id %q length = %d, want 10", id, len(id))
		}
		if id[0] != 'm' || id[1] != '_' {
			t.Errorf("id %q should start with m_", id)
		}
	}
}

func TestNewMsgID_NoPanic(t *testing.T) {
	// 循环大量生成确保回退路径不会 panic。
	for range 10000 {
		id := NewMsgID()
		if id == "" {
			t.Fatal("NewMsgID returned empty string")
		}
	}
}
