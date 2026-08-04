package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
)

// stubProvider 记录请求并返回固定响应，供抽取/检查点测试复用。
type stubProvider struct {
	lastReq *core.ChatRequest
	resp    string
	err     error
}

func (p *stubProvider) Name() string  { return "stub" }
func (p *stubProvider) Model() string { return "stub-model" }
func (p *stubProvider) Chat(_ context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	p.lastReq = req
	if p.err != nil {
		return nil, p.err
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: p.resp}}}}, nil
}
func (p *stubProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

func TestApplyDecisions_FullFlow(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	// 已有两条，供 update/delete 引用
	s.AddFact(&Fact{ID: "f_old", Content: "old fact", Topic: "t", Source: "checkpoint:cp-0"})
	s.AddFact(&Fact{ID: "f_gone", Content: "to be deleted", Topic: "t", Source: "checkpoint:cp-0"})
	current, _ := s.ListFacts(10)

	decs := []Decision{
		{Action: DecisionAdd, Content: "new fact", Topic: "n"},
		{Action: DecisionAdd, Content: "old fact", Topic: "t"}, // 与已有重复 → 跳过
		{Action: DecisionAdd, Content: "   ", Topic: "t"},      // 空内容 → 跳过
		{Action: DecisionUpdate, ID: "f_old", Content: "refined fact", Topic: "t2"},
		{Action: DecisionUpdate, ID: "f_nonexistent", Content: "ghost"}, // 未知 id → 忽略
		{Action: DecisionDelete, ID: "f_gone"},
		{Action: DecisionNoop, Content: "nothing"},
	}
	applied, err := ApplyDecisions(s, decs, current, "checkpoint:cp-1")
	if err != nil {
		t.Fatalf("ApplyDecisions: %v", err)
	}
	if applied != 3 { // add + update + delete
		t.Errorf("applied = %d, want 3", applied)
	}
	facts, _ := s.ListFacts(10)
	if len(facts) != 2 { // f_old 更新后仍在，f_gone 删了，new fact 加入
		t.Fatalf("facts = %d, want 2 (%+v)", len(facts), facts)
	}
	var updated *Fact
	for i := range facts {
		if facts[i].ID == "f_old" {
			updated = &facts[i]
		}
	}
	if updated == nil || updated.Content != "refined fact" || updated.Topic != "t2" {
		t.Errorf("f_old not updated: %+v", facts)
	}
	// 新加的事实 Source 带上检查点前缀
	for _, f := range facts {
		if f.Content == "new fact" && f.Source != "checkpoint:cp-1" {
			t.Errorf("new fact Source = %q, want checkpoint:cp-1", f.Source)
		}
	}
}
