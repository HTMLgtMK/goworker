package agent

import (
	"context"
	"sync"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/session"
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
				local[j] = session.NewMsgID()
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
		id := session.NewMsgID()
		if id == "" {
			t.Fatal("NewMsgID returned empty string")
		}
	}
}

func TestStampMessageIDs_FillEmpty(t *testing.T) {
	msgs := []core.Message{
		{Role: "system", Content: "sys"},
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "resp"},
	}
	out := stampMessageIDs(msgs)
	if len(out) != len(msgs) {
		t.Fatalf("len = %d, want %d", len(out), len(msgs))
	}
	ids := make(map[string]struct{})
	for i, m := range out {
		if m.MsgID == "" {
			t.Errorf("msg[%d] MsgID is empty after stamping", i)
		}
		if m.CreatedAt.IsZero() {
			t.Errorf("msg[%d] CreatedAt is zero after stamping", i)
		}
		if _, dup := ids[m.MsgID]; dup {
			t.Errorf("msg[%d] has duplicate MsgID %q", i, m.MsgID)
		}
		ids[m.MsgID] = struct{}{}
	}
	// 原 slice 不应被修改。
	for i, m := range msgs {
		if m.MsgID != "" {
			t.Errorf("original msg[%d] was mutated (MsgID = %q)", i, m.MsgID)
		}
	}
}

func TestStampMessageIDs_PreserveExisting(t *testing.T) {
	msgs := []core.Message{
		{Role: "user", Content: "a", MsgID: "m_existing"},
		{Role: "user", Content: "b"},
		{Role: "user", Content: "c", MsgID: "m_keepme"},
	}
	out := stampMessageIDs(msgs)
	if out[0].MsgID != "m_existing" {
		t.Errorf("msg[0] MsgID = %q, want m_existing", out[0].MsgID)
	}
	if out[1].MsgID == "" {
		t.Error("msg[1] MsgID should be filled")
	}
	if out[2].MsgID != "m_keepme" {
		t.Errorf("msg[2] MsgID = %q, want m_keepme", out[2].MsgID)
	}
}

func TestStampMessageIDs_Idempotent(t *testing.T) {
	msgs := []core.Message{
		{Role: "user", Content: "x"},
		{Role: "assistant", Content: "y", MsgID: "m_already"},
	}
	first := stampMessageIDs(msgs)
	second := stampMessageIDs(first)
	if len(first) != len(second) {
		t.Fatal("lengths differ between first and second pass")
	}
	for i := range first {
		if first[i].MsgID != second[i].MsgID {
			t.Errorf("msg[%d] MsgID changed: %q -> %q", i, first[i].MsgID, second[i].MsgID)
		}
		if !first[i].CreatedAt.Equal(second[i].CreatedAt) {
			t.Errorf("msg[%d] CreatedAt changed on second pass", i)
		}
	}
}

// toolThenFinalProvider 首次调用返回 tool call，第二次返回 final 答复。
// 用于验证工具消息+最终消息都被盖章。
type toolThenFinalProvider struct {
	calls int
}

func (p *toolThenFinalProvider) Name() string  { return "toolfinal" }
func (p *toolThenFinalProvider) Model() string { return "m" }
func (p *toolThenFinalProvider) Chat(_ context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.calls++
	if p.calls == 1 {
		return &core.ChatResponse{Choices: []core.ResponseChoice{{
			Message: core.Message{
				Role: "assistant",
				ToolCalls: []core.ToolCall{{
					ID: "t1", Type: "function",
					Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"echo ok"}`},
				}},
			},
		}}}, nil
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{
		Message: core.Message{Role: "assistant", Content: "final"},
	}}}, nil
}
func (p *toolThenFinalProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, nil
}

func TestAgentRun_MessagesAllStamped(t *testing.T) {
	a := NewAgent(&toolThenFinalProvider{}, "", DefaultTools(nil), nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	for tok := range tokenCh {
		if tok.Done {
			break
		}
	}
	msgs := <-msgCh
	if len(msgs) == 0 {
		t.Fatal("no messages returned")
	}
	ids := make(map[string]struct{}, len(msgs))
	for i, m := range msgs {
		if m.MsgID == "" {
			t.Errorf("msg[%d] (role=%q) has empty MsgID after Run", i, m.Role)
		}
		if m.CreatedAt.IsZero() {
			t.Errorf("msg[%d] (role=%q) has zero CreatedAt after Run", i, m.Role)
		}
		if _, dup := ids[m.MsgID]; dup {
			t.Errorf("msg[%d] (role=%q) duplicate MsgID %q", i, m.Role, m.MsgID)
		}
		ids[m.MsgID] = struct{}{}
	}
	// tool 消息也应该被盖章。
	var hasTool bool
	for _, m := range msgs {
		if m.Role == "tool" {
			hasTool = true
			break
		}
	}
	if !hasTool {
		t.Error("expected at least one tool message in history")
	}
}

func TestAgentRun_ProviderErrorMessagesStamped(t *testing.T) {
	a := NewAgent(errProvider{}, "", nil, nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "hi")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	for tok := range tokenCh {
		if tok.Done {
			break
		}
	}
	msgs := <-msgCh
	// 错误场景下所有消息也被盖章（包括 emitAgentNote 注入的 assistant 错误消息）。
	ids := make(map[string]struct{}, len(msgs))
	for i, m := range msgs {
		if m.MsgID == "" {
			t.Errorf("msg[%d] (role=%q) has empty MsgID after error Run", i, m.Role)
		}
		if _, dup := ids[m.MsgID]; dup {
			t.Errorf("msg[%d] (role=%q) duplicate MsgID %q", i, m.Role, m.MsgID)
		}
		ids[m.MsgID] = struct{}{}
	}
}
