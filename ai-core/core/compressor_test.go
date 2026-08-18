package core

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// stubProvider 记录请求并返回固定内容，用于 Compressor 测试。
type stubProvider struct {
	lastReq *ChatRequest
	resp    string
	err     error
}

func (p *stubProvider) Name() string  { return "stub" }
func (p *stubProvider) Model() string { return "stub-model" }
func (p *stubProvider) Chat(_ context.Context, req *ChatRequest) (*ChatResponse, error) {
	p.lastReq = req
	if p.err != nil {
		return nil, p.err
	}
	return &ChatResponse{Choices: []ResponseChoice{{Message: Message{Role: "assistant", Content: p.resp}}}}, nil
}
func (p *stubProvider) ChatStream(context.Context, *ChatRequest) (<-chan Token, error) {
	return nil, errors.New("not implemented")
}

func msg(role, content string) Message {
	return Message{Role: role, Content: content}
}

func toolCallMsg(id string) Message {
	return Message{Role: "assistant", Content: "call tool", ToolCalls: []ToolCall{{ID: id, Function: ToolCallFunction{Name: "bash", Arguments: "echo hi"}}}}
}

func toolMsg(id, content string) Message {
	return Message{Role: "tool", Content: content, ToolCallID: id}
}

func TestCompressor_RollsHistory(t *testing.T) {
	// 12 条历史，keepLast=5 → 朴素切点 7（user，安全）→ 只发前 7 条给模型
	var hist []Message
	for i := 0; i < 6; i++ {
		hist = append(hist, msg("user", "q"))
		hist = append(hist, msg("assistant", "a"))
	}

	stub := &stubProvider{resp: "SUMMARY"}
	c := NewCompressor(stub, 5, false)
	out, err := c.Compress(context.Background(), hist)
	if err != nil {
		t.Fatalf("Compress err = %v", err)
	}

	if len(out) != 6 { // 1 摘要 + 5 条原文
		t.Fatalf("out len = %d, want 6", len(out))
	}
	if out[0].Role != "system" || out[0].Content != "SUMMARY" {
		t.Errorf("out[0] = %+v, want system summary", out[0])
	}
	if out[5].Content != "a" {
		t.Errorf("last message content = %q, want last original", out[5].Content)
	}

	// 模型只收到旧段 7 条，且首条是摘要指令
	if stub.lastReq == nil {
		t.Fatal("provider not called")
	}
	if len(stub.lastReq.Messages) != 8 { // system 指令 + 7 条旧段
		t.Errorf("summary req messages = %d, want 8", len(stub.lastReq.Messages))
	}
	if stub.lastReq.Messages[0].Role != "system" {
		t.Errorf("summary req[0] role = %s, want system prompt", stub.lastReq.Messages[0].Role)
	}
}

func TestCompressor_NoOpWhenShort(t *testing.T) {
	hist := []Message{msg("user", "hi"), msg("assistant", "yo")}
	stub := &stubProvider{}
	c := NewCompressor(stub, 10, false)
	out, err := c.Compress(context.Background(), hist)
	if err != nil {
		t.Fatalf("Compress err = %v", err)
	}
	if len(out) != 2 {
		t.Errorf("out len = %d, want unchanged 2", len(out))
	}
	if stub.lastReq != nil {
		t.Error("provider should not be called when history is short")
	}
}

func TestCompressor_BoundaryMovesBackPastToolTail(t *testing.T) {
	// 3 组 tool 配对，keepLast=4，朴素切点 5 落在 tool 上 → 回退到 4（asst(c2)，前一条是 user）→ 安全
	hist := []Message{
		msg("user", "q1"), toolCallMsg("c1"), toolMsg("c1", "r1"),
		msg("user", "q2"), toolCallMsg("c2"), toolMsg("c2", "r2"),
		msg("user", "q3"), toolCallMsg("c3"), toolMsg("c3", "r3"),
	}

	stub := &stubProvider{resp: "SUMMARY"}
	c := NewCompressor(stub, 4, false)
	out, err := c.Compress(context.Background(), hist)
	if err != nil {
		t.Fatalf("Compress err = %v", err)
	}
	// 切在 4：old=前 4 条，recent 原文从 asst(c2) 开始
	if len(out) != 6 {
		t.Fatalf("out len = %d, want 6 (summary + 5 recent)", len(out))
	}
	wantRecent := hist[4:]
	if !reflect.DeepEqual(out[1:], wantRecent) {
		t.Errorf("recent portion mismatch:\n got %+v\nwant %+v", out[1:], wantRecent)
	}
}

func TestCompressor_NoBoundaryNoOp(t *testing.T) {
	// [asst(tool), tool]，keepLast=1：唯一候选切点落在 tool 上，
	// 拆开就把 tool 和它的 asst 分离 → 无安全切点，不压
	hist := []Message{
		toolCallMsg("c1"), toolMsg("c1", "r1"),
	}
	stub := &stubProvider{resp: "S"}
	c := NewCompressor(stub, 1, false)
	out, err := c.Compress(context.Background(), hist)
	if err != nil {
		t.Fatalf("Compress err = %v", err)
	}
	if len(out) != len(hist) {
		t.Errorf("expected no-op when no safe boundary, got %d → %d", len(hist), len(out))
	}
	if stub.lastReq != nil {
		t.Error("provider should not be called without a safe split point")
	}
}

func TestCompressor_KeepsSystemPromptVerbatim(t *testing.T) {
	// 首位 system 是系统提示，压缩后必须原样保留，不能进摘要
	hist := []Message{
		msg("system", "You are a coding assistant with tool access."),
		msg("user", "q1"), msg("assistant", "a1"),
		msg("user", "q2"), msg("assistant", "a2"),
		msg("user", "q3"), msg("assistant", "a3"),
	}
	stub := &stubProvider{resp: "SUMMARY"}
	c := NewCompressor(stub, 2, true)
	out, err := c.Compress(context.Background(), hist)
	if err != nil {
		t.Fatalf("Compress err = %v", err)
	}
	if len(out) != 4 { // system 提示 + 摘要 + 最近 2 条
		t.Fatalf("out len = %d, want 4", len(out))
	}
	if out[0].Content != "You are a coding assistant with tool access." {
		t.Errorf("system prompt lost: out[0] = %+v", out[0])
	}
	if out[1].Role != "system" || out[1].Content != "SUMMARY" {
		t.Errorf("out[1] = %+v, want summary", out[1])
	}
}

func TestCompressor_ProtectsAllLeadingSystemMessages(t *testing.T) {
	// 自动压缩场景：前导 system 是 agent 提示 + memory middleware 注入的记忆块。
	// 两者都必须原样保留、不进摘要 —— 摘要没有 [记忆] 前缀剥不掉，进 STM 会污染固化。
	hist := []Message{
		msg("system", "You are a coding assistant with tool access."),
		msg("system", "[记忆] 来自之前的会话..."),
		msg("user", "q1"), msg("assistant", "a1"),
		msg("user", "q2"), msg("assistant", "a2"),
		msg("user", "q3"), msg("assistant", "a3"),
	}
	stub := &stubProvider{resp: "SUMMARY"}
	c := NewCompressor(stub, 2, true)
	out, err := c.Compress(context.Background(), hist)
	if err != nil {
		t.Fatalf("Compress err = %v", err)
	}
	// 2 个前导 system 原样保留 + 摘要 + 最近 2 条
	if len(out) != 5 {
		t.Fatalf("out len = %d, want 5", len(out))
	}
	if out[0].Content != "You are a coding assistant with tool access." {
		t.Errorf("agent prompt lost: out[0] = %+v", out[0])
	}
	if out[1].Content != "[记忆] 来自之前的会话..." {
		t.Errorf("memory block lost: out[1] = %+v", out[1])
	}
	if out[2].Role != "system" || out[2].Content != "SUMMARY" {
		t.Errorf("out[2] = %+v, want summary", out[2])
	}
}

func TestCompressor_ReRollsLeadingSummaryWhenNotProtected(t *testing.T) {
	// /compact 场景：p.conversation[0] 是上次压缩留下的摘要（system 角色），
	// protectSystem=false 时它必须被再次滚动，不能原样冻结 —— 否则摘要一条条累积。
	hist := []Message{
		msg("system", "S1 上次的摘要"),
		msg("user", "q1"), msg("assistant", "a1"),
		msg("user", "q2"), msg("assistant", "a2"),
	}
	stub := &stubProvider{resp: "S2 合并后的摘要"}
	c := NewCompressor(stub, 1, false)
	out, err := c.Compress(context.Background(), hist)
	if err != nil {
		t.Fatalf("Compress err = %v", err)
	}
	// S1 和 q1 被压进 S2，只剩 S2 + 最近 1 条
	if len(out) != 2 {
		t.Fatalf("out len = %d, want 2 (new summary + last message)", len(out))
	}
	if out[0].Content != "S2 合并后的摘要" {
		t.Errorf("out[0] = %+v, want re-rolled summary", out[0])
	}
	if out[1].Content != "a2" {
		t.Errorf("out[1] = %+v, want last original", out[1])
	}
}

func TestCompressor_ReportsCompression(t *testing.T) {
	var report CompressReport
	reported := false
	hist := []Message{msg("user", "a"), msg("user", "b"), msg("user", "c")}
	stub := &stubProvider{resp: "S"}
	c := NewCompressor(stub, 1, false, func(r CompressReport) { report = r; reported = true })
	out, err := c.Compress(context.Background(), hist)
	if err != nil {
		t.Fatalf("Compress err = %v", err)
	}
	if !reported {
		t.Fatal("onCompress not called")
	}
	if report.BeforeMsgs != 3 || report.AfterMsgs != len(out) {
		t.Errorf("report = %+v, want Before=3 After=%d", report, len(out))
	}
	// 模型没返回 usage → Tokens 退回旧段估算
	if report.Tokens == 0 {
		t.Error("report.Tokens should fall back to estimate when model omits usage")
	}
}

func TestCompressor_NoReportWithoutCallback(t *testing.T) {
	hist := []Message{msg("user", "a"), msg("user", "b"), msg("user", "c")}
	stub := &stubProvider{resp: "S"}
	c := NewCompressor(stub, 1, false) // 3 参，无回调
	if _, err := c.Compress(context.Background(), hist); err != nil {
		t.Fatalf("Compress err = %v", err)
	}
}

func TestCompressor_NoPanicOnZeroKeepLast(t *testing.T) {
	// 导出 API 防御：keepLast <= 0 时不得越界 panic，应安全退化
	hist := []Message{msg("user", "a"), msg("user", "b"), msg("user", "c")}
	stub := &stubProvider{resp: "S"}
	c := NewCompressor(stub, 0, false)
	out, err := c.Compress(context.Background(), hist)
	if err != nil {
		t.Fatalf("Compress err = %v", err)
	}
	if len(out) != len(hist) {
		t.Errorf("keepLast=0 should no-op, got %d → %d", len(hist), len(out))
	}
	if stub.lastReq != nil {
		t.Error("provider should not be called with keepLast=0")
	}
}

func TestCompressor_ProviderErrorPropagates(t *testing.T) {
	hist := []Message{msg("user", "a"), msg("user", "b"), msg("user", "c")}
	stub := &stubProvider{err: errors.New("api down")}
	c := NewCompressor(stub, 1, false)
	_, err := c.Compress(context.Background(), hist)
	if err == nil || !strings.Contains(err.Error(), "api down") {
		t.Errorf("err = %v, want wrapped provider error", err)
	}
}
