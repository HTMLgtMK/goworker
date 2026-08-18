package agent

import (
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-sandbox"
)

// testSession 构造一个带 fake provider 的 Session，绕开真实 LLM 端点。
// NewProvider 注入点让 Session.Run 可以被单元测试直接驱动 —— 这是把会话状态
// 收敛进 Session 带来的附带收益：原来 handleAgent 直接 NewOpenAIProvider 无法测试。
func testSession(pv core.Provider) (*Session, *runtimeconfig.Config) {
	cfg := runtimeconfig.Default()
	s := NewSession(SessionDeps{
		Config:       cfg,
		AuditDir:     "",
		Memory:       nil,
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider:  func(*runtimeconfig.Config) core.Provider { return pv },
	})
	return s, cfg
}

func TestSessionRun_WritesBackConversation(t *testing.T) {
	s, _ := testSession(&captureProvider{})

	ctx, _ := newContext("hello")
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(s.conversation) == 0 {
		t.Fatal("conversation not written back after Run")
	}
	first := s.conversation[0]
	if first.Role != "user" || first.Content != "hello" {
		t.Errorf("conversation[0] = %+v, want user hello", first)
	}
	last := s.conversation[len(s.conversation)-1]
	if last.Role != "assistant" || last.Content != "final" {
		t.Errorf("last message = %+v, want assistant final", last)
	}
}

func TestSessionRun_AccumulatesUsageAcrossRuns(t *testing.T) {
	s, _ := testSession(&captureProvider{})
	// 预置一次历史调用；会话累计语义下 Run 不清零，只追加
	s.usage.Record(0, 10, &core.UsageInfo{TotalTokens: 100})
	if n := len(s.usage.Calls()); n != 1 {
		t.Fatalf("precondition: calls = %d, want 1", n)
	}

	ctx, _ := newContext("hello")
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 本轮 captureProvider 记 1 次 → 累计 2 次
	if n := len(s.usage.Calls()); n != 2 {
		t.Errorf("calls = %d, want 2 (accumulated across runs)", n)
	}
}

func TestSessionRun_EmptyInputShowsUsage(t *testing.T) {
	s, _ := testSession(&captureProvider{})

	ctx, buf := newContext()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(buf.String(), "用法: /agent") {
		t.Errorf("output = %q, want usage hint", buf.String())
	}
	if len(s.conversation) != 0 {
		t.Errorf("conversation should stay empty, got %d msgs", len(s.conversation))
	}
}
