package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// 本文件是 P4-1 ApprovalManager 骨架的验收测试(纯单元面,零 Provider、零
// LLM):Wait/Resolve/Pending 的决策-超时-取消三出口、单次有效决策、迟到
// 拒绝、过期过滤。语义铁律见 approval.go 头注释(总方案 §6)。

// approvalReq 构造一笔最小 InterruptRequest:ExpiresAt 由用例给定(唯一超时
// 事实源)。
func approvalReq(id string, expiresAt time.Time) *hitl.InterruptRequest {
	return &hitl.InterruptRequest{
		ID:        id,
		ToolName:  "bash",
		Command:   `{"cmd":"ls"}`,
		CreatedAt: time.Now(),
		ExpiresAt: expiresAt,
	}
}

// waitPending 轮询直至 Pending(sessionID) 恰含 want 条审批(Wait 的注册在
// 调用者 goroutine 内异步完成),有界等待防挂死。
func waitPending(t *testing.T, m *ApprovalManager, sessionID string, want int) []PendingApproval {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if got := m.Pending(sessionID); len(got) == want {
			return got
		}
		select {
		case <-deadline:
			t.Fatalf("Pending(%q) never reached %d entries", sessionID, want)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestApprovalWaitResolves:Wait 挂起 → Resolve(approve) → 返回该决策;
// 二次 Resolve → ErrApprovalResolved(相同 requestID 只接受一次有效决策)。
func TestApprovalWaitResolves(t *testing.T) {
	m := NewApprovalManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := approvalReq("req-1", time.Now().Add(10*time.Second))

	got := make(chan hitl.Decision, 1)
	go func() { got <- m.Wait(ctx, "sess-a", "run-1", req) }()

	// 挂起可见:Pending 快照携带完整归属(SessionID/RunID/原始中断)。
	pending := waitPending(t, m, "sess-a", 1)
	if pending[0].RequestID != "req-1" || pending[0].SessionID != "sess-a" ||
		pending[0].RunID != "run-1" || pending[0].Request.ID != "req-1" {
		t.Fatalf("Pending snapshot = %+v, want req-1/sess-a/run-1", pending[0])
	}
	if other := m.Pending("sess-b"); len(other) != 0 {
		t.Fatalf("Pending(%q) = %+v, want empty(会话过滤)", "sess-b", other)
	}

	// 未知 requestID:sentinel 面(首个有效决策之前核实)。
	if err := m.Resolve("req-missing", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, ErrApprovalUnknown) {
		t.Fatalf("Resolve(unknown) = %v, want ErrApprovalUnknown", err)
	}

	if err := m.Resolve("req-1", hitl.Decision{Type: hitl.DecisionApprove, Message: "go ahead"}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	select {
	case d := <-got:
		if d.Type != hitl.DecisionApprove || d.InterruptID != "req-1" {
			t.Fatalf("Wait returned %+v, want approve for req-1", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after Resolve")
	}

	// 二次 Resolve:单次生效,迟到/重复显式失败。
	if err := m.Resolve("req-1", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, ErrApprovalResolved) {
		t.Fatalf("second Resolve = %v, want ErrApprovalResolved", err)
	}
}

// TestApprovalWaitTimeoutRejects:ExpiresAt 已过期 / 短窗超时 → reject
// (超时默认拒绝,不隐式通过)。
func TestApprovalWaitTimeoutRejects(t *testing.T) {
	m := NewApprovalManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("already expired", func(t *testing.T) {
		req := approvalReq("req-exp", time.Now().Add(-time.Second))
		d := m.Wait(ctx, "sess-a", "run-1", req) // 已过期:立即拒绝,不等待
		if d.Type != hitl.DecisionReject || d.InterruptID != "req-exp" {
			t.Fatalf("Wait on expired request = %+v, want reject for req-exp", d)
		}
	})

	t.Run("short window", func(t *testing.T) {
		req := approvalReq("req-short", time.Now().Add(50*time.Millisecond))
		start := time.Now()
		d := m.Wait(ctx, "sess-a", "run-1", req)
		if d.Type != hitl.DecisionReject || d.InterruptID != "req-short" {
			t.Fatalf("Wait on short window = %+v, want reject for req-short", d)
		}
		// 拒绝来自到点的 ExpiresAt 而非提前放弃:至少持满窗口(计时器不早触发)。
		if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
			t.Fatalf("Wait returned after %v, want it to hold the expiry window", elapsed)
		}
	})
}

// TestApprovalWaitCtxCancelRejects:runCtx 取消 → reject(等待期间的取消绝不
// 隐式通过);取消后该审批不再出现在 Pending(resolved 收口)。
func TestApprovalWaitCtxCancelRejects(t *testing.T) {
	m := NewApprovalManager()
	ctx, cancel := context.WithCancel(context.Background())
	req := approvalReq("req-ctx", time.Now().Add(30*time.Second))

	got := make(chan hitl.Decision, 1)
	go func() { got <- m.Wait(ctx, "sess-a", "run-1", req) }()
	waitPending(t, m, "sess-a", 1)

	cancel() // runCtx 取消是唯一出口
	select {
	case d := <-got:
		if d.Type != hitl.DecisionReject || d.InterruptID != "req-ctx" {
			t.Fatalf("Wait after ctx cancel = %+v, want reject for req-ctx", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after runCtx cancel")
	}
	if ps := m.Pending("sess-a"); len(ps) != 0 {
		t.Fatalf("Pending after cancel = %+v, want empty(resolved 收口)", ps)
	}
}

// TestApprovalPendingFiltersExpired:Pending 只回未过期条目——会话内已过期
// 审批不出现且被读时清扫逐出,跨会话审批互不可见。
func TestApprovalPendingFiltersExpired(t *testing.T) {
	m := NewApprovalManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sess-a:未过期 req-a(挂起);sess-b:未过期 req-b(挂起);sess-a:
	// 已过期 req-c(Wait 立即 reject,entry 过期+resolved)。
	go func() { m.Wait(ctx, "sess-a", "run-1", approvalReq("req-a", time.Now().Add(30*time.Second))) }()
	go func() { m.Wait(ctx, "sess-b", "run-2", approvalReq("req-b", time.Now().Add(30*time.Second))) }()
	waitPending(t, m, "sess-a", 1)
	waitPending(t, m, "sess-b", 1)

	done := make(chan hitl.Decision, 1)
	go func() { done <- m.Wait(ctx, "sess-a", "run-1", approvalReq("req-c", time.Now().Add(-time.Second))) }()
	select {
	case d := <-done:
		if d.Type != hitl.DecisionReject {
			t.Fatalf("Wait on expired request = %+v, want reject", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait on expired request did not return")
	}

	if pa := m.Pending("sess-a"); len(pa) != 1 || pa[0].RequestID != "req-a" {
		t.Fatalf("Pending(sess-a) = %+v, want only unexpired req-a(已过期 req-c 被过滤)", pa)
	}
	if pb := m.Pending("sess-b"); len(pb) != 1 || pb[0].RequestID != "req-b" {
		t.Fatalf("Pending(sess-b) = %+v, want only req-b(会话过滤)", pb)
	}
	// 再读一次:过期的 req-c 已被首读清扫逐出,快照稳定。
	if pa := m.Pending("sess-a"); len(pa) != 1 || pa[0].RequestID != "req-a" {
		t.Fatalf("Pending(sess-a) second read = %+v, want only req-a(清扫幂等)", pa)
	}
}

// TestApprovalLateResolveRejected:超时后 Resolve → ErrApprovalResolved
// (迟到决策无效,对齐 hitl/channel_decision_provider.go:20-21)。
func TestApprovalLateResolveRejected(t *testing.T) {
	m := NewApprovalManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := approvalReq("req-late", time.Now().Add(50*time.Millisecond))
	d := m.Wait(ctx, "sess-a", "run-1", req) // 超时默认拒绝
	if d.Type != hitl.DecisionReject {
		t.Fatalf("Wait after timeout = %+v, want reject", d)
	}
	if err := m.Resolve("req-late", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, ErrApprovalResolved) {
		t.Fatalf("late Resolve = %v, want ErrApprovalResolved", err)
	}
}
