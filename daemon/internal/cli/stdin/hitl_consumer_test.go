package stdin

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// syncBuffer 线程安全的字符串缓冲：HITL writer 在 Run goroutine 写，
// 测试主 goroutine 读，直接用 strings.Builder 会数据竞争。
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (s *syncBuffer) Write(p string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.WriteString(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// runHITLAsync 在 goroutine 中执行 HITL 会话，主测试 goroutine 投递输入。
// 返回前等 HITL 进入活跃读行状态。
type hitlResult struct {
	decision hitl.Decision
	canceled bool
}

func runHITLAsync(c *HITLConsumer, req *hitl.InterruptRequest) chan hitlResult {
	ch := make(chan hitlResult, 1)
	go func() {
		d, canceled := c.Run(req)
		ch <- hitlResult{d, canceled}
	}()
	for !c.active.Load() {
		time.Sleep(time.Millisecond)
	}
	return ch
}

func newTestHITL() (*HITLConsumer, *syncBuffer, chan struct{}) {
	cancelCh := make(chan struct{}, 1)
	out := &syncBuffer{}
	c := NewHITLConsumer(cancelCh, out.Write)
	return c, out, cancelCh
}

func requireDecision(t *testing.T, r hitlResult, typ hitl.DecisionType) {
	t.Helper()
	if r.decision.Type != typ {
		t.Fatalf("decision type got %q, want %q", r.decision.Type, typ)
	}
	if r.canceled {
		t.Fatal("unexpected canceled")
	}
}

// waitFor 轮询等待条件成立，超时失败。用于等待 Run goroutine 推进到下一个阶段。
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

func TestHITLRun_Approve(t *testing.T) {
	for _, input := range []string{"", "a", "approve"} {
		c, _, _ := newTestHITL()
		ch := runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "rm -rf /", RiskReason: "危险"})

		typeString(c, input)
		typeKey(c, key(KeyEnter))

		select {
		case r := <-ch:
			requireDecision(t, r, hitl.DecisionApprove)
		case <-time.After(time.Second):
			t.Fatalf("timeout for input %q", input)
		}
	}
}

func TestHITLRun_Reject(t *testing.T) {
	for _, input := range []string{"r", "reject"} {
		c, _, _ := newTestHITL()
		ch := runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "rm -rf /"})

		typeString(c, input)
		typeKey(c, key(KeyEnter))

		select {
		case r := <-ch:
			requireDecision(t, r, hitl.DecisionReject)
		case <-time.After(time.Second):
			t.Fatalf("timeout for input %q", input)
		}
	}
}

func TestHITLRun_DefaultReject(t *testing.T) {
	c, _, _ := newTestHITL()
	ch := runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "rm -rf /"})

	typeString(c, "garbage")
	typeKey(c, key(KeyEnter))

	select {
	case r := <-ch:
		requireDecision(t, r, hitl.DecisionReject)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestHITLRun_Edit(t *testing.T) {
	c, out, _ := newTestHITL()
	ch := runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "rm -rf /"})

	// 决策行：e
	typeString(c, "e")
	typeKey(c, key(KeyEnter))
	// 等 Run 推进到编辑阶段（提示出现），再投第二行输入
	waitFor(t, func() bool { return strings.Contains(out.String(), "New command") })
	// 新命令行
	typeString(c, "ls -l")
	typeKey(c, key(KeyEnter))

	select {
	case r := <-ch:
		requireDecision(t, r, hitl.DecisionEdit)
		if r.decision.Command != "ls -l" {
			t.Fatalf("edited command got %q, want %q", r.decision.Command, "ls -l")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestHITLRun_Respond(t *testing.T) {
	c, out, _ := newTestHITL()
	ch := runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "rm -rf /"})

	typeString(c, "p")
	typeKey(c, key(KeyEnter))
	waitFor(t, func() bool { return strings.Contains(out.String(), "Your instruction") })
	typeString(c, "别删")
	typeKey(c, key(KeyEnter))

	select {
	case r := <-ch:
		requireDecision(t, r, hitl.DecisionRespond)
		if r.decision.Message != "别删" {
			t.Fatalf("message got %q, want %q", r.decision.Message, "别删")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestHITLRun_BackspaceEdit(t *testing.T) {
	// 输入回显编辑：打错字用退格修正（abz → ab → 非 a/r/e/p，默认 reject）
	c, _, _ := newTestHITL()
	ch := runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "rm -rf /"})

	typeString(c, "abz")
	typeKey(c, key(KeyBackspace)) // abz → ab
	typeKey(c, key(KeyEnter))

	select {
	case r := <-ch:
		requireDecision(t, r, hitl.DecisionReject)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestHITLRun_Cancel(t *testing.T) {
	c, _, cancelCh := newTestHITL()
	ch := runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "rm -rf /"})

	cancelCh <- struct{}{} // 模拟 keyWatcher 收到 Esc

	select {
	case r := <-ch:
		if !r.canceled {
			t.Fatal("expected canceled")
		}
		if r.decision.Type != hitl.DecisionReject {
			t.Fatalf("canceled decision should be reject, got %q", r.decision.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestHITLRun_EditCancel(t *testing.T) {
	c, out, cancelCh := newTestHITL()
	ch := runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "rm -rf /"})

	typeString(c, "e")
	typeKey(c, key(KeyEnter))
	waitFor(t, func() bool { return strings.Contains(out.String(), "New command") })
	// 第二行被取消
	cancelCh <- struct{}{}

	select {
	case r := <-ch:
		if !r.canceled {
			t.Fatal("expected canceled on second line")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestHITLConsume_Delegates(t *testing.T) {
	// Consume 只认编辑键且 active：inactive 放行，active 消费
	c, _, _ := newTestHITL()
	if c.Consume(char('x')) {
		t.Fatal("inactive HITL consumer should not consume")
	}

	ch := runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "cmd"})
	if !c.Consume(char('a')) {
		t.Fatal("active HITL consumer should consume")
	}
	typeKey(c, key(KeyEnter))

	select {
	case r := <-ch:
		requireDecision(t, r, hitl.DecisionApprove)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestHITLConsume_NeverKeyEsc(t *testing.T) {
	// 取消键无条件放行给 keyWatcher
	c, _, _ := newTestHITL()
	runHITLAsync(c, &hitl.InterruptRequest{ID: "req-1", Command: "cmd"})
	if c.Consume(key(KeyEsc)) {
		t.Fatal("HITL consumer should never consume cancel keys")
	}
}
