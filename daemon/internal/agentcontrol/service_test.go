package agentcontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-sandbox"
)

// service_test.go 是 Service 的组合层测试(真实 SessionManager + 真实会话,零真实
// LLM):验证「寻址 → fencing → 幂等 → 直转 manager」的编排语义与错误契约。
// 全链路(多会话路由、journal 权威分支、跨会话独立、接管风暴)由 plugin 包的
// runcontrol_test.go 适配保留——那边持有会话边界/journal 的生产接线。

// fakeRouter 是 SessionRouter 的测试实现,镜像 plugin 纯内存形态的解析序:
// "current" 与空串都路由到唯一 runtime(current 优先),其余未注册诚实失败。
type fakeRouter struct {
	rt *runtimeagent.SessionRuntime
}

func (f *fakeRouter) RouteSession(sessionID string) *runtimeagent.SessionRuntime {
	if f.rt != nil && (sessionID == f.rt.ID || (sessionID == "" && f.rt.ID == "current")) {
		return f.rt
	}
	return nil
}

// fixedProvider 每次调用返回固定 assistant 内容(零真实 LLM)。
type fixedProvider struct{ content string }

func (p fixedProvider) Name() string  { return "fixed" }
func (p fixedProvider) Model() string { return "m" }
func (p fixedProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: p.content}}}}, nil
}
func (p fixedProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// newTestService 构造真实 SessionManager + 单会话 runtime("current",纯内存)
// 的 Service 底座;journals 传 nil,RunStatus 的 journal 权威分支跳过、幂等表是
// 唯一进程内观测面(对应 plugin 的纯内存形态)。
func newTestService(t *testing.T, pv core.Provider) (*Service, *runtimeagent.SessionRuntime) {
	t.Helper()
	cfg := runtimeconfig.Default()
	deps := runtimeagent.SessionDeps{
		Config: cfg,
		CollectTools: func(*sandbox.Config) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
	}
	s := runtimeagent.NewSession(deps)
	rt := runtimeagent.NewSessionRuntime("current", s)
	mgr := runtimeagent.NewSessionManager()
	if err := mgr.Register(rt); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return NewService(mgr, &fakeRouter{rt: rt}, nil), rt
}

// waitForRunStatus 轮询 RunStatus 直至 want 状态或超时。
func waitForRunStatus(t *testing.T, s *Service, sid, runID, want string) (string, uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, latest, _, err := s.RunStatus(sid, runID)
		if err != nil {
			t.Fatalf("RunStatus(%s): %v", runID, err)
		}
		if state == want {
			return state, latest
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach %q in 5s, last state = %q", runID, want, state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestServiceStartRunFencing:未持有 lease → ErrControlLeaseLost;持有 → 提交
// 成功(queued 快照)并收敛 completed;未注册会话 → ErrSessionNotFound(路由
// 先于 lease 校验)。
func TestServiceStartRunFencing(t *testing.T) {
	s, _ := newTestService(t, fixedProvider{content: "svc-done"})

	// fencing:未持有的连接一律拒绝,且不入幂等表。
	if _, _, err := s.StartRun("current", "conn-other", "hello", "req-fenced", runtimeagent.RunCallbacks{}); !errors.Is(err, ErrControlLeaseLost) {
		t.Fatalf("StartRun without lease err = %v, want ErrControlLeaseLost", err)
	}
	if _, tracked := s.TrackedRequest("current", "req-fenced"); tracked {
		t.Fatal("fenced StartRun recorded an idempotency entry (拒绝路径不得入表)")
	}
	if s.AcquireControl("current", "conn-A") {
		t.Fatal("test setup: fresh acquire must not report takeover")
	}

	runID, state, err := s.StartRun("current", "conn-A", "hello", "req-1", runtimeagent.RunCallbacks{})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if runID == "" {
		t.Fatal("StartRun returned empty runID")
	}
	if state != string(runtimeagent.RunQueued) {
		t.Fatalf("submit state = %q, want %q", state, runtimeagent.RunQueued)
	}
	waitForRunStatus(t, s, "current", runID, string(runtimeagent.RunCompleted))

	// 未注册会话:路由先于 lease 校验,ErrSessionNotFound 语义。
	if _, _, err := s.StartRun("bogus", "conn-A", "x", "req-2", runtimeagent.RunCallbacks{}); !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("StartRun with unknown session err = %v, want ErrSessionNotFound", err)
	}
}

// TestServiceStartRunIdempotency(组合层幂等):同键同 Input 幂等短路(同
// runID,底层不再起跑);同键不同 Input → ErrRequestConflict;不同 requestID
// 起新 run;终态后 RunStatus 经幂等表回退(无 journal 形态)仍可观测。
func TestServiceStartRunIdempotency(t *testing.T) {
	s, _ := newTestService(t, fixedProvider{content: "svc-idem"})

	if s.AcquireControl("current", "conn-A") {
		t.Fatal("test setup: fresh acquire must not report takeover")
	}
	id1, _, err := s.StartRun("current", "conn-A", "q", "req-dup", runtimeagent.RunCallbacks{})
	if err != nil {
		t.Fatalf("StartRun #1: %v", err)
	}
	id2, _, err := s.StartRun("current", "conn-A", "q", "req-dup", runtimeagent.RunCallbacks{})
	if err != nil {
		t.Fatalf("StartRun retry: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("retry runID = %q, want original %q", id2, id1)
	}

	if _, _, err := s.StartRun("current", "conn-A", "q-changed", "req-dup", runtimeagent.RunCallbacks{}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("different-input retry err = %v, want ErrRequestConflict", err)
	}

	waitForRunStatus(t, s, "current", id1, string(runtimeagent.RunCompleted))
	id3, _, err := s.StartRun("current", "conn-A", "q", "req-other", runtimeagent.RunCallbacks{})
	if err != nil {
		t.Fatalf("StartRun #3: %v", err)
	}
	if id3 == id1 {
		t.Fatal("different requestID reused the original runID")
	}
	waitForRunStatus(t, s, "current", id3, string(runtimeagent.RunCompleted))

	// 终态 + 无 journal:RunStatus 经幂等表反查(byRun)仍返回 completed。
	state, _, integrity, err := s.RunStatus("current", id1)
	if err != nil {
		t.Fatalf("RunStatus after completion: %v", err)
	}
	if state != string(runtimeagent.RunCompleted) {
		t.Fatalf("RunStatus after completion = %q, want completed", state)
	}
	if integrity != runtimeagent.ReplayIntegrityComplete {
		t.Fatalf("replayIntegrity = %q, want complete (纯内存无事件流)", integrity)
	}
	if _, _, _, err := s.RunStatus("current", "run-missing"); !errors.Is(err, runtimeagent.ErrRunNotFound) {
		t.Fatalf("RunStatus unknown run err = %v, want ErrRunNotFound", err)
	}
}

// TestServiceCancelRun:未持有 lease 拒绝;持有后取消被接受(未知 run 的
// ErrRunNotFound 语义原样上抛)。
func TestServiceCancelRun(t *testing.T) {
	s, _ := newTestService(t, fixedProvider{content: "svc-cancel"})

	if err := s.CancelRun("current", "conn-other", "run-x"); !errors.Is(err, ErrControlLeaseLost) {
		t.Fatalf("CancelRun without lease err = %v, want ErrControlLeaseLost", err)
	}
	if s.AcquireControl("current", "conn-A") {
		t.Fatal("test setup: fresh acquire must not report takeover")
	}
	if err := s.CancelRun("current", "conn-A", "run-missing"); !errors.Is(err, runtimeagent.ErrRunNotFound) {
		t.Fatalf("CancelRun unknown run err = %v, want ErrRunNotFound", err)
	}
	if err := s.CancelRun("bogus", "conn-A", "run-x"); !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("CancelRun unknown session err = %v, want ErrSessionNotFound", err)
	}
}

// TestServiceResolveApprovalOwnership:未知 requestID → ErrApprovalUnknown;
// 归属会话本人应答成功且决策送达 Wait;未持有 lease → fencing 拒绝;未注册
// 会话 → ErrSessionNotFound。跨会话越权(ErrApprovalOwnership)由 plugin 包的
// TestCrossSessionApprovalOwnership 覆盖(需要两个真实注册会话)。
func TestServiceResolveApprovalOwnership(t *testing.T) {
	s, rt := newTestService(t, fixedProvider{content: "svc-approval"})
	sid := rt.ID

	if s.AcquireControl(sid, "conn-A") {
		t.Fatal("test setup: fresh acquire must not report takeover")
	}
	if err := s.ResolveApproval(sid, "conn-other", "req-x", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, ErrControlLeaseLost) {
		t.Fatalf("ResolveApproval without lease err = %v, want ErrControlLeaseLost", err)
	}
	if err := s.ResolveApproval("bogus", "conn-A", "req-x", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("ResolveApproval unknown session err = %v, want ErrSessionNotFound", err)
	}
	if err := s.ResolveApproval(sid, "conn-A", "req-missing", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, runtimeagent.ErrApprovalUnknown) {
		t.Fatalf("ResolveApproval unknown requestID err = %v, want ErrApprovalUnknown", err)
	}

	// 归属会话本人应答:经 SDK 审批管理面注册(归属 sid),决策送达 Wait。
	mgr := s.sessions.Approvals()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	decided := make(chan hitl.Decision, 1)
	go func() {
		decided <- mgr.Wait(ctx, sid, "run-approval", &hitl.InterruptRequest{
			ID:        "req-own",
			ToolName:  "bash",
			Command:   `{"cmd":"ls"}`,
			CreatedAt: time.Now(),
			ExpiresAt: time.Now().Add(30 * time.Second),
		})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := s.sessions.ApprovalOwner("req-own"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("approval req-own never registered with the manager")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := s.ResolveApproval(sid, "conn-A", "req-own", hitl.Decision{Type: hitl.DecisionApprove, Message: "go"}); err != nil {
		t.Fatalf("ResolveApproval(own approval): %v", err)
	}
	select {
	case d := <-decided:
		if d.Type != hitl.DecisionApprove || d.InterruptID != "req-own" {
			t.Fatalf("Wait decision = %+v, want approve for req-own", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after ResolveApproval")
	}
}

// TestServiceLeaseControls:Service 的 lease 四操作与注册表语义一致(接管/持有/
// 按会话释放/断连收口),守卫语义与 WithControl 校验共用同一判定。
func TestServiceLeaseControls(t *testing.T) {
	s, _ := newTestService(t, fixedProvider{content: "svc-lease"})

	if s.AcquireControl("sess", "conn-A") {
		t.Fatal("fresh acquire must not report takeover")
	}
	if !s.HoldControl("sess", "conn-A") {
		t.Fatal("conn-A should hold the lease")
	}
	if !s.AcquireControl("sess", "conn-B") {
		t.Fatal("second holder must take over")
	}
	if s.HoldControl("sess", "conn-A") || !s.HoldControl("sess", "conn-B") {
		t.Fatal("takeover must move the lease to conn-B")
	}
	s.ReleaseControl("sess", "conn-A")
	if !s.HoldControl("sess", "conn-B") {
		t.Fatal("release by a non-holder must not clear the lease")
	}
	if s.AcquireControl("sess2", "conn-B") {
		t.Fatal("acquire on idle sess2 must not report takeover")
	}
	s.ReleaseConnection("conn-B")
	if s.HoldControl("sess", "conn-B") || s.HoldControl("sess2", "conn-B") {
		t.Fatal("ReleaseConnection must clear every lease held by the connection")
	}
}
