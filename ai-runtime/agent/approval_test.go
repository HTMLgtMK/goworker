package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-sandbox"
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

// TestApprovalSweepsResolvedExpiredEntries(P4 评审 issue3):已 resolved 且
// 已过期超过 approvalResolvedRetention 宽限窗的滞留条目,在后续 Wait / Resolve
// 入口被顺带清扫(Pending 可能从不被调——客户端的事实源是 permission.requested
// 事件流);宽限窗内的迟到 Resolve 仍报 ErrApprovalResolved(TestApprovalLate
// ResolveRejected 钉死),宽限窗外落 ErrApprovalUnknown。ExpiresAt 回拨为
// 白箱操作(同包单元测试,免真实等待 30s);断言直读 m.pending——Pending 的
// 读时清扫会抢跑,不能作为本用例的观察面。
func TestApprovalSweepsResolvedExpiredEntries(t *testing.T) {
	m := NewApprovalManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 造第一笔滞留条目:已过期请求 Wait 立即 reject(resolved 收口,entry 留表),
	// 再把其 ExpiresAt 回拨到宽限窗之外。
	if d := m.Wait(ctx, "sess-a", "run-1", approvalReq("req-stale", time.Now().Add(-time.Hour))); d.Type != hitl.DecisionReject {
		t.Fatalf("Wait on expired request = %+v, want reject", d)
	}
	m.mu.Lock()
	entry, ok := m.pending["req-stale"]
	if !ok {
		m.mu.Unlock()
		t.Fatal("resolved+expired entry must linger in the map before any sweep")
	}
	entry.pa.Request.ExpiresAt = time.Now().Add(-approvalResolvedRetention - time.Hour)
	m.mu.Unlock()

	// 后续 Wait(另一 requestID)入口的顺带清扫把它逐出。
	got := make(chan hitl.Decision, 1)
	go func() { got <- m.Wait(ctx, "sess-a", "run-2", approvalReq("req-live", time.Now().Add(30*time.Second))) }()
	deadline := time.After(5 * time.Second)
	for {
		m.mu.Lock()
		_, hasStale := m.pending["req-stale"]
		_, hasLive := m.pending["req-live"]
		m.mu.Unlock()
		if hasLive && !hasStale {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("resolved+expired entry survived a subsequent Wait (hasLive=%v, hasStale=%v)", hasLive, hasStale)
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel() // 收敛 req-live 的等待 goroutine
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("live Wait did not return after ctx cancel")
	}

	// Resolve 入口同清扫:再造一笔宽限窗外的滞留条目并对它迟到 Resolve——
	// 条目被逐出,返回 ErrApprovalUnknown。
	if d := m.Wait(ctx, "sess-a", "run-1", approvalReq("req-stale2", time.Now().Add(-time.Hour))); d.Type != hitl.DecisionReject {
		t.Fatalf("Wait on expired request = %+v, want reject", d)
	}
	m.mu.Lock()
	entry2, ok := m.pending["req-stale2"]
	if !ok {
		m.mu.Unlock()
		t.Fatal("second stale entry must linger in the map before any sweep")
	}
	entry2.pa.Request.ExpiresAt = time.Now().Add(-approvalResolvedRetention - time.Hour)
	m.mu.Unlock()
	if err := m.Resolve("req-stale2", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, ErrApprovalUnknown) {
		t.Fatalf("Resolve past retention = %v, want ErrApprovalUnknown(宽限窗外条目已被入口清扫)", err)
	}
	m.mu.Lock()
	_, still := m.pending["req-stale2"]
	m.mu.Unlock()
	if still {
		t.Fatal("Resolve must sweep the past-retention entry")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// P4-2:StartRun 审批回退接线 + waiting 迁移(总方案第十一节
// TestApprovalAfterReconnect → 本步)。
//
// 测试装置:scriptedChatProvider(非流式脚本,含 tool call 轮,零真实 LLM)+
// newHitlRunRuntime(真实 bash + 真实 HITL 中间件链:CollectTools=DefaultTools、
// 沙箱 normal 模式——管道命令命中 DefaultRiskyPatterns 的 `\|`,RiskMedium →
// DecisionHitl,ai-sandbox/defaults.go:31、policy.go:106-113;实测探针确认
// decision=hitl)。审批面零真实连接:「原连接断开」= 提交方 goroutine 返回后
// 不再存在(StartRun 无 ctx 参数,API 形状即保证连接无关);「新控制端」= 测试
// 主 goroutine 经 manager 的 ResolveApproval 直转投递。
// ─────────────────────────────────────────────────────────────────────────────

// hitlProbeCommand 是 HITL 触发命令:管道命中默认风险规则(normal 模式 →
// DecisionHitl),但命令本身无副作用、可真实执行(approve 后输出 HITL-OK)。
const hitlProbeCommand = `echo hitl-ok | tr a-z A-Z`

// hitlToolCallRound 构造第 0 轮脚本消息:assistant 发起一次 bash tool call,
// 命令即 hitlProbeCommand(经真实 kernel 工具循环,HITL 中间件拦截)。
func hitlToolCallRound() core.Message {
	return core.Message{Role: "assistant", ToolCalls: []core.ToolCall{{
		ID:       "call-hitl-1",
		Type:     "function",
		Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"` + hitlProbeCommand + `"}`},
	}}}
}

// scriptedChatProvider 按调用序回放预置 assistant 消息(第 i 轮 = msgs[i];
// 越界回落最终文本)。ChatStream 恒报错 → Session 回退非流式
// (ai-core/agent/agent.go:184-188 的 chat() 回退),tool call 经真实 kernel
// 循环 + 中间件链执行。
type scriptedChatProvider struct {
	mu    sync.Mutex
	calls int
	msgs  []core.Message
}

func (p *scriptedChatProvider) Name() string  { return "scripted-chat" }
func (p *scriptedChatProvider) Model() string { return "m" }

func (p *scriptedChatProvider) Chat(_ context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.mu.Lock()
	i := p.calls
	p.calls++
	p.mu.Unlock()
	msg := core.Message{Role: "assistant", Content: "final"}
	if i < len(p.msgs) {
		msg = p.msgs[i]
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: msg}}}, nil
}

func (p *scriptedChatProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// newHitlRunRuntime 构造「真实 bash+HITL 链」的 runtime 并注册进 manager:
// CollectTools 返回 DefaultTools(真实 bash 执行面,default_tools.go),沙箱用
// 默认配置(Mode=normal,ai-runtime/config/config.go:195)并把 AllowedWorkDir
// 锚定到临时目录(bash cmd.Dir;探针命令无文件副作用)。RunJournal 可为 nil
// (journalRunState 按 nil 跳过)。返回会话 id(= runtime id,StartRun/
// PendingApprovals 的寻址键)。
func newHitlRunRuntime(t *testing.T, m *SessionManager, id string, pv core.Provider, jr *stubRunJournal) string {
	t.Helper()
	cfg := runtimeconfig.Default()
	cfg.Sandbox.AllowedWorkDir = t.TempDir()
	deps := SessionDeps{
		Config: cfg,
		CollectTools: func(sb *sandbox.Config) []core.Tool {
			return DefaultTools(sb)
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store: &fakeConversationStore{},
	}
	// RunJournal 仅在非 nil 时装配:接口字段必须真 nil(spec.go 的 typed-nil
	// 纪律)——typed-nil 会让 journalRunState(runtime.go:200)的 nil 检查失效,
	// nil 接收者 panic。
	if jr != nil {
		deps.RunJournal = jr
	}
	s := NewSession(deps)
	rt := NewSessionRuntime(id, s)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}
	return id
}

// waitApprovalPending 轮询直至会话审批挂起非空且 run 迁移 RunWaiting
// (decideFunc 的 transition 先于 Wait 注册 entry,两条件同时稳定可达),
// 有界等待防挂死。返回挂起审批的 requestID。
func waitApprovalPending(t *testing.T, m *SessionManager, sessionID string, h RunHandle) string {
	t.Helper()
	var pending []PendingApproval
	deadline := time.After(5 * time.Second)
	for {
		pending = m.PendingApprovals(sessionID)
		if len(pending) == 1 && h.State() == RunWaiting {
			return pending[0].RequestID
		}
		select {
		case <-deadline:
			t.Fatalf("approval pending/waiting never reached: pending=%d state=%s", len(pending), h.State())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestApprovalAfterReconnect(总方案第十一节 → 本步):cb.Decide 为 nil 的
// StartRun 走审批回退(decideFunc)→ 真实 bash 的危险命令触发 DecisionHitl →
// run 迁移 RunWaiting 且审批挂起可见 → 「原连接断开」零操作 → 「新控制端」
// ResolveApproval(approve)→ run RunCompleted 且工具结果为命令真实输出
// (非拒绝文案)——后台 Run 的 HITL 决策跨连接化,断连不影响等待。
func TestApprovalAfterReconnect(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	pv := &scriptedChatProvider{msgs: []core.Message{
		hitlToolCallRound(),
		{Role: "assistant", Content: "approved run done"},
	}}
	sid := newHitlRunRuntime(t, m, "hitl-reconnect", pv, nil)

	var out strings.Builder
	// 提交方即「原控制端」:cb.Decide 为 nil(审批回退接管),StartRun 返回后
	// 本 goroutine 不再持有任何连接语义(契约:StartRun 无 ctx 参数)。
	h, err := m.StartRun(sid, RunRequest{Input: "run the pipe"}, testRunCallbacks(&out))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// 等待面:审批挂起可见(PendingApprovals 直转包装)且 run 已迁移
	// RunWaiting(decideFunc 的 waiting 迁移)。
	reqID := waitApprovalPending(t, m, sid, h)
	if reqID == "" {
		t.Fatal("pending approval has empty request id")
	}
	if h.State() != RunWaiting {
		t.Fatalf("state = %q, want %q", h.State(), RunWaiting)
	}

	// 「原连接断开」零操作:StartRun 无 ctx 参数,提交方 ctx 不参与
	// (P2P3 §6 语义在此复用为「断开不影响等待」)——此处显式无任何动作。

	// 「新控制端」放行:决策按 requestID 跨连接投递,不经任何连接对象。
	if err := m.ResolveApproval(reqID, hitl.Decision{Type: hitl.DecisionApprove}); err != nil {
		t.Fatalf("ResolveApproval: %v", err)
	}

	res := waitHandle(t, h) // RunCompleted+Persisted=true(waitHandle 内断言)

	// 决策落地后审批不再挂起(resolved 收口,单次有效)。
	if ps := m.PendingApprovals(sid); len(ps) != 0 {
		t.Fatalf("PendingApprovals after resolve = %+v, want empty(resolved 收口)", ps)
	}
	// 工具结果非拒绝文案:approve 后 bash 真实执行,结果经 WriteToken 流出。
	if strings.Contains(out.String(), "⛔ rejected by user") {
		t.Fatalf("tool output contains rejection text: %q", out.String())
	}
	if !strings.Contains(out.String(), "HITL-OK") {
		t.Fatalf("tool output %q missing command output HITL-OK(approve 未放行真实执行)", out.String())
	}
	if res.State != RunCompleted {
		t.Fatalf("state = %q, want RunCompleted", res.State)
	}
}

// TestApprovalTimeoutFailsTool:超时 = 安全拒绝(不挂死、不隐式放行)。
// 中间件给 interrupt 硬编码 ExpiresAt=now+DefaultTimeout(2min,hitl.go:280,
// 常量不可注入),故本测试以「调用方自带 Decide」形态(契约③:StartRun 不
// 覆盖)把中间件原始请求收短成可测窗口后交给**同一条** Wait——超时出口
// (ExpiresAt 到点 reject)由 Wait 真实计时触发,P4-1 单元测试覆盖的到点
// 路径在真实 bash+HITL 链上端到端收敛:工具结果 = 「⛔ rejected by user」
// (hitl.go:443 文案),命令从未执行,run 继续 RunCompleted。
// (decideFunc 只是 Wait 的纯转发,等待期迁移由 TestApprovalAfterReconnect/
// TestStartRunJournalsWaiting 覆盖,不因本窗口收短而失真。)
func TestApprovalTimeoutFailsTool(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	pv := &scriptedChatProvider{msgs: []core.Message{
		hitlToolCallRound(),
		{Role: "assistant", Content: "continue after timeout"},
	}}
	sid := newHitlRunRuntime(t, m, "hitl-timeout", pv, nil)

	var out strings.Builder
	h, err := m.StartRun(sid, RunRequest{Input: "run the pipe"}, RunCallbacks{
		Write: func(string) {},
		WriteToken: func(_ RenderKind, s string, _ bool) {
			out.WriteString(s)
		},
		Decide: func(req *hitl.InterruptRequest) hitl.Decision {
			short := *req
			short.ExpiresAt = time.Now().Add(150 * time.Millisecond)
			return m.Approvals().Wait(context.Background(), sid, "run-timeout-probe", &short)
		},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	waitHandle(t, h) // 超时拒绝后模型收到拒绝文案继续 → RunCompleted(不挂死)

	// 工具结果 = 拒绝文案,绝无命令输出(超时不隐式放行)。
	if !strings.Contains(out.String(), "⛔ rejected by user") {
		t.Fatalf("tool output %q missing rejection text ⛔ rejected by user", out.String())
	}
	if strings.Contains(out.String(), "HITL-OK") {
		t.Fatalf("tool output %q contains command output — timeout must not execute the command", out.String())
	}
}

// TestStartRunJournalsWaiting:审批等待落 journal(durable-first 观测面)——
// 记录流 queued → running → waiting_input → completed,waiting_input 先于
// 终态记录(decideFunc 的 journalRunState 在 Wait 阻塞前落盘,finalizeRun 的
// 终态在 Wait 返回后)。恢复扫描据此可把卡在 waiting 的 run 标 interrupted
// (isTerminalRunState 不含 waiting_input)。
func TestStartRunJournalsWaiting(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	jr := &stubRunJournal{}
	pv := &scriptedChatProvider{msgs: []core.Message{
		hitlToolCallRound(),
		{Role: "assistant", Content: "approved run done"},
	}}
	sid := newHitlRunRuntime(t, m, "hitl-journal", pv, jr)

	h, err := m.StartRun(sid, RunRequest{Input: "run the pipe"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// 等待面出现(Pending 非空 + RunWaiting)⇒ waiting_input 记录必已落盘:
	// decideFunc 先 journalRunState 再 Wait 注册,同一 goroutine 内先后有序。
	reqID := waitApprovalPending(t, m, sid, h)

	if err := m.ResolveApproval(reqID, hitl.Decision{Type: hitl.DecisionApprove}); err != nil {
		t.Fatalf("ResolveApproval: %v", err)
	}
	waitHandle(t, h)

	recs := jr.snapshot()
	wantStates := []string{
		string(RunQueued),
		string(RunRunning),
		string(RunWaiting),
		string(RunCompleted),
	}
	if len(recs) != len(wantStates) {
		var got []string
		for _, rec := range recs {
			got = append(got, rec.Kind+":"+rec.State)
		}
		t.Fatalf("journal records = %v, want states %v", got, wantStates)
	}
	waitingIdx, terminalIdx := -1, -1
	for i, rec := range recs {
		if rec.RunID != h.RunID || rec.SessionID != sid {
			t.Fatalf("record %d = {run:%q session:%q}, want run=%q session=%q(RunID/SessionID 同源)", i, rec.RunID, rec.SessionID, h.RunID, sid)
		}
		if rec.Kind != "state" {
			t.Fatalf("record %d kind = %q, want state", i, rec.Kind)
		}
		if rec.State != wantStates[i] {
			t.Fatalf("record %d state = %q, want %q", i, rec.State, wantStates[i])
		}
		if rec.State == string(RunWaiting) {
			waitingIdx = i
		}
		if rec.State == string(RunCompleted) || rec.State == string(RunFailed) || rec.State == string(RunCancelled) {
			terminalIdx = i
		}
	}
	// waiting_input 存在且先于终态记录(计划验收口径,序列断言的显式对偶)。
	if waitingIdx < 0 || terminalIdx < 0 || waitingIdx >= terminalIdx {
		t.Fatalf("waiting record (idx %d) must exist and precede terminal record (idx %d)", waitingIdx, terminalIdx)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// P4 评审 issue1 回归(确定性时序缺陷,跨组件:GetDecision × ApprovalManager.Wait):
// 中间件侧 GetDecision(hitl/channel_decision_provider.go:12-53)与 agent 侧
// decideFunc 的 ApprovalManager.Wait(approval.go:78-143)对同一 interrupt 的
// 同一 ExpiresAt 各起计时器、共用容量 1 的 decisions 缓冲(session.go:149)。
// 一次超时后(GetDecision 先到期返回),Wait 的超时 reject 残留缓冲无人消费
// (后台 decideFunc → streamTokens 投递);同 run 下一个 interrupt 的 GetDecision
// 若不排空,select 立即吃到上一轮的决策——第二次审批用户看不到就被自动拒绝
// 且级联。sync 路径同型隐患:调用方 Decide 在窗口关闭后的迟到响应同样残留
// 缓冲。以下用例在纯 provider 面复现该序列(agent 级端到端复现不可行:中间件
// 恒硬编码 ExpiresAt=now+DefaultTimeout 2min 窗口,hitl.go:137/238/280/343,
// TestApprovalTimeoutFailsTool 头注释已记录该约束——其「收短 Decide 侧副本」
// 恰好让陈旧值被就地消费,缓冲保持干净,无法触发毒化)。
// ─────────────────────────────────────────────────────────────────────────────

// TestChannelDecisionProviderDrainsStaleReject:第一个 interrupt 短窗超时后,
// 其陈旧 reject(后台 Wait 超时的形态)残留缓冲;第二个 interrupt 的
// GetDecision 必须等待自己的完整窗口并接受自己的决策(InterruptID 匹配),
// 绝不立即返回上一轮的陈旧 reject。
func TestChannelDecisionProviderDrainsStaleReject(t *testing.T) {
	decisions := make(chan hitl.Decision, 1) // 容量与创建形态对齐 session.go:149
	p := hitl.NewChannelDecisionProvider(decisions)
	ctx := context.Background()

	// 第一轮:窗口 50ms,无决策 → 按窗口超时 reject。
	if d := p.GetDecision(ctx, approvalReq("req-1", time.Now().Add(50*time.Millisecond))); d.Type != hitl.DecisionReject || d.InterruptID != "req-1" {
		t.Fatalf("first GetDecision = %+v, want reject for req-1", d)
	}
	// 重现毒化:agent 侧 Wait 的计时器晚于中间件侧到期,其超时 reject 在
	// GetDecision 返回后才经 streamTokens 入缓冲(session.go:280-283)。
	decisions <- hitl.Decision{InterruptID: "req-1", Type: hitl.DecisionReject}

	// 第二轮:全新窗口。有缺陷的实现微秒级吃到缓冲里的 req-1 reject;
	// 修复后必须排空并等待本窗口。
	got := make(chan hitl.Decision, 1)
	start := time.Now()
	go func() { got <- p.GetDecision(ctx, approvalReq("req-2", time.Now().Add(400*time.Millisecond))) }()

	select {
	case d := <-got:
		t.Fatalf("GetDecision(req-2) returned early after %v: %+v(上一轮的陈旧 reject 未被排空)", time.Since(start), d)
	case <-time.After(100 * time.Millisecond):
	}

	// 本轮自己的决策到达 → 返回该决策(而非上一轮的陈旧 reject)。
	decisions <- hitl.Decision{InterruptID: "req-2", Type: hitl.DecisionApprove}
	select {
	case d := <-got:
		if d.Type != hitl.DecisionApprove || d.InterruptID != "req-2" {
			t.Fatalf("GetDecision(req-2) = %+v, want approve for req-2", d)
		}
		if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
			t.Fatalf("GetDecision(req-2) returned after %v, want it to hold its own window", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetDecision(req-2) did not return after its own decision arrived")
	}
}

// TestChannelDecisionProviderIgnoresLateApprove:sync 路径既有隐患——用户在
// 上一轮窗口关闭后才 approve,迟到决策残留缓冲;下一轮 interrupt 绝不能被它
// 放行:短窗无人决策时必须返回本请求的 reject(而非缓冲里的陈旧 approve)。
func TestChannelDecisionProviderIgnoresLateApprove(t *testing.T) {
	decisions := make(chan hitl.Decision, 1)
	p := hitl.NewChannelDecisionProvider(decisions)
	ctx := context.Background()

	if d := p.GetDecision(ctx, approvalReq("req-1", time.Now().Add(50*time.Millisecond))); d.Type != hitl.DecisionReject {
		t.Fatalf("first GetDecision = %+v, want reject for req-1", d)
	}
	// 迟到的 approve:sync 路径 streamTokens 的投递晚于中间件窗口。
	decisions <- hitl.Decision{InterruptID: "req-1", Type: hitl.DecisionApprove}

	start := time.Now()
	d := p.GetDecision(ctx, approvalReq("req-2", time.Now().Add(50*time.Millisecond)))
	if d.Type != hitl.DecisionReject || d.InterruptID != "req-2" {
		t.Fatalf("GetDecision(req-2) = %+v, want reject for req-2(迟到的 approve 绝不能放行下一个 interrupt)", d)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("GetDecision(req-2) returned after %v, want it to hold its own window", elapsed)
	}
}

// TestChannelDecisionProviderDrainRespectsCtx:排空循环的退出边界是 ctx/超时
// ——缓冲中有陈旧决策且 ctx 已取消时,必须返回本请求的 reject;select 与
// ctx.Done 的竞争双方都不得把陈旧决策当作本请求的决策返回。
func TestChannelDecisionProviderDrainRespectsCtx(t *testing.T) {
	decisions := make(chan hitl.Decision, 1)
	p := hitl.NewChannelDecisionProvider(decisions)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 取消先于调用:ctx.Done 与缓冲中的陈旧决策同时就绪(竞争确定性出口)
	decisions <- hitl.Decision{InterruptID: "req-old", Type: hitl.DecisionApprove}

	d := p.GetDecision(ctx, approvalReq("req-new", time.Now().Add(time.Minute)))
	if d.Type != hitl.DecisionReject || d.InterruptID != "req-new" {
		t.Fatalf("GetDecision with cancelled ctx = %+v, want reject for req-new(陈旧 approve 绝不能在取消后放行)", d)
	}
}
