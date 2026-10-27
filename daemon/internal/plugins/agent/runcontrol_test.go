package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// P5-6:RunControlSource 的 AgentPlugin 实现测试(后台 Run 的 daemon 面)。
// 全部用模拟 Provider(零真实 LLM);固定 provider 形态沿用 newAgentPluginP +
// fixedProvider 先例(plugin_test.go:197-222)。

// waitForRunStatus 轮询 RunStatus 直至 want 状态或超时(P5-6 的 RunStatus 是
// 唯一跨包可用的状态观测面;StartRun 返回的 RunHandle 归 supervisor,daemon
// 侧不持句柄)。
func waitForRunStatus(t *testing.T, p *AgentPlugin, sid, runID, want string) (string, uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, latest, _, err := p.RunStatus(sid, runID)
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

// TestStartRunThroughPlugin(P5-6):经插件 StartRun 提交后台 Run → 轮询
// RunStatus 至 completed(与持有 RunHandle.Wait 等价的完成观测);未注册的
// sessionID → 明确错误(C3 后按 sessionRuntime 多会话路由,任意未注册 id 在
// 路由层诚实失败,ErrSessionNotFound 语义)。
func TestStartRunThroughPlugin(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "bg-done"})
	sid := p.CurrentSessionID() // 纯内存形态为空串,经 sessionRuntime 的 current 回退寻址当前会话

	runID, state, err := p.StartRun(sid, "hello", "req-start-1")
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if runID == "" {
		t.Fatal("StartRun returned empty runID")
	}
	// 同步段 durable-first 落盘 queued 记录后才返回,提交时状态恒为 queued。
	if state != string(runtimeagent.RunQueued) {
		t.Fatalf("submit state = %q, want %q", state, runtimeagent.RunQueued)
	}

	got, _, _, err := p.RunStatus(sid, runID)
	if err != nil {
		t.Fatalf("RunStatus: %v", err)
	}
	if got != string(runtimeagent.RunQueued) && got != string(runtimeagent.RunRunning) && got != string(runtimeagent.RunCompleted) {
		t.Fatalf("RunStatus = %q, want a live/terminal run state", got)
	}
	waitForRunStatus(t, p, sid, runID, string(runtimeagent.RunCompleted))

	// 未注册的 sessionID → 明确错误(非 current-only 拒绝:同 id 或空串仍可达)。
	if _, _, err := p.StartRun("not-a-registered-session", "x", "req-start-2"); !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("StartRun with unknown sessionID err = %v, want ErrSessionNotFound", err)
	}
}

// TestCapabilityWaitTimeoutClampedToToolCtxBudget(P4 评审 issue2):daemon 出口
// 与 SDK 侧 resolveCapabilityWait 同型钳制——cfg.Session.CapabilityWaitTimeout
// >55s 钳到 55s 并告警(硬约束 < ai-core 工具执行 60s,55s 给 Invoke 留余量),
// <=0 兜底 45s(拍板③),1~55s 区间原样放行。
func TestCapabilityWaitTimeoutClampedToToolCtxBudget(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := &AgentPlugin{hub: hub, cfg: cfg} // 只读 cfg 面,无需 Init

	cfg.Session.CapabilityWaitTimeout = 90
	if got := p.CapabilityWaitTimeout(); got != 55*time.Second {
		t.Fatalf("CapabilityWaitTimeout = %v, want 55s(>55s 配置被钳制)", got)
	}
	cfg.Session.CapabilityWaitTimeout = 0
	if got := p.CapabilityWaitTimeout(); got != 45*time.Second {
		t.Fatalf("CapabilityWaitTimeout = %v, want 45s(拍板③默认)", got)
	}
	cfg.Session.CapabilityWaitTimeout = 30
	if got := p.CapabilityWaitTimeout(); got != 30*time.Second {
		t.Fatalf("CapabilityWaitTimeout = %v, want 30s(区间内原样放行)", got)
	}
}

// TestRequestIdempotency(P5-6):同 requestID 重试 → 同 runID(不重复起跑),
// 状态取当前快照;不同 requestID → 新 runID。
func TestRequestIdempotency(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "idem"})
	sid := p.CurrentSessionID()

	id1, st1, err := p.StartRun(sid, "q", "req-dup")
	if err != nil {
		t.Fatalf("StartRun #1: %v", err)
	}
	if st1 != string(runtimeagent.RunQueued) {
		t.Fatalf("submit state = %q, want queued", st1)
	}

	// 同 requestID 重试:首个 run 可能仍在跑(返回原 runID 幂等短路)或已完成
	// (返回原 runID + 终态快照)——两种时序都必须同 runID、无错误。
	id2, st2, err := p.StartRun(sid, "q", "req-dup")
	if err != nil {
		t.Fatalf("StartRun retry: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("retry runID = %q, want original %q", id2, id1)
	}
	switch runtimeagent.RunState(st2) {
	case runtimeagent.RunQueued, runtimeagent.RunRunning, runtimeagent.RunCompleted:
	default:
		t.Fatalf("retry state = %q, want queued/running/completed", st2)
	}

	// 等首个 run 终态后再用不同 requestID:必须起新 run,不复用。
	waitForRunStatus(t, p, sid, id1, string(runtimeagent.RunCompleted))
	id3, _, err := p.StartRun(sid, "q", "req-other")
	if err != nil {
		t.Fatalf("StartRun #3: %v", err)
	}
	if id3 == id1 {
		t.Fatal("different requestID reused the original runID")
	}
	waitForRunStatus(t, p, sid, id3, string(runtimeagent.RunCompleted))
}

// TestControlLeaseTakeover(P5-6,拍板②;I3 后按会话记账):A 占会话 X → B 对
// 同一会话显式接管(tookOver=true)→ A 的 Hold(X) 失效;非持有者 Release 无
// 操作;持有者 Release 后该会话归还空闲。
func TestControlLeaseTakeover(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "lease"})
	sid := p.CurrentSessionID() // 纯内存形态为空串,是合法租约键(见 AcquireControl 注释)

	if p.AcquireControl(sid, "conn-A") {
		t.Fatal("first acquire on an idle lease must not report takeover")
	}
	if !p.HoldControl(sid, "conn-A") {
		t.Fatal("conn-A should hold the lease after acquiring")
	}
	if p.AcquireControl(sid, "conn-A") {
		t.Fatal("re-acquire by the same holder must not report takeover")
	}
	if !p.AcquireControl(sid, "conn-B") {
		t.Fatal("second holder must explicitly take over (tookOver=true)")
	}
	if p.HoldControl(sid, "conn-A") {
		t.Fatal("previous holder must lose the lease after takeover")
	}
	if !p.HoldControl(sid, "conn-B") {
		t.Fatal("new holder should hold the lease after takeover")
	}
	p.ReleaseControl(sid, "conn-A") // 非持有者:无操作
	if !p.HoldControl(sid, "conn-B") {
		t.Fatal("release by a non-holder must not clear the lease")
	}
	p.ReleaseControl(sid, "conn-B")
	if p.HoldControl(sid, "conn-B") {
		t.Fatal("lease should be idle after the holder releases it")
	}
}

// TestControlLeasePerSession(I3 修复验收,拍板②原文「每会话至多一个 control
// 持有者」):两连接分别控制两会话互不影响(I3 修复前全局单字符串,B 的
// acquire 会顶掉 A);B 接管 A 的会话 X 后 A 的 Hold(X)==false 且 B 在其他
// 会话的持有不受牵连;ReleaseConnection 只清该连接自己的全部租约;空
// connectionID 一律拒绝。
func TestControlLeasePerSession(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "lease-A"})
	sidA := p.CurrentSessionID()
	sidB := newPersistedSession(t, p, fixedProvider{content: "lease-B"})

	// 两连接分别控制两会话:均空闲获取,互不影响。
	if p.AcquireControl(sidA, "conn-A") {
		t.Fatal("acquire on idle session A must not report takeover")
	}
	if p.AcquireControl(sidB, "conn-B") {
		t.Fatal("acquire on idle session B must not report takeover (A's lease is on session A)")
	}
	if !p.HoldControl(sidA, "conn-A") || !p.HoldControl(sidB, "conn-B") {
		t.Fatal("each connection must hold its own session's lease")
	}
	if p.HoldControl(sidA, "conn-B") || p.HoldControl(sidB, "conn-A") {
		t.Fatal("per-session lease must not leak across sessions")
	}

	// B 接管 A 的会话 X=sidA:tookOver=true;A 的 Hold(X)==false;B 在 sidB
	// 的持有不受牵连;A 在 sidB 本就无持有。
	if !p.AcquireControl(sidA, "conn-B") {
		t.Fatal("takeover of a held lease must report tookOver=true")
	}
	if p.HoldControl(sidA, "conn-A") {
		t.Fatal("evicted holder must lose the lease on the taken-over session")
	}
	if !p.HoldControl(sidA, "conn-B") {
		t.Fatal("taker must hold the lease on the taken-over session")
	}
	if !p.HoldControl(sidB, "conn-B") {
		t.Fatal("takeover on session A must not disturb the taker's lease on session B")
	}

	// 释放语义:非持有者按会话释放无操作;持有者按会话释放只清该会话。
	p.ReleaseControl(sidA, "conn-A") // 已被接管,无操作
	if !p.HoldControl(sidA, "conn-B") {
		t.Fatal("release by the evicted holder must not clear the new holder's lease")
	}
	p.ReleaseControl(sidB, "conn-B")
	if p.HoldControl(sidB, "conn-B") || p.HoldControl(sidB, "conn-A") {
		t.Fatal("holder's per-session release must clear exactly that session")
	}
	if !p.HoldControl(sidA, "conn-B") {
		t.Fatal("per-session release on B must not disturb the lease on A")
	}

	// ReleaseConnection:conn-B 重新占住 sidB(空闲重取,非接管)后,一次调用
	// 清掉 B 的全部租约(sidA + sidB),A(本测试中未持有)不受影响。
	if p.AcquireControl(sidB, "conn-B") {
		t.Fatal("re-acquire on released session B must be a fresh acquire (tookOver=false)")
	}
	p.ReleaseConnection("conn-B")
	if p.HoldControl(sidA, "conn-B") || p.HoldControl(sidB, "conn-B") {
		t.Fatal("ReleaseConnection must clear every lease held by the connection")
	}
	if p.AcquireControl(sidA, "conn-A") {
		t.Fatal("session A must be idle again after conn-B's leases were released (fresh acquire, no takeover)")
	}

	// 空 connectionID:Acquire/Hold/Release/ReleaseConnection 一律拒绝或无操作。
	if p.AcquireControl(sidA, "") {
		t.Fatal("empty connectionID acquire must be rejected")
	}
	if p.HoldControl(sidA, "") {
		t.Fatal("empty connectionID must never hold a lease")
	}
	p.ReleaseControl(sidA, "")
	p.ReleaseConnection("")
	if !p.HoldControl(sidA, "conn-A") {
		t.Fatal("empty connectionID calls must not disturb existing leases")
	}
}

// TestAttachEnsureHubAndJournalStatus(P5-6 追加覆盖,持久化变体):① attach
// 先于任何 StartRun → EnsureHub 即建 hub(遗留③「重启后 attach 时 hub 未建」
// 的解法,SeqRange 磁盘播种 latest=0);② StartRun 完成后 active 已清,
// RunStatus 走 journal 权威路径返回 completed 且 latestSeq > 0(事件已落盘);
// ③ hub 复用同一实例(bindEvents 创建恰好一次);④ RunStatus/Attach 对非
// current 会话诚实报错。
func TestAttachEnsureHubAndJournalStatus(t *testing.T) {
	root := t.TempDir()
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = root
	cfg.Session.Enabled = true
	cfg.Memory.Enabled = false
	hub := initHub(cfg)
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) {
		return fixedProvider{content: "bg-journal"}, nil
	}
	p.refreshSession()

	sid := p.CurrentSessionID()
	if sid == "" {
		t.Fatal("no current session id (store mount failed)")
	}
	if got := p.CapabilityWaitTimeout(); got != 45*time.Second {
		t.Fatalf("CapabilityWaitTimeout = %v, want 45s (拍板③默认)", got)
	}

	// ① attach 即建 hub:任何 StartRun 之前 hub 为 nil,EnsureHub 补建。
	evHub, latest, err := p.Attach(sid)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if evHub == nil {
		t.Fatal("Attach returned nil hub")
	}
	if latest != 0 {
		t.Fatalf("fresh attach latest = %d, want 0 (no events yet)", latest)
	}

	// ② 后台 Run 完成 → RunStatus 经 journal 路径返回 completed + latestSeq>0。
	runID, _, err := p.StartRun(sid, "hello", "req-journal")
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	_, latestSeq := waitForRunStatus(t, p, sid, runID, string(runtimeagent.RunCompleted))
	if latestSeq == 0 {
		t.Fatalf("RunStatus latestSeq = 0, want > 0 (run lifecycle events were published)")
	}

	// ③ hub 复用同一实例:StartRun 的 bindEvents 不得再建第二个 hub。
	if got := p.currentSession().Hub(); got != evHub {
		t.Fatal("EnsureHub and StartRun must share one EventHub instance (创建恰好一次)")
	}

	// ④ 未注册的 sessionID → 诚实报错(C3 后是路由层 ErrSessionNotFound 语义)。
	if _, _, _, err := p.RunStatus("bogus-session", runID); !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("RunStatus with unknown sessionID err = %v, want ErrSessionNotFound", err)
	}
	if _, _, err := p.Attach("bogus-session"); !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("Attach with unknown sessionID err = %v, want ErrSessionNotFound", err)
	}
	if _, _, _, err := p.RunStatus(sid, "run-1-1"); err == nil {
		t.Fatal("RunStatus with unknown runID should fail")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// C3+I5 修复验收(GPT 评审):多会话路由 + 跨会话 requestId 幂等。
// ─────────────────────────────────────────────────────────────────────────────

// gatedProvider 是 daemon 侧测试的闸门 provider:首轮 Chat 关闭 started(测试
// 确认 run 已真实开工)并挂在 release 上;select ctx.Done 使 CancelRun 后的
// runCtx 取消能直接解锁(ai-runtime/agent run_lifecycle_test.go gatedProvider
// 先例的 daemon 自持版)。ChatStream 返回错误,Session 回退非流式。
type gatedProvider struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *gatedProvider) Name() string  { return "gated" }
func (p *gatedProvider) Model() string { return "m" }

func (p *gatedProvider) Chat(ctx context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "bg-gated"}}}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *gatedProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// newPersistedSession 模拟 /new 的会话边界:repo 复用 → repo.Create 新 store →
// RunJournalStore 同源注入 deps → refreshSession(新 id 注册置 current,旧
// runtime 保留在册),返回新会话的稳定 SessionID。手工重建 deps 而不调
// startSession 的原因:startSession 全量组装会把 NewProvider 换回真实 provider
// 工厂,覆盖测试注入的固定 provider;这里按 startSession 的组装序只重建测试
// 需要的段,会话边界语义(新 id 注册、current 切换)与生产一致。
func newPersistedSession(t *testing.T, p *AgentPlugin, pv core.Provider) string {
	t.Helper()
	if p.repo == nil {
		t.Fatal("repo is nil: session persistence disabled (cfg.Session.Enabled=false?)")
	}
	st, err := p.repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatalf("repo.Create: %v", err)
	}
	journalStore := session.NewRunJournalStore(p.repo, st)
	p.deps.Store = st
	p.deps.RunJournal = journalStore
	p.deps.RunEvents = journalStore
	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) { return pv, nil }
	p.refreshSession()
	return st.SessionID()
}

// newPersistedPlugin 构造持久化启用的插件(跨会话测试的公共底座):真实
// NewPlugin+Init(真实 startSession 建 repo/首会话),测试再按生产形状注入
// 固定 provider 重建首会话(refreshSession 同 TestAttachEnsureHubAndJournalStatus)。
func newPersistedPlugin(t *testing.T, pv core.Provider) *AgentPlugin {
	t.Helper()
	root := t.TempDir()
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = root
	cfg.Session.Enabled = true
	cfg.Memory.Enabled = false
	hub := initHub(cfg)
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })
	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) { return pv, nil }
	p.refreshSession()
	if id := p.CurrentSessionID(); id == "" {
		t.Fatal("no current session id (store mount failed)")
	}
	return p
}

// TestCrossSessionRunControl(C3 修复验收):Session A 后台 run 运行中,创建
// Session B 使 CurrentSessionID 已是 B → CancelRun(A)/RunStatus(A)/Attach(A)/
// PendingApprovals(A) 全部可达且作用于 A(A 的 hub 被复用、A 的 run 被取消且
// 对 B 不可见),B 的控制面不受影响。
func TestCrossSessionRunControl(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sidA := p.CurrentSessionID()

	// A 的后台 run 提交并等它真实开工(started 关闭 ⇒ run.started 事件已发布)。
	runA, _, err := p.StartRun(sidA, "hello-A", "req-cross-A")
	if err != nil {
		t.Fatalf("StartRun(A): %v", err)
	}
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background run did not start in time")
	}

	// 会话边界:创建 Session B,current 切到 B(A 的 runtime 保留在册)。
	sidB := newPersistedSession(t, p, fixedProvider{content: "bg-B"})
	if sidB == sidA {
		t.Fatal("new session reused session A's id")
	}
	if got := p.CurrentSessionID(); got != sidB {
		t.Fatalf("CurrentSessionID = %q, want %q (B)", got, sidB)
	}

	// C3:current=B 时,A 的控制面全部可达,且作用于 A 自己的 runtime/journal。
	hubA, latest, err := p.Attach(sidA)
	if err != nil {
		t.Fatalf("Attach(A): %v", err)
	}
	if latest == 0 {
		t.Fatal("Attach(A) latest = 0, want > 0 (run.started was published before the provider gate)")
	}
	if got := p.manager.Get(sidA).Hub(); got != hubA {
		t.Fatal("Attach(A) must resolve session A's runtime and reuse its own EventHub")
	}

	state, _, _, err := p.RunStatus(sidA, runA)
	if err != nil {
		t.Fatalf("RunStatus(A, runA): %v", err)
	}
	if state != string(runtimeagent.RunRunning) {
		t.Fatalf("RunStatus(A, runA) = %q, want running", state)
	}
	pending, err := p.PendingApprovals(sidA)
	if err != nil || pending == nil {
		t.Fatalf("PendingApprovals(A) = %v, %v; want empty slice without error", pending, err)
	}

	// CancelRun(A) 作用于 A:取消被接受,A 的 run 收敛 cancelled。
	if err := p.CancelRun(sidA, runA); err != nil {
		t.Fatalf("CancelRun(A, runA): %v", err)
	}
	close(gated.release) // 闸门放行兜底:provider 返回后 execute 层按 runCtx 归一取消态
	waitForRunStatus(t, p, sidA, runA, string(runtimeagent.RunCancelled))

	// 隔离面:A 的 run 不出现在 B 的观测面(journal 按会话隔离)。
	if _, _, _, err := p.RunStatus(sidB, runA); err == nil {
		t.Fatal("RunStatus(B, runA) must not find session A's run")
	}

	// B 的控制面不受影响:B 正常起跑并完成。
	runB, _, err := p.StartRun(sidB, "world-B", "req-cross-B")
	if err != nil {
		t.Fatalf("StartRun(B): %v", err)
	}
	waitForRunStatus(t, p, sidB, runB, string(runtimeagent.RunCompleted))
}

// TestCrossSessionRequestIdempotency(I5 修复验收):A 与 B 用同 requestId 不同
// Input → B 返回显式 conflict 而非误返 A 的 run;同键同 Input(current 已切走,
// A 的重试按 sessionID 路由)→ 幂等返回原 runID;B 自己的键同 Input 幂等、
// 不同 Input 冲突。
func TestCrossSessionRequestIdempotency(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "idem-A"})
	sidA := p.CurrentSessionID()

	// A 用 req-x 提交并等终态:后续判定不被 queued/running 时序干扰。
	runA, _, err := p.StartRun(sidA, "payload-A", "req-x")
	if err != nil {
		t.Fatalf("StartRun(A): %v", err)
	}
	waitForRunStatus(t, p, sidA, runA, string(runtimeagent.RunCompleted))

	// 会话边界:current 切到 B。
	sidB := newPersistedSession(t, p, fixedProvider{content: "idem-B"})

	// I5:B 用同 requestId、不同 Input → conflict 错误,绝不返回 A 的 run。
	id, _, err := p.StartRun(sidB, "payload-B", "req-x")
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("StartRun(B, same requestID, different input) err = %v, want ErrRequestConflict", err)
	}
	if id != "" {
		t.Fatalf("conflicting cross-session submit returned runID %q (must not be A's run %q)", id, runA)
	}

	// 同键同 Input(current 已切到 B,A 的重试按 sessionID 路由)→ 幂等返回
	// 原 runID 与其终态快照。
	id2, st2, err := p.StartRun(sidA, "payload-A", "req-x")
	if err != nil {
		t.Fatalf("StartRun(A, same requestID, same input): %v", err)
	}
	if id2 != runA || st2 != string(runtimeagent.RunCompleted) {
		t.Fatalf("idempotent retry = (%q, %q), want (%q, completed)", id2, st2, runA)
	}

	// B 自己的键:同键同 Input 幂等返回;同键不同 Input 冲突。
	id3, _, err := p.StartRun(sidB, "payload-B1", "req-y")
	if err != nil {
		t.Fatalf("StartRun(B, req-y): %v", err)
	}
	waitForRunStatus(t, p, sidB, id3, string(runtimeagent.RunCompleted))
	id4, _, err := p.StartRun(sidB, "payload-B1", "req-y")
	if err != nil || id4 != id3 {
		t.Fatalf("same-key retry = (%q, %v), want (%q, nil)", id4, err, id3)
	}
	if _, _, err := p.StartRun(sidB, "payload-B1-changed", "req-y"); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("same-key different-input err = %v, want ErrRequestConflict", err)
	}
}
