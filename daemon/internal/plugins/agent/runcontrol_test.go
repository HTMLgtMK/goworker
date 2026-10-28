package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
)

// P5-6:RunControlSource 的 AgentPlugin 实现测试(后台 Run 的 daemon 面)。
// 全部用模拟 Provider(零真实 LLM);固定 provider 形态沿用 newAgentPluginP +
// fixedProvider 先例(plugin_test.go:197-222)。
//
// PR1(总方案 §四/§五)适配:StartRun/CancelRun/RunStatus/ResolveApproval 与
// lease 四操作已迁 agentcontrol.Service,本文件经插件委托面全链路验证——fencing
// 确定性、幂等 single-flight、跨会话独立、接管风暴的语义逐条保留(错误 sentinel
// 现定义在 agentcontrol);幂等表记账断言改经 Service.TrackedRequest(原 reqMu/
// runRequests 内部字段随迁删除)。

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
// RunStatus 至 completed(与持有 RunHandle.Wait 等价的完成观测);未持 lease 的
// 连接 → agentcontrol.ErrControlLeaseLost(第三轮评审 I4 fencing);未注册的 sessionID →
// 明确错误(C3 后按 sessionRuntime 多会话路由,任意未注册 id 在路由层诚实失败,
// ErrSessionNotFound 语义——路由先于 lease 校验)。
func TestStartRunThroughPlugin(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "bg-done"})
	sid := p.CurrentSessionID() // 纯内存形态为空串,经 RouteSession 的 current 回退寻址当前会话

	// fencing(第三轮评审 I4):未持有 lease 的连接一律拒绝。
	if _, _, err := p.StartRun(sid, "conn-other", "hello", "req-fenced"); !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("StartRun without lease err = %v, want agentcontrol.ErrControlLeaseLost", err)
	}
	if p.AcquireControl(sid, "conn-A") {
		t.Fatal("test setup: fresh acquire must not report takeover")
	}

	runID, state, err := p.StartRun(sid, "conn-A", "hello", "req-start-1")
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

	// 未注册的 sessionID → 明确错误(非 current-only 拒绝:同 id 或空串仍可达;
	// 路由先于 lease 校验,ErrSessionNotFound 语义不受 fencing 影响)。
	if _, _, err := p.StartRun("not-a-registered-session", "conn-A", "x", "req-start-2"); !errors.Is(err, runtimeagent.ErrSessionNotFound) {
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
	p.AcquireControl(sid, "conn-A") // fencing(第三轮评审 I4):控制操作须持 lease

	id1, st1, err := p.StartRun(sid, "conn-A", "q", "req-dup")
	if err != nil {
		t.Fatalf("StartRun #1: %v", err)
	}
	if st1 != string(runtimeagent.RunQueued) {
		t.Fatalf("submit state = %q, want queued", st1)
	}

	// 同 requestID 重试:首个 run 可能仍在跑(返回原 runID 幂等短路)或已完成
	// (返回原 runID + 终态快照)——两种时序都必须同 runID、无错误。
	id2, st2, err := p.StartRun(sid, "conn-A", "q", "req-dup")
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
	id3, _, err := p.StartRun(sid, "conn-A", "q", "req-other")
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
// 先于任何 StartRun → SubscribeEvents 经 EnsureHub 即建 hub(遗留③「重启后
// attach 时 hub 未建」的解法,SeqRange 磁盘播种 latest=0);② StartRun 完成后
// active 已清,RunStatus 走 journal 权威路径返回 completed 且 latestSeq > 0
// (事件已落盘);③ hub 复用同一实例(bindEvents 创建恰好一次);④
// RunStatus/SubscribeEvents 对非 current 会话诚实报错。
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

	// ① attach 即建 hub:任何 StartRun 之前 hub 为 nil,SubscribeEvents 经
	// EnsureHub 补建(PR3 订阅契约:返回订阅句柄与游标,hub 指针经 rt.Hub()
	// 观测;EnsureHub 语义由 Service 侧编排,plugin 只余委托)。
	res, err := p.SubscribeEvents(sid, 0)
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	if res.Subscription == nil {
		t.Fatal("SubscribeEvents returned nil subscription")
	}
	defer func() { _ = res.Subscription.Close() }()
	evHub := p.currentSession().Hub()
	if evHub == nil {
		t.Fatal("SubscribeEvents did not create the event hub")
	}
	if res.LatestSeq != 0 {
		t.Fatalf("fresh attach latestSeq = %d, want 0 (no events yet)", res.LatestSeq)
	}

	// ② 后台 Run 完成 → RunStatus 经 journal 路径返回 completed + latestSeq>0。
	p.AcquireControl(sid, "conn-A") // fencing(第三轮评审 I4):控制操作须持 lease
	runID, _, err := p.StartRun(sid, "conn-A", "hello", "req-journal")
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
	if res, err := p.SubscribeEvents("bogus-session", 0); !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("SubscribeEvents with unknown sessionID err = %v, want ErrSessionNotFound", err)
	} else if res.Subscription != nil {
		t.Fatal("SubscribeEvents with unknown sessionID must not return a subscription")
	}
	if _, _, _, err := p.RunStatus(sid, "run-1-1"); err == nil {
		t.Fatal("RunStatus with unknown runID should fail")
	}
}

// failingSeedJournal 是 EventJournal 注错 stub(第三轮评审 I3 测试面):seedErr
// 非 nil 时 SeqRange 注错(模拟 Journal 读取失败),置 nil 恢复;AppendEvent/
// ReadEvents 走内存面。
type failingSeedJournal struct {
	mu      sync.Mutex
	seedErr error
	recs    []session.EventRecord
}

var _ runtimeagent.EventJournal = (*failingSeedJournal)(nil)

func (j *failingSeedJournal) AppendEvent(rec session.EventRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.recs = append(j.recs, rec)
	return nil
}

func (j *failingSeedJournal) ReadEvents(afterSeq uint64) ([]session.EventRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []session.EventRecord
	for _, rec := range j.recs {
		if rec.Seq > afterSeq {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (j *failingSeedJournal) SeqRange() (earliest, latest uint64, ok bool, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.seedErr != nil {
		return 0, 0, false, j.seedErr
	}
	if len(j.recs) == 0 {
		return 0, 0, false, nil
	}
	earliest, latest = j.recs[0].Seq, j.recs[0].Seq
	for _, rec := range j.recs[1:] {
		if rec.Seq < earliest {
			earliest = rec.Seq
		}
		if rec.Seq > latest {
			latest = rec.Seq
		}
	}
	return earliest, latest, true, nil
}

// TestEnsureHubSeedErrorPropagates(第三轮评审 I3 修复验收,daemon 面):会话
// deps.RunEvents 的 SeqRange 播种失败 → SubscribeEvents(Service 经 rt.EnsureHub
// 编排)与 StartRun(manager.StartRun ⑥ bindEvents)显式报错——绝不返回看似
// 健康的订阅或句柄;StartRun 拒绝后零残留(active 已清、幂等表未记账),恢复
// 播种后重提成功(pending/active 无残留的联合证明,同 I1 断言形态)。
func TestEnsureHubSeedErrorPropagates(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "bg-i3"})
	sid := p.CurrentSessionID() // 纯内存形态为空串,RouteSession 的 current 回退寻址

	// 注入播种注错的 EventJournal 并按会话边界重建(SessionDeps 值拷贝,必须
	// refresh 传导进 runtime 的 session deps)。
	cause := errors.New("runs dir unreadable")
	ej := &failingSeedJournal{seedErr: cause}
	p.deps.RunEvents = ej
	p.refreshSession()

	// SubscribeEvents:EnsureHub → NewEventHub 播种失败 → 显式错误,无订阅。
	res, err := p.SubscribeEvents(sid, 0)
	if err == nil {
		t.Fatal("SubscribeEvents with failing SeqRange = nil error, want error(I3:绝不返回看似健康的订阅)")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("SubscribeEvents err = %v, want wrap of %v", err, cause)
	}
	if res.Subscription != nil {
		t.Fatalf("SubscribeEvents returned subscription %+v, want nil", res.Subscription)
	}

	// StartRun:lease 照常(fencing 校验与播种无关),manager.StartRun ⑥ 接线
	// 失败 → 拒绝 + 空 runID。
	p.AcquireControl(sid, "conn-A")
	runID, _, err := p.StartRun(sid, "conn-A", "hello", "req-i3-seed")
	if err == nil {
		t.Fatal("StartRun with failing SeqRange = nil error, want error(I3)")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("StartRun err = %v, want wrap of %v", err, cause)
	}
	if runID != "" {
		t.Fatalf("rejected StartRun returned runID %q, want empty", runID)
	}

	// 无残留:active 已清、幂等表未记账(被拒提交不得入表;记账断言经
	// Service.TrackedRequest——原 reqMu/runRequests 内部字段随迁 agentcontrol)。
	if act := p.RouteSession(sid).Active(); act != nil {
		t.Fatalf("active after rejected StartRun = %+v, want nil", act)
	}
	if _, tracked := p.control.TrackedRequest(p.RouteSession(sid).ID, "req-i3-seed"); tracked {
		t.Fatal("idempotency table recorded a rejected run(拒绝路径不得入表)")
	}

	// 恢复播种后重提成功 = active 清退与 pending 令牌归还没有残留的联合证明;
	// 随后走完正常生命周期。
	ej.seedErr = nil
	runID2, _, err := p.StartRun(sid, "conn-A", "hello-2", "req-i3-retry")
	if err != nil {
		t.Fatalf("StartRun after seeding recovery = %v, want nil(failed wiring left residue)", err)
	}
	waitForRunStatus(t, p, sid, runID2, string(runtimeagent.RunCompleted))
}

// ─────────────────────────────────────────────────────────────────────────────
// C3+I5 修复验收(GPT 评审):多会话路由 + requestId 组合键幂等(第二轮评审
// I5 收窄:幂等作用域是会话,跨会话冲突扫描已删)。
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
// Session B 使 CurrentSessionID 已是 B → CancelRun(A)/RunStatus(A)/
// SubscribeEvents(A)/PendingApprovals(A) 全部可达且作用于 A(A 的 hub 被复用、
// A 的 run 被取消且对 B 不可见),B 的控制面不受影响。
func TestCrossSessionRunControl(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sidA := p.CurrentSessionID()

	// fencing(第三轮评审 I4):两连接各持各会话的 lease。
	p.AcquireControl(sidA, "conn-A")

	// A 的后台 run 提交并等它真实开工(started 关闭 ⇒ run.started 事件已发布)。
	runA, _, err := p.StartRun(sidA, "conn-A", "hello-A", "req-cross-A")
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
	hubA := p.manager.Get(sidA).Hub() // StartRun 已建;PR3 起经订阅契约观察复用
	if hubA == nil {
		t.Fatal("setup: session A's hub not created by StartRun")
	}
	res, err := p.SubscribeEvents(sidA, 0)
	if err != nil {
		t.Fatalf("SubscribeEvents(A): %v", err)
	}
	if res.LatestSeq == 0 {
		t.Fatal("SubscribeEvents(A) latestSeq = 0, want > 0 (run.started was published before the provider gate)")
	}
	if got := p.manager.Get(sidA).Hub(); got != hubA {
		t.Fatal("SubscribeEvents(A) must resolve session A's runtime and reuse its own EventHub")
	}
	_ = res.Subscription.Close()

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
	if err := p.CancelRun(sidA, "conn-A", runA); err != nil {
		t.Fatalf("CancelRun(A, runA): %v", err)
	}
	close(gated.release) // 闸门放行兜底:provider 返回后 execute 层按 runCtx 归一取消态
	waitForRunStatus(t, p, sidA, runA, string(runtimeagent.RunCancelled))

	// 隔离面:A 的 run 不出现在 B 的观测面(journal 按会话隔离)。
	if _, _, _, err := p.RunStatus(sidB, runA); err == nil {
		t.Fatal("RunStatus(B, runA) must not find session A's run")
	}

	// B 的控制面不受影响:B 正常起跑并完成(持自己的 lease)。
	p.AcquireControl(sidB, "conn-B")
	runB, _, err := p.StartRun(sidB, "conn-B", "world-B", "req-cross-B")
	if err != nil {
		t.Fatalf("StartRun(B): %v", err)
	}
	waitForRunStatus(t, p, sidB, runB, string(runtimeagent.RunCompleted))
}

// TestCrossSessionRequestIdempotency(I5 修复验收,第二轮评审 I5 收窄到会话作用
// 域):A 与 B 用同 requestId、不同 Input 各自启动 → 均成功、互不影响(跨会话
// 冲突扫描已删,各自局部递增 requestID 的两客户端不再互相拒绝);同会话同
// requestID:同 Input 幂等返回原 runID/state,不同 Input → conflict。
func TestCrossSessionRequestIdempotency(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "idem-A"})
	sidA := p.CurrentSessionID()
	p.AcquireControl(sidA, "conn-A") // fencing(第三轮评审 I4):控制操作须持 lease

	// A 用 req-x 提交并等终态:后续判定不被 queued/running 时序干扰。
	runA, _, err := p.StartRun(sidA, "conn-A", "payload-A", "req-x")
	if err != nil {
		t.Fatalf("StartRun(A): %v", err)
	}
	waitForRunStatus(t, p, sidA, runA, string(runtimeagent.RunCompleted))

	// 会话边界:current 切到 B。
	sidB := newPersistedSession(t, p, fixedProvider{content: "idem-B"})

	// 第二轮评审 I5:B 用同 requestId、不同 Input → 独立起跑成功(幂等作用域
	// 是会话),拿到自己的新 run,绝不误返 A 的 run。
	p.AcquireControl(sidB, "conn-B")
	runB, _, err := p.StartRun(sidB, "conn-B", "payload-B", "req-x")
	if err != nil {
		t.Fatalf("StartRun(B, same requestID, different input): %v", err)
	}
	if runB == runA {
		t.Fatal("StartRun(B, req-x) returned session A's run (组合键失效)")
	}
	waitForRunStatus(t, p, sidB, runB, string(runtimeagent.RunCompleted))

	// A 的 run 不受 B 提交影响:同键同 Input 幂等返回原 runID 与终态快照。
	id2, st2, err := p.StartRun(sidA, "conn-A", "payload-A", "req-x")
	if err != nil {
		t.Fatalf("StartRun(A, same requestID, same input): %v", err)
	}
	if id2 != runA || st2 != string(runtimeagent.RunCompleted) {
		t.Fatalf("idempotent retry = (%q, %q), want (%q, completed)", id2, st2, runA)
	}

	// 同会话(B)同 requestID(req-x):同 Input 幂等返回原 runID;不同 Input →
	// agentcontrol.ErrRequestConflict(上轮 I5 的 Input 指纹校验收窄到同键范围)。
	id3, _, err := p.StartRun(sidB, "conn-B", "payload-B", "req-x")
	if err != nil || id3 != runB {
		t.Fatalf("same-key retry = (%q, %v), want (%q, nil)", id3, err, runB)
	}
	if _, _, err := p.StartRun(sidB, "conn-B", "payload-B-changed", "req-x"); !errors.Is(err, agentcontrol.ErrRequestConflict) {
		t.Fatalf("same-session same-key different-input err = %v, want agentcontrol.ErrRequestConflict", err)
	}
}

// TestCrossSessionApprovalOwnership(C1 修复验收,真实 RunControlSource 实现):
// 服务端记录的审批归属是授权事实源——会话 B 挂起的审批,由声明 sidA 的调用方
// 应答 → agentcontrol.ErrApprovalOwnership 越权拒绝(即便 sidA 是真实注册会话)且审批不被
// 消费;归属会话本人应答 → 成功且决策送达 Wait;resolved 后归属仍可查(滞留
// 条目),迟到决策报 ErrApprovalResolved 而非归属错误/未知;未知 requestID →
// ErrApprovalUnknown;未知会话 → 路由层 ErrSessionNotFound。修复前 Resolve
// 按 requestID 全局寻址直接投递,无任何归属校验。
func TestCrossSessionApprovalOwnership(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "approval-A"})
	sidA := p.CurrentSessionID()
	sidB := newPersistedSession(t, p, fixedProvider{content: "approval-B"})
	// fencing(第三轮评审 I4):两连接各持各会话的 lease(① 声明 sidA、②③④⑤
	// 声明 sidB 的调用形,均以持照连接发起)。
	p.AcquireControl(sidA, "conn-A")
	p.AcquireControl(sidB, "conn-B")

	// 会话 B 的待决审批:直接经 SDK 审批管理面注册(归属 sidB),模拟 B 的
	// run 命中 HITL(decideFunc → ApprovalManager.Wait 的落账形态)。
	mgr := p.manager.Approvals()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	decided := make(chan hitl.Decision, 1)
	go func() {
		decided <- mgr.Wait(ctx, sidB, "run-b", &hitl.InterruptRequest{
			ID:        "req-b-1",
			ToolName:  "bash",
			Command:   `{"cmd":"ls"}`,
			CreatedAt: time.Now(),
			ExpiresAt: time.Now().Add(30 * time.Second),
		})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := p.manager.ApprovalOwner("req-b-1"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("approval req-b-1 never registered with the manager")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// ① 越权:声明 sidA 应答归属 sidB 的审批 → agentcontrol.ErrApprovalOwnership,审批不被
	// 消费(归属原样,后续归属会话本人仍可应答)。
	err := p.ResolveApproval(sidA, "conn-A", "req-b-1", hitl.Decision{Type: hitl.DecisionApprove})
	if !errors.Is(err, agentcontrol.ErrApprovalOwnership) {
		t.Fatalf("ResolveApproval(sidA, B's approval) err = %v, want agentcontrol.ErrApprovalOwnership", err)
	}
	if owner, ok := p.manager.ApprovalOwner("req-b-1"); !ok || owner != sidB {
		t.Fatalf("ownership after rejected respond = (%q, %v), want (%q, true) (被拒决策不得消费审批)", owner, ok, sidB)
	}

	// ② 未知 requestID → ErrApprovalUnknown 语义的明确错误。
	if err := p.ResolveApproval(sidB, "conn-B", "req-missing", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, runtimeagent.ErrApprovalUnknown) {
		t.Fatalf("ResolveApproval(unknown requestID) err = %v, want ErrApprovalUnknown", err)
	}

	// ③ 归属会话本人应答 → 成功,决策送达 Wait(首个有效决策获胜)。
	if err := p.ResolveApproval(sidB, "conn-B", "req-b-1", hitl.Decision{Type: hitl.DecisionApprove, Message: "go"}); err != nil {
		t.Fatalf("ResolveApproval(sidB, own approval): %v", err)
	}
	select {
	case d := <-decided:
		if d.Type != hitl.DecisionApprove || d.InterruptID != "req-b-1" {
			t.Fatalf("Wait decision = %+v, want approve for req-b-1", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after ResolveApproval")
	}

	// ④ resolved 后归属仍可查(滞留条目,宽限窗内):迟到决策报
	// ErrApprovalResolved 而非归属错误/未知——归属校验不得提前吞掉迟到语义。
	if err := p.ResolveApproval(sidB, "conn-B", "req-b-1", hitl.Decision{Type: hitl.DecisionReject}); !errors.Is(err, runtimeagent.ErrApprovalResolved) {
		t.Fatalf("late ResolveApproval err = %v, want ErrApprovalResolved", err)
	}

	// ⑤ 未知会话 → 路由层 ErrSessionNotFound(在 lease/归属校验之前诚实失败;
	// fencing 的路由序见 StartRun/ResolveApproval 注释)。
	if err := p.ResolveApproval("bogus-session", "conn-B", "req-b-1", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("ResolveApproval(unknown session) err = %v, want ErrSessionNotFound", err)
	}
}

// TestActiveRunNonCurrentSession(第二轮评审 I1 修复验收):Session A 的后台
// run 在途时创建 Session B(current 切到 B,A 的 runtime 保留在册)——
// ActiveRun(A) 必须返回 A 自己的在途快照 (runA, running, true)(修复前
// requireCurrentSession 对非 current 会话恒 ok=false,attach 响应的
// currentRun=null 与「A 确有在途 run」的事实不符);对照:B 无在途 run →
// false;未知会话 → false;A 的 run 终态后 active 清空 → ActiveRun(A) 归位
// false。ActiveRun 的快照与 attach 响应 currentRun 的事实源同观察点
// (rt.Active();PR3 起后者经 SubscribeEvents 的 CurrentRun 快照上 wire,
// wire 面转发契约由 service/acp 的 TestXGWSessionAttachCrossConnectionCurrentRun
// 钉住)。
func TestActiveRunNonCurrentSession(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sidA := p.CurrentSessionID()
	p.AcquireControl(sidA, "conn-A") // fencing(第三轮评审 I4):控制操作须持 lease

	// A 的后台 run 提交并等它真实开工(started 关闭 ⇒ run 已 running)。
	runA, _, err := p.StartRun(sidA, "conn-A", "hello-A", "req-active-A")
	if err != nil {
		t.Fatalf("StartRun(A): %v", err)
	}
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background run did not start in time")
	}

	// 会话边界:current 切到 B(A 的 runtime 保留在册)。
	sidB := newPersistedSession(t, p, fixedProvider{content: "bg-B"})
	if got := p.CurrentSessionID(); got != sidB {
		t.Fatalf("CurrentSessionID = %q, want %q (B)", got, sidB)
	}

	// I1:非 current 会话的在途快照可达,且就是 A 的那个 run。
	runID, state, ok := p.ActiveRun(sidA)
	if !ok {
		t.Fatal("ActiveRun(non-current session with in-flight run) = ok=false, want true")
	}
	if runID != runA {
		t.Fatalf("ActiveRun(A) runID = %q, want %q", runID, runA)
	}
	if state != string(runtimeagent.RunRunning) {
		t.Fatalf("ActiveRun(A) state = %q, want %q", state, runtimeagent.RunRunning)
	}

	// 对照:B 无在途 run → false;未知会话 → false(快照观察,非错误)。
	if _, _, ok := p.ActiveRun(sidB); ok {
		t.Fatal("ActiveRun(B) with no run = ok=true, want false")
	}
	if _, _, ok := p.ActiveRun("bogus-session"); ok {
		t.Fatal("ActiveRun(unknown session) = ok=true, want false")
	}

	// A 的 run 终态后 active 清空:ActiveRun(A) 归位 false(active 语义与
	// RunStatus 的 active 分支同源)。
	close(gated.release)
	waitForRunStatus(t, p, sidA, runA, string(runtimeagent.RunCompleted))
	if _, _, ok := p.ActiveRun(sidA); ok {
		t.Fatal("ActiveRun(A) after the run completed = ok=true, want false (active cleared)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 第三轮评审 I4 修复验收(fencing):lease 校验与控制操作提交的原子性——校验
// 内移 source、与提交同处本会话 guard.mu 临界区(接管 AcquireControl 同锁;
// DE2-I1 后锁粒度=会话,跨会话互不阻塞)。
// ─────────────────────────────────────────────────────────────────────────────

// TestControlFencingTakeoverStorm(第三轮评审 I4 修复验收):A 持 lease,
// 「A 的 ResolveApproval/StartRun/CancelRun」与「B AcquireControl 接管」并发
// 轰炸。断言:操作开始时已观察到接管 → 该操作必返回 agentcontrol.ErrControlLeaseLost
// (fencing 生效:校验通过后到提交完成之间不可能被接管);风暴落定后 A 的全部
// 控制操作一律 agentcontrol.ErrControlLeaseLost;接管前基线提交的审批决策恰好送达一次,
// 风暴期的应答要么接管前成功(送达恰好一次,Manager 单次投递保证)要么被
// fencing 拒绝,绝不出现第二条。
func TestControlFencingTakeoverStorm(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()

	// 预置两笔待决审批(归属 sid,建模 run 命中 HITL 的 ApprovalManager.Wait
	// 落账形态):req-storm-1 由接管前基线消费(「接管前已提交的审批恰好一次」),
	// req-storm-2 供风暴期 A 反复应答(接管后必须全被 fencing 拒绝)。
	mgr := p.manager.Approvals()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	decided1 := make(chan hitl.Decision, 1)
	decided2 := make(chan hitl.Decision, 1)
	mkReq := func(id string) *hitl.InterruptRequest {
		return &hitl.InterruptRequest{
			ID:        id,
			ToolName:  "bash",
			Command:   `{"cmd":"ls"}`,
			CreatedAt: time.Now(),
			ExpiresAt: time.Now().Add(30 * time.Second),
		}
	}
	go func() { decided1 <- mgr.Wait(ctx, sid, "run-storm", mkReq("req-storm-1")) }()
	go func() { decided2 <- mgr.Wait(ctx, sid, "run-storm", mkReq("req-storm-2")) }()
	waitApproval := func(id string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, ok := p.manager.ApprovalOwner(id); ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("approval %s never registered with the manager", id)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	waitApproval("req-storm-1")
	waitApproval("req-storm-2")

	// A 持 lease(空闲获取)。
	if p.AcquireControl(sid, "conn-A") {
		t.Fatal("test setup: fresh acquire must not report takeover")
	}

	// —— 接管前基线:三类控制操作各成功一次,审批决策恰好送达一次 ——
	runID, _, err := p.StartRun(sid, "conn-A", "storm", "req-storm-run")
	if err != nil {
		t.Fatalf("pre-takeover StartRun: %v", err)
	}
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background run did not start in time")
	}
	if err := p.ResolveApproval(sid, "conn-A", "req-storm-1", hitl.Decision{Type: hitl.DecisionApprove}); err != nil {
		t.Fatalf("pre-takeover ResolveApproval: %v", err)
	}
	select {
	case d := <-decided1:
		if d.Type != hitl.DecisionApprove || d.InterruptID != "req-storm-1" {
			t.Fatalf("delivered decision = %+v, want approve for req-storm-1", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("approval decision not delivered")
	}
	if err := p.CancelRun(sid, "conn-A", runID); err != nil {
		t.Fatalf("pre-takeover CancelRun: %v", err)
	}
	close(gated.release) // 已取消,兜底放行(provider 的 ctx.Done 分支先命中)

	// —— 接管风暴:B 反复接管+释放(制造「conn-B 持有 / 空闲」翻转,A 永非
	// 持有者);A 并发反复三类控制操作。A 每个操作前快照接管标志:已观察到接管
	// (B 的 tookOver=true 已返回 ⇒ 本会话 guard 内 A 的租约已被夺走)→ 操作必返回
	// agentcontrol.ErrControlLeaseLost;未观察到的窗口内成功/业务错误(ErrApprovalResolved、
	// ErrRunNotFound)/并发接管皆可,由落定后的终态断言收束。
	var takenOver atomic.Bool
	const iters = 100
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // B:接管风暴
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if p.AcquireControl(sid, "conn-B") {
				takenOver.Store(true)
			}
			p.ReleaseControl(sid, "conn-B")
		}
	}()
	go func() { // A:控制操作轰炸
		defer wg.Done()
		assertFenced := func(op string, observedOver bool, err error) {
			if !observedOver {
				return // 接管未观察到的窗口:交由终态断言收束
			}
			if !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
				t.Errorf("%s after observed takeover err = %v, want agentcontrol.ErrControlLeaseLost (fencing)", op, err)
			}
		}
		for i := 0; i < iters; i++ {
			over := takenOver.Load()
			_, _, errS := p.StartRun(sid, "conn-A", "storm", "req-storm-run")
			assertFenced("StartRun", over, errS)

			over = takenOver.Load()
			errR := p.ResolveApproval(sid, "conn-A", "req-storm-2", hitl.Decision{Type: hitl.DecisionApprove})
			assertFenced("ResolveApproval", over, errR)

			over = takenOver.Load()
			errC := p.CancelRun(sid, "conn-A", runID)
			assertFenced("CancelRun", over, errC)
		}
	}()
	wg.Wait()

	// —— 终态断言(风暴落定,B 的末次操作是释放 ⇒ lease 空闲,A 非持有者):
	// fencing 的确定性收束,A 的全部控制操作一律明确错误 ——
	if _, _, err := p.StartRun(sid, "conn-A", "storm", "req-storm-run"); !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("post-storm StartRun err = %v, want agentcontrol.ErrControlLeaseLost", err)
	}
	if err := p.ResolveApproval(sid, "conn-A", "req-storm-2", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("post-storm ResolveApproval err = %v, want agentcontrol.ErrControlLeaseLost", err)
	}
	if err := p.CancelRun(sid, "conn-A", runID); !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("post-storm CancelRun err = %v, want agentcontrol.ErrControlLeaseLost", err)
	}

	// 接管前已提交的审批恰好一次:req-storm-1 的决策已在基线送达;风暴期 A 对
	// req-storm-2 的应答要么接管前成功(决策随 Resolve 内联进缓冲通道,至多
	// 一条——Manager 单次投递),要么被 fencing 拒绝(零送达)。wg.Wait 后
	// 非阻塞取数即终态,无需宽限等待。
	select {
	case d := <-decided2:
		if d.Type != hitl.DecisionApprove {
			t.Fatalf("storm-period delivered decision = %+v, want approve", d)
		}
	default: // 零送达:A 的风暴应答全部发生在接管后(fencing 拒绝)——同样合法
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 第四轮评审 DE2-I1/DE2-M1 修复验收:per-session fencing guard——①确定性
// fencing(无竞态依赖,接管后旧持有者的三类控制操作必被拒);②跨会话独立
// (某会话 StartRun 同步段的 journal fsync 挂起不再阻塞其他会话的控制面)。
// ─────────────────────────────────────────────────────────────────────────────

// TestControlFencingDeterministicTakeover(DE2-M1 修复验收):纯顺序、零
// goroutine 的确定性 fencing 断言——风暴测试(TestControlFencingTakeoverStorm)
// 的「接管先于操作」由 goroutine 调度决定,本测试把时序钉死:A 空闲获取 →
// A 未释放时 B 接管(tookOver=true)→ A 的 StartRun/ResolveApproval/CancelRun
// 全部 agentcontrol.ErrControlLeaseLost。fencing 校验先于幂等表/审批归属/manager 存在性判
// 定(请求ID 未记账、审批不存在、run 不存在都不影响拒绝语义),且被拒的
// StartRun 不得入幂等表。
func TestControlFencingDeterministicTakeover(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "fence-det"})
	sid := p.CurrentSessionID() // 纯内存形态为空串,是合法租约键(见 AcquireControl 注释)

	// ① A 空闲获取:非接管。
	if p.AcquireControl(sid, "conn-A") {
		t.Fatal("fresh acquire must not report takeover")
	}

	// ② A 未释放时 B 接管:tookOver=true,持有权从 A 移到 B。
	if !p.AcquireControl(sid, "conn-B") {
		t.Fatal("acquire while conn-A holds the lease must report tookOver=true")
	}
	if p.HoldControl(sid, "conn-A") {
		t.Fatal("previous holder must lose the lease after the deterministic takeover")
	}
	if !p.HoldControl(sid, "conn-B") {
		t.Fatal("taker must hold the lease after the deterministic takeover")
	}

	// ③ A 的三类控制操作全部 agentcontrol.ErrControlLeaseLost:守卫拒绝先于一切提交判定。
	if _, _, err := p.StartRun(sid, "conn-A", "fenced", "req-det"); !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("fenced StartRun err = %v, want agentcontrol.ErrControlLeaseLost", err)
	}
	if err := p.ResolveApproval(sid, "conn-A", "req-det-approval", hitl.Decision{Type: hitl.DecisionApprove}); !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("fenced ResolveApproval err = %v, want agentcontrol.ErrControlLeaseLost", err)
	}
	if err := p.CancelRun(sid, "conn-A", "run-det"); !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("fenced CancelRun err = %v, want agentcontrol.ErrControlLeaseLost", err)
	}

	// ④ 被拒的 StartRun 不得入幂等表(fencing 拒绝发生在幂等查表之前;断言经
	// Service.TrackedRequest,原 reqMu/runRequests 内部字段随迁 agentcontrol)。
	if _, tracked := p.control.TrackedRequest(p.RouteSession(sid).ID, "req-det"); tracked {
		t.Fatal("fenced StartRun recorded an idempotency entry (拒绝路径不得入表)")
	}
}

// gatedRunJournal 是 RunStateJournal 闸门包装(DE2-I1 修复验收的确定性接缝,
// 挂在生产真实存在的 journal 写点上,非测试专用执行分支):首个 AppendState
// (StartRun 同步段 ⑤ 的 queued 记录 durable-first,含 journal fsync)挂起在
// release 上,模拟本会话慢盘;close(release) 后透传内嵌的真实 store——
// queued/running/completed 记录照常落账,RunStatus 的 journal 权威分支不受影
// 响。entered 在进入 AppendState 时关闭,给测试一个确定性的「已进入同步段写
// 点(此刻 guard 必被本会话 StartRun 持有)」信号。
type gatedRunJournal struct {
	*session.RunJournalStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

var _ runtimeagent.RunStateJournal = (*gatedRunJournal)(nil)

func (j *gatedRunJournal) AppendState(rec session.RunStateRecord) error {
	j.once.Do(func() { close(j.entered) })
	<-j.release
	return j.RunJournalStore.AppendState(rec)
}

// TestCrossSessionStartRunNotBlockedByFsync(DE2-I1 修复验收):会话 A 的
// StartRun 停在同步段 journal 写点(guard 持有中,fsync 挂起)期间——
// ① 会话 B 的 AcquireControl/StartRun 照常完成(per-session guard:慢盘不再
// 跨会话串行;修复前全局 leaseMu 会把 A 的慢 fsync 放大为 B 的控制面卡顿);
// ② B 对 A 会话的接管在 A 提交完成前无法插入(校验+提交同临界区,fencing 在
// guard 锁内线性化——确定性断言,不依赖调度);③ 放行后 A 的提交完成、接管
// 随后发生,且 A 已提交的后台 Run 不被接管撤销(DE2-I2 语义)、两会话的 run
// 各自收敛 completed。
func TestCrossSessionStartRunNotBlockedByFsync(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "cross-fsync"})
	sidA := p.CurrentSessionID()

	// 会话 A 的 Run journal 换成闸门包装(内嵌真实 store,事件面 RunEvents 不
	// 动):refreshSession 重建 current runtime,新会话本体吃进闸门 journal。
	real := p.runJournalStore()
	if real == nil {
		t.Fatal("session A has no run journal store (persistence disabled?)")
	}
	gated := &gatedRunJournal{
		RunJournalStore: real,
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	p.deps.RunJournal = gated
	p.refreshSession()
	if got := p.CurrentSessionID(); got != sidA {
		t.Fatalf("refreshSession changed session id = %q, want %q", got, sidA)
	}

	if p.AcquireControl(sidA, "conn-A") {
		t.Fatal("test setup: fresh acquire must not report takeover")
	}

	// A 的 StartRun 提交后停在同步段写点(guard 持有中)。
	startedA := make(chan struct{}, 1)
	var startErrA error
	var runA string
	go func() {
		id, _, err := p.StartRun(sidA, "conn-A", "slow-A", "req-fsync-A")
		runA, startErrA = id, err
		close(startedA)
	}()
	select {
	case <-gated.entered:
	case <-startedA:
		t.Fatalf("StartRun(A) returned (%q, %v) before entering the queued-record write", runA, startErrA)
	case <-time.After(5 * time.Second):
		t.Fatal("StartRun(A) never reached the queued-record journal write")
	}

	// ① 跨会话独立:A 挂起期间,B 的控制面照常(先建会话 B 再获取/提交)。
	sidB := newPersistedSession(t, p, fixedProvider{content: "fast-B"})
	if p.AcquireControl(sidB, "conn-B") {
		t.Fatal("acquire on idle session B must not report takeover (A 的慢 fsync 不得牵连 B)")
	}
	runB, _, err := p.StartRun(sidB, "conn-B", "fast-B", "req-fsync-B")
	if err != nil {
		t.Fatalf("StartRun(B) while A's fsync is stuck: %v (跨会话控制操作被串行化,DE2-I1 回归)", err)
	}

	// ② fencing 线性化(确定性):A 持 guard 提交期间,B 对会话 A 的接管无法
	// 插入。接管 goroutine 在放行前不得完成——若完成即 fencing 破洞(校验与
	// 提交不在同一临界区)。
	takeover := make(chan bool, 1)
	go func() { takeover <- p.AcquireControl(sidA, "conn-B") }()
	select {
	case <-takeover:
		t.Fatal("takeover of session A completed while conn-A's fenced commit is still in flight (guard 未覆盖提交段)")
	case <-time.After(100 * time.Millisecond):
		// 仍在等待 A 的 guard:线性化语义成立(接管被挡在提交临界区外)。
	}

	// ③ 放行 A 的 fsync:A 的提交完成,接管随后发生;DE2-I2:接管只影响后续
	// 控制操作,不撤销已成功提交的后台 Run。
	close(gated.release)
	select {
	case <-startedA:
		if startErrA != nil {
			t.Fatalf("StartRun(A) after release: %v", startErrA)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StartRun(A) did not return after the fsync gate was released")
	}
	select {
	case took := <-takeover:
		if !took {
			t.Fatal("takeover after conn-A's commit completed must report tookOver=true")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("takeover of session A did not complete after the fenced commit finished")
	}
	if p.HoldControl(sidA, "conn-A") {
		t.Fatal("conn-A must lose session A's lease after the takeover")
	}

	// 收束:A 的 run 不受接管影响继续完成;B 的 run 正常完成(两会话事件面/
	// journal 各自隔离,RunStatus 走各自会话的 journal 权威分支)。
	waitForRunStatus(t, p, sidA, runA, string(runtimeagent.RunCompleted))
	waitForRunStatus(t, p, sidB, runB, string(runtimeagent.RunCompleted))
}
