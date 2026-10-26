package service

import (
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
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
		state, latest, err := p.RunStatus(sid, runID)
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
// RunStatus 至 completed(与持有 RunHandle.Wait 等价的完成观测);sessionID
// 不匹配 → 明确错误(拍板⑤:V1 仅 current 会话可路由)。
func TestStartRunThroughPlugin(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "bg-done"})
	sid := p.CurrentSessionID() // 纯内存形态为空串,与 requireCurrentSession 的匹配语义一致

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

	got, _, err := p.RunStatus(sid, runID)
	if err != nil {
		t.Fatalf("RunStatus: %v", err)
	}
	if got != string(runtimeagent.RunQueued) && got != string(runtimeagent.RunRunning) && got != string(runtimeagent.RunCompleted) {
		t.Fatalf("RunStatus = %q, want a live/terminal run state", got)
	}
	waitForRunStatus(t, p, sid, runID, string(runtimeagent.RunCompleted))

	// sessionID 不匹配 → 明确错误。
	if _, _, err := p.StartRun("not-the-current-session", "x", "req-start-2"); err == nil {
		t.Fatal("StartRun with mismatched sessionID should fail (V1 current-only routing)")
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

// TestControlLeaseTakeover(P5-6,拍板②):A 占 → B 显式接管(tookOver=true)→
// A 的 Hold 失效;非持有者 Release 无操作;持有者 Release 后归还空闲。
func TestControlLeaseTakeover(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "lease"})

	if p.AcquireControl("conn-A") {
		t.Fatal("first acquire on an idle lease must not report takeover")
	}
	if !p.HoldControl("conn-A") {
		t.Fatal("conn-A should hold the lease after acquiring")
	}
	if p.AcquireControl("conn-A") {
		t.Fatal("re-acquire by the same holder must not report takeover")
	}
	if !p.AcquireControl("conn-B") {
		t.Fatal("second holder must explicitly take over (tookOver=true)")
	}
	if p.HoldControl("conn-A") {
		t.Fatal("previous holder must lose the lease after takeover")
	}
	if !p.HoldControl("conn-B") {
		t.Fatal("new holder should hold the lease after takeover")
	}
	p.ReleaseControl("conn-A") // 非持有者:无操作
	if !p.HoldControl("conn-B") {
		t.Fatal("release by a non-holder must not clear the lease")
	}
	p.ReleaseControl("conn-B")
	if p.HoldControl("conn-B") {
		t.Fatal("lease should be idle after the holder releases it")
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

	// ④ 非 current 会话 → 诚实报错。
	if _, _, err := p.RunStatus("bogus-session", runID); err == nil {
		t.Fatal("RunStatus with mismatched sessionID should fail")
	}
	if _, _, err := p.Attach("bogus-session"); err == nil {
		t.Fatal("Attach with mismatched sessionID should fail")
	}
	if _, _, err := p.RunStatus(sid, "run-1-1"); err == nil {
		t.Fatal("RunStatus with unknown runID should fail")
	}
}
