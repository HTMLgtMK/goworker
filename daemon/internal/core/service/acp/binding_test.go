package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// binding_test.go 是 PR2(统一 Session 路由,总方案 §六/§十)的协议级集成测试:
// 两条 FrontendHandler 连接各自 session/new 后,标准 ACP(prompt)按连接级
// SessionBinding 路由、x-goworker/*(run/start|get|cancel、attach)按显式
// sessionId(CR4)路由,两连接互不影响。控制面走真实 agentcontrol.Service
// (per-session lease fencing),执行面走真实 SessionManager/Session/EventHub,
// 零真实 LLM(模拟 Provider)。
//
// 与 xgwLiveControl 的差异:那边单 runtime(current 校验锚点=SetSession 写入的
// transport id);本 shim 双 runtime("A"/"B",store id 形态),current 由测试
// 显式切换(模拟 /new 换轨——A 保留在册,CR4),CurrentSessionID 返回 runtime
// id(与生产 plugin 的 store id 同语义)。SessionSource 的元数据面(list/load
// 重放/modes)是 PR2 binding 面的非消费方,给最小空实现。

// bindingGateProvider 是闸门 Provider:Chat 进入即 close(started),阻塞到
// release 关闭(或 ctx 取消——run/cancel 的取消语义经 runCtx 传导至此)。
type bindingGateProvider struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBindingGateProvider() *bindingGateProvider {
	return &bindingGateProvider{started: make(chan struct{}), release: make(chan struct{})}
}

func (p *bindingGateProvider) Name() string  { return "pr2-gate" }
func (p *bindingGateProvider) Model() string { return "m" }

func (p *bindingGateProvider) Chat(ctx context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "done"}}}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *bindingGateProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// bindingFixedProvider 立即完成一轮回复(B 会话的正常收尾面)。
type bindingFixedProvider struct{ content string }

func (p bindingFixedProvider) Name() string  { return "pr2-fixed" }
func (p bindingFixedProvider) Model() string { return "m" }

func (p bindingFixedProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: p.content}}}}, nil
}

func (p bindingFixedProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// pr2DualControl 是双 runtime 的 SessionSource + RunControlSource + SessionRouter
// 测试 shim:控制操作全部委托真实 agentcontrol.Service(per-session lease、幂等
// single-flight 与生产同源);SubscribeEvents 走真实 Service(路由/EnsureHub/
// hub.Subscribe/完整性快照与生产同源)。current 由测试
// 经 switchCurrent 驱动(模拟 /new 换轨),SetSession 只记 transport 活动声明
// (与生产 SetSession 的 activeTransportSession 语义一致,不推 current)。
type pr2DualControl struct {
	mu      sync.Mutex
	current string // 当前 runtime id("A"/"B");CurrentSessionID 的返回值
	active  string // 最近一次 transport 会话声明(session/new|load-current 经 SetSession)

	m       *runtimeagent.SessionManager
	control *agentcontrol.Service
}

func (f *pr2DualControl) switchCurrent(id string) {
	f.mu.Lock()
	f.current = id
	f.mu.Unlock()
}

// RouteSession 实现 agentcontrol.SessionRouter(mirror runcontrol.go 的解析序,
// 去掉纯内存空串寻址形——本 shim 的 runtime 恒有稳定 id):
// current 命中 → current runtime;否则 manager.Get 注册表权威(CR4)。
func (f *pr2DualControl) RouteSession(sessionID string) *runtimeagent.SessionRuntime {
	f.mu.Lock()
	cur := f.current
	f.mu.Unlock()
	if rt := f.m.Get(cur); rt != nil && sessionID == rt.ID {
		return rt
	}
	return f.m.Get(sessionID)
}

func (f *pr2DualControl) routeOrErr(sessionID string) (*runtimeagent.SessionRuntime, error) {
	if rt := f.RouteSession(sessionID); rt != nil {
		return rt, nil
	}
	return nil, fmt.Errorf("acp: session %q: %w", sessionID, runtimeagent.ErrSessionNotFound)
}

// ---- SessionSource 面(最小空实现;PR2 binding 面只消费 CurrentSessionID) ----

func (f *pr2DualControl) ListSessions() []model.SessionSummary { return []model.SessionSummary{} }

func (f *pr2DualControl) CurrentSessionID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

func (f *pr2DualControl) ActiveSessionID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}

func (f *pr2DualControl) SetSession(sessionID, _ string) {
	f.mu.Lock()
	f.active = sessionID
	f.mu.Unlock()
}

func (f *pr2DualControl) ReloadCurrentSession() error                  { return nil }
func (f *pr2DualControl) CurrentSessionHistory() []model.SessionUpdate { return nil }

func (f *pr2DualControl) ArchivedSessionHistory(sessionID string) ([]model.SessionUpdate, error) {
	return nil, errors.New("pr2DualControl: no archive for " + sessionID)
}

func (f *pr2DualControl) SessionModes() *model.SessionModeState { return nil }

// ---- RunControlSource 面(薄委托真实 agentcontrol.Service) ----

func (f *pr2DualControl) StartRun(sessionID, connectionID, input, requestID string) (string, string, error) {
	return f.control.StartRun(sessionID, connectionID, input, requestID, runtimeagent.RunCallbacks{})
}

func (f *pr2DualControl) CancelRun(sessionID, connectionID, runID string) error {
	return f.control.CancelRun(sessionID, connectionID, runID)
}

func (f *pr2DualControl) RunStatus(sessionID, runID string) (string, uint64, string, error) {
	return f.control.RunStatus(sessionID, runID)
}

func (f *pr2DualControl) ResolveApproval(sessionID, connectionID, requestID string, decision hitl.Decision) error {
	return f.control.ResolveApproval(sessionID, connectionID, requestID, decision)
}

func (f *pr2DualControl) AcquireControl(sessionID, connectionID string) bool {
	return f.control.AcquireControl(sessionID, connectionID)
}

func (f *pr2DualControl) ReleaseControl(sessionID, connectionID string) {
	f.control.ReleaseControl(sessionID, connectionID)
}

func (f *pr2DualControl) HoldControl(sessionID, connectionID string) bool {
	return f.control.HoldControl(sessionID, connectionID)
}

func (f *pr2DualControl) ReleaseConnection(connectionID string) {
	f.control.ReleaseConnection(connectionID)
}

// SubscribeEvents(P5-8 测试面;PR3 订阅契约):薄委托真实 agentcontrol.Service
// ——路由/EnsureHub/hub.Subscribe/订阅后完整性快照与生产逐字同源,Attach 的
// shim 级 mirror 随契约收敛删除。
func (f *pr2DualControl) SubscribeEvents(sessionID string, afterSeq uint64) (agentcontrol.AttachResult, error) {
	return f.control.SubscribeEvents(sessionID, afterSeq)
}

func (f *pr2DualControl) ActiveRun(sessionID string) (string, string, bool) {
	rt := f.RouteSession(sessionID)
	if rt == nil {
		return "", "", false
	}
	if run := rt.Active(); run != nil {
		return run.ID, string(run.State()), true
	}
	return "", "", false
}

func (f *pr2DualControl) PendingApprovals(sessionID string) ([]model.PendingApprovalInfo, error) {
	rt, err := f.routeOrErr(sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]model.PendingApprovalInfo, 0)
	for _, pa := range f.m.PendingApprovals(rt.ID) {
		out = append(out, model.PendingApprovalInfo{
			RequestID: pa.RequestID,
			SessionID: pa.SessionID,
			RunID:     pa.RunID,
			ToolName:  pa.Request.ToolName,
			Command:   pa.Request.Command,
			RiskLevel: pa.Request.RiskLevel,
			ExpiresAt: pa.Request.ExpiresAt,
		})
	}
	return out, nil
}

func (f *pr2DualControl) UnbindCapabilities(connectionID string) {
	f.m.Capabilities().UnbindConnection(connectionID)
}

func (f *pr2DualControl) CapabilityWaitTimeout() time.Duration { return 45 * time.Second }

// newPR2DualControl 构造 shim 并按 ids 逐个注册真实 runtime(纯内存,每个
// runtime 自带 RunEvents=memEventJournal → attach 可建真 hub);ids[0] 为初始
// current。providers 缺省时回落 bindingFixedProvider。
func newPR2DualControl(t *testing.T, providers map[string]core.Provider, ids ...string) *pr2DualControl {
	t.Helper()
	if len(ids) < 2 {
		t.Fatal("pr2DualControl needs at least two runtime ids")
	}
	m := runtimeagent.NewSessionManager()
	t.Cleanup(func() { m.Close() })
	cfg := runtimeconfig.Default()
	cfg.Sandbox.AllowedWorkDir = t.TempDir()
	for _, id := range ids {
		pv, ok := providers[id]
		if !ok {
			pv = bindingFixedProvider{content: "fixed-" + id}
		}
		deps := runtimeagent.SessionDeps{
			Config:       cfg,
			CollectTools: func(*sandbox.Config) []core.Tool { return nil },
			NewProvider:  func(*runtimeconfig.Config) (core.Provider, error) { return pv, nil },
			RunEvents:    &memEventJournal{},
		}
		rt := runtimeagent.NewSessionRuntime(id, runtimeagent.NewSession(deps))
		if err := m.Register(rt); err != nil {
			t.Fatalf("register runtime %s: %v", id, err)
		}
	}
	f := &pr2DualControl{m: m, current: ids[0]}
	// 真实 Application Control Plane(per-session lease + 幂等 single-flight);
	// journals=nil → RunStatus 走纯内存分支(与 xgwLiveControl 同形)。
	f.control = agentcontrol.NewService(m, f, nil)
	return f
}

// bindingCapture 是捕获「prompt 执行链收到的连接级 Binding」的 evaluate:
// 断言路由面(agentcontrol.WithBinding 注入 → BindingFromCtx 取回)的通道与值。
type bindingCapture struct {
	mu       sync.Mutex
	bindings []*agentcontrol.SessionBinding
	ids      []string
}

func (c *bindingCapture) eval(ctx *model.Context, _ string) error {
	b := agentcontrol.BindingFromCtx(ctx.Ctx)
	c.mu.Lock()
	c.bindings = append(c.bindings, b)
	if b != nil {
		c.ids = append(c.ids, b.Get())
	}
	c.mu.Unlock()
	return nil
}

func (c *bindingCapture) last() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ids) == 0 {
		return "", false
	}
	return c.ids[len(c.ids)-1], true
}

// newPR2Stack 与 newXGWLiveStack 同构(net.Pipe 全栈:NewServer →
// registerXGWMethods → Start),会话源参数放宽为 model.SessionSource,返回
// handler 供断言连接私有字段(binding/active/connectionID)。两连接共享同一
// gate(生产形态:装配层创建一份、全 daemon 共享)。
func newPR2Stack(t *testing.T, evaluate Evaluator, src model.SessionSource, gate *SessionGate) (*aiacp.Client, *FrontendHandler) {
	t.Helper()
	if evaluate == nil {
		evaluate = func(*model.Context, string) error { return nil }
	}
	serverEnd, clientEnd := net.Pipe()
	handler := NewFrontendHandlerFactory(evaluate, nil, src, gate)().(*FrontendHandler)
	server, err := aiacp.NewServer(serverEnd, handler)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	registerXGWMethods(server, handler)
	server.Start()
	t.Cleanup(func() { _ = server.Close() })
	client, err := aiacp.NewClient("pr2", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, handler
}

// ---- xgw 调用 helper(wire 面经 client.Call,与 liveCallRaw 同形) ----

func pr2RunStart(t *testing.T, client *aiacp.Client, sessionID, requestID string) string {
	t.Helper()
	var raw json.RawMessage
	if err := client.Call(context.Background(), XMethodRunStart, map[string]any{
		"sessionId": sessionID, "input": "hi-" + sessionID, "requestId": requestID,
	}, &raw); err != nil {
		t.Fatalf("run/start(%s): %v", sessionID, err)
	}
	var res XRunStartResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("decode run/start result %s: %v", raw, err)
	}
	return res.RunID
}

func pr2RunGet(t *testing.T, client *aiacp.Client, sessionID, runID string) XRunGetResult {
	t.Helper()
	var raw json.RawMessage
	if err := client.Call(context.Background(), XMethodRunGet, map[string]any{
		"sessionId": sessionID, "runId": runID,
	}, &raw); err != nil {
		t.Fatalf("run/get(%s, %s): %v", sessionID, runID, err)
	}
	var res XRunGetResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("decode run/get result %s: %v", raw, err)
	}
	return res
}

func pr2RunGetErr(t *testing.T, client *aiacp.Client, sessionID, runID string) *protocol.RPCError {
	t.Helper()
	var raw json.RawMessage
	err := client.Call(context.Background(), XMethodRunGet, map[string]any{
		"sessionId": sessionID, "runId": runID,
	}, &raw)
	if err == nil {
		t.Fatalf("run/get(%s, %s) = %s, want error", sessionID, runID, raw)
	}
	rpcErr, ok := err.(*protocol.RPCError)
	if !ok {
		t.Fatalf("run/get(%s, %s) err = %v (%T), want *protocol.RPCError", sessionID, runID, err, err)
	}
	return rpcErr
}

func pr2RunCancel(t *testing.T, client *aiacp.Client, sessionID, runID string) bool {
	t.Helper()
	var raw json.RawMessage
	if err := client.Call(context.Background(), XMethodRunCancel, map[string]any{
		"sessionId": sessionID, "runId": runID,
	}, &raw); err != nil {
		t.Fatalf("run/cancel(%s, %s): %v", sessionID, runID, err)
	}
	var res XRunCancelResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("decode run/cancel result %s: %v", raw, err)
	}
	return res.Accepted
}

func pr2Attach(t *testing.T, client *aiacp.Client, sessionID string) XSessionAttachResult {
	t.Helper()
	var raw json.RawMessage
	if err := client.Call(context.Background(), XMethodSessionAttach, map[string]any{
		"sessionId": sessionID, "mode": "control", "afterSeq": 0,
	}, &raw); err != nil {
		t.Fatalf("attach(%s): %v", sessionID, err)
	}
	var res XSessionAttachResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("decode attach result %s: %v", raw, err)
	}
	return res
}

// TestPR2TwoConnectionsEachOperateOwnSession(总方案 §十 PR2 完成标准,场景 1):
// 两条连接各自 session/new(Session A/B)→ 各自 prompt/attach/cancel 互不影响。
// prompt 按连接级 Binding 路由(标准 ACP);run/attach 按显式 sessionId(CR4);
// 两会话的 lease、run、事件流互不可见、互不阻塞。
func TestPR2TwoConnectionsEachOperateOwnSession(t *testing.T) {
	gateA := newBindingGateProvider()
	gateB := newBindingGateProvider()
	ctrl := newPR2DualControl(t, map[string]core.Provider{"A": gateA, "B": gateB}, "A", "B")
	gate := NewSessionGate()

	cap1 := &bindingCapture{}
	cap2 := &bindingCapture{}
	client1, handler1 := newPR2Stack(t, cap1.eval, ctrl, gate)
	client2, handler2 := newPR2Stack(t, cap2.eval, ctrl, gate)
	for _, c := range []*aiacp.Client{client1, client2} {
		if _, err := c.Initialize(context.Background()); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
	}

	// 连接 1 session/new:Binding 指向声明落地会话 A(current)。
	sid1 := func() string {
		sid, err := client1.NewSession(context.Background(), "/repo-a")
		if err != nil || sid == "" {
			t.Fatalf("conn1 NewSession: %v, %q", err, sid)
		}
		return sid
	}()
	if got := handler1.binding.Get(); got != "A" {
		t.Fatalf("conn1 binding = %q, want A (session/new 后 Set 已注册 runtime id)", got)
	}

	// current 切到 B(模拟 /new 换轨;A 保留在册——CR4),连接 2 session/new。
	ctrl.switchCurrent("B")
	sid2 := func() string {
		sid, err := client2.NewSession(context.Background(), "/repo-b")
		if err != nil || sid == "" {
			t.Fatalf("conn2 NewSession: %v, %q", err, sid)
		}
		return sid
	}()
	if got := handler2.binding.Get(); got != "B" {
		t.Fatalf("conn2 binding = %q, want B", got)
	}
	// 连接级隔离的第一面:连接 2 的声明不动连接 1 的 Binding(§6.2 第 4 条)。
	if got := handler1.binding.Get(); got != "A" {
		t.Fatalf("conn1 binding after conn2 session/new = %q, want A (unchanged)", got)
	}

	// 各自 prompt:执行链收到各自的 Binding(指针身份 + 指向值)——标准 ACP 的
	// 会话解析优先读 Binding,两条连接路由到两个会话。
	if _, err := client1.Prompt(context.Background(), sid1, []protocol.ContentBlock{protocol.TextBlock("hi-a")}); err != nil {
		t.Fatalf("conn1 Prompt: %v", err)
	}
	if _, err := client2.Prompt(context.Background(), sid2, []protocol.ContentBlock{protocol.TextBlock("hi-b")}); err != nil {
		t.Fatalf("conn2 Prompt: %v", err)
	}
	if got, ok := cap1.last(); !ok || got != "A" {
		t.Fatalf("conn1 prompt routed via binding %q (ok=%v), want A", got, ok)
	}
	if got, ok := cap2.last(); !ok || got != "B" {
		t.Fatalf("conn2 prompt routed via binding %q (ok=%v), want B", got, ok)
	}

	// 各自 attach(mode=control):per-session lease,两连接各控各会话、零接管。
	if res := pr2Attach(t, client1, "A"); res.TookOver {
		t.Fatal("conn1 attach(A) tookOver = true, want fresh lease")
	}
	if res := pr2Attach(t, client2, "B"); res.TookOver {
		t.Fatal("conn2 attach(B) tookOver = true, want fresh lease")
	}
	if !ctrl.HoldControl("A", handler1.connectionID) || !ctrl.HoldControl("B", handler2.connectionID) {
		t.Fatal("per-session leases not held by their own connections")
	}

	// 各自 run/start:两会话并行在途(闸门 provider 阻塞),互不阻塞。
	runA := pr2RunStart(t, client1, "A", "req-pr2-a")
	runB := pr2RunStart(t, client2, "B", "req-pr2-b")
	waitFor(t, "run A running", func() bool { return pr2RunGet(t, client1, "A", runA).State == string(runtimeagent.RunRunning) })
	waitFor(t, "run B running", func() bool { return pr2RunGet(t, client2, "B", runB).State == string(runtimeagent.RunRunning) })

	// 观测隔离:B 的连接看不到 A 的 run(journal/幂等表按会话寻址)。
	if rpcErr := pr2RunGetErr(t, client2, "B", runA); rpcErr.Code != -32000 {
		t.Fatalf("run/get(B, runA) code = %d, want -32000 (run not in session B)", rpcErr.Code)
	}

	// 各自 cancel:互不影响,各自收敛 cancelled。
	if !pr2RunCancel(t, client1, "A", runA) {
		t.Fatal("conn1 cancel(runA) not accepted")
	}
	if !pr2RunCancel(t, client2, "B", runB) {
		t.Fatal("conn2 cancel(runB) not accepted")
	}
	close(gateA.release)
	close(gateB.release)
	waitFor(t, "run A cancelled", func() bool { return pr2RunGet(t, client1, "A", runA).State == string(runtimeagent.RunCancelled) })
	waitFor(t, "run B cancelled", func() bool { return pr2RunGet(t, client2, "B", runB).State == string(runtimeagent.RunCancelled) })
}

// TestPR2LoadSwitchDoesNotDisturbOtherConnection(总方案 §十 PR2 完成标准,场景 2):
// 连接 1 load/切到 Session B 不影响连接 2 的 Session A 的在途 run 与 attach。
func TestPR2LoadSwitchDoesNotDisturbOtherConnection(t *testing.T) {
	gateA := newBindingGateProvider()
	ctrl := newPR2DualControl(t, map[string]core.Provider{"A": gateA}, "A", "B")
	gate := NewSessionGate()

	cap1 := &bindingCapture{}
	cap2 := &bindingCapture{}
	client1, handler1 := newPR2Stack(t, cap1.eval, ctrl, gate)
	client2, handler2 := newPR2Stack(t, cap2.eval, ctrl, gate)
	for _, c := range []*aiacp.Client{client1, client2} {
		if _, err := c.Initialize(context.Background()); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
	}

	// 连接 2 绑定 current A,attach(control) 后起一个在途 run(闸门阻塞)。
	sid2, err := client2.NewSession(context.Background(), "/repo-a")
	if err != nil || sid2 == "" {
		t.Fatalf("conn2 NewSession: %v, %q", err, sid2)
	}
	if res := pr2Attach(t, client2, "A"); res.TookOver {
		t.Fatal("conn2 attach(A) tookOver = true, want fresh lease")
	}
	runA := pr2RunStart(t, client2, "A", "req-pr2-load")
	<-gateA.started // run 真实开工(started 关闭 ⇒ 事件已发布)
	if st := pr2RunGet(t, client2, "A", runA).State; st != string(runtimeagent.RunRunning) {
		t.Fatalf("runA state = %q, want running", st)
	}

	// current 切到 B;连接 1 session/new(Binding=B),再 load-current(B)——
	// 「连接 1 load/切到 Session B」。
	ctrl.switchCurrent("B")
	sid1, err := client1.NewSession(context.Background(), "/repo-b")
	if err != nil || sid1 == "" {
		t.Fatalf("conn1 NewSession: %v, %q", err, sid1)
	}
	if got := handler1.binding.Get(); got != "B" {
		t.Fatalf("conn1 binding = %q, want B", got)
	}
	if _, err := client1.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "B", Cwd: "/repo-b"}); err != nil {
		t.Fatalf("conn1 LoadSession(B): %v", err)
	}

	// 连接 2 的会话选择、在途 run 与 control lease 全部不受影响(§6.2:load/重选
	// 不取消后台 Run、不动他人 Binding、不夺 control lease)。
	if got := handler2.binding.Get(); got != "A" {
		t.Fatalf("conn2 binding after conn1's load = %q, want A (unchanged)", got)
	}
	if !ctrl.HoldControl("A", handler2.connectionID) {
		t.Fatal("conn2 lost its control lease on A by conn1's load-current")
	}
	if st := pr2RunGet(t, client2, "A", runA).State; st != string(runtimeagent.RunRunning) {
		t.Fatalf("runA state after conn1's load = %q, want still running", st)
	}

	// 路由面:连接 1 的 prompt 走 B(刚 load 的会话),连接 2 的 prompt 仍走 A。
	if _, err := client1.Prompt(context.Background(), "B", []protocol.ContentBlock{protocol.TextBlock("hi-1")}); err != nil {
		t.Fatalf("conn1 Prompt(B): %v", err)
	}
	if got, ok := cap1.last(); !ok || got != "B" {
		t.Fatalf("conn1 prompt routed via binding %q (ok=%v), want B", got, ok)
	}
	if _, err := client2.Prompt(context.Background(), sid2, []protocol.ContentBlock{protocol.TextBlock("hi-2")}); err != nil {
		t.Fatalf("conn2 Prompt: %v", err)
	}
	if got, ok := cap2.last(); !ok || got != "A" {
		t.Fatalf("conn2 prompt routed via binding %q (ok=%v), want A", got, ok)
	}

	// 收尾:连接 2 cancel 自己的 run(仍持 lease,fencing 通过)并收敛。
	if !pr2RunCancel(t, client2, "A", runA) {
		t.Fatal("conn2 cancel(runA) not accepted after conn1's load")
	}
	close(gateA.release)
	waitFor(t, "run A cancelled", func() bool { return pr2RunGet(t, client2, "A", runA).State == string(runtimeagent.RunCancelled) })
}
