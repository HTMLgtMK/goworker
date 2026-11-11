package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// This net.Pipe integration test exercises the real ai-acp client/server, handler, session
// manager, HITL, approval manager, and event hub with mock providers. It covers the wire-to-runtime
// seam for background events, cross-connection approvals, and idempotency; companion xgw_test.go
// focuses on scripted sources and wire errors. The real supervisor tee supplies run events.

// xgwDeviceToolName 是 ④ 的 fake 设备工具注册名(sys_ 前缀 + 客户端原名)。
const xgwDeviceToolName = "sys_vibrate"

// xgwDeviceToolsPayload 是 fake caller 对 x-device/tools 的应答(benign 单工具,
// 形状与 plugins/agent capability_test 的同款编排一致)。
const xgwDeviceToolsPayload = `{"tools":[{"name":"vibrate","description":"buzz the device","schema":"{\"type\":\"object\"}","risk_level":"benign"}]}`

// xgwScriptedProvider 按调用序回放预置 assistant 消息(第 i 轮 = msgs[i],越界
// 回落最终文本)。ChatStream 恒报错 → Session 回退非流式,tool call 经真实
// kernel 循环 + 沙箱/HITL 中间件链执行(形态对齐 SDK scriptedChatProvider,
// approval_test.go:233)。
type xgwScriptedProvider struct {
	mu    sync.Mutex
	calls int
	msgs  []core.Message
}

func (p *xgwScriptedProvider) Name() string  { return "xgw-scripted" }
func (p *xgwScriptedProvider) Model() string { return "m" }

func (p *xgwScriptedProvider) Chat(_ context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
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

func (p *xgwScriptedProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// xgwLiveControl mirrors the production run-control contract with in-memory idempotency and
// leases, while using a real runtime for runs, approvals, and event subscriptions. Its only
// test-specific part is session/lease bookkeeping.
type xgwLiveControl struct {
	*fakeSessionSource

	m   *runtimeagent.SessionManager
	sid string // runtime id(manager 注册键 = StartRun/EnsureHub 的会话键)

	leaseMu sync.Mutex
	leases  map[string]string // sessionID → connectionID(I3 per-session 租约)

	reqMu     sync.Mutex
	runByReq  map[string]runtimeagent.RunHandle
	startRuns int // 仅真实新起跑;幂等命中不记
}

// newXGWLiveControl registers a real runtime with a scripted provider and in-memory event
// journal. HITL uses the real default tools and normal sandbox; leaving RunJournal nil exercises
// the in-memory RunStatus path.
func newXGWLiveControl(t *testing.T, id string, pv core.Provider, collect func(*sandbox.Config) []core.Tool) *xgwLiveControl {
	t.Helper()
	m := runtimeagent.NewSessionManager()
	t.Cleanup(func() { m.Close() })
	cfg := runtimeconfig.Default()
	cfg.Sandbox.AllowedWorkDir = t.TempDir()
	deps := runtimeagent.SessionDeps{
		Config: cfg,
		CollectTools: func(sb *sandbox.Config) []core.Tool {
			if collect == nil {
				return nil
			}
			return collect(sb)
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) { return pv, nil },
		RunEvents:   &memEventJournal{},
	}
	rt := runtimeagent.NewSessionRuntime(id, runtimeagent.NewSession(deps))
	if err := m.Register(rt); err != nil {
		t.Fatalf("register runtime: %v", err)
	}
	return &xgwLiveControl{fakeSessionSource: &fakeSessionSource{}, m: m, sid: id, leases: map[string]string{}}
}

// SetSession 覆写:Plan A2b1 起 handler 的 session/new 不再调用 SetSession
// (声明切换到 BoundSessionSource,见下方 CreateBoundSession)——本覆写保留为
// fake 的兼容记账面,由 liveNewSession 在声明成功后显式驱动(见其注释)。
func (f *xgwLiveControl) SetSession(sessionID, cwd string) {
	f.mu.Lock()
	f.currentID = sessionID
	f.mu.Unlock()
	f.fakeSessionSource.SetSession(sessionID, cwd)
}

// CreateBoundSession 实现 model.BoundSessionSource(Plan A2b1 语义对齐):
// session/new 真新建在本 shim 的形态 = 声明落地其唯一 runtime(单 runtime 世界
// 里「新建」收敛为返回既有 runtime 的 id,与 pr2DualControl 的创建队列不同——
// 本 shim 的用例都是单会话编排便签)。cwd 不落任何状态(shim 无持久化事实)。
func (f *xgwLiveControl) CreateBoundSession(cwd string) (string, error) {
	return f.sid, nil
}

// LoadBoundSession mirror 生产二分支(单 runtime 形态):已注册(runtime id 命中)
// → reload no-op(store 无,ReloadFromStore 本身 no-op);不在册 → 诚实未知
// (ErrSessionUnknown 契约,handler 映射 -32000)。cwd 不动声明。
func (f *xgwLiveControl) LoadBoundSession(sessionID, cwd string) error {
	if sessionID != f.sid {
		return fmt.Errorf("xgwLiveControl: session %q: %w", sessionID, model.ErrSessionUnknown)
	}
	return nil
}

// SessionHistory mirror 生产(单 runtime 形态):已注册 → 空重放;未知 → 报错。
func (f *xgwLiveControl) SessionHistory(sessionID string) ([]model.SessionUpdate, error) {
	if sessionID != f.sid {
		return nil, fmt.Errorf("xgwLiveControl: session %q: %w", sessionID, model.ErrSessionUnknown)
	}
	return []model.SessionUpdate{}, nil
}

// SessionArchived 按 fake 的归档脚本作答(TestXGWIntegrationStandardMethodShapes
// 的归档 load 依赖:archived map 里的 id 报告终态,handler 据此走只读重放——
// 基接口 ArchivedSessionHistory 的脚本化历史)。单 runtime 的活会话恒 false。
func (f *xgwLiveControl) SessionArchived(sessionID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.archived[sessionID]
	return ok
}

// ArchiveSession in this fixture marks the session archived and rejects active runs. It does
// not simulate lease fencing, which is covered by plugin-level tests. The run check uses the
// manager lock before taking f.mu; production serializes check and archive under its lease guard.
func (f *xgwLiveControl) ArchiveSession(sessionID, clientID string) error {
	if rt := f.m.Get(f.sid); rt != nil {
		if run := rt.Active(); run != nil {
			return fmt.Errorf("xgwLiveControl: session %q is busy: run in flight (state %s)", sessionID, run.State())
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.archived == nil {
		f.archived = make(map[string][]model.SessionUpdate) // 惰性建表:仅归档用例消费
	}
	if _, ok := f.archived[sessionID]; ok {
		return nil // 幂等:已终态
	}
	if sessionID != f.sid {
		return fmt.Errorf("xgwLiveControl: session %q: %w", sessionID, model.ErrSessionUnknown)
	}
	f.archived[sessionID] = nil
	return nil
}

// requireCurrent mirror runcontrol.go 的 V1 路由校验(拍板⑤):非 current 会话
// 一律拒绝,错误文案与生产同形(客户端按 -32000 透传感知)。
func (f *xgwLiveControl) requireCurrent(sessionID string) error {
	if cur := f.CurrentSessionID(); sessionID != cur {
		return fmt.Errorf("xgwLiveControl: session %q is not the current session %q (V1 routes the current session only)", sessionID, cur)
	}
	return nil
}

// StartRun mirror runcontrol.go(current 校验 → fencing:lease 校验与提交同锁,
// 第三轮评审 I4):幂等表命中短路(返回原 runID 与当前状态快照)→
// manager.StartRun(空回调后台提交)→ 记账。
func (f *xgwLiveControl) StartRun(sessionID, connectionID, input, requestID string) (string, string, error) {
	if err := f.requireCurrent(sessionID); err != nil {
		return "", "", err
	}
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	if connectionID == "" || f.leases[sessionID] != connectionID {
		return "", "", fmt.Errorf("xgwLiveControl: session %q: connection %q: %w", sessionID, connectionID, errFakeLeaseLost)
	}
	f.reqMu.Lock()
	defer f.reqMu.Unlock()
	if requestID != "" {
		if h, ok := f.runByReq[requestID]; ok {
			return h.RunID, string(h.State()), nil
		}
	}
	f.startRuns++
	h, err := f.m.StartRun(f.sid, runtimeagent.RunRequest{Input: input}, runtimeagent.RunCallbacks{})
	if err != nil {
		return "", "", err
	}
	if requestID != "" {
		if f.runByReq == nil {
			f.runByReq = make(map[string]runtimeagent.RunHandle)
		}
		f.runByReq[requestID] = h
	}
	// fresh 提交返回接受时刻快照(SubmitState,恒 queued),镜像生产的
	// runcontrol.StartRun;活快照 State() 在 fast provider 下可能是 completed,
	// 会让 wire 契约断言变成调度竞争。
	return h.RunID, string(h.SubmitState()), nil
}

// CancelRun mirror runcontrol.go(fencing 同 StartRun):直转 manager(未知 run
// → ErrRunNotFound)。
func (f *xgwLiveControl) CancelRun(sessionID, connectionID, runID string) error {
	if err := f.requireCurrent(sessionID); err != nil {
		return err
	}
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	if connectionID == "" || f.leases[sessionID] != connectionID {
		return fmt.Errorf("xgwLiveControl: session %q: connection %q: %w", sessionID, connectionID, errFakeLeaseLost)
	}
	return f.m.CancelRun(f.sid, runID)
}

// RunStatus mirror runcontrol.go 纯内存两分支:rt.Active() 命中 → 实时快照;
// 幂等表按 runID 反查 → 终态保留快照。latestSeq 取 hub(会话维度游标);
// replayIntegrity(I4)取 hub.ReplayIntegrity(),hub 未建 → complete(无事件
// 流即无「不完整回放」可言,同生产 runcontrol.go 的 nil-hub 分支)。
func (f *xgwLiveControl) RunStatus(sessionID, runID string) (string, uint64, string, error) {
	if err := f.requireCurrent(sessionID); err != nil {
		return "", 0, runtimeagent.ReplayIntegrityComplete, err
	}
	rt := f.m.Get(f.sid)
	if rt == nil {
		return "", 0, runtimeagent.ReplayIntegrityComplete, errors.New("xgwLiveControl: no active runtime")
	}
	latest := uint64(0)
	replayIntegrity := runtimeagent.ReplayIntegrityComplete
	if hub := rt.Hub(); hub != nil {
		latest = hub.LatestSeq()
		replayIntegrity = hub.ReplayIntegrity()
	}
	if run := rt.Active(); run != nil && run.ID == runID {
		return string(run.State()), latest, replayIntegrity, nil
	}
	f.reqMu.Lock()
	var h runtimeagent.RunHandle
	tracked := false
	for _, cand := range f.runByReq {
		if cand.RunID == runID {
			h, tracked = cand, true
			break
		}
	}
	f.reqMu.Unlock()
	if tracked {
		return string(h.State()), latest, replayIntegrity, nil
	}
	return "", 0, replayIntegrity, fmt.Errorf("xgwLiveControl: run %q: %w", runID, runtimeagent.ErrRunNotFound)
}

// PendingApprovals mirror runcontrol.go:manager 审批快照 → model 扁平结构。
func (f *xgwLiveControl) PendingApprovals(sessionID string) ([]model.PendingApprovalInfo, error) {
	if err := f.requireCurrent(sessionID); err != nil {
		return nil, err
	}
	pending := f.m.PendingApprovals(f.sid)
	out := make([]model.PendingApprovalInfo, 0, len(pending))
	for _, pa := range pending {
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

// ResolveApproval mirrors production fencing: under one lease lock it verifies the holder and
// server-recorded approval owner before submission. The owner is compared with the runtime
// session ID; unknown, cross-session, or stale-holder requests fail.
func (f *xgwLiveControl) ResolveApproval(sessionID, connectionID, requestID string, decision hitl.Decision) error {
	if err := f.requireCurrent(sessionID); err != nil {
		return err
	}
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	if connectionID == "" || f.leases[sessionID] != connectionID {
		return fmt.Errorf("xgwLiveControl: session %q: connection %q: %w", sessionID, connectionID, errFakeLeaseLost)
	}
	owner, ok := f.m.ApprovalOwner(requestID)
	if !ok {
		return fmt.Errorf("xgwLiveControl: approval %q: %w", requestID, runtimeagent.ErrApprovalUnknown)
	}
	if owner != f.sid {
		return fmt.Errorf("xgwLiveControl: approval %q belongs to session %q, not %q", requestID, owner, f.sid)
	}
	return f.m.ResolveApproval(requestID, decision)
}

// SubscribeEvents mirrors production: validate the session, ensure a hub, subscribe from the
// requested cursor, then read cursor/integrity and active-run snapshots. Cursor errors propagate.
func (f *xgwLiveControl) SubscribeEvents(sessionID string, afterSeq uint64) (agentcontrol.AttachResult, error) {
	if err := f.requireCurrent(sessionID); err != nil {
		return agentcontrol.AttachResult{}, err
	}
	rt := f.m.Get(f.sid)
	if rt == nil {
		return agentcontrol.AttachResult{}, errors.New("xgwLiveControl: no active runtime")
	}
	hub, err := rt.EnsureHub()
	if err != nil {
		return agentcontrol.AttachResult{}, fmt.Errorf("xgwLiveControl: ensure event hub: %w", err)
	}
	if hub == nil {
		return agentcontrol.AttachResult{}, fmt.Errorf("xgwLiveControl: event stream not enabled for session %q", sessionID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := hub.Subscribe(ctx, afterSeq)
	if err != nil {
		cancel()
		return agentcontrol.AttachResult{}, err
	}
	res := agentcontrol.AttachResult{
		LatestSeq:       hub.LatestSeq(),
		ReplayIntegrity: hub.ReplayIntegrity(),
		Subscription:    &testSubscription{ch: ch, cancel: cancel},
	}
	if run := rt.Active(); run != nil {
		res.CurrentRun = &agentcontrol.RunSnapshot{RunID: run.ID, State: string(run.State())}
	}
	return res, nil
}

// ActiveRun mirror runcontrol.go 的快照观察面。I1 修正后生产按 sessionRuntime
// 多会话路由;本 shim 只注册一个 runtime 且 wire sessionId 与 runtime 键分立,
// 会话寻址锚在 current(SetSession 写入)——单一 runtime 世界里这就是
// 「定位 runtime,nil → ok=false」的等价形,行为不变,仅注释分叉说明。
func (f *xgwLiveControl) ActiveRun(sessionID string) (string, string, bool) {
	if err := f.requireCurrent(sessionID); err != nil {
		return "", "", false
	}
	rt := f.m.Get(f.sid)
	if rt == nil {
		return "", "", false
	}
	if run := rt.Active(); run != nil {
		return run.ID, string(run.State()), true
	}
	return "", "", false
}

func (f *xgwLiveControl) AcquireControl(sessionID, connectionID string) bool {
	if connectionID == "" {
		return false
	}
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	if f.leases[sessionID] == connectionID {
		return false
	}
	tookOver := f.leases[sessionID] != ""
	f.leases[sessionID] = connectionID
	return tookOver
}

func (f *xgwLiveControl) ReleaseControl(sessionID, connectionID string) {
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	if f.leases[sessionID] == connectionID {
		delete(f.leases, sessionID)
	}
}

func (f *xgwLiveControl) HoldControl(sessionID, connectionID string) bool {
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	return f.leases[sessionID] == connectionID
}

func (f *xgwLiveControl) ReleaseConnection(connectionID string) {
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	for sessionID, holder := range f.leases {
		if holder == connectionID {
			delete(f.leases, sessionID)
		}
	}
}

func (f *xgwLiveControl) UnbindCapabilities(connectionID string) {
	f.m.Capabilities().UnbindConnection(connectionID)
}

func (f *xgwLiveControl) CapabilityWaitTimeout() time.Duration { return 45 * time.Second }

// xgwDeviceExecutor 把 SDK ClientToolExecutor 收窄面适配到 x-device/call
// (mirror runcontrol.go 的 deviceToolExecutor;CallDeviceTool 统一剥 sys_ 前缀)。
type xgwDeviceExecutor struct {
	caller model.DeviceCaller
}

func (e xgwDeviceExecutor) Invoke(ctx context.Context, toolName string, argsJSON []byte) (string, error) {
	return acpworker.CallDeviceTool(ctx, e.caller, toolName, argsJSON)
}

// ClientToolSource 实现 model.ClientToolBinder(mirror runcontrol.go):探测
// (SpecDeviceTools,失败/空 → nil 收缩)→ Bind(rt.ID, connID, 注册名,
// ttl=2*wait) → NewClientToolProxy。④ 用它证明 handler 的 binder 优先接线
// 在协议面成立。
func (f *xgwLiveControl) ClientToolSource(connectionID string, caller model.DeviceCaller) func(context.Context) []core.Tool {
	return func(_ context.Context) []core.Tool {
		specs, err := acpworker.SpecDeviceTools(caller)
		if err != nil || len(specs) == 0 {
			return nil
		}
		rt := f.m.Get(f.sid)
		if rt == nil {
			return nil
		}
		wait := f.CapabilityWaitTimeout()
		names := make([]string, 0, len(specs))
		for _, spec := range specs {
			names = append(names, spec.Name)
		}
		reg := f.m.Capabilities()
		reg.Bind(f.sid, connectionID, names, 2*wait, xgwDeviceExecutor{caller: caller})
		return runtimeagent.NewClientToolProxy(reg, f.sid, specs, wait)
	}
}

// startRunCount 供幂等断言(真实新起跑次数)。
func (f *xgwLiveControl) startRunCount() int {
	f.reqMu.Lock()
	defer f.reqMu.Unlock()
	return f.startRuns
}

// xgwLiveStack 是真实后端的集成栈:服务端管线与 newXGWStack 完全一致
// (NewServer → registerXGWMethods → Start,与 Service.accept 生产序列一致),
// 会话源换成 xgwLiveControl。
type xgwLiveStack struct {
	client  *aiacp.Client
	handler *FrontendHandler
	src     *xgwLiveControl
}

func newXGWLiveStack(t *testing.T, evaluate Evaluator, src *xgwLiveControl) *xgwLiveStack {
	t.Helper()
	if evaluate == nil {
		evaluate = func(*model.Context, string) error { return nil }
	}
	serverEnd, clientEnd := net.Pipe()
	handler := NewFrontendHandlerFactory(evaluate, nil, src, NewSessionGate())().(*FrontendHandler)
	server, err := aiacp.NewServer(serverEnd, handler)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	registerXGWMethods(server, handler)
	server.Start()
	t.Cleanup(func() { _ = server.Close() })
	client, err := aiacp.NewClient("xgw-live", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &xgwLiveStack{client: client, handler: handler, src: src}
}

// liveInitialize 走标准握手(协议版本回归)。
func (s *xgwLiveStack) liveInitialize(t *testing.T) {
	t.Helper()
	init, err := s.client.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if init.ProtocolVersion != protocol.Version {
		t.Fatalf("initialize protocol version = %d, want %d", init.ProtocolVersion, protocol.Version)
	}
}

// liveInitializeWithDeviceCaps 握手并声明 x-device 能力(④ 的能力门控入口)。
func (s *xgwLiveStack) liveInitializeWithDeviceCaps(t *testing.T) {
	t.Helper()
	_, err := s.client.InitializeWithRequest(context.Background(), protocol.InitializeRequest{
		ProtocolVersion:    protocol.Version,
		ClientCapabilities: decodeCaps(t, `{"x-device": {"tools": true}}`),
	})
	if err != nil {
		t.Fatalf("Initialize with x-device caps: %v", err)
	}
}

// liveNewSession calls session/new and records the returned transport ID as this single-runtime
// fixture's current-session anchor. The Binding uses the runtime ID; event assertions distinguish
// that runtime ID from the transport ID where relevant.
func (s *xgwLiveStack) liveNewSession(t *testing.T) string {
	t.Helper()
	sid, err := s.client.NewSession(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if sid == "" {
		t.Fatal("session/new returned empty sessionId")
	}
	s.src.mu.Lock()
	s.src.currentID = sid
	s.src.mu.Unlock()
	return sid
}

// liveCallRaw 以原始 map 入参发起请求并取回原始 result(与 xgwStack.callRaw
// 同款:两侧不过 Go 结构体,钉 wire 键名契约)。
func (s *xgwLiveStack) liveCallRaw(t *testing.T, method string, params map[string]any) (json.RawMessage, error) {
	t.Helper()
	var raw json.RawMessage
	err := s.client.Call(context.Background(), method, params, &raw)
	return raw, err
}

// liveAttachRaw 发起 attach 并解出 result 键表;错误原样返回供 -32000 断言。
func (s *xgwLiveStack) liveAttachRaw(t *testing.T, params map[string]any) (map[string]any, error) {
	t.Helper()
	raw, err := s.liveCallRaw(t, XMethodSessionAttach, params)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode attach result %s: %v", raw, err)
	}
	return fields, nil
}

// xgwInterruptPayload 是 permission.requested 事件 payload 的最小解码面
// (hitl.InterruptRequest 的线上蛇形键,spec.go:24-36)。
type sessionPayload struct {
	ID   string `json:"id"`
	From string `json:"from"`
	To   string `json:"to"`
}

// waitForEventPayload 轮询日志直至出现 want 类型的事件并返回其 payload 解码。
func waitForEventPayload(t *testing.T, log *xgwEventLog, what, wantType string) sessionPayload {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		events, _, bad := log.snapshot()
		if len(bad) > 0 {
			t.Fatalf("notification decode failures: %v", bad)
		}
		for _, ev := range events {
			if ev.Type != wantType {
				continue
			}
			var p sessionPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("decode %s payload %s: %v", wantType, ev.Payload, err)
			}
			return p
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s (%s event)", what, wantType)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// This case covers initialize → new → attach → background events, plus idempotent retry and
// afterSeq replay including an expired-cursor error.

func TestXGWIntegrationBackgroundRunEventsAndIdempotency(t *testing.T) {
	src := newXGWLiveControl(t, "xgw-proto-run", &xgwScriptedProvider{}, nil)
	l := newXGWLiveStack(t, nil, src)
	l.liveInitialize(t)
	sid := l.liveNewSession(t)
	log := &xgwEventLog{}
	l.client.HandleNotification(XMethodSessionEvents, log.handle)
	t.Cleanup(l.handler.Close)

	// attach(control):空闲获取(tookOver 省略)、currentRun=null、latestSeq=0
	// (尚无 run,事件流为空)。
	fields, err := l.liveAttachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if _, present := fields["tookOver"]; present {
		t.Fatalf("fresh acquire contains tookOver = %v, want omitted", fields["tookOver"])
	}
	if fields["currentRun"] != nil {
		t.Fatalf("attach currentRun = %v, want null before any run", fields["currentRun"])
	}
	if fields["latestSeq"] != float64(0) {
		t.Fatalf("attach latestSeq = %v, want 0 (no events yet)", fields["latestSeq"])
	}
	if !l.src.HoldControl(sid, l.handler.connectionID) {
		t.Fatal("attach(mode=control) did not acquire the lease")
	}

	// run/start(持照):{runId,state} 立即返回,不等 LLM。
	raw, err := l.liveCallRaw(t, XMethodRunStart, map[string]any{
		"sessionId": sid, "input": "分析当前项目架构", "requestId": "proto-req-001",
	})
	if err != nil {
		t.Fatalf("run/start: %v", err)
	}
	var start map[string]any
	if err := json.Unmarshal(raw, &start); err != nil {
		t.Fatalf("decode run/start result %s: %v", raw, err)
	}
	if len(start) != 2 || start["state"] != "queued" {
		t.Fatalf("run/start result = %s, want exactly {runId,state:queued}", raw)
	}
	runID, _ := start["runId"].(string)
	if runID == "" {
		t.Fatalf("run/start result %s missing runId", raw)
	}

	// Verify the real run's event order from run.started through run.finished, including usage,
	// text, and finalized-message events.
	waitFor(t, "run finished event", func() bool {
		events, _, bad := log.snapshot()
		return len(bad) == 0 && len(events) > 0 && events[len(events)-1].Type == session_event_run_finished
	})
	events, raws, bad := log.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	if len(events) < 4 {
		t.Fatalf("received %d events, want >= 4 (生命周期边界 + usage/token 派生,C2 修复的 token 面)", len(events))
	}
	if events[0].Type != session_event_run_started {
		t.Fatalf("first event = %q, want run.started", events[0].Type)
	}
	if last := events[len(events)-1]; last.Type != session_event_run_finished {
		t.Fatalf("last event = %q, want run.finished", last.Type)
	}
	// 全流逐条:Seq 从 1 连续、RunID/SessionID 同源;usage.progress 至少一条,
	// text.delta 先于 assistant.message_finalized 且都夹在生命周期边界内。
	idxText, idxFinal, usageCount := -1, -1, 0
	for i, ev := range events {
		if ev.Seq != uint64(i+1) {
			t.Fatalf("event[%d] seq = %d, want %d (会话游标从 1 起连续)", i, ev.Seq, i+1)
		}
		if ev.RunID != runID {
			t.Fatalf("event[%d] run_id = %q, want %q", i, ev.RunID, runID)
		}
		if ev.SessionID != "xgw-proto-run" {
			t.Fatalf("event[%d] session_id = %q, want runtime id (hub 以 rt.ID 为会话键)", i, ev.SessionID)
		}
		switch ev.Type {
		case session.EventUsageProgress:
			usageCount++
		case session_event_text_delta:
			idxText = i
		case session_event_message_finalized:
			idxFinal = i
		}
	}
	if usageCount == 0 {
		t.Fatal("no usage.progress events (中间件 usage/iteration 未进后台事件流,C2)")
	}
	if idxText <= 0 || idxFinal <= idxText || idxFinal >= len(events)-1 {
		t.Fatalf("text.delta@%d message_finalized@%d (len=%d), want run.started < text.delta < message_finalized < run.finished (C2 修复的 token 面)", idxText, idxFinal, len(events))
	}
	if !strings.Contains(raws[0], `"session_id"`) {
		t.Fatalf("notification params = %s, want SessionEvent as-is JSON (蛇形 session_id 键)", raws[0])
	}

	// run/get:completed + latestSeq(只读观察面,无需 lease)。
	raw, err = l.liveCallRaw(t, XMethodRunGet, map[string]any{"sessionId": sid, "runId": runID})
	if err != nil {
		t.Fatalf("run/get: %v", err)
	}
	var status map[string]any
	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatalf("decode run/get result %s: %v", raw, err)
	}
	if status["state"] != "completed" || status["latestSeq"] != float64(len(events)) {
		t.Fatalf("run/get result = %s, want {state:completed,latestSeq:%d}", raw, len(events))
	}

	// ③ requestId 幂等(真实 manager 面):run 完成后同 requestId 重试 → 原
	// runID + 当前状态快照,不重复起跑;空 requestId 协议面拒绝。
	raw, err = l.liveCallRaw(t, XMethodRunStart, map[string]any{
		"sessionId": sid, "input": "分析当前项目架构", "requestId": "proto-req-001",
	})
	if err != nil {
		t.Fatalf("run/start idempotent retry: %v", err)
	}
	var retry map[string]any
	if err := json.Unmarshal(raw, &retry); err != nil {
		t.Fatalf("decode retry result %s: %v", raw, err)
	}
	if retry["runId"] != runID || retry["state"] != "completed" {
		t.Fatalf("retry result = %s, want {runId:%s,state:completed} (原 run 快照)", raw, runID)
	}
	if got := l.src.startRunCount(); got != 1 {
		t.Fatalf("real start runs = %d, want 1 (幂等重试不得重复起跑)", got)
	}
	if _, err := l.liveCallRaw(t, XMethodRunStart, map[string]any{"sessionId": sid, "input": "x", "requestId": ""}); err == nil {
		t.Fatal("run/start without requestId: want error")
	} else if rpcErr := wantXGWError(t, "run/start without requestId", err); !strings.Contains(rpcErr.Message, "requestId") {
		t.Fatalf("empty requestId message = %q, want requestId mention", rpcErr.Message)
	}

	// ③ ErrCursorExpired:游标超前 → -32000(cursor expired),订阅不建立。
	_, err = l.liveAttachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 9999})
	rpcErr := wantXGWError(t, "attach with future cursor", err)
	if !strings.Contains(rpcErr.Message, "cursor expired") {
		t.Fatalf("future cursor message = %q, want ErrCursorExpired text", rpcErr.Message)
	}

	// Resume from the acknowledged key-event cursor and assert the remaining run.finished event
	// arrives once; merged delta replay must respect individual sequence numbers.
	before := log.count()
	resumeFrom := uint64(events[idxFinal].Seq)
	resumeWant := len(events) - int(resumeFrom) // = 1(run.finished)
	fields, err = l.liveAttachRaw(t, map[string]any{"sessionId": sid, "afterSeq": resumeFrom})
	if err != nil {
		t.Fatalf("resume attach: %v", err)
	}
	if fields["latestSeq"] != float64(len(events)) {
		t.Fatalf("resume attach latestSeq = %v, want %d", fields["latestSeq"], len(events))
	}
	waitFor(t, "resumed events", func() bool { return log.count() >= before+resumeWant })
	time.Sleep(100 * time.Millisecond) // 宽限:断言「恰好补齐」前排除在途尾包
	events, _, bad = log.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	if got := len(events) - before; got != resumeWant {
		t.Fatalf("resume delivered %d new events, want exactly %d (断点续传无缺无重)", got, resumeWant)
	}
	// 续传段逐条对齐首轮流的后缀(seq 与类型;快照前缀即首轮事件,下标对齐)。
	for i, ev := range events[before:] {
		if ev.Seq != uint64(int(resumeFrom)+1+i) || ev.Type != events[int(resumeFrom)+i].Type {
			t.Fatalf("resumed event[%d] = {seq:%d type:%s}, want first-pass {seq:%d type:%s}", i, ev.Seq, ev.Type, int(resumeFrom)+1+i, events[int(resumeFrom)+i].Type)
		}
	}
}

// 事件类型常量的本地别名(session 包键名,别名仅为本文件可读性)。
const (
	session_event_run_started  = "run.started"
	session_event_run_finished = "run.finished"
)

// This case covers HITL approval across connections: a second control attachment takes over,
// responds to the approval, and observes the run complete through run/get.

func TestXGWIntegrationApprovalAfterReconnect(t *testing.T) {
	// 真实 bash + 真实 HITL 中间件链:管道命令命中 DefaultRiskyPatterns 的
	// `\|`(normal 模式 → DecisionHitl),approve 后真实执行输出 HITL-OK。
	pv := &xgwScriptedProvider{msgs: []core.Message{
		{Role: "assistant", ToolCalls: []core.ToolCall{{
			ID:       "call-hitl-1",
			Type:     "function",
			Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"echo hitl-ok | tr a-z A-Z"}`},
		}}},
		{Role: "assistant", Content: "approved run done"},
	}}
	src := newXGWLiveControl(t, "xgw-proto-hitl", pv, runtimeagent.DefaultTools)

	stackA := newXGWLiveStack(t, nil, src)
	stackA.liveInitialize(t)
	sid := stackA.liveNewSession(t)
	logA := &xgwEventLog{}
	stackA.client.HandleNotification(XMethodSessionEvents, logA.handle)
	t.Cleanup(stackA.handler.Close)

	// A:attach(control) 后提交 run(后台;提交即返回,不等 HITL)。
	if _, err := stackA.liveAttachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach A control: %v", err)
	}
	raw, err := stackA.liveCallRaw(t, XMethodRunStart, map[string]any{
		"sessionId": sid, "input": "run the pipe", "requestId": "proto-hitel-1",
	})
	if err != nil {
		t.Fatalf("run/start: %v", err)
	}
	var start map[string]any
	if err := json.Unmarshal(raw, &start); err != nil {
		t.Fatalf("decode run/start result %s: %v", raw, err)
	}
	runID, _ := start["runId"].(string)

	// run 迁移 waiting_input(decideFunc 的 transition 先于 state_changed 事件,
	// 收到该事件即状态已稳定可观测)。
	ev := waitForEventPayload(t, logA, "waiting transition", session_event_state_changed)
	if ev.To != "waiting_input" {
		t.Fatalf("first state_changed to = %q, want waiting_input", ev.To)
	}
	// permission.requested 事件携带审批 requestID(hitl.InterruptRequest 的
	// 线上 id,req-N 命名空间)——客户端据它构造 approval/respond。
	req := waitForEventPayload(t, logA, "pending approval", session_event_permission_requested)
	if req.ID == "" || !strings.HasPrefix(req.ID, "req-") {
		t.Fatalf("permission.requested payload id = %q, want req-N", req.ID)
	}

	// B:第二连接(只 initialize,不 session/new —— attach 对既有会话)显式
	// 接管 control:tookOver=true,currentRun 快照 waiting_input。
	stackB := newXGWLiveStack(t, nil, src)
	stackB.liveInitialize(t)
	logB := &xgwEventLog{}
	stackB.client.HandleNotification(XMethodSessionEvents, logB.handle)
	t.Cleanup(stackB.handler.Close)
	fields, err := stackB.liveAttachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"})
	if err != nil {
		t.Fatalf("attach B control: %v", err)
	}
	if fields["tookOver"] != true {
		t.Fatalf("attach B tookOver = %v, want true (显式接管)", fields["tookOver"])
	}
	cur, ok := fields["currentRun"].(map[string]any)
	if !ok || cur["runId"] != runID || cur["state"] != "waiting_input" {
		t.Fatalf("attach B currentRun = %v, want {runId:%s,state:waiting_input}", fields["currentRun"], runID)
	}
	// B 的 attach 游标(回放/实时边界):实时段逐条对齐的比较锚点,见文末。
	bLatestF, ok := fields["latestSeq"].(float64)
	if !ok {
		t.Fatalf("attach B latestSeq missing: %v", fields)
	}
	bAttachLatest := uint64(bLatestF)

	// 接管后原 holder 的 approval/respond 被拒(-32000,lease 在 daemon 侧
	// 强制,不依赖前端 UI 自律),且不触达审批管理面。
	_, err = stackA.liveCallRaw(t, XMethodApprovalRespond, map[string]any{
		"sessionId": sid, "requestId": req.ID, "decision": map[string]any{"interrupt_id": req.ID, "type": "approve"},
	})
	rpcErr := wantXGWError(t, "respond from evicted holder", err)
	if !strings.Contains(rpcErr.Message, "control lease") {
		t.Fatalf("evicted holder message = %q, want control lease mention", rpcErr.Message)
	}
	if pending, _ := src.PendingApprovals(sid); len(pending) != 1 {
		t.Fatalf("pending approvals after evicted respond = %d, want 1 (被拒决策不得触达管理面)", len(pending))
	}

	// 新 holder 应答 approve → 审批链路打通,run 真实完成(工具真实执行)。
	raw, err = stackB.liveCallRaw(t, XMethodApprovalRespond, map[string]any{
		"sessionId": sid, "requestId": req.ID, "decision": map[string]any{"interrupt_id": req.ID, "type": "approve"},
	})
	if err != nil {
		t.Fatalf("respond from new holder: %v", err)
	}
	var accepted map[string]any
	if err := json.Unmarshal(raw, &accepted); err != nil || accepted["accepted"] != true {
		t.Fatalf("respond result = %s (%v), want {accepted:true}", raw, err)
	}

	// run 完成:run/get(B,观察面无需 lease)返回 completed + latestSeq,且
	// latestSeq 与 A 收到的事件流末位一致(同一会话游标)。run.finished 事件
	// 先于终态 state 记录发布,交付到 A 的泵是异步的——以「A 的日志末位是
	// run.finished」为收齐锚点。
	deadline := time.Now().Add(5 * time.Second)
	var status map[string]any
	for {
		raw, err = stackB.liveCallRaw(t, XMethodRunGet, map[string]any{"sessionId": sid, "runId": runID})
		if err != nil {
			t.Fatalf("run/get: %v", err)
		}
		if err := json.Unmarshal(raw, &status); err != nil {
			t.Fatalf("decode run/get result %s: %v", raw, err)
		}
		if status["state"] == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run never completed: %s", raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
	waitFor(t, "run.finished delivered to A", func() bool {
		events, _, bad := logA.snapshot()
		return len(bad) == 0 && len(events) > 0 && events[len(events)-1].Type == session_event_run_finished
	})
	eventsA, _, bad := logA.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	lastSeq := eventsA[len(eventsA)-1].Seq
	if status["latestSeq"] != float64(lastSeq) {
		t.Fatalf("run/get latestSeq = %v, want %v (与事件流末位一致)", status["latestSeq"], lastSeq)
	}

	// 事件流全貌:seq 连续无重;run.started 打头、run.finished 收尾;审批事件
	// 对成对出现且 requested 先于 resolved(真实 decideFunc/tee 的次序)。
	seen := map[uint64]int{}
	for _, ev := range eventsA {
		seen[ev.Seq]++
	}
	for seq, n := range seen {
		if n != 1 || seq == 0 {
			t.Fatalf("event seq %d delivered %d time(s), want exactly 1 and non-zero", seq, n)
		}
	}
	for i := uint64(1); i <= uint64(len(eventsA)); i++ {
		if seen[i] != 1 {
			t.Fatalf("event seq %d missing (交付 %d 次),流必须从 1 连续到 %d", i, seen[i], len(eventsA))
		}
	}
	if eventsA[0].Type != session_event_run_started {
		t.Fatalf("first event = %q, want run.started", eventsA[0].Type)
	}
	if last := eventsA[len(eventsA)-1]; last.Type != session_event_run_finished {
		t.Fatalf("last event = %q, want run.finished", last.Type)
	}
	var requestedAt, resolvedAt = -1, -1
	for i, ev := range eventsA {
		switch ev.Type {
		case session_event_permission_requested:
			requestedAt = i
		case session_event_permission_resolved:
			resolvedAt = i
		}
	}
	if requestedAt < 0 || resolvedAt < 0 || resolvedAt < requestedAt {
		t.Fatalf("permission events: requested@%d resolved@%d, want 成对且 requested 在先", requestedAt, resolvedAt)
	}

	// Both connections observe the same replay/live stream; assert strictly increasing sequence
	// numbers and matching event tails across subscribers.
	waitFor(t, "B receives the same stream", func() bool {
		eventsB, _, badB := logB.snapshot()
		return len(badB) == 0 && len(eventsB) > 0 && eventsB[len(eventsB)-1].Seq == lastSeq
	})
	eventsB, _, badB := logB.snapshot()
	if len(badB) > 0 {
		t.Fatalf("B notification decode failures: %v", badB)
	}
	if last := eventsB[len(eventsB)-1]; last.Seq != lastSeq || last.Type != session_event_run_finished {
		t.Fatalf("B last event = {seq:%d type:%s}, want {seq:%d type:%s} (同一会话游标)\nB=%s",
			last.Seq, last.Type, lastSeq, session_event_run_finished, seqDump(eventsB))
	}
	for i := 1; i < len(eventsB); i++ {
		if eventsB[i].Seq <= eventsB[i-1].Seq {
			t.Fatalf("B stream seq not strictly increasing at #%d\nB=%s", i, seqDump(eventsB))
		}
	}
	var tailA, tailB []session.SessionEvent
	for _, ev := range eventsA {
		if ev.Seq > bAttachLatest {
			tailA = append(tailA, ev)
		}
	}
	for _, ev := range eventsB {
		if ev.Seq > bAttachLatest {
			tailB = append(tailB, ev)
		}
	}
	if len(tailB) != len(tailA) {
		t.Fatalf("B live tail = %d events, want %d (attach 点后两连接同流)\nA=%s\nB=%s",
			len(tailB), len(tailA), seqDump(eventsA), seqDump(eventsB))
	}
	for i := range tailA {
		if tailB[i].Seq != tailA[i].Seq || tailB[i].Type != tailA[i].Type {
			t.Fatalf("B tail[%d] = {seq:%d type:%s}, want A's {seq:%d type:%s}\nA=%s\nB=%s",
				i, tailB[i].Seq, tailB[i].Type, tailA[i].Seq, tailA[i].Type, seqDump(eventsA), seqDump(eventsB))
		}
	}
}

// seqDump 把事件流转成 seq:type 紧凑串(失败诊断用)。
func seqDump(events []session.SessionEvent) string {
	parts := make([]string, 0, len(events))
	for _, ev := range events {
		parts = append(parts, fmt.Sprintf("%d:%s", ev.Seq, ev.Type))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// 事件类型常量(② 使用;与 ① 的别名同源)。
const (
	session_event_state_changed        = "run.state_changed"
	session_event_permission_requested = "permission.requested"
	session_event_permission_resolved  = "permission.resolved"
)

// This case checks token-derived event wire shapes published through the event hub and delivered
// after attach.

func TestXGWIntegrationTokenEventWireShapes(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)

	// 先建立订阅(observe),后产出事件:live 侧逐条到达。
	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	stack.runs.AcquireControl(sid, stack.handler.connectionID)
	if _, err := stack.callRaw(t, XMethodRunStart, map[string]any{
		"sessionId": sid, "input": "x", "requestId": "wire-shape-1",
	}); err != nil {
		t.Fatalf("run/start: %v", err)
	}
	publishHubEvent(t, hub, "run-1", session_event_text_delta)
	publishHubEvent(t, hub, "run-1", session_event_message_finalized)
	waitFor(t, "token-derived events", func() bool { return log.count() >= 5 })

	events, raws, bad := log.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	if len(events) != 5 {
		t.Fatalf("received %d events, want exactly 5", len(events))
	}
	wantTypes := []string{
		session_event_run_started, session_event_tool_call, session_event_run_finished,
		session_event_text_delta, session_event_message_finalized,
	}
	for i, ev := range events {
		if ev.Seq != uint64(i+1) {
			t.Fatalf("event[%d] seq = %d, want %d", i, ev.Seq, i+1)
		}
		if ev.Type != wantTypes[i] {
			t.Fatalf("event[%d] type = %q, want %q", i, ev.Type, wantTypes[i])
		}
		if !strings.Contains(raws[i], `"session_id"`) || !strings.Contains(raws[i], `"run_id"`) {
			t.Fatalf("notification params = %s, want SessionEvent as-is JSON(蛇形键信封)", raws[i])
		}
	}
}

// 事件类型常量(token 形状测试使用)。
const (
	session_event_tool_call         = "tool_call"
	session_event_tool_result       = "tool_result"
	session_event_text_delta        = "text.delta"
	session_event_message_finalized = "assistant.message_finalized"
)

// Real background-run event chain: run/start with empty callbacks still emits token-derived
// events through the supervisor tee and attach subscription. This complements the direct-publish
// wire-shape test below.

func TestXGWIntegrationTokenEventWireShapesRealChain(t *testing.T) {
	// 未注册工具探针(与 SDK TestStartRunEmitsJournalEvents 同型):kernel 照常
	// 产出 tool_call/tool_result token("unknown tool" 结果),无 HITL 参与;
	// 非流式路径整段文本 → text.delta,kernel 收尾 Done → message_finalized。
	pv := &xgwScriptedProvider{msgs: []core.Message{
		{Role: "assistant", ToolCalls: []core.ToolCall{{
			ID:       "call-wire-1",
			Type:     "function",
			Function: core.ToolCallFunction{Name: "missing_tool", Arguments: `{"x":"1"}`},
		}}},
		{Role: "assistant", Content: "real chain final"},
	}}
	src := newXGWLiveControl(t, "xgw-proto-wire", pv, nil)
	l := newXGWLiveStack(t, nil, src)
	l.liveInitialize(t)
	sid := l.liveNewSession(t)
	log := &xgwEventLog{}
	l.client.HandleNotification(XMethodSessionEvents, log.handle)
	t.Cleanup(l.handler.Close)

	// 先建立订阅(attach control:run/start 持照),后 run/start:live 侧逐条到达。
	if _, err := l.liveAttachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	raw, err := l.liveCallRaw(t, XMethodRunStart, map[string]any{
		"sessionId": sid, "input": "x", "requestId": "wire-shape-real-1",
	})
	if err != nil {
		t.Fatalf("run/start: %v", err)
	}
	var start map[string]any
	if err := json.Unmarshal(raw, &start); err != nil {
		t.Fatalf("decode run/start result %s: %v", raw, err)
	}
	runID, _ := start["runId"].(string)
	if runID == "" {
		t.Fatalf("run/start result %s missing runId", raw)
	}

	// 以「末位是 run.finished」为收齐锚点(该 run 的最后一条事件)。
	waitFor(t, "real-chain run finished", func() bool {
		events, _, bad := log.snapshot()
		return len(bad) == 0 && len(events) > 0 && events[len(events)-1].Type == session_event_run_finished
	})
	events, raws, bad := log.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	if len(events) < 4 {
		t.Fatalf("received %d events, want >= 4 (token 派生事件未进真实后台流,C2)\n%s", len(events), seqDump(events))
	}

	// 四类 token 派生事件全部在流中,且次序正确:tool_call < tool_result <
	// text.delta < assistant.message_finalized < run.finished(流末位);每条
	// RunID 同源、信封蛇形键。
	idx := map[string]int{}
	for i, ev := range events {
		if ev.RunID != runID {
			t.Fatalf("event[%d] run_id = %q, want %q", i, ev.RunID, runID)
		}
		if !strings.Contains(raws[i], `"session_id"`) || !strings.Contains(raws[i], `"run_id"`) {
			t.Fatalf("notification params = %s, want SessionEvent as-is JSON(蛇形键信封)", raws[i])
		}
		if _, dup := idx[ev.Type]; !dup {
			idx[ev.Type] = i
		}
	}
	for _, typ := range []string{session_event_tool_call, session_event_tool_result, session_event_text_delta, session_event_message_finalized} {
		if _, ok := idx[typ]; !ok {
			t.Fatalf("real-chain stream missing %s —— token 派生事件未进后台流,C2\n%s", typ, seqDump(events))
		}
	}
	if !(idx[session_event_tool_call] < idx[session_event_tool_result] &&
		idx[session_event_tool_result] < idx[session_event_text_delta] &&
		idx[session_event_text_delta] < idx[session_event_message_finalized] &&
		idx[session_event_message_finalized] < len(events)-1) {
		t.Fatalf("token event order wrong: tool_call@%d tool_result@%d text.delta@%d message_finalized@%d (len=%d)\n%s",
			idx[session_event_tool_call], idx[session_event_tool_result], idx[session_event_text_delta], idx[session_event_message_finalized], len(events), seqDump(events))
	}

	// tool_call 载荷携带结构化关联(整个 core.Token 的 JSON,含工具名)。
	for _, ev := range events {
		if ev.Type == session_event_tool_call {
			if !strings.Contains(string(ev.Payload), "missing_tool") {
				t.Fatalf("tool_call payload = %s, want structured ToolCall(missing_tool)", ev.Payload)
			}
			break
		}
	}
}

// This case verifies standard ACP method responses remain unchanged when x-goworker extensions
// are registered alongside the live session source.

func TestXGWIntegrationStandardMethodShapes(t *testing.T) {
	src := newXGWLiveControl(t, "xgw-proto-std", &xgwScriptedProvider{}, nil)
	src.mu.Lock()
	src.list = []model.SessionSummary{{SessionID: "archived-1", Cwd: "/archive", Title: "old"}}
	src.archived = map[string][]model.SessionUpdate{
		"archived-1": {{Kind: model.SessionUpdateAgentMessageChunk, Content: &model.SessionContent{Type: "text", Text: "hi"}}},
	}
	src.mu.Unlock()
	l := newXGWLiveStack(t, nil, src)
	l.liveInitialize(t)

	// session/new:响应形状 = 客户端拿到非空 sessionId。
	sid := l.liveNewSession(t)

	// session/list:domain → wire 映射形状不变。
	list, err := l.client.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("session/list: %v", err)
	}
	if len(list.Sessions) != 1 || list.Sessions[0].SessionID != "archived-1" || list.Sessions[0].Cwd != "/archive" {
		t.Fatalf("session/list = %+v, want one archived-1 entry", list.Sessions)
	}

	// session/load(归档,只读重放):不因 x-goworker 注册而变化。
	if _, err := l.client.SessionLoad(context.Background(), "archived-1", "/archive"); err != nil {
		t.Fatalf("session/load: %v", err)
	}

	// session/prompt:响应逐字 = {"stopReason":"end_turn"}(原始 result 断言)。
	var raw json.RawMessage
	if err := l.client.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: sid,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("hello")},
	}, &raw); err != nil {
		t.Fatalf("session/prompt: %v", err)
	}
	if string(raw) != `{"stopReason":"end_turn"}` {
		t.Fatalf("session/prompt response = %s, want exactly end_turn shape", raw)
	}
}

// TestXGWIntegrationSessionArchive checks the archive response, error normalization, busy gate,
// and read-only load after archival. Active-run rejection and lease fencing are covered by the
// plugin-level archive tests.
func TestXGWIntegrationSessionArchive(t *testing.T) {
	src := newXGWLiveControl(t, "xgw-proto-archive", &xgwScriptedProvider{}, nil)
	l := newXGWLiveStack(t, nil, src)
	l.liveInitialize(t)
	l.liveNewSession(t)

	// 缺 sessionId:参数校验先于 source,协议面诚实拒绝。
	_, err := l.liveCallRaw(t, XMethodSessionArchive, map[string]any{})
	rpcErr := wantXGWError(t, "archive without sessionId", err)
	if !strings.Contains(rpcErr.Message, "sessionId") {
		t.Fatalf("missing sessionId message = %q, want sessionId mention", rpcErr.Message)
	}

	// 未知会话:source 的 ErrSessionUnknown 归一 -32000。
	_, err = l.liveCallRaw(t, XMethodSessionArchive, map[string]any{"sessionId": "session_ffffffffffffffffffffffffffffffff"})
	rpcErr = wantXGWError(t, "archive unknown session", err)
	if !strings.Contains(rpcErr.Message, "unknown session") {
		t.Fatalf("unknown session message = %q, want unknown session mention", rpcErr.Message)
	}

	// 管理门禁:满即 busy(tryAcquire 语义,绝不排队攥住门禁)。
	if !l.handler.gate.tryAcquire() {
		t.Fatal("setup: gate acquire failed")
	}
	_, err = l.liveCallRaw(t, XMethodSessionArchive, map[string]any{"sessionId": src.sid})
	rpcErr = wantXGWError(t, "archive while gate held", err)
	if !strings.Contains(rpcErr.Message, "busy") {
		t.Fatalf("gate-busy message = %q, want busy mention", rpcErr.Message)
	}
	l.handler.gate.release()

	// 归档成功:响应逐字 {"archived":true};重复归档幂等同形。
	raw, err := l.liveCallRaw(t, XMethodSessionArchive, map[string]any{"sessionId": src.sid})
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if string(raw) != `{"archived":true}` {
		t.Fatalf("archive response = %s, want exactly {\"archived\":true}", raw)
	}
	raw, err = l.liveCallRaw(t, XMethodSessionArchive, map[string]any{"sessionId": src.sid})
	if err != nil || string(raw) != `{"archived":true}` {
		t.Fatalf("re-archive = %s, %v, want idempotent {\"archived\":true}", raw, err)
	}

	// 终态可观测:SessionArchived 探测为真(session/load 只读分流的前提),
	// session/load 随即走归档只读重放(不注册 runtime、Binding 不动)。
	if !src.SessionArchived(src.sid) {
		t.Fatal("SessionArchived after archive = false, want true")
	}
	if _, err := l.client.SessionLoad(context.Background(), src.sid, ""); err != nil {
		t.Fatalf("session/load of archived session: %v", err)
	}
}

// Device-tool wire coverage: initialize negotiates x-device, prompt discovers and calls a tool,
// and disconnect unbinds it. Cross-connection rebind/timeout behavior is covered at the plugin
// integration layer because this handler serializes prompts through one session gate.

func TestXGWIntegrationDeviceToolBinderProbeAndDisconnectUnbind(t *testing.T) {
	pv := &xgwScriptedProvider{msgs: []core.Message{
		{Role: "assistant", ToolCalls: []core.ToolCall{{
			ID:       "call-dev-1",
			Type:     "function",
			Function: core.ToolCallFunction{Name: xgwDeviceToolName, Arguments: `{"duration":1}`},
		}}},
		{Role: "assistant", Content: "buzzed"},
	}}
	src := newXGWLiveControl(t, "xgw-proto-dev", pv, nil)

	// evaluate mirror 插件的同步 prompt 路径(commands.go handleAgent):持
	// handler 门禁的请求 goroutine 上经 RecordRun 跑真实会话,ToolSource 用
	// handler 注入的 binder 工具源(④ 的被测接缝)。
	evaluate := func(ctx *model.Context, input string) error {
		rt := src.m.Get(src.sid)
		if rt == nil {
			return errors.New("xgw integration: no runtime")
		}
		cb := runtimeagent.RunCallbacks{Write: ctx.Writer, Decide: ctx.Decide}
		if ctx.FrontendContext.EmitToken != nil {
			cb.EmitToken = ctx.FrontendContext.EmitToken
		}
		res := rt.RecordRun(func(s *runtimeagent.Session) (runtimeagent.RunResult, error) {
			return s.Run(ctx.Ctx, runtimeagent.RunRequest{
				Input:      input,
				ToolSource: ctx.FrontendContext.ToolSource,
			}, cb)
		})
		return res.Err
	}
	l := newXGWLiveStack(t, evaluate, src)
	t.Cleanup(l.handler.Close)
	l.liveInitializeWithDeviceCaps(t)

	// 客户端义务:x-device/tools 应答声明清单;x-device/call 计数并回放结果
	// (协议级 fake caller —— caller 即 ai-acp Server 的 client-ward Call 通道)。
	var callMu sync.Mutex
	callCount := 0
	l.client.Handle(acpworker.MethodDeviceTools, func(context.Context, json.RawMessage) (any, error) {
		return json.RawMessage(xgwDeviceToolsPayload), nil
	})
	l.client.Handle(acpworker.MethodDeviceCall, func(context.Context, json.RawMessage) (any, error) {
		callMu.Lock()
		callCount++
		callMu.Unlock()
		return json.RawMessage(`{"result":"from-A"}`), nil
	})

	sid := l.liveNewSession(t)
	type promptResult struct {
		stop string
		err  error
	}
	done := make(chan promptResult, 1)
	go func() {
		stop, err := l.client.Prompt(context.Background(), sid, []protocol.ContentBlock{protocol.TextBlock("vibrate the device")})
		done <- promptResult{stop: stop, err: err}
	}()
	var res promptResult
	select {
	case res = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("prompt never returned (device tool round-trip stalled)")
	}
	if res.err != nil {
		t.Fatalf("session/prompt: %v", res.err)
	}
	if res.stop != "end_turn" {
		t.Fatalf("prompt stopReason = %q, want end_turn", res.stop)
	}

	// binder 接线成立:探测过线 → 绑定登记(runtime id × 注册名);工具执行过线
	// 恰一次(结果来自客户端)。
	if _, ok := src.m.Capabilities().Lookup(src.sid, xgwDeviceToolName); !ok {
		t.Fatal("binding for (runtime, sys_vibrate) must be registered after the wire probe")
	}
	callMu.Lock()
	got := callCount
	callMu.Unlock()
	if got != 1 {
		t.Fatalf("x-device/call forwarded %d time(s), want 1", got)
	}

	// 断连收口:连接关闭 → handler.Close(Service.accept 路径的断连等价物,
	// 接线本身由 TestXGWServiceAcceptClosesHandler 钉死)→ UnbindCapabilities
	// → 绑定消失(在途等待语义由 P4-4 接管,见 ④ 头注)。
	if err := l.client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	l.handler.Close()
	waitFor(t, "unbind on disconnect", func() bool {
		_, ok := src.m.Capabilities().Lookup(src.sid, xgwDeviceToolName)
		return !ok
	})
}
