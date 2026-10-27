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
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// xgw_integration_test.go 是 x-goworker 扩展协议的协议级集成测试(P5-10 完成线,
// 总方案第十一节):JSON-RPC over net.Pipe 全栈(真实 ai-acp Server/Client +
// 真实 FrontendHandler 注册面 + 真实 SessionManager/Session/HITL 中间件/
// ApprovalManager/EventHub),零真实 LLM(全部模拟 Provider)。
//
// 与 xgw_test.go 的分工:xgw_test.go 用脚本化 fakeRunControl 钉 wire 契约与
// lease 守卫;本文件把 RunControlSource 换成「真实 SDK 编排」的测试 shim
// (xgwLiveControl,mirror runcontrol.go 的纯内存契约),验证协议面与运行面
// 的接缝:attach 后 run/start 的事件经真实 tee/decideFunc 流到客户端、HITL
// 审批跨连接应答、requestId 幂等在真实 manager 上成立、标准方法面逐字不变。
//
// 复用 xgw_test.go 的既有编排 helper:memEventJournal(内存事件 journal)、
// xgwEventLog(通知收集)、waitFor、wantXGWError;复用 handler_test.go 的
// decodeCaps。栈构造(newXGWLiveStack)与 helper 形态对齐 newXGWStack,因
// 会话源类型不同(xgwLiveControl ≠ *fakeRunControl)单独成列。
//
// 事件面(C2 修复后的真实链路覆盖,原「诚实适配」信息项已消解):supervisor 的
// tee 在 hub 非 nil 时主动供给 EmitToken/Publish 的事件采集(ai-runtime/agent/
// supervisor.go teeRunCallbacks,C2 修复)——daemon 的 run/start 虽仍以
// RunCallbacks{} 全 nil 提交后台 Run(runcontrol.go 注释对齐),真实 token 派生
// 事件(text.delta/assistant.message_finalized)与 usage 类事件照常进事件流。
// 真实后台 run 全链由 TestXGWIntegrationBackgroundRunEventsAndIdempotency 与
// TestXGWIntegrationTokenEventWireShapesRealChain(追加的真实链路变体,含
// tool_call)经 attach 订阅逐条钉住;TestXGWIntegrationTokenEventWireShapes 保留
// 为 hub.Publish(teePublish 的生产发布面)直发的信封形状对偶面。tool_call 的
// SDK 侧 journal 回归在 ai-runtime/agent 的 TestStartRunJournalsRealTokens
// (真 journal 全链)。

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

// xgwLiveControl 是「真实 SDK 编排」的 RunControlSource 测试 shim:内嵌
// fakeSessionSource 补齐 SessionSource 面(工厂 type-assert 同时命中两接口),
// RunControlSource 语义 mirror runcontrol.go 的纯内存契约——requestId 幂等表
// (reqMu + map)、current 会话校验(CurrentSessionID 由 handler 的
// session/new → SetSession 写入)、lease 三件套、Attach 走 rt.EnsureHub、
// RunStatus 走 rt.Active + 幂等表反查(纯内存分支,无 RunJournalStore)。
// 与生产实现的差异仅在簿记宿主(测试 shim vs AgentPlugin),管理面
// (manager.StartRun/ResolveApproval/EnsureHub)全部真实。
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

// newXGWLiveControl 构造 shim 并注册一个真实 runtime:真实 Session(脚本化
// provider + 真实工具链) + memEventJournal 作 RunEvents(StartRun ⑥ 的
// bindEvents 据此建真 hub;RunEvents 为 nil 则 hub 为 nil,attach 诚实报错)。
// RunJournal 留真 nil(typed-nil 纪律):RunStatus 走纯内存分支。HITL 集成
// (测试②)传 CollectTools=DefaultTools + 沙箱 normal,危险管道命令触发
// DecisionHitl(与 SDK TestApprovalAfterReconnect 同款探针)。
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

// SetSession 覆写:handler 的 session/new 经 SetSession 声明会话(fake 只记
// activeID),本 shim 同步推进 currentID —— requireCurrent 的 V1 路由校验锚点
// 与生产的 CurrentSessionID 语义对齐(生产里 session/new 即当前会话易主)。
func (f *xgwLiveControl) SetSession(sessionID, cwd string) {
	f.mu.Lock()
	f.currentID = sessionID
	f.mu.Unlock()
	f.fakeSessionSource.SetSession(sessionID, cwd)
}

// requireCurrent mirror runcontrol.go 的 V1 路由校验(拍板⑤):非 current 会话
// 一律拒绝,错误文案与生产同形(客户端按 -32000 透传感知)。
func (f *xgwLiveControl) requireCurrent(sessionID string) error {
	if cur := f.CurrentSessionID(); sessionID != cur {
		return fmt.Errorf("xgwLiveControl: session %q is not the current session %q (V1 routes the current session only)", sessionID, cur)
	}
	return nil
}

// StartRun mirror runcontrol.go:current 校验 → 幂等表命中短路(返回原 runID
// 与当前状态快照)→ manager.StartRun(空回调后台提交)→ 记账。
func (f *xgwLiveControl) StartRun(sessionID, input, requestID string) (string, string, error) {
	if err := f.requireCurrent(sessionID); err != nil {
		return "", "", err
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
	return h.RunID, string(h.State()), nil
}

// CancelRun mirror runcontrol.go:直转 manager(未知 run → ErrRunNotFound)。
func (f *xgwLiveControl) CancelRun(sessionID, runID string) error {
	if err := f.requireCurrent(sessionID); err != nil {
		return err
	}
	return f.m.CancelRun(f.sid, runID)
}

// RunStatus mirror runcontrol.go 纯内存两分支:rt.Active() 命中 → 实时快照;
// 幂等表按 runID 反查 → 终态保留快照。latestSeq 取 hub(会话维度游标);
// eventsComplete(I6lite)取 hub.EventsComplete(),hub 未建 → true(无失败证据)。
func (f *xgwLiveControl) RunStatus(sessionID, runID string) (string, uint64, bool, error) {
	if err := f.requireCurrent(sessionID); err != nil {
		return "", 0, true, err
	}
	rt := f.m.Get(f.sid)
	if rt == nil {
		return "", 0, true, errors.New("xgwLiveControl: no active runtime")
	}
	latest := uint64(0)
	eventsComplete := true
	if hub := rt.Hub(); hub != nil {
		latest = hub.LatestSeq()
		eventsComplete = hub.EventsComplete()
	}
	if run := rt.Active(); run != nil && run.ID == runID {
		return string(run.State()), latest, eventsComplete, nil
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
		return string(h.State()), latest, eventsComplete, nil
	}
	return "", 0, eventsComplete, fmt.Errorf("xgwLiveControl: run %q: %w", runID, runtimeagent.ErrRunNotFound)
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

// ResolveApproval mirror runcontrol.go:直转 manager(首个有效决策获胜)。
func (f *xgwLiveControl) ResolveApproval(requestID string, decision hitl.Decision) error {
	return f.m.ResolveApproval(requestID, decision)
}

// Attach mirror runcontrol.go:current 校验 → rt.EnsureHub(遗留③解法:attach
// 即建,含 SeqRange 播种)→ (hub, latest)。
func (f *xgwLiveControl) Attach(sessionID string) (*runtimeagent.EventHub, uint64, error) {
	if err := f.requireCurrent(sessionID); err != nil {
		return nil, 0, err
	}
	rt := f.m.Get(f.sid)
	if rt == nil {
		return nil, 0, errors.New("xgwLiveControl: no active runtime")
	}
	hub := rt.EnsureHub()
	if hub == nil {
		return nil, 0, fmt.Errorf("xgwLiveControl: event stream not enabled for session %q", sessionID)
	}
	return hub, hub.LatestSeq(), nil
}

// ActiveRun mirror runcontrol.go:current 会话的 rt.Active() 快照直转。
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

// liveNewSession 走 session/new,返回服务端签发的会话 id(handler.CreateSession
// 经 SetSession 把它写成 fakeSessionSource 的 current —— shim 的 current 校验
// 以此为锚)。
func (s *xgwLiveStack) liveNewSession(t *testing.T) string {
	t.Helper()
	sid, err := s.client.NewSession(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if sid == "" {
		t.Fatal("session/new returned empty sessionId")
	}
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

// ─────────────────────────────────────────────────────────────────────────────
// ①:initialize → session/new → attach(control) → run/start → 逐条收
// session/events(Seq 连续)+ ③ 的 requestId 幂等 / afterSeq 断点续传 /
// ErrCursorExpired。
// ─────────────────────────────────────────────────────────────────────────────

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

	// 逐条收 session/events:真实后台 run 的事件流(C2 修复后 tee 自供给采集,
	// 见文件头)= run.started 打头 → usage.progress(iteration/usage 中间件经
	// cb.Publish 的映射)→ text.delta(非流式整段文本)→
	// assistant.message_finalized(kernel 收尾 Done)→ run.finished 收尾。
	// 以「末位是 run.finished」为收齐锚点(流长度不再恒为 2)。
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

	// ③ afterSeq 断点续传:以 assistant.message_finalized(key 事件,不进合并
	// 缓冲)的 seq 为已确认游标重新 attach → 恰好补齐 (cursor, latest] =
	// [run.finished] 一条,无缺无重。不以 seq=1 续传:回放侧的合并 delta 记录
	// 按「一条记录一条 ToSeq 事件」塌缩交付(P3-1 契约;I2lite 起 FromSeq 随
	// 记录上 wire 作范围声明,交付粒度仍是每记录一条 ToSeq 事件),(1, latest]
	// 内的 usage.progress 合并段不逐条重放——断点续传的客户端契约是「游标后
	// 事件不缺终段」,对 delta 合并段以 ToSeq 为准。
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

// ─────────────────────────────────────────────────────────────────────────────
// ②:run 内触发 HITL → 第二连接 attach(control,接管)→ approval/respond →
// run 完成 → run/get 返回 completed + latestSeq(TestApprovalAfterReconnect 的
// 协议级对偶)。
// ─────────────────────────────────────────────────────────────────────────────

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
		"requestId": req.ID, "decision": map[string]any{"interrupt_id": req.ID, "type": "approve"},
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
		"requestId": req.ID, "decision": map[string]any{"interrupt_id": req.ID, "type": "approve"},
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

	// 双连接同流:B 的订阅(attach afterSeq=0:回放历史 + live 续传)与 A 并存
	// (多观察者;观察/控制分离)。回放侧的合并 delta 记录按「一条记录一条
	// ToSeq 事件」塌缩交付(P3-1 契约;I2lite 起 FromSeq 随记录上 wire 作范围
	// 声明,交付粒度不变),逐条全等只对 attach 点之后的实时段成立:B 的
	// Seq>bAttachLatest 段与 A 的同段逐条对齐,且两流末位 seq/类型一致(同一
	// 会话游标)、B 全流 seq 严格递增。
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

// ─────────────────────────────────────────────────────────────────────────────
// ①(补):token 派生事件(tool_call / text.delta / assistant.message_finalized)
// 的线上形状 —— 经 hub.Publish(teePublish 的生产发布面)脚本产出,attach 后
// 逐条到达;SessionEvent 信封(蛇形键 + seq 连续)是协议契约。
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// ①(补二,C2 修复回归):token 派生事件的真实后台生产链 —— run/start 以全 nil
// 回调提交的后台 run 经 tee 自供给的采集回调(supervisor.go teeRunCallbacks)产
// 出 tool_call / tool_result / text.delta / assistant.message_finalized,attach
// 订阅逐条到达。与 TestXGWIntegrationTokenEventWireShapes(hub.Publish 直发)
// 互为对偶:生产者是真实 manager → Session → ai-core kernel → tee → hub 链,
// 线上信封(蛇形键 + seq 连续)同一契约。
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// ③(补):标准方法(session/new|load|prompt|list)响应形状回归 —— x-goworker
// 注册与真实运行面并存时,标准面逐字不变(与 TestXGWStandardMethodsUnaffected
// 的 fake 面互为对偶,此处经真实 shim)。
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// ④:设备工具(P5-9)协议级可达面 —— initialize 声明 x-device → session/prompt
// 经 wire 触发 binder 探测(x-device/tools 过线)→ 工具经 x-device/call 过线
// 在客户端执行 → 断连收口解绑。等待重绑/超时的 run 级编排(后台 run + 断连 +
// 二连接重绑)在协议栈内需跨连接的 prompt 工具收集,而 handler 会话门禁串行化
// 了跨连接 prompt(在飞 prompt 持 gate 至返回),协议级编排会自锁——该语义由
// daemon 集成层覆盖(daemon/internal/plugins/agent/capability_test.go:
// TestDeviceToolBindingRebindAfterDisconnect / TestDeviceToolBindingTimeoutFails,
// 真实 binder + 真实 registry + 真实 proxy + fake caller),本测试钉协议接缝。
// ─────────────────────────────────────────────────────────────────────────────

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
