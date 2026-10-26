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
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// xgw_test.go 是 x-goworker 扩展协议面(P5-7)的测试:JSON-RPC 全栈(管道两端
// 跑真实 ai-acp Server/Client,零 mock 传输层)验证 wire 契约与 control lease
// 守卫,外加 Service.accept 路径的 Close 钩子测试。管道用 net.Pipe(全双工
// 内存管道,io.Pipe 的成对等价物)——ai-acp client_server_test 的既有全栈模式。
// 后台 Run 面全部用模拟 source(零真实 LLM),幂等承诺由 fake 建模(承诺在
// source 层,P5-6)。

// fakeStartCall / fakeCancelCall / fakeApprovalCall 是 source 层收到的调用记录。
type fakeStartCall struct {
	SessionID string
	Input     string
	RequestID string
}

type fakeCancelCall struct {
	SessionID string
	RunID     string
}

type fakeApprovalCall struct {
	RequestID string
	Decision  hitl.Decision
}

// fakeRunControl 是 model.RunControlSource 的脚本化 fake(内嵌 fakeSessionSource
// 补齐 SessionSource 面,供工厂 type-assert 命中)。幂等:runByReq 记账,同
// requestID 重试返回原 runID 与当前状态快照(建模 P5-6 承诺,不起跑)。
type fakeRunControl struct {
	*fakeSessionSource

	mu          sync.Mutex
	lease       string
	startCalls  []fakeStartCall // 仅真实新起跑;幂等命中不记
	runByReq    map[string]string
	cancelCalls []fakeCancelCall
	approvals   []fakeApprovalCall
	released    []string
	statusFn    func(sessionID, runID string) (string, uint64, error) // 缺省 running/42
	resolveErr  error                                                 // ResolveApproval 脚本化失败
	nextRun     int

	// P5-8 附加面:attachHub 配置后,StartRun 在其上产出 3 条 key 事件(建模
	// 「StartRun 产事件」,事件经真 EventHub 的 Publish 路径)并把活动 run 快照
	// 置为 (run-N, running) 供 ActiveRun 返回;attachErr 脚本化 Attach 失败。
	attachHub   *runtimeagent.EventHub
	attachErr   error
	curRunID    string
	curRunState string
}

func newFakeRunControl() *fakeRunControl {
	return &fakeRunControl{
		fakeSessionSource: &fakeSessionSource{},
		runByReq:          map[string]string{},
	}
}

func (f *fakeRunControl) StartRun(sessionID, input, requestID string) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if runID, ok := f.runByReq[requestID]; ok {
		// 幂等命中:P5-6 契约=返回原 runID 与其「当前」状态快照。fake 固定
		// 建模快照已推进到 running,与首次的 queued 可辨,供透传断言。
		return runID, "running", nil
	}
	f.nextRun++
	runID := fmt.Sprintf("run-%d", f.nextRun)
	f.runByReq[requestID] = runID
	f.startCalls = append(f.startCalls, fakeStartCall{SessionID: sessionID, Input: input, RequestID: requestID})
	// P5-8:「先 StartRun 产事件」的 fake 建模——经真 EventHub.Publish 产出
	// 三条 key 事件(durable-first 路径,内存 journal 同步落账),活动 run 快照
	// 置为 running。
	if f.attachHub != nil {
		for _, ev := range []struct {
			typ     string
			payload string
		}{
			{session.EventRunStarted, `{"state":"queued"}`},
			{session.EventToolCall, `{"name":"bash"}`},
			{session.EventRunFinished, `{"state":"completed"}`},
		} {
			if err := f.attachHub.Publish(runtimeagent.IngressEvent{
				RunID:   runID,
				Type:    ev.typ,
				Payload: json.RawMessage(ev.payload),
			}); err != nil {
				panic("fakeRunControl: publish scripted event: " + err.Error())
			}
		}
		f.curRunID = runID
		f.curRunState = "running"
	}
	return runID, "queued", nil
}

func (f *fakeRunControl) CancelRun(sessionID, runID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls = append(f.cancelCalls, fakeCancelCall{SessionID: sessionID, RunID: runID})
	return nil
}

func (f *fakeRunControl) RunStatus(sessionID, runID string) (string, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusFn != nil {
		return f.statusFn(sessionID, runID)
	}
	return "running", 42, nil
}

func (f *fakeRunControl) PendingApprovals(sessionID string) ([]model.PendingApprovalInfo, error) {
	return nil, nil
}

func (f *fakeRunControl) ResolveApproval(requestID string, decision hitl.Decision) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.resolveErr != nil {
		return f.resolveErr
	}
	f.approvals = append(f.approvals, fakeApprovalCall{RequestID: requestID, Decision: decision})
	return nil
}

// Attach(P5-8 测试面):返回配置的 attachHub 与其 latest;attachErr 脚本化失败。
func (f *fakeRunControl) Attach(sessionID string) (*runtimeagent.EventHub, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attachErr != nil {
		return nil, 0, f.attachErr
	}
	if f.attachHub == nil {
		return nil, 0, errors.New("fakeRunControl: no attach hub configured")
	}
	return f.attachHub, f.attachHub.LatestSeq(), nil
}

// ActiveRun(P5-8 测试面):StartRun 在 attachHub 配置下置的活动 run 快照;
// 无活动 run → ok=false(建模 currentRun=null)。
func (f *fakeRunControl) ActiveRun(sessionID string) (string, string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.curRunID == "" {
		return "", "", false
	}
	return f.curRunID, f.curRunState, true
}

func (f *fakeRunControl) AcquireControl(connectionID string) bool {
	if connectionID == "" {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lease == connectionID {
		return false
	}
	// 返回值语义对齐 P5-6 契约(runcontrol.go):空闲→占用并返回 false,他人
	// 持有→显式接管返回 true。P5-7 期间本 fake 恒返回 true,返回值首次被
	// P5-8 的 tookOver 回显断言消费时暴露(信息项:P5-7 测试未消费返回值)。
	tookOver := f.lease != ""
	f.lease = connectionID
	return tookOver
}

func (f *fakeRunControl) ReleaseControl(connectionID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lease == connectionID {
		f.lease = ""
	}
	f.released = append(f.released, connectionID)
}

func (f *fakeRunControl) HoldControl(connectionID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lease == connectionID
}

func (f *fakeRunControl) UnbindCapabilities(connectionID string) {}

func (f *fakeRunControl) CapabilityWaitTimeout() time.Duration { return time.Second }

func (f *fakeRunControl) startCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.startCalls)
}

func (f *fakeRunControl) releaseCount(connectionID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, id := range f.released {
		if id == connectionID {
			n++
		}
	}
	return n
}

// xgwStack 是一条全栈管道:服务端走 NewServer → registerXGWMethods → Start
// (与 Service.accept 的生产序列一致,accept 路径另有专项测试),客户端是
// 真实 aiacp.Client。evaluate 默认 no-op,可注入记录形。
type xgwStack struct {
	client  *aiacp.Client
	handler *FrontendHandler
	runs    *fakeRunControl
}

func newXGWStack(t *testing.T, evaluate Evaluator, frc *fakeRunControl) *xgwStack {
	t.Helper()
	if evaluate == nil {
		evaluate = func(*model.Context, string) error { return nil }
	}
	serverEnd, clientEnd := net.Pipe()
	handler := NewFrontendHandlerFactory(evaluate, nil, frc, NewSessionGate())().(*FrontendHandler)
	server, err := aiacp.NewServer(serverEnd, handler)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	registerXGWMethods(server, handler)
	server.Start()
	t.Cleanup(func() { _ = server.Close() })
	client, err := aiacp.NewClient("xgw-test", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &xgwStack{client: client, handler: handler, runs: frc}
}

// initAndNew 走标准握手 + session/new,返回服务端签发的会话 id。
func (s *xgwStack) initAndNew(t *testing.T) string {
	t.Helper()
	init, err := s.client.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if init.ProtocolVersion != protocol.Version {
		t.Fatalf("initialize protocol version = %d, want %d", init.ProtocolVersion, protocol.Version)
	}
	sid, err := s.client.NewSession(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	return sid
}

// callRaw 以原始 map 入参发起 x- 请求并取回原始 result:两侧都不过 Go 结构体,
// wire 键名与总方案 §7 报文的逐字契约由此钉死(结构体 tag 无法自证)。
func (s *xgwStack) callRaw(t *testing.T, method string, params map[string]any) (json.RawMessage, error) {
	t.Helper()
	var raw json.RawMessage
	err := s.client.Call(context.Background(), method, params, &raw)
	return raw, err
}

// wantXGWError 断言 err 是本协议约定的 -32000 RPC 错误并返回它供文案断言。
func wantXGWError(t *testing.T, what string, err error) *protocol.RPCError {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: want -32000 error, got nil", what)
	}
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("%s: error %v is not *protocol.RPCError", what, err)
	}
	if rpcErr.Code != xgwErrorCode {
		t.Fatalf("%s: error code = %d, want %d (message %q)", what, rpcErr.Code, xgwErrorCode, rpcErr.Message)
	}
	return rpcErr
}

// waitFor 轮询条件直至成立或超时(全栈异步拆卸的确定性等待)。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestXGWRunStartRoundTripAndRequestIdempotency(P5-7 主路径):持照连接
// run/start → {runId,state};wire 键名逐字(sessionId/input/requestId 入,
// runId/state 出,总方案 §7 :571-573 报文);同 requestId 重试返回同 runID
// 与 source 的当前状态快照,底层不再起跑(幂等在 source 层的端到端证明)。
func TestXGWRunStartRoundTripAndRequestIdempotency(t *testing.T) {
	frc := newFakeRunControl()
	stack := newXGWStack(t, nil, frc)
	sid := stack.initAndNew(t)
	// 测试面经 source 直接持照(AcquireControl 返回值是 tookOver 语义,非成败;
	// 持照以 HoldControl 断言)。
	stack.runs.AcquireControl(stack.handler.connectionID)
	if !stack.runs.HoldControl(stack.handler.connectionID) {
		t.Fatal("test setup: acquire control lease failed")
	}

	raw, err := stack.callRaw(t, XMethodRunStart, map[string]any{
		"sessionId": sid, "input": "分析当前项目架构", "requestId": "client-req-001",
	})
	if err != nil {
		t.Fatalf("run/start: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode run/start result %s: %v", raw, err)
	}
	if len(fields) != 2 {
		t.Fatalf("run/start result = %s, want exactly runId+state keys", raw)
	}
	if fields["runId"] != "run-1" || fields["state"] != "queued" {
		t.Fatalf("run/start result = %s, want {runId:run-1,state:queued}", raw)
	}

	// 同 requestId 重试:同 runID、source 当前状态快照透传(fake 建模 queued→running),
	// 不重复起跑。
	raw2, err := stack.callRaw(t, XMethodRunStart, map[string]any{
		"sessionId": sid, "input": "分析当前项目架构", "requestId": "client-req-001",
	})
	if err != nil {
		t.Fatalf("run/start retry: %v", err)
	}
	var fields2 map[string]any
	if err := json.Unmarshal(raw2, &fields2); err != nil {
		t.Fatalf("decode retry result %s: %v", raw2, err)
	}
	if fields2["runId"] != fields["runId"] {
		t.Fatalf("retry runId = %v, want original %v", fields2["runId"], fields["runId"])
	}
	if fields2["state"] != "running" {
		t.Fatalf("retry state = %v, want source's current snapshot %q", fields2["state"], "running")
	}
	if got := stack.runs.startCallCount(); got != 1 {
		t.Fatalf("source start calls = %d, want 1 (idempotent retry must not re-start)", got)
	}

	// 转发保真:source 收到的三元组与 wire 入参一致。
	stack.runs.mu.Lock()
	call := stack.runs.startCalls[0]
	stack.runs.mu.Unlock()
	if call.SessionID != sid || call.Input != "分析当前项目架构" || call.RequestID != "client-req-001" {
		t.Fatalf("source got %+v, want {sessionId:%q, input, requestId:client-req-001}", call, sid)
	}

	// 空 requestId:幂等键缺失,协议面诚实拒绝(不起跑)。
	_, err = stack.callRaw(t, XMethodRunStart, map[string]any{"sessionId": sid, "input": "x", "requestId": ""})
	rpcErr := wantXGWError(t, "run/start without requestId", err)
	if !strings.Contains(rpcErr.Message, "requestId") {
		t.Fatalf("empty requestId error message = %q, want requestId mention", rpcErr.Message)
	}
	if got := stack.runs.startCallCount(); got != 1 {
		t.Fatalf("source start calls after empty requestId = %d, want 1", got)
	}
}

// TestXGWRequiresControlLease(P5-7 守卫面):未持 control lease 的连接,
// run/start、run/cancel、approval/respond 一律 -32000 且不触碰 source;
// run/get 是只读观察面,不要求 lease(观察者/控制者分离,总方案 §7.4)。
func TestXGWRequiresControlLease(t *testing.T) {
	frc := newFakeRunControl()
	stack := newXGWStack(t, nil, frc)
	sid := stack.initAndNew(t)

	for _, tc := range []struct {
		what   string
		method string
		params map[string]any
	}{
		{"run/start", XMethodRunStart, map[string]any{"sessionId": sid, "input": "x", "requestId": "r1"}},
		{"run/cancel", XMethodRunCancel, map[string]any{"sessionId": sid, "runId": "run-1"}},
		{"approval/respond", XMethodApprovalRespond, map[string]any{
			"requestId": "req-1", "decision": map[string]any{"interrupt_id": "req-1", "type": "approve"},
		}},
	} {
		_, err := stack.callRaw(t, tc.method, tc.params)
		rpcErr := wantXGWError(t, tc.what+" without lease", err)
		if !strings.Contains(rpcErr.Message, "control lease") {
			t.Fatalf("%s without lease message = %q, want control lease mention", tc.what, rpcErr.Message)
		}
	}

	// 守卫先于 source:三次拒绝都没产生任何副作用。
	if got := stack.runs.startCallCount(); got != 0 {
		t.Fatalf("source start calls = %d, want 0", got)
	}
	stack.runs.mu.Lock()
	cancelled, approved := len(stack.runs.cancelCalls), len(stack.runs.approvals)
	stack.runs.mu.Unlock()
	if cancelled != 0 || approved != 0 {
		t.Fatalf("source saw cancel=%d approve=%d, want 0/0", cancelled, approved)
	}

	// run/get 无 lease 亦可(观察面)。
	raw, err := stack.callRaw(t, XMethodRunGet, map[string]any{"sessionId": sid, "runId": "run-1"})
	if err != nil {
		t.Fatalf("run/get without lease: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode run/get result %s: %v", raw, err)
	}
	if fields["state"] != "running" || fields["latestSeq"] != float64(42) {
		t.Fatalf("run/get result = %s, want {state:running,latestSeq:42}", raw)
	}
}

// TestXGWRunGetAndCancelForwarding(P5-7 转发面):run/get 透传 source 的状态
// 与 latestSeq、错误归一 -32000;run/cancel 持照后 accepted=true 且三元组转发,
// 释放后重新被拒。
func TestXGWRunGetAndCancelForwarding(t *testing.T) {
	frc := newFakeRunControl()
	stack := newXGWStack(t, nil, frc)
	sid := stack.initAndNew(t)

	// source 错误(如 ErrRunNotFound 语义)→ -32000 + 文案透传。
	stack.runs.mu.Lock()
	stack.runs.statusFn = func(string, string) (string, uint64, error) {
		return "", 0, fmt.Errorf("run %q: %w", "run-x", errors.New("not found"))
	}
	stack.runs.mu.Unlock()
	_, err := stack.callRaw(t, XMethodRunGet, map[string]any{"sessionId": sid, "runId": "run-x"})
	rpcErr := wantXGWError(t, "run/get unknown run", err)
	if !strings.Contains(rpcErr.Message, "not found") {
		t.Fatalf("run/get error message = %q, want source error text", rpcErr.Message)
	}

	stack.runs.AcquireControl(stack.handler.connectionID)
	raw, err := stack.callRaw(t, XMethodRunCancel, map[string]any{"sessionId": sid, "runId": "run-1"})
	if err != nil {
		t.Fatalf("run/cancel: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode run/cancel result %s: %v", raw, err)
	}
	if fields["accepted"] != true {
		t.Fatalf("run/cancel result = %s, want {accepted:true}", raw)
	}
	stack.runs.mu.Lock()
	call := stack.runs.cancelCalls[0]
	stack.runs.mu.Unlock()
	if call.SessionID != sid || call.RunID != "run-1" {
		t.Fatalf("source got cancel %+v, want {sessionId:%q, runId:run-1}", call, sid)
	}

	stack.runs.ReleaseControl(stack.handler.connectionID)
	_, err = stack.callRaw(t, XMethodRunCancel, map[string]any{"sessionId": sid, "runId": "run-1"})
	rpcErr = wantXGWError(t, "run/cancel after release", err)
	if !strings.Contains(rpcErr.Message, "control lease") {
		t.Fatalf("post-release cancel message = %q, want control lease mention", rpcErr.Message)
	}
}

// TestXGWApprovalRespondForwardsDecision(P5-7 审批面):decision wire 形状
// (interrupt_id/type,与 hitl.Decision JSON 同形)白名单映射转发;未知/空
// type 一律 -32000 且绝不触达 source(零值 DecisionType 穿透中间件=放行,
// 映射的失败方向必须是拒绝);source 错误归一 -32000。
func TestXGWApprovalRespondForwardsDecision(t *testing.T) {
	frc := newFakeRunControl()
	stack := newXGWStack(t, nil, frc)
	stack.initAndNew(t)
	stack.runs.AcquireControl(stack.handler.connectionID)

	respond := func(t *testing.T, decision map[string]any) (json.RawMessage, error) {
		t.Helper()
		return stack.callRaw(t, XMethodApprovalRespond, map[string]any{"requestId": "req-9", "decision": decision})
	}

	raw, err := respond(t, map[string]any{"interrupt_id": "req-9", "type": "approve"})
	if err != nil {
		t.Fatalf("approval/respond approve: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode respond result %s: %v", raw, err)
	}
	if fields["accepted"] != true {
		t.Fatalf("approval/respond result = %s, want {accepted:true}", raw)
	}

	for _, tc := range []struct {
		wireType string
		want     hitl.DecisionType
	}{
		{"approve", hitl.DecisionApprove},
		{"approve_always", hitl.DecisionApproveAlways},
		{"reject", hitl.DecisionReject},
		{"edit", hitl.DecisionEdit},
		{"respond", hitl.DecisionRespond},
	} {
		if _, err := respond(t, map[string]any{"interrupt_id": "req-9", "type": tc.wireType}); err != nil {
			t.Fatalf("approval/respond type %q: %v", tc.wireType, err)
		}
	}

	stack.runs.mu.Lock()
	approvals := append([]fakeApprovalCall(nil), stack.runs.approvals...)
	stack.runs.mu.Unlock()
	if len(approvals) != 6 {
		t.Fatalf("source approvals = %d, want 6", len(approvals))
	}
	for i, tc := range []struct {
		wantType hitl.DecisionType
	}{
		{hitl.DecisionApprove}, {hitl.DecisionApprove}, {hitl.DecisionApproveAlways},
		{hitl.DecisionReject}, {hitl.DecisionEdit}, {hitl.DecisionRespond},
	} {
		if approvals[i].RequestID != "req-9" {
			t.Fatalf("approval[%d] requestID = %q, want req-9", i, approvals[i].RequestID)
		}
		if approvals[i].Decision.Type != tc.wantType {
			t.Fatalf("approval[%d] type = %q, want %q", i, approvals[i].Decision.Type, tc.wantType)
		}
	}

	for _, tc := range []struct {
		what, wireType string
	}{
		{"unknown type", "explode"},
		{"empty type", ""},
	} {
		_, err := respond(t, map[string]any{"interrupt_id": "req-9", "type": tc.wireType})
		rpcErr := wantXGWError(t, tc.what, err)
		if !strings.Contains(rpcErr.Message, "decision type") {
			t.Fatalf("%s message = %q, want decision type mention", tc.what, rpcErr.Message)
		}
	}
	stack.runs.mu.Lock()
	got := len(stack.runs.approvals)
	stack.runs.mu.Unlock()
	if got != 6 {
		t.Fatalf("source approvals after invalid types = %d, want 6 (invalid decisions must not reach source)", got)
	}

	// source 错误(ErrApprovalResolved/Unknown 语义)→ -32000 + 文案透传。
	stack.runs.mu.Lock()
	stack.runs.resolveErr = errors.New("approval resolved")
	stack.runs.mu.Unlock()
	_, err = respond(t, map[string]any{"interrupt_id": "req-9", "type": "approve"})
	rpcErr := wantXGWError(t, "approval/respond source error", err)
	if !strings.Contains(rpcErr.Message, "approval resolved") {
		t.Fatalf("source error message = %q, want passthrough", rpcErr.Message)
	}
}

// TestXGWNotAvailableWithoutRunControlSource(P5-7 降级面):后端只有
// SessionSource(工厂 type-assert 不命中)→ 全部 x-goworker 方法诚实报错,
// 与 session load 降级同型;标准方法不受影响由 TestXGWStandardMethodsUnaffected
// 单独覆盖。
func TestXGWNotAvailableWithoutRunControlSource(t *testing.T) {
	stack := newXGWStack(t, nil, newFakeRunControl())
	// 换一个无 RunControlSource 的会话源重建 handler,直接断言工厂降级字段。
	bare := NewFrontendHandlerFactory(nil, nil, &fakeSessionSource{}, NewSessionGate())().(*FrontendHandler)
	if bare.runs != nil {
		t.Fatal("factory must leave runs nil for a SessionSource-only backend")
	}
	if bare.connectionID == "" || stack.handler.connectionID == "" {
		t.Fatal("factory must assign connectionID even without run control source")
	}
	_ = stack // stack 的 handler 有 runs;降级路径用 bare 走方法直调(不经 wire,已由其他用例覆盖形状)
	for _, tc := range []struct {
		what string
		call func() error
	}{
		{"run/start", func() error { _, err := bare.xgwRunStart(context.Background(), json.RawMessage(`{}`)); return err }},
		{"run/get", func() error { _, err := bare.xgwRunGet(context.Background(), json.RawMessage(`{}`)); return err }},
		{"run/cancel", func() error { _, err := bare.xgwRunCancel(context.Background(), json.RawMessage(`{}`)); return err }},
		{"approval/respond", func() error {
			_, err := bare.xgwApprovalRespond(context.Background(), json.RawMessage(`{}`))
			return err
		}},
	} {
		err := tc.call()
		rpcErr := wantXGWError(t, tc.what+" without run control source", err)
		if !strings.Contains(rpcErr.Message, "no run control capability") {
			t.Fatalf("%s message = %q, want not-available mention", tc.what, rpcErr.Message)
		}
	}
}

// TestXGWConnectionIDUniquePerFactoryCall(P5-7 工厂面):每连接一份
// connectionID,conn_ 前缀 + crypto/rand hex,形态对齐 ai-acp newSessionID。
func TestXGWConnectionIDUniquePerFactoryCall(t *testing.T) {
	factory := NewFrontendHandlerFactory(nil, nil, nil, NewSessionGate())
	first := factory().(*FrontendHandler)
	second := factory().(*FrontendHandler)
	if first.connectionID == "" || second.connectionID == "" {
		t.Fatal("factory must assign non-empty connectionID")
	}
	if first.connectionID == second.connectionID {
		t.Fatalf("connectionID reused across connections: %q", first.connectionID)
	}
	if !strings.HasPrefix(first.connectionID, "conn_") {
		t.Fatalf("connectionID = %q, want conn_ prefix", first.connectionID)
	}
	if len(first.connectionID) != len("conn_")+16 {
		t.Fatalf("connectionID = %q, want conn_ + 16 hex chars", first.connectionID)
	}
}

// TestXGWStandardMethodsUnaffected(P5-7 回归底线):x-goworker 注册之后,
// 固定 6 方法(initialize/session/new/load/list/prompt/cancel)响应形状逐字
// 不变——session/prompt 响应只含 stopReason:"end_turn" 一个键。
func TestXGWStandardMethodsUnaffected(t *testing.T) {
	frc := newFakeRunControl()
	stack := newXGWStack(t, nil, frc)
	sid := stack.initAndNew(t)

	// session/list:domain → wire 映射形状不变。
	frc.mu.Lock()
	frc.list = []model.SessionSummary{{SessionID: "archived-1", Cwd: "/archive", Title: "old"}}
	frc.mu.Unlock()
	list, err := stack.client.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("session/list: %v", err)
	}
	if len(list.Sessions) != 1 || list.Sessions[0].SessionID != "archived-1" || list.Sessions[0].Cwd != "/archive" {
		t.Fatalf("session/list = %+v, want one archived-1 entry", list.Sessions)
	}

	// session/load(归档,只读重放路径):不因扩展注册而变化。
	frc.mu.Lock()
	frc.archived = map[string][]model.SessionUpdate{
		"archived-1": {{Kind: model.SessionUpdateAgentMessageChunk, Content: &model.SessionContent{Type: "text", Text: "hi"}}},
	}
	frc.mu.Unlock()
	if _, err := stack.client.SessionLoad(context.Background(), "archived-1", "/archive"); err != nil {
		t.Fatalf("session/load: %v", err)
	}

	// session/prompt:响应逐字 = {"stopReason":"end_turn"}(原始 result 断言,
	// 不经 Go 结构体——形状回归须钉住 wire 字节)。
	var raw json.RawMessage
	err = stack.client.Call(context.Background(), protocol.MethodSessionPrompt, protocol.PromptRequest{
		SessionID: sid,
		Prompt:    []protocol.ContentBlock{protocol.TextBlock("hello")},
	}, &raw)
	if err != nil {
		t.Fatalf("session/prompt: %v", err)
	}
	if string(raw) != `{"stopReason":"end_turn"}` {
		t.Fatalf("session/prompt response = %s, want exactly end_turn shape", raw)
	}

	// session/cancel:通知面照常派发,连接随后仍可用(流未破坏)。
	if err := stack.client.Cancel(sid); err != nil {
		t.Fatalf("session/cancel: %v", err)
	}
	if _, err := stack.callRaw(t, XMethodRunGet, map[string]any{"sessionId": sid, "runId": "run-1"}); err != nil {
		t.Fatalf("run/get after cancel notification: %v", err)
	}
}

// TestXGWServiceAcceptClosesHandler(P5-7 接线面):Service.accept 路径
// (NewServer → registerXGWMethods → Start → Done → Close 钩子)端到端——
// 经 Service 的连接可调 x- 方法,连接断开后 handler.Close 释放 control lease
// (恰好一次)。
func TestXGWServiceAcceptClosesHandler(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	frc := newFakeRunControl()
	captured := make(chan *FrontendHandler, 1)
	inner := NewFrontendHandlerFactory(func(*model.Context, string) error { return nil }, nil, frc, NewSessionGate())
	handlerFactory := HandlerFactory(func() aiacp.SessionHandler {
		h := inner().(*FrontendHandler)
		captured <- h
		return h
	})
	svc, err := Start(handlerFactory, func() (Listener, error) {
		l, err := NewConnectionListener(serverEnd)
		if err != nil {
			return nil, err
		}
		return l, nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })

	var handler *FrontendHandler
	select {
	case handler = <-captured:
	case <-time.After(5 * time.Second):
		t.Fatal("service never created a handler")
	}

	client, err := aiacp.NewClient("xgw-close", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sid, err := client.NewSession(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	frc.AcquireControl(handler.connectionID)
	var raw json.RawMessage
	if err := client.Call(context.Background(), XMethodRunStart,
		map[string]any{"sessionId": sid, "input": "x", "requestId": "r1"}, &raw); err != nil {
		t.Fatalf("run/start through service accept path: %v", err)
	}

	// 断开 → server 退出 → accept 返回路径调 handler.Close → lease 释放恰好一次。
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	waitFor(t, "control lease release on disconnect", func() bool {
		return frc.releaseCount(handler.connectionID) == 1
	})
	if n := frc.releaseCount(handler.connectionID); n != 1 {
		t.Fatalf("release count = %d, want exactly 1", n)
	}
}

// —— P5-8:session/attach|detach + session/events 通知泵 ——

// memEventJournal 是 runtimeagent.EventJournal 的内存实现(P5-8 测试面):回放
// 断言不需要磁盘,但 Publish 的 durable-first AppendEvent 与 Subscribe 的
// ReadEvents/SeqRange 全部走真路径。
type memEventJournal struct {
	mu   sync.Mutex
	recs []session.EventRecord
}

func (j *memEventJournal) AppendEvent(rec session.EventRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.recs = append(j.recs, rec)
	return nil
}

func (j *memEventJournal) ReadEvents(afterSeq uint64) ([]session.EventRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]session.EventRecord, 0, len(j.recs))
	for _, rec := range j.recs {
		if rec.Seq > afterSeq {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (j *memEventJournal) SeqRange() (earliest, latest uint64, ok bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.recs) == 0 {
		return 0, 0, false
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
	return earliest, latest, true
}

// xgwEventLog 收集客户端收到的 x-goworker/session/events 通知:原始 params 解回
// session.SessionEvent,同时钉住「SessionEvent 原样 JSON」的双向往返与蛇形键
// 线上形状;解码失败记入 bad(断言零解码失败,通知 handler 内不 panic——ai-acp
// 会兜住通知 panic,panic 反而把失败藏进超时)。
type xgwEventLog struct {
	mu     sync.Mutex
	events []session.SessionEvent
	raws   []string
	bad    []string
}

func (l *xgwEventLog) handle(params json.RawMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.raws = append(l.raws, string(params))
	var ev session.SessionEvent
	if err := json.Unmarshal(params, &ev); err != nil {
		l.bad = append(l.bad, err.Error())
		return
	}
	l.events = append(l.events, ev)
}

func (l *xgwEventLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

func (l *xgwEventLog) snapshot() (events []session.SessionEvent, raws, bad []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]session.SessionEvent(nil), l.events...),
		append([]string(nil), l.raws...),
		append([]string(nil), l.bad...)
}

// newAttachStack 造一条带内存事件枢纽的 P5-8 测试栈;cleanup 先收订阅泵
// (handler.Close)再由 newXGWStack 的 LIFO cleanup 关连接,测试零泵泄漏。
func newAttachStack(t *testing.T) (*xgwStack, *runtimeagent.EventHub) {
	t.Helper()
	frc := newFakeRunControl()
	hub := runtimeagent.NewEventHub("sess-store-1", &memEventJournal{})
	frc.mu.Lock()
	frc.attachHub = hub
	frc.mu.Unlock()
	stack := newXGWStack(t, nil, frc)
	t.Cleanup(stack.handler.Close)
	return stack, hub
}

// attachRaw 发起 attach 并解出 result 键表;错误原样返回供 -32000 断言。
func (s *xgwStack) attachRaw(t *testing.T, params map[string]any) (map[string]any, error) {
	t.Helper()
	raw, err := s.callRaw(t, XMethodSessionAttach, params)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode attach result %s: %v", raw, err)
	}
	return fields, nil
}

// publishHubEvent 向枢纽直发一条探测事件(不走 StartRun 的脚本路径)。
func publishHubEvent(t *testing.T, hub *runtimeagent.EventHub, runID, typ string) {
	t.Helper()
	if err := hub.Publish(runtimeagent.IngressEvent{
		RunID: runID, Type: typ, Payload: json.RawMessage(`{"probe":true}`),
	}); err != nil {
		t.Fatalf("publish %s: %v", typ, err)
	}
}

// handlerSubCount 在 h.mu 内读本连接的订阅数(泵退出自清的确定性观察点)。
func handlerSubCount(h *FrontendHandler) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// TestXGWSessionAttachReplayNoGap(P5-8 主路径):先经协议面 run/start 产事件
// (fake 在真 EventHub 上产出 seq 1..3)→ 后 attach afterSeq=0 → 客户端收齐
// (0, latest] 无重无缺,Replay+Live 无缝续上 attach 后发布的事件;响应形状
// {currentRun,latestSeq}(tookOver 省略)、事件线上形状 = SessionEvent 原样
// JSON(蛇形键,总方案 §7 报文)。
func TestXGWSessionAttachReplayNoGap(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)

	// 先 StartRun 产事件:run/start 要求 control lease(测试面直接经 source 设置,
	// 与 P5-7 既有用例同款;attach 自身的 lease 获取路径由接管/observe 用例覆盖)。
	stack.runs.AcquireControl(stack.handler.connectionID)
	if !stack.runs.HoldControl(stack.handler.connectionID) {
		t.Fatal("test setup: acquire control lease failed")
	}
	if _, err := stack.callRaw(t, XMethodRunStart, map[string]any{
		"sessionId": sid, "input": "x", "requestId": "r1",
	}); err != nil {
		t.Fatalf("run/start: %v", err)
	}
	if latest := hub.LatestSeq(); latest != 3 {
		t.Fatalf("setup: hub latestSeq = %d, want 3 scripted events", latest)
	}

	// 后 attach afterSeq=0(mode 省略 = observe 默认,总方案 §7 示例报文形态)。
	fields, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("attach result = %v, want exactly currentRun+latestSeq keys", fields)
	}
	cur, ok := fields["currentRun"].(map[string]any)
	if !ok || cur["runId"] != "run-1" || cur["state"] != "running" {
		t.Fatalf("attach currentRun = %v, want {runId:run-1,state:running}", fields["currentRun"])
	}
	if fields["latestSeq"] != float64(3) {
		t.Fatalf("attach latestSeq = %v, want 3", fields["latestSeq"])
	}

	// 收齐 (0, latest]:逐条保序、无重无缺。
	waitFor(t, "replayed events", func() bool { return log.count() >= 3 })
	events, raws, bad := log.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	if len(events) != 3 {
		t.Fatalf("replayed %d events, want exactly 3", len(events))
	}
	wantTypes := []string{session.EventRunStarted, session.EventToolCall, session.EventRunFinished}
	for i, ev := range events {
		if ev.Seq != uint64(i+1) || ev.Type != wantTypes[i] || ev.RunID != "run-1" {
			t.Fatalf("replay[%d] = {seq:%d type:%s run:%s}, want {seq:%d type:%s run:run-1}",
				i, ev.Seq, ev.Type, ev.RunID, i+1, wantTypes[i])
		}
	}
	// 线上形状 = SessionEvent 原样 JSON:蛇形 session_id 键(journal.go:88-95)。
	if !strings.Contains(raws[0], `"session_id"`) {
		t.Fatalf("notification params = %s, want SessionEvent as-is JSON (session_id key)", raws[0])
	}

	// Replay+Live 无缝:attach 后发布的事件经 live 侧到达,全流无重复。
	publishHubEvent(t, hub, "run-1", session.EventToolResult)
	waitFor(t, "live event after attach", func() bool { return log.count() >= 4 })
	events, _, bad = log.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	counts := map[uint64]int{}
	for _, ev := range events {
		counts[ev.Seq]++
	}
	if len(counts) != 4 {
		t.Fatalf("delivered %d distinct seqs, want 4", len(counts))
	}
	for seq, n := range counts {
		if n != 1 {
			t.Fatalf("event seq %d delivered %d times, want exactly 1", seq, n)
		}
	}
	if last := events[len(events)-1]; last.Seq != 4 || last.Type != session.EventToolResult {
		t.Fatalf("last event = {seq:%d type:%s}, want {seq:4 type:tool_result}", last.Seq, last.Type)
	}
}

// TestXGWSessionAttachCursorExpired(P5-8 窗口校验):afterSeq 超前 latest →
// hub 的 ErrCursorExpired 原样上抛为 -32000,订阅不建立、无通知下行;范围内
// 游标(afterSeq=1)照常建立并从中段续播。
func TestXGWSessionAttachCursorExpired(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)
	for _, typ := range []string{session.EventRunStarted, session.EventToolCall, session.EventRunFinished} {
		publishHubEvent(t, hub, "run-1", typ)
	}

	_, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 10})
	rpcErr := wantXGWError(t, "attach with future cursor", err)
	if !strings.Contains(rpcErr.Message, "cursor expired") {
		t.Fatalf("future cursor message = %q, want ErrCursorExpired text", rpcErr.Message)
	}
	if n := handlerSubCount(stack.handler); n != 0 {
		t.Fatalf("subscriptions after failed attach = %d, want 0", n)
	}
	if got := log.count(); got != 0 {
		t.Fatalf("notifications after failed attach = %d, want 0", got)
	}

	// 范围内游标:中段续播 (1, latest]。
	fields, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 1})
	if err != nil {
		t.Fatalf("attach from seq 1: %v", err)
	}
	if fields["latestSeq"] != float64(3) {
		t.Fatalf("attach latestSeq = %v, want 3", fields["latestSeq"])
	}
	waitFor(t, "resumed events from cursor 1", func() bool { return log.count() >= 2 })
	events, _, bad := log.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	if len(events) != 2 || events[0].Seq != 2 || events[1].Seq != 3 {
		t.Fatalf("resumed events = %v, want exactly [seq 2, seq 3]", events)
	}
}

// TestXGWSessionAttachControlTakeover(P5-8 Observe/Control):attach(mode=control)
// 经 wire 获取 lease(空闲获取 tookOver 省略);第二连接 attach(mode=control)
// 显式接管(tookOver=true 回显);接管后原 holder 的 approval/respond 被
// requireControl 拒绝,新 holder 可用;lease 易主不断原 holder 的事件流。
func TestXGWSessionAttachControlTakeover(t *testing.T) {
	frc := newFakeRunControl()
	hub := runtimeagent.NewEventHub("sess-store-1", &memEventJournal{})
	frc.mu.Lock()
	frc.attachHub = hub
	frc.mu.Unlock()
	stackA := newXGWStack(t, nil, frc)
	stackB := newXGWStack(t, nil, frc)
	t.Cleanup(stackA.handler.Close)
	t.Cleanup(stackB.handler.Close)
	sidA := stackA.initAndNew(t)
	sidB := stackB.initAndNew(t)
	logA := &xgwEventLog{}
	stackA.client.HandleNotification(XMethodSessionEvents, logA.handle)

	// A:空闲获取,tookOver 省略(false),currentRun=null(无活动 run)。
	fieldsA, err := stackA.attachRaw(t, map[string]any{"sessionId": sidA, "afterSeq": 0, "mode": "control"})
	if err != nil {
		t.Fatalf("attach A control: %v", err)
	}
	if _, present := fieldsA["tookOver"]; present {
		t.Fatalf("attach A result contains tookOver = %v, want omitted on fresh acquire", fieldsA["tookOver"])
	}
	if fieldsA["currentRun"] != nil {
		t.Fatalf("attach A currentRun = %v, want null (no active run)", fieldsA["currentRun"])
	}
	if !frc.HoldControl(stackA.handler.connectionID) {
		t.Fatal("attach(mode=control) did not acquire the lease")
	}

	// B:显式接管,tookOver=true 回显;A 失去 lease。
	fieldsB, err := stackB.attachRaw(t, map[string]any{"sessionId": sidB, "afterSeq": 0, "mode": "control"})
	if err != nil {
		t.Fatalf("attach B control: %v", err)
	}
	if fieldsB["tookOver"] != true {
		t.Fatalf("attach B tookOver = %v, want true (explicit takeover)", fieldsB["tookOver"])
	}
	if frc.HoldControl(stackA.handler.connectionID) {
		t.Fatal("original holder still holds the lease after takeover")
	}
	if !frc.HoldControl(stackB.handler.connectionID) {
		t.Fatal("takeover connection does not hold the lease")
	}

	// 原 holder 的 approval/respond 被拒(-32000,control lease),不触达 source。
	_, err = stackA.callRaw(t, XMethodApprovalRespond, map[string]any{
		"requestId": "req-1", "decision": map[string]any{"interrupt_id": "req-1", "type": "approve"},
	})
	rpcErr := wantXGWError(t, "respond from evicted holder", err)
	if !strings.Contains(rpcErr.Message, "control lease") {
		t.Fatalf("evicted holder message = %q, want control lease mention", rpcErr.Message)
	}
	frc.mu.Lock()
	got := len(frc.approvals)
	frc.mu.Unlock()
	if got != 0 {
		t.Fatalf("source approvals after evicted respond = %d, want 0", got)
	}

	// 新 holder 的 respond 照常转发。
	raw, err := stackB.callRaw(t, XMethodApprovalRespond, map[string]any{
		"requestId": "req-1", "decision": map[string]any{"interrupt_id": "req-1", "type": "approve"},
	})
	if err != nil {
		t.Fatalf("respond from new holder: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode respond result %s: %v", raw, err)
	}
	if fields["accepted"] != true {
		t.Fatalf("respond result = %s, want {accepted:true}", raw)
	}

	// lease 易主不断事件流:A 的订阅泵仍在,实时事件照收(观察/控制分离)。
	publishHubEvent(t, hub, "run-1", session.EventToolCall)
	waitFor(t, "event on evicted holder's stream", func() bool { return logA.count() >= 1 })
}

// TestXGWSessionAttachObserveCannotRespond(P5-8 observe 面):mode=observe
// 不动 lease,approval/respond 被 requireControl 拒绝(-32000)且不触达 source;
// 观察流本身正常(observe=只读观察面,总方案 §7.4)。
func TestXGWSessionAttachObserveCannotRespond(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)

	fields, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "observe"})
	if err != nil {
		t.Fatalf("attach observe: %v", err)
	}
	if _, present := fields["tookOver"]; present {
		t.Fatalf("observe attach contains tookOver = %v, want omitted", fields["tookOver"])
	}
	if frc := stack.runs; frc.HoldControl(stack.handler.connectionID) {
		t.Fatal("observe attach must not touch the control lease")
	}

	_, err = stack.callRaw(t, XMethodApprovalRespond, map[string]any{
		"requestId": "req-1", "decision": map[string]any{"interrupt_id": "req-1", "type": "approve"},
	})
	rpcErr := wantXGWError(t, "respond from observe connection", err)
	if !strings.Contains(rpcErr.Message, "control lease") {
		t.Fatalf("observe respond message = %q, want control lease mention", rpcErr.Message)
	}
	frc := stack.runs
	frc.mu.Lock()
	got := len(frc.approvals)
	frc.mu.Unlock()
	if got != 0 {
		t.Fatalf("source approvals after observe respond = %d, want 0", got)
	}

	// 观察流正常:实时事件照收。
	publishHubEvent(t, hub, "run-1", session.EventToolCall)
	waitFor(t, "observe stream event", func() bool { return log.count() >= 1 })
}

// TestXGWSessionAttachReplacesPump(P5-8 换代面):重复 attach 先 cancel 旧泵
// 再建新——旧订阅句柄已 stop(done 关闭)、表内恰一新泵;换代后发布的实时事件
// 恰好投递一次(旧泵不重发;done 优先检查使丢弃确定性成立)。
func TestXGWSessionAttachReplacesPump(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)
	for _, typ := range []string{session.EventRunStarted, session.EventToolCall, session.EventRunFinished} {
		publishHubEvent(t, hub, "run-1", typ)
	}

	// attach#1 afterSeq=0:收齐 1..3。
	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0}); err != nil {
		t.Fatalf("attach #1: %v", err)
	}
	waitFor(t, "first subscription replay", func() bool { return log.count() >= 3 })
	stack.handler.mu.Lock()
	sub1 := stack.handler.subs[sid]
	stack.handler.mu.Unlock()
	if sub1 == nil {
		t.Fatal("attach #1 left no subscription")
	}

	// attach#2 afterSeq=3(客户端已确认游标):换泵,表内恰一、句柄已换代。
	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 3}); err != nil {
		t.Fatalf("attach #2: %v", err)
	}
	stack.handler.mu.Lock()
	sub2, n := stack.handler.subs[sid], len(stack.handler.subs)
	stack.handler.mu.Unlock()
	if n != 1 {
		t.Fatalf("subscriptions after re-attach = %d, want exactly 1", n)
	}
	if sub2 == nil || sub2 == sub1 {
		t.Fatal("re-attach did not replace the subscription handle")
	}
	select {
	case <-sub1.done:
	default:
		t.Fatal("old pump was not canceled by the replacing attach")
	}

	// 换代后发布的实时事件恰好一次(旧泵已收,不重发)。
	publishHubEvent(t, hub, "run-1", session.EventToolResult)
	waitFor(t, "live event after re-attach", func() bool { return log.count() >= 4 })
	events, _, bad := log.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	counts := map[uint64]int{}
	for _, ev := range events {
		counts[ev.Seq]++
	}
	if len(counts) != 4 {
		t.Fatalf("delivered %d distinct seqs, want 4", len(counts))
	}
	for seq, c := range counts {
		if c != 1 {
			t.Fatalf("event seq %d delivered %d times, want exactly 1", seq, c)
		}
	}
}

// TestXGWSessionDetachCleansUp(P5-8 detach 面):detach(mode=control) 收泵
// (泵退出自清,订阅表收空)并归还 lease;detach 后发布的事件不再下行;
// detach 幂等;detach(observe) 只收泵、lease 不动。
func TestXGWSessionDetachCleansUp(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)

	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach control: %v", err)
	}
	if !stack.runs.HoldControl(stack.handler.connectionID) {
		t.Fatal("test setup: attach(control) did not acquire the lease")
	}

	raw, err := stack.callRaw(t, XMethodSessionDetach, map[string]any{"sessionId": sid, "mode": "control"})
	if err != nil {
		t.Fatalf("detach: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode detach result %s: %v", raw, err)
	}
	if fields["detached"] != true {
		t.Fatalf("detach result = %s, want {detached:true}", raw)
	}
	if stack.runs.HoldControl(stack.handler.connectionID) {
		t.Fatal("detach(mode=control) did not release the lease")
	}
	// 泵退出自清:订阅表收空(泵退出在 SDK 泵关通道之后,自清晚于订阅摘除)。
	waitFor(t, "pump self-clean after detach", func() bool { return handlerSubCount(stack.handler) == 0 })

	// detach 后发布的事件不再下行:协议泵已退出(Notify 唯一下行路径),且
	// SDK 侧订阅已随 cancel 摘除——零投递是结构保证,宽限后断言。
	publishHubEvent(t, hub, "run-1", session.EventToolCall)
	time.Sleep(100 * time.Millisecond)
	if got := log.count(); got != 0 {
		t.Fatalf("notifications after detach = %d, want 0", got)
	}

	// detach 幂等:无订阅再 detach 仍 detached=true。
	raw, err = stack.callRaw(t, XMethodSessionDetach, map[string]any{"sessionId": sid})
	if err != nil {
		t.Fatalf("idempotent detach: %v", err)
	}
	if err := json.Unmarshal(raw, &fields); err != nil || fields["detached"] != true {
		t.Fatalf("idempotent detach result = %s (%v), want {detached:true}", raw, err)
	}

	// detach(observe) 只收泵:lease 由前次 attach(control) 重新获取后保持。
	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("re-attach control: %v", err)
	}
	if _, err := stack.callRaw(t, XMethodSessionDetach, map[string]any{"sessionId": sid, "mode": "observe"}); err != nil {
		t.Fatalf("detach observe: %v", err)
	}
	if !stack.runs.HoldControl(stack.handler.connectionID) {
		t.Fatal("detach(mode=observe) must not release the lease")
	}
}

// TestXGWHandlerCloseReclaimsSubscriptions(P5-8 Close 收口):handler.Close
// 统一回收全部订阅泵(多会话多泵一并收空、lease 归还、重复 Close 幂等),
// 泵回收后无下行。
func TestXGWHandlerCloseReclaimsSubscriptions(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid1 := stack.initAndNew(t)
	sid2 := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)

	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid1, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach sid1 control: %v", err)
	}
	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid2, "afterSeq": 0}); err != nil {
		t.Fatalf("attach sid2 observe: %v", err)
	}
	if n := handlerSubCount(stack.handler); n != 2 {
		t.Fatalf("subscriptions before close = %d, want 2 (one per session)", n)
	}

	stack.handler.Close()
	waitFor(t, "pumps reclaimed by Close", func() bool { return handlerSubCount(stack.handler) == 0 })
	if stack.runs.HoldControl(stack.handler.connectionID) {
		t.Fatal("Close did not release the lease acquired via attach(control)")
	}
	publishHubEvent(t, hub, "run-1", session.EventToolCall)
	time.Sleep(100 * time.Millisecond)
	if got := log.count(); got != 0 {
		t.Fatalf("notifications after Close = %d, want 0", got)
	}
	// 重复 Close 幂等(无订阅、lease 已空)。
	stack.handler.Close()
}
