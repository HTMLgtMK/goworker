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

// Attach 按 model.RunControlSource 签名返回 hub;P5-7 的测试面不消费 attach
// (P5-8 接线),fake 诚实报错。
func (f *fakeRunControl) Attach(sessionID string) (*runtimeagent.EventHub, uint64, error) {
	return nil, 0, errors.New("fakeRunControl: attach not wired in P5-7")
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
	f.lease = connectionID
	return true
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
	if !stack.runs.AcquireControl(stack.handler.connectionID) {
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
