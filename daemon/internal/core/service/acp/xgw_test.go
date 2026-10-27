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
	SessionID string
	RequestID string
	Decision  hitl.Decision
}

// fakeRunControl 是 model.RunControlSource 的脚本化 fake(内嵌 fakeSessionSource
// 补齐 SessionSource 面,供工厂 type-assert 命中)。幂等:runByReq 记账,同
// requestID 重试返回原 runID 与当前状态快照(建模 P5-6 承诺,不起跑)。
// approvalOwner 是服务端记录的审批归属(C1 修复建模:mirror runcontrol.go——
// 未知 requestID → ErrApprovalUnknown 语义;归属会话与调用方声明不一致 → 越权
// 拒绝),由用例按 wire 会话 id 预置。
type fakeRunControl struct {
	*fakeSessionSource

	mu            sync.Mutex
	leases        map[string]string // sessionID → connectionID(I3 per-session 租约)
	startCalls    []fakeStartCall   // 仅真实新起跑;幂等命中不记
	runByReq      map[string]string
	cancelCalls   []fakeCancelCall
	approvals     []fakeApprovalCall
	approvalOwner map[string]string // requestID → 归属会话(服务端记录)
	released      []string
	statusFn      func(sessionID, runID string) (string, uint64, string, error) // 缺省 running/42/complete
	resolveErr    error                                                         // ResolveApproval 脚本化失败
	nextRun       int

	// P5-8 附加面:attachHub 配置后,StartRun 在其上产出 3 条 key 事件(建模
	// 「StartRun 产事件」,事件经真 EventHub 的 Publish 路径)并把活动 run 快照
	// 置为 (run-N, running) 供 ActiveRun 返回;attachErr 脚本化 Attach 失败。
	// I1 修正后 ActiveRun 按会话路由,fake 同型按会话记账(mirror runcontrol.go:
	// 每会话各自的活动快照,StartRun 置账、本 fake 不清账——快照观察面,对照
	// 会话查不到即 null)。
	attachHub  *runtimeagent.EventHub
	attachErr  error
	activeRuns map[string]fakeActiveRun
}

// fakeActiveRun 是 fakeRunControl 按会话记账的活动 run 快照(ActiveRun 返回值)。
type fakeActiveRun struct {
	runID string
	state string
}

func newFakeRunControl() *fakeRunControl {
	return &fakeRunControl{
		fakeSessionSource: &fakeSessionSource{},
		runByReq:          map[string]string{},
		leases:            map[string]string{},
		approvalOwner:     map[string]string{},
		activeRuns:        map[string]fakeActiveRun{},
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
		f.activeRuns[sessionID] = fakeActiveRun{runID: runID, state: "running"}
	}
	return runID, "queued", nil
}

func (f *fakeRunControl) CancelRun(sessionID, runID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls = append(f.cancelCalls, fakeCancelCall{SessionID: sessionID, RunID: runID})
	return nil
}

func (f *fakeRunControl) RunStatus(sessionID, runID string) (string, uint64, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusFn != nil {
		return f.statusFn(sessionID, runID)
	}
	return "running", 42, runtimeagent.ReplayIntegrityComplete, nil
}

func (f *fakeRunControl) PendingApprovals(sessionID string) ([]model.PendingApprovalInfo, error) {
	return nil, nil
}

// ResolveApproval mirror runcontrol.go 的 C1 契约(归属三段校验的 source 侧):
// resolveErr 脚本化失败优先;未知 requestID → ErrApprovalUnknown 语义;归属会话
// 与调用方声明不一致 → 越权拒绝(服务端记录为准,不信客户端声明);匹配才记账。
func (f *fakeRunControl) ResolveApproval(sessionID, requestID string, decision hitl.Decision) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.resolveErr != nil {
		return f.resolveErr
	}
	owner, ok := f.approvalOwner[requestID]
	if !ok {
		return fmt.Errorf("approval %q: %w", requestID, runtimeagent.ErrApprovalUnknown)
	}
	if owner != sessionID {
		return fmt.Errorf("approval %q belongs to session %q, not %q", requestID, owner, sessionID)
	}
	f.approvals = append(f.approvals, fakeApprovalCall{SessionID: sessionID, RequestID: requestID, Decision: decision})
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

// ActiveRun(P5-8 测试面;I1 修正后按会话路由,fake 同型 mirror runcontrol.go):
// StartRun 在 attachHub 配置下按会话置的活动 run 快照;该会话无活动 run →
// ok=false(建模 currentRun=null,非错误)。
func (f *fakeRunControl) ActiveRun(sessionID string) (string, string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ar, ok := f.activeRuns[sessionID]
	if !ok {
		return "", "", false
	}
	return ar.runID, ar.state, true
}

// AcquireControl / ReleaseControl / HoldControl / ReleaseConnection(I3
// per-session 租约,mirror runcontrol.go 契约): leases 按 sessionID 记账,同会话
// 空闲→false、他人持有→接管 true、同持有者重复→false;Release 仅本人有效;
// ReleaseConnection 释放该连接全部租约(released 记账供断连收口断言)。
func (f *fakeRunControl) AcquireControl(sessionID, connectionID string) bool {
	if connectionID == "" {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases[sessionID] == connectionID {
		return false
	}
	tookOver := f.leases[sessionID] != ""
	f.leases[sessionID] = connectionID
	return tookOver
}

func (f *fakeRunControl) ReleaseControl(sessionID, connectionID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases[sessionID] == connectionID {
		delete(f.leases, sessionID)
	}
	f.released = append(f.released, connectionID)
}

func (f *fakeRunControl) HoldControl(sessionID, connectionID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leases[sessionID] == connectionID
}

func (f *fakeRunControl) ReleaseConnection(connectionID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for sessionID, holder := range f.leases {
		if holder == connectionID {
			delete(f.leases, sessionID)
		}
	}
	f.released = append(f.released, connectionID)
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
	stack.runs.AcquireControl(sid, stack.handler.connectionID)
	if !stack.runs.HoldControl(sid, stack.handler.connectionID) {
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
			"sessionId": sid, "requestId": "req-1", "decision": map[string]any{"interrupt_id": "req-1", "type": "approve"},
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
	// replayIntegrity(I4)三态上 wire:fake 缺省无失败证据 → complete,恒显式
	// 携带(run/get 响应面,I4 老字段 eventsComplete 的替换)。
	if fields["replayIntegrity"] != runtimeagent.ReplayIntegrityComplete {
		t.Fatalf("run/get replayIntegrity = %v, want %q", fields["replayIntegrity"], runtimeagent.ReplayIntegrityComplete)
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
	stack.runs.statusFn = func(string, string) (string, uint64, string, error) {
		return "", 0, runtimeagent.ReplayIntegrityComplete, fmt.Errorf("run %q: %w", "run-x", errors.New("not found"))
	}
	stack.runs.mu.Unlock()
	_, err := stack.callRaw(t, XMethodRunGet, map[string]any{"sessionId": sid, "runId": "run-x"})
	rpcErr := wantXGWError(t, "run/get unknown run", err)
	if !strings.Contains(rpcErr.Message, "not found") {
		t.Fatalf("run/get error message = %q, want source error text", rpcErr.Message)
	}

	stack.runs.AcquireControl(sid, stack.handler.connectionID)
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

	stack.runs.ReleaseControl(sid, stack.handler.connectionID)
	_, err = stack.callRaw(t, XMethodRunCancel, map[string]any{"sessionId": sid, "runId": "run-1"})
	rpcErr = wantXGWError(t, "run/cancel after release", err)
	if !strings.Contains(rpcErr.Message, "control lease") {
		t.Fatalf("post-release cancel message = %q, want control lease mention", rpcErr.Message)
	}
}

// TestXGWReplayIntegrityForwarding(I4 验收):回放完整性三态在 daemon 两个响应
// 面的 wire 断言(老字段 eventsComplete 布尔 → replayIntegrity 三态字符串):
//   - run/get:source RunStatus 返回的三态原样透传(此处脚本 incomplete,模拟
//     本进程发生过 key 事件落盘失败/合并 flush 失败的粘滞态);
//   - attach:真 EventHub 的 replayIntegrity 上 wire——注错 journal 的 key 事件
//     发布失败 → incomplete(粘滞),响应显式携带;complete 面由
//     TestXGWSessionAttachReplayNoGap(健康 hub)覆盖。
func TestXGWReplayIntegrityForwarding(t *testing.T) {
	frc := newFakeRunControl()
	stack := newXGWStack(t, nil, frc)
	sid := stack.initAndNew(t)
	stack.runs.mu.Lock()
	stack.runs.statusFn = func(string, string) (string, uint64, string, error) {
		return "running", 7, runtimeagent.ReplayIntegrityIncomplete, nil
	}
	stack.runs.mu.Unlock()
	raw, err := stack.callRaw(t, XMethodRunGet, map[string]any{"sessionId": sid, "runId": "run-1"})
	if err != nil {
		t.Fatalf("run/get: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode run/get result %s: %v", raw, err)
	}
	if fields["replayIntegrity"] != runtimeagent.ReplayIntegrityIncomplete {
		t.Fatalf("run/get replayIntegrity = %v, want %q(source 三态原样透传)",
			fields["replayIntegrity"], runtimeagent.ReplayIntegrityIncomplete)
	}

	// attach 面:注错 journal → key 事件发布失败 → hub 置 incomplete(粘滞),
	// attach 响应显式携带。Subscribe 仍可建立(err journal 读面恒空)。
	badHub := runtimeagent.NewEventHub("sess-store-1", &errEventJournal{})
	if err := badHub.Publish(runtimeagent.IngressEvent{
		RunID: "run-1", Type: session.EventToolCall, Payload: json.RawMessage(`"c"`),
	}); err == nil {
		t.Fatal("setup: publish key event on failing journal = nil, want error")
	}
	if got := badHub.ReplayIntegrity(); got != runtimeagent.ReplayIntegrityIncomplete {
		t.Fatalf("setup: hub replayIntegrity = %q, want %q", got, runtimeagent.ReplayIntegrityIncomplete)
	}
	frc2 := newFakeRunControl()
	frc2.mu.Lock()
	frc2.attachHub = badHub
	frc2.mu.Unlock()
	stack2 := newXGWStack(t, nil, frc2)
	sid2 := stack2.initAndNew(t)
	fields2, err := stack2.attachRaw(t, map[string]any{"sessionId": sid2, "afterSeq": 0})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if fields2["replayIntegrity"] != runtimeagent.ReplayIntegrityIncomplete {
		t.Fatalf("attach replayIntegrity = %v, want %q(注错 journal 的粘滞三态上 wire)",
			fields2["replayIntegrity"], runtimeagent.ReplayIntegrityIncomplete)
	}
}

// TestXGWApprovalRespondForwardsDecision(P5-7 审批面):decision wire 形状
// (interrupt_id/type,与 hitl.Decision JSON 同形)白名单映射转发;未知/空
// type 一律 -32000 且绝不触达 source(零值 DecisionType 穿透中间件=放行,
// 映射的失败方向必须是拒绝);source 错误归一 -32000。持照走生产路径
// attach(mode=control) 建立 sid 的 lease,respond 携带 sessionId(C1 修复
// 后的 wire 形状)按会话过守卫;fake 的服务端归属预置 req-9 → sid。
func TestXGWApprovalRespondForwardsDecision(t *testing.T) {
	frc := newFakeRunControl()
	hub := runtimeagent.NewEventHub("sess-store-1", &memEventJournal{})
	frc.mu.Lock()
	frc.attachHub = hub
	frc.mu.Unlock()
	stack := newXGWStack(t, nil, frc)
	sid := stack.initAndNew(t)
	frc.mu.Lock()
	frc.approvalOwner["req-9"] = sid
	frc.mu.Unlock()
	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach control: %v", err)
	}

	respond := func(t *testing.T, decision map[string]any) (json.RawMessage, error) {
		t.Helper()
		return stack.callRaw(t, XMethodApprovalRespond, map[string]any{
			"sessionId": sid, "requestId": "req-9", "decision": decision,
		})
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
		if approvals[i].SessionID != sid {
			t.Fatalf("approval[%d] sessionID = %q, want wire sessionId %q (wire→source 转发保真)", i, approvals[i].SessionID, sid)
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

// TestXGWApprovalCrossSessionOwnership(C1 修复验收):A 控制会话 A、B 控制会话
// B,B 会话有挂起审批(服务端记录归属 sidB)时——A 声明 sidB 应答 → handler 守卫
// 按 wire sessionId 判 lease 拒绝(A 不持 sidB 的 lease,不触达 source);A 声明
// 自己控制的 sidA 应答 B 的审批 → handler 守卫放行但 source 按服务端记录的归属
// 拒绝(「服务端记录为准,不信客户端声明」,审批不被消费);A/B 各自应答本会话
// 的审批 → 成功且 source 收到的会话归属与 wire 声明一致;未知 requestID → 明确
// 错误。修复前 requireControlForApproval 的「持有任意会话 lease 即放行」允许 ①
// 直接应答他会话审批。
func TestXGWApprovalCrossSessionOwnership(t *testing.T) {
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

	// 服务端记录的审批归属(建模 SDK ApprovalManager.Wait 的落账):req-A 归属
	// 会话 A、req-B 归属会话 B。
	frc.mu.Lock()
	frc.approvalOwner["req-A"] = sidA
	frc.approvalOwner["req-B"] = sidB
	frc.mu.Unlock()

	// A 控制 sidA、B 控制 sidB(经 attach(control) 走生产获取路径)。
	if _, err := stackA.attachRaw(t, map[string]any{"sessionId": sidA, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach A control: %v", err)
	}
	if _, err := stackB.attachRaw(t, map[string]any{"sessionId": sidB, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach B control: %v", err)
	}

	respond := func(s *xgwStack, declaredSession, requestID string) error {
		t.Helper()
		_, err := s.callRaw(t, XMethodApprovalRespond, map[string]any{
			"sessionId": declaredSession, "requestId": requestID,
			"decision": map[string]any{"interrupt_id": requestID, "type": "approve"},
		})
		return err
	}

	// ① A 声明 sidB 应答 B 的审批:handler 守卫按会话判 lease → -32000
	// (control lease),不触达 source。
	if err := respond(stackA, sidB, "req-B"); err == nil {
		t.Fatal("respond declaring session B from A's connection must be rejected")
	} else if rpcErr := wantXGWError(t, "cross-session respond (handler guard)", err); !strings.Contains(rpcErr.Message, "control lease") {
		t.Fatalf("cross-session respond message = %q, want control lease mention", rpcErr.Message)
	}

	// ② A 声明自己控制的 sidA 应答 B 的审批:handler 守卫放行(真持 sidA 的
	// lease),source 归属校验拒绝(越权,非 lease 文案)。
	if err := respond(stackA, sidA, "req-B"); err == nil {
		t.Fatal("respond for session B's approval declaring session A must be rejected by the ownership check")
	} else if rpcErr := wantXGWError(t, "cross-session respond (ownership)", err); strings.Contains(rpcErr.Message, "control lease") || !strings.Contains(rpcErr.Message, "belongs to session") {
		t.Fatalf("ownership rejection message = %q, want belongs-to-session text (非 lease 文案)", rpcErr.Message)
	}

	// ③ 未知 requestID → source 明确错误透传 -32000。
	if err := respond(stackA, sidA, "req-missing"); err == nil {
		t.Fatal("respond for unknown requestID must fail")
	} else if rpcErr := wantXGWError(t, "unknown requestID", err); !strings.Contains(rpcErr.Message, "not found") {
		t.Fatalf("unknown requestID message = %q, want not-found text", rpcErr.Message)
	}

	// ④⑤ 各自应答本会话的审批 → 成功。
	raw, err := stackA.callRaw(t, XMethodApprovalRespond, map[string]any{
		"sessionId": sidA, "requestId": "req-A",
		"decision": map[string]any{"interrupt_id": "req-A", "type": "approve"},
	})
	if err != nil {
		t.Fatalf("respond for own session's approval: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil || fields["accepted"] != true {
		t.Fatalf("own-approval respond result = %s (%v), want {accepted:true}", raw, err)
	}
	if _, err := stackB.callRaw(t, XMethodApprovalRespond, map[string]any{
		"sessionId": sidB, "requestId": "req-B",
		"decision": map[string]any{"interrupt_id": "req-B", "type": "approve"},
	}); err != nil {
		t.Fatalf("respond from B for its own approval: %v", err)
	}

	// 转发保真:到达 source 的决策恰好两笔(④⑤),会话归属与 wire 声明一致;
	// ①②③ 的拒绝零副作用。
	frc.mu.Lock()
	approvals := append([]fakeApprovalCall(nil), frc.approvals...)
	frc.mu.Unlock()
	if len(approvals) != 2 {
		t.Fatalf("source approvals = %d, want 2 (越权/未知拒绝不得触达 source)", len(approvals))
	}
	if approvals[0].SessionID != sidA || approvals[0].RequestID != "req-A" {
		t.Fatalf("approval[0] = %+v, want {session:sidA, requestID:req-A}", approvals[0])
	}
	if approvals[1].SessionID != sidB || approvals[1].RequestID != "req-B" {
		t.Fatalf("approval[1] = %+v, want {session:sidB, requestID:req-B}", approvals[1])
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
	frc.AcquireControl(sid, handler.connectionID)
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

// errEventJournal 是注错 journal(I4 测试面):AppendEvent 一律失败(模拟磁盘
// 故障),读面恒空——SeqRange ok=false(空历史播种,不触发 Unknown)、
// ReadEvents 空(Subscribe/attach 可正常建立),专供 replayIntegrity 的失败
// 置位路径。编译期断言同 memEventJournal。
var _ runtimeagent.EventJournal = (*errEventJournal)(nil)

type errEventJournal struct{}

func (j *errEventJournal) AppendEvent(session.EventRecord) error { return errors.New("fsync boom") }

func (j *errEventJournal) ReadEvents(uint64) ([]session.EventRecord, error) { return nil, nil }

func (j *errEventJournal) SeqRange() (uint64, uint64, bool) { return 0, 0, false }

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
// {currentRun,latestSeq,replayIntegrity}(tookOver 省略,I4 起 eventsComplete
// 布尔改三态字符串)、事件线上形状 = SessionEvent 原样 JSON(蛇形键,总方案 §7
// 报文)。
func TestXGWSessionAttachReplayNoGap(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)

	// 先 StartRun 产事件:run/start 要求 control lease(测试面直接经 source 设置,
	// 与 P5-7 既有用例同款;attach 自身的 lease 获取路径由接管/observe 用例覆盖)。
	stack.runs.AcquireControl(sid, stack.handler.connectionID)
	if !stack.runs.HoldControl(sid, stack.handler.connectionID) {
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
	if len(fields) != 3 {
		t.Fatalf("attach result = %v, want exactly currentRun+latestSeq+replayIntegrity keys", fields)
	}
	if fields["replayIntegrity"] != runtimeagent.ReplayIntegrityComplete {
		t.Fatalf("attach replayIntegrity = %v, want %q(健康 hub 无失败证据、无历史播种)",
			fields["replayIntegrity"], runtimeagent.ReplayIntegrityComplete)
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

// TestXGWSessionAttachControlTakeover(P5-8 Observe/Control;I3 后同会话接管):
// attach(mode=control) 经 wire 获取该会话 lease(空闲获取 tookOver 省略);
// 第二连接对**同一会话** attach(mode=control) 显式接管(tookOver=true 回显);
// 接管后原 holder 的 approval/respond 被 requireControl(req.SessionID) 拒绝
// (其对该会话的 lease 已易主),新 holder 可用;lease 易主不断原 holder 的事
// 件流。跨会话互不影响由 TestXGWSessionAttachControlPerSession 单独覆盖(I3:
// 接管只发生在同一会话上)。
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
	sid := stackA.initAndNew(t)
	logA := &xgwEventLog{}
	stackA.client.HandleNotification(XMethodSessionEvents, logA.handle)

	// A:空闲获取,tookOver 省略(false),currentRun=null(无活动 run)。
	fieldsA, err := stackA.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"})
	if err != nil {
		t.Fatalf("attach A control: %v", err)
	}
	if _, present := fieldsA["tookOver"]; present {
		t.Fatalf("attach A result contains tookOver = %v, want omitted on fresh acquire", fieldsA["tookOver"])
	}
	if fieldsA["currentRun"] != nil {
		t.Fatalf("attach A currentRun = %v, want null (no active run)", fieldsA["currentRun"])
	}
	if !frc.HoldControl(sid, stackA.handler.connectionID) {
		t.Fatal("attach(mode=control) did not acquire the lease")
	}

	// B:对同一会话显式接管,tookOver=true 回显;A 失去该会话的 lease。
	fieldsB, err := stackB.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"})
	if err != nil {
		t.Fatalf("attach B control: %v", err)
	}
	if fieldsB["tookOver"] != true {
		t.Fatalf("attach B tookOver = %v, want true (explicit takeover)", fieldsB["tookOver"])
	}
	if frc.HoldControl(sid, stackA.handler.connectionID) {
		t.Fatal("original holder still holds the lease after takeover")
	}
	if !frc.HoldControl(sid, stackB.handler.connectionID) {
		t.Fatal("takeover connection does not hold the lease")
	}

	// 原 holder 的 approval/respond 被拒(-32000,control lease),不触达 source。
	_, err = stackA.callRaw(t, XMethodApprovalRespond, map[string]any{
		"sessionId": sid, "requestId": "req-1", "decision": map[string]any{"interrupt_id": "req-1", "type": "approve"},
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

	// 新 holder 的 respond 照常转发(服务端归属预置 req-1 → sid)。
	frc.mu.Lock()
	frc.approvalOwner["req-1"] = sid
	frc.mu.Unlock()
	raw, err := stackB.callRaw(t, XMethodApprovalRespond, map[string]any{
		"sessionId": sid, "requestId": "req-1", "decision": map[string]any{"interrupt_id": "req-1", "type": "approve"},
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

// TestXGWSessionAttachControlPerSession(I3 验收):两连接分别 attach(control)
// 各自的会话 → 各自持有、互不影响(I3 修复前 B 会顶掉 A 的全局 lease);被
// 操作会话授权(run/start 按 sessionId 守卫):A 持 sidA 的 lease 可 run/start
// sidA,对 sidB 的 run/start 被拒,反之亦然;各连接的观察流照常。
func TestXGWSessionAttachControlPerSession(t *testing.T) {
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
	logB := &xgwEventLog{}
	stackA.client.HandleNotification(XMethodSessionEvents, logA.handle)
	stackB.client.HandleNotification(XMethodSessionEvents, logB.handle)

	// 各自 attach(control) 各自的会话:均空闲获取(无 tookOver),互不顶掉。
	fieldsA, err := stackA.attachRaw(t, map[string]any{"sessionId": sidA, "afterSeq": 0, "mode": "control"})
	if err != nil {
		t.Fatalf("attach A control: %v", err)
	}
	if _, present := fieldsA["tookOver"]; present {
		t.Fatalf("attach A tookOver = %v, want omitted (per-session lease, B holds nothing on sidA)", fieldsA["tookOver"])
	}
	fieldsB, err := stackB.attachRaw(t, map[string]any{"sessionId": sidB, "afterSeq": 0, "mode": "control"})
	if err != nil {
		t.Fatalf("attach B control: %v", err)
	}
	if _, present := fieldsB["tookOver"]; present {
		t.Fatalf("attach B tookOver = %v, want omitted (per-session lease, A's lease is on sidA)", fieldsB["tookOver"])
	}
	if !frc.HoldControl(sidA, stackA.handler.connectionID) || !frc.HoldControl(sidB, stackB.handler.connectionID) {
		t.Fatal("each connection must hold its own session's lease")
	}
	if frc.HoldControl(sidA, stackB.handler.connectionID) || frc.HoldControl(sidB, stackA.handler.connectionID) {
		t.Fatal("per-session lease must not leak across sessions")
	}

	// 被操作会话授权:各连接对本会话 run/start 可达,对对方会话被拒。
	runStart := func(s *xgwStack, target string) error {
		t.Helper()
		_, err := s.callRaw(t, XMethodRunStart, map[string]any{
			"sessionId": target, "input": "x", "requestId": "per-session-" + target,
		})
		return err
	}
	if err := runStart(stackA, sidA); err != nil {
		t.Fatalf("run/start A on sidA: %v", err)
	}
	if err := runStart(stackB, sidB); err != nil {
		t.Fatalf("run/start B on sidB: %v", err)
	}
	if err := runStart(stackA, sidB); err == nil {
		t.Fatal("run/start A on sidB must be rejected (A holds no lease on sidB)")
	} else if rpcErr := wantXGWError(t, "cross-session start A→B", err); !strings.Contains(rpcErr.Message, "control lease") {
		t.Fatalf("cross-session start message = %q, want control lease mention", rpcErr.Message)
	}
	if err := runStart(stackB, sidA); err == nil {
		t.Fatal("run/start B on sidA must be rejected (B holds no lease on sidA)")
	}
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
	if frc := stack.runs; frc.HoldControl(sid, stack.handler.connectionID) {
		t.Fatal("observe attach must not touch the control lease")
	}

	_, err = stack.callRaw(t, XMethodApprovalRespond, map[string]any{
		"sessionId": sid, "requestId": "req-1", "decision": map[string]any{"interrupt_id": "req-1", "type": "approve"},
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
	if !stack.runs.HoldControl(sid, stack.handler.connectionID) {
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
	if stack.runs.HoldControl(sid, stack.handler.connectionID) {
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
	if !stack.runs.HoldControl(sid, stack.handler.connectionID) {
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
	if stack.runs.HoldControl(sid1, stack.handler.connectionID) {
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

// TestXGWSessionAttachFailureKeepsSubscriptionAndLease(I4 修复验收,准备/提交
// 两阶段的失败路径):A attach(control) 建立订阅并持有 lease 后,B 对同一会话
// 带超前游标 attach(control) → hub.Subscribe 的 ErrCursorExpired 原样上抛为
// -32000,且准备阶段的失败绝不触碰既有状态——旧订阅句柄原样(A 仍收实时
// 事件)、A 的 lease 未被夺走(B 的 would-be 接管回滚/未发生,B 无 lease 亦无
// 候选,B 的 approval/respond 同样被拒)。
func TestXGWSessionAttachFailureKeepsSubscriptionAndLease(t *testing.T) {
	frc := newFakeRunControl()
	hub := runtimeagent.NewEventHub("sess-store-1", &memEventJournal{})
	frc.mu.Lock()
	frc.attachHub = hub
	frc.mu.Unlock()
	stackA := newXGWStack(t, nil, frc)
	stackB := newXGWStack(t, nil, frc)
	t.Cleanup(stackA.handler.Close)
	t.Cleanup(stackB.handler.Close)
	sid := stackA.initAndNew(t)
	logA := &xgwEventLog{}
	stackA.client.HandleNotification(XMethodSessionEvents, logA.handle)

	// A:attach(control) → 持有 lease + 订阅建立。
	if _, err := stackA.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach A control: %v", err)
	}
	if !frc.HoldControl(sid, stackA.handler.connectionID) {
		t.Fatal("test setup: attach(control) did not acquire the lease")
	}
	stackA.handler.mu.Lock()
	subA := stackA.handler.subs[sid]
	stackA.handler.mu.Unlock()
	if subA == nil {
		t.Fatal("test setup: attach A left no subscription")
	}

	// B:同一会话、mode=control、超前游标 → Subscribe 失败(ErrCursorExpired)
	// → -32000;准备阶段失败零副作用。
	_, err := stackB.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 10, "mode": "control"})
	rpcErr := wantXGWError(t, "attach(control) with future cursor", err)
	if !strings.Contains(rpcErr.Message, "cursor expired") {
		t.Fatalf("future cursor message = %q, want ErrCursorExpired text", rpcErr.Message)
	}

	// 回滚断言(接管未发生):A 的 lease 原样,B 无 lease;A 的订阅句柄原样。
	if !frc.HoldControl(sid, stackA.handler.connectionID) {
		t.Fatal("failed attach must not evict the original holder's lease")
	}
	if frc.HoldControl(sid, stackB.handler.connectionID) {
		t.Fatal("failed attach must not grant the lease to the failed connection")
	}
	stackA.handler.mu.Lock()
	subAfter := stackA.handler.subs[sid]
	stackA.handler.mu.Unlock()
	if subAfter != subA {
		t.Fatal("failed attach must not replace the original subscription handle")
	}
	select {
	case <-subA.done:
		t.Fatal("failed attach must not stop the original pump")
	default:
	}
	if n := handlerSubCount(stackB.handler); n != 0 {
		t.Fatalf("failed connection's subscriptions = %d, want 0", n)
	}

	// B 的 approval/respond 被拒(无该会话 lease);A 的 approval/respond 照常
	// (lease 完好,服务端归属预置 req-1 → sid)。
	frc.mu.Lock()
	frc.approvalOwner["req-1"] = sid
	frc.mu.Unlock()
	respond := func(s *xgwStack) error {
		t.Helper()
		_, err := s.callRaw(t, XMethodApprovalRespond, map[string]any{
			"sessionId": sid, "requestId": "req-1", "decision": map[string]any{"interrupt_id": "req-1", "type": "approve"},
		})
		return err
	}
	if err := respond(stackB); err == nil {
		t.Fatal("approval/respond from the failed attach's connection must be rejected")
	}
	if err := respond(stackA); err != nil {
		t.Fatalf("approval/respond from the intact holder: %v", err)
	}

	// 旧订阅仍收事件:A 的实时流未受失败 attach 影响。
	publishHubEvent(t, hub, "run-1", session.EventToolCall)
	waitFor(t, "live event on the untouched subscription", func() bool { return logA.count() >= 1 })
}

// TestXGWSessionAttachConcurrentSinglePump(I4 修复验收,并发 attach):同连接
// 同会话并发 attach 的句柄替换在 h.mu 内原子换入——并发结束后表内恰一现任
// 句柄(done 未关),settle 后发布的实时事件恰好投递一次(无未登记的残留泵
// 在投递;换代 replay 的跨代重复是客户端显式要求 afterSeq=0 的预期行为,不在
// 本断言面)。并发的 lease 获取幂等(同连接同会话)。
func TestXGWSessionAttachConcurrentSinglePump(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)
	for _, typ := range []string{session.EventRunStarted, session.EventToolCall, session.EventRunFinished} {
		publishHubEvent(t, hub, "run-1", typ)
	}

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = stack.callRaw(t, XMethodSessionAttach, map[string]any{
				"sessionId": sid, "afterSeq": 0, "mode": "control",
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent attach #%d: %v", i, err)
		}
	}

	// 表内恰一现任句柄,且未被收泵。
	stack.handler.mu.Lock()
	subFinal, total := stack.handler.subs[sid], len(stack.handler.subs)
	stack.handler.mu.Unlock()
	if total != 1 {
		t.Fatalf("subscriptions after concurrent attach = %d, want exactly 1", total)
	}
	if subFinal == nil {
		t.Fatal("concurrent attach left no subscription")
	}
	select {
	case <-subFinal.done:
		t.Fatal("the surviving subscription must not be stopped")
	default:
	}

	// settle 后发布的实时事件恰好一次:证明没有未登记的残留泵仍在投递。
	// (等待以「probe 已到达」为锚:换代中被换下泵的在途 replay 会被丢弃,跨代
	// replay 总数不确定,不能作为计数锚点。)
	publishHubEvent(t, hub, "run-1", session.EventToolResult)
	waitFor(t, "live probe after concurrent attach", func() bool {
		events, _, _ := log.snapshot()
		for _, ev := range events {
			if ev.Seq == 4 {
				return true
			}
		}
		return false
	})
	events, _, bad := log.snapshot()
	if len(bad) > 0 {
		t.Fatalf("notification decode failures: %v", bad)
	}
	probes := 0
	for _, ev := range events {
		if ev.Seq == 4 {
			probes++
		}
	}
	if probes != 1 {
		t.Fatalf("probe event (seq 4) delivered %d times, want exactly 1 (no zombie pump)", probes)
	}
}

// TestXGWSessionAttachCloseCommitBarrier(I2 修复验收,commitHook 确定性交错,
// 不用 time.Sleep):attach 的提交段内(h.mu 已释放的插桩窗口)同步跑生产
// Close → 提交段二次确认 closed → attach 被拒。复现评审 I2 的 Close/Attach
// 竞态:修复前该窗口内 attach 会照常换入订阅、启动泵并 AcquireControl——连接
// 关闭后复活订阅与 lease。断言四件事:attach 报错;订阅未复活(表空、泵未
// 启动,负向无投递是结构事实,不做宽限睡眠断言);lease 未获取(HoldControl
// 恒 false);closed 置位后后续 attach 一律拒绝。
func TestXGWSessionAttachCloseCommitBarrier(t *testing.T) {
	stack, _ := newAttachStack(t)
	sid := stack.initAndNew(t)

	stack.handler.mu.Lock()
	stack.handler.commitHook = func() { stack.handler.Close() }
	stack.handler.mu.Unlock()

	_, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"})
	rpcErr := wantXGWError(t, "attach racing Close inside its commit", err)
	if !strings.Contains(rpcErr.Message, "closed") {
		t.Fatalf("attach-after-close message = %q, want closed mention", rpcErr.Message)
	}

	// 订阅未复活:表空(泵未启动,Close 的清表已先行摘除)。
	if n := handlerSubCount(stack.handler); n != 0 {
		t.Fatalf("subscriptions after attach lost to Close = %d, want 0", n)
	}
	// lease 未获取:提交段在二次确认处让位,AcquireControl 未达;Close 的
	// ReleaseConnection 也不会被后续路径复活。
	if stack.runs.HoldControl(sid, stack.handler.connectionID) {
		t.Fatal("attach must not acquire the lease once Close won the race")
	}

	// closed 置位后 attach 一律拒绝(不复活订阅与 lease)。
	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"}); err == nil {
		t.Fatal("attach after Close must be rejected unconditionally")
	}
	if n := handlerSubCount(stack.handler); n != 0 {
		t.Fatalf("subscriptions after post-close attach = %d, want 0", n)
	}
	if stack.runs.HoldControl(sid, stack.handler.connectionID) {
		t.Fatal("post-close attach must not acquire the lease either")
	}
}

// TestXGWSessionAttachDetachConcurrentLifecycle(I2 修复验收):attach 与
// detach 并发时,终态订阅/lease 状态符合 h.mu 内操作序——attach 提交段(换表
// +取 lease)与 detach(摘表+还 lease)各自单事务,以 h.mu 全序,后者完整胜出:
// 要么订阅存活且 lease 在手(attach 段在后),要么表空且 lease 空闲(detach 段
// 在后);被换下/被摘除的订阅句柄一律已 stop(无未登记泵)。第一段用 commitHook
// 把生产 detach 确定性插进 attach 提交段中间(detach 的 mu 操作序严格在后 →
// attach 让位);第二段并发轰 attach(control)+detach(control) 断言不变式。
func TestXGWSessionAttachDetachConcurrentLifecycle(t *testing.T) {
	stack, hub := newAttachStack(t)
	sid := stack.initAndNew(t)
	log := &xgwEventLog{}
	stack.client.HandleNotification(XMethodSessionEvents, log.handle)

	// attach#1 建立订阅与 lease,供确定性段验证「被换下者随让位一并收泵」。
	if _, err := stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach #1: %v", err)
	}
	stack.handler.mu.Lock()
	sub1 := stack.handler.subs[sid]
	stack.handler.mu.Unlock()
	if sub1 == nil {
		t.Fatal("attach #1 left no subscription")
	}

	// —— 确定性段:commitHook 内跑生产 detach(control),其 h.mu 段严格晚于
	// attach#2 的换表 → detach 是全序最后一方 → attach#2 被顶替拒绝,终态为空。
	detachParams, err := json.Marshal(XSessionDetachParams{SessionID: sid, Mode: XAttachModeControl})
	if err != nil {
		t.Fatalf("marshal detach params: %v", err)
	}
	stack.handler.mu.Lock()
	stack.handler.commitHook = func() {
		if _, err := stack.handler.xgwSessionDetach(context.Background(), detachParams); err != nil {
			t.Errorf("detach inside commit hook: %v", err)
		}
	}
	stack.handler.mu.Unlock()
	_, err = stack.attachRaw(t, map[string]any{"sessionId": sid, "afterSeq": 0, "mode": "control"})
	if err == nil {
		t.Fatal("attach overtaken by a detach inside its commit must be rejected")
	}
	if rpcErr := wantXGWError(t, "attach overtaken by detach", err); !strings.Contains(rpcErr.Message, "superseded") {
		t.Fatalf("overtaken attach message = %q, want superseded mention", rpcErr.Message)
	}
	stack.handler.mu.Lock()
	stack.handler.commitHook = nil
	rest := len(stack.handler.subs)
	stack.handler.mu.Unlock()
	if rest != 0 {
		t.Fatalf("subscriptions after detach-wins interleave = %d, want 0", rest)
	}
	if stack.runs.HoldControl(sid, stack.handler.connectionID) {
		t.Fatal("detach inside the commit window must leave the lease free")
	}
	select {
	case <-sub1.done:
	default:
		t.Fatal("the displaced subscription must be stopped when the attach yields")
	}

	// —— 并发段:attach(control) 与 detach(control) 同会话并发,断言终态
	// 不变式:订阅存活 ⇔ lease 在手;存活句柄未 stop;被换下句柄必已 stop。
	var prev *xgwSubscription
	for i := 0; i < 30; i++ {
		var attErr, detErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, attErr = stack.callRaw(t, XMethodSessionAttach, map[string]any{
				"sessionId": sid, "afterSeq": 0, "mode": "control",
			})
		}()
		go func() {
			defer wg.Done()
			_, detErr = stack.callRaw(t, XMethodSessionDetach, map[string]any{
				"sessionId": sid, "mode": "control",
			})
		}()
		wg.Wait()
		if attErr != nil || detErr != nil {
			t.Fatalf("iteration %d: attach=%v detach=%v", i, attErr, detErr)
		}
		stack.handler.mu.Lock()
		sub := stack.handler.subs[sid]
		stack.handler.mu.Unlock()
		held := stack.runs.HoldControl(sid, stack.handler.connectionID)
		if (sub != nil) != held {
			t.Fatalf("iteration %d: subscription present=%v but lease held=%v (订阅与 lease 必须同生同死)", i, sub != nil, held)
		}
		if sub != nil {
			select {
			case <-sub.done:
				t.Fatalf("iteration %d: surviving subscription must not be stopped", i)
			default:
			}
		}
		if prev != nil && prev != sub {
			select {
			case <-prev.done:
			default:
				t.Fatalf("iteration %d: displaced subscription not stopped", i)
			}
		}
		prev = sub
	}

	// 终局僵尸泵检查:并发结束后若仍留存活订阅,发布探测事件必须恰好投递一次
	// (无未登记泵在投递);表空则无泵存活,结构性无投递,不发布。
	stack.handler.mu.Lock()
	final := stack.handler.subs[sid]
	stack.handler.mu.Unlock()
	if final != nil {
		probeSeq := hub.LatestSeq() + 1
		publishHubEvent(t, hub, "run-1", session.EventToolResult)
		waitFor(t, "final probe delivery", func() bool {
			events, _, _ := log.snapshot()
			for _, ev := range events {
				if ev.Seq == probeSeq {
					return true
				}
			}
			return false
		})
		events, _, bad := log.snapshot()
		if len(bad) > 0 {
			t.Fatalf("notification decode failures: %v", bad)
		}
		probes := 0
		for _, ev := range events {
			if ev.Seq == probeSeq {
				probes++
			}
		}
		if probes != 1 {
			t.Fatalf("final probe (seq %d) delivered %d times, want exactly 1 (no zombie pump)", probeSeq, probes)
		}
	}
}

// TestXGWSessionAttachCrossConnectionCurrentRun(I1 修复验收,wire 面):会话 A
// 有在途 run 时,另一连接(其当前会话是 B)对 A 的 attach 响应 currentRun 非
// null 且为 A 的在途快照——评审 I1 的场景「Session A: Run 在后台执行;Client
// attach(A) → 响应不得 currentRun=null」。协议面忠实转发 source 的 ActiveRun
// (按会话路由),source 侧 current-only 回归在 runcontrol_test.go 的
// TestActiveRunNonCurrentSession;对照:B 对无在途 run 的 sidB attach →
// currentRun=null。
func TestXGWSessionAttachCrossConnectionCurrentRun(t *testing.T) {
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

	// A:attach(control) 持照 → run/start 产事件,并把 A 的活动 run 快照置为
	// (run-1, running)。
	if _, err := stackA.attachRaw(t, map[string]any{"sessionId": sidA, "afterSeq": 0, "mode": "control"}); err != nil {
		t.Fatalf("attach A control: %v", err)
	}
	if _, err := stackA.callRaw(t, XMethodRunStart, map[string]any{
		"sessionId": sidA, "input": "x", "requestId": "r1",
	}); err != nil {
		t.Fatalf("run/start on A: %v", err)
	}

	// B attach sidA(B 的当前会话是 sidB,sidA 是它的「非当前会话」):
	// currentRun 非 null 且为 A 的在途快照。
	fields, err := stackB.attachRaw(t, map[string]any{"sessionId": sidA, "afterSeq": 0, "mode": "observe"})
	if err != nil {
		t.Fatalf("attach B on session A: %v", err)
	}
	cur, ok := fields["currentRun"].(map[string]any)
	if !ok || cur["runId"] != "run-1" || cur["state"] != "running" {
		t.Fatalf("attach B currentRun = %v, want {runId:run-1,state:running}(非当前会话的在途 run 必须如实上报)", fields["currentRun"])
	}

	// 对照:B attach 自己的 sidB(无在途 run)→ currentRun=null。
	fields, err = stackB.attachRaw(t, map[string]any{"sessionId": sidB, "afterSeq": 0, "mode": "observe"})
	if err != nil {
		t.Fatalf("attach B on own session: %v", err)
	}
	if fields["currentRun"] != nil {
		t.Fatalf("attach own-session currentRun = %v, want null (no active run)", fields["currentRun"])
	}
}
