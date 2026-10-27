package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
)

// 本文件是 P5-9 设备工具切 CapabilityRegistry + proxy 的 daemon 集成测试(总方案
// §8 的 daemon 接线,P5 显式可见变化 #1):经插件的真实 binder
// (AgentPlugin.ClientToolSource)+ 真实 manager.Capabilities() 注册表 + 真实
// deviceToolExecutor(x-device/call 适配)编排「绑定 → 断连(Unbind + dead
// executor)→ 等待重绑 → 完成」与「超时 → 明确失败文本」。全部模拟 Provider
// 与 fake caller,零真实 LLM;编排形态对齐 SDK 侧 TestToolBindingDisconnect
// (ai-runtime/agent/capability_test.go:443)。
//
// HITL 注意:fake caller 声明 canonical "benign"(与 handler_test 的 capsReporter
// payload 同约定)——归一后 metadata risk_level=benign + requires_permission=true,
// normal 模式下 checkPolicyTool 直接放行(benign ≤ low,无 ForceHITL);声明 legacy
// "never" 会归一成 "low" 落 checkSys 的 hitl 分支,把 run 卡进 waiting_input。

// deviceToolName 是 fake caller 声明并归一后的注册工具名(与原始中继同名:
// sys_ 前缀 + 客户端原名)。
const deviceToolName = "sys_vibrate"

// deviceToolsPayload 是 fake caller 对 x-device/tools 的应答:单个 benign 设备工具。
const deviceToolsPayload = `{"tools":[{"name":"vibrate","description":"buzz the device","schema":"{\"type\":\"object\"}","risk_level":"benign"}]}`

// scriptedDeviceCaller 是 x-device 端点的测试桩:x-device/tools 回放预置清单
// (toolsErr 注入探测失败),x-device/call 回放预置结果或断连错误。callCalls
// 只数工具执行转发、不计探测——「dead executor Invoke 计数==0」断言用它钉死
// (绝不静默路由到已断开连接,总方案 §8 :650 的 daemon 对偶)。
type scriptedDeviceCaller struct {
	mu        sync.Mutex
	toolsJSON string
	toolsErr  error
	callRes   string
	callErr   error
	callCalls int
}

func (f *scriptedDeviceCaller) Call(_ context.Context, method string, _, result any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch method {
	case acpworker.MethodDeviceTools:
		if f.toolsErr != nil {
			return f.toolsErr
		}
		return json.Unmarshal([]byte(f.toolsJSON), result)
	case acpworker.MethodDeviceCall:
		f.callCalls++
		if f.callErr != nil {
			return f.callErr
		}
		return json.Unmarshal([]byte(f.callRes), result)
	default:
		return fmt.Errorf("unexpected client-ward method %q", method)
	}
}

func (f *scriptedDeviceCaller) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCalls
}

func (f *scriptedDeviceCaller) failCalls(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callErr = err
}

// scriptedToolProvider 按调用序回放预置 assistant 消息(第 i 轮 = msgs[i],越界
// 回落最终文本)。ChatStream 恒报错 → Session 回退非流式,tool call 经真实
// kernel 循环 + 沙箱/HITL 中间件链执行(形态对齐 SDK 侧 scriptedChatProvider,
// approval_test.go:233)。
type scriptedToolProvider struct {
	mu    sync.Mutex
	calls int
	msgs  []core.Message
}

func (p *scriptedToolProvider) Name() string  { return "scripted-tool" }
func (p *scriptedToolProvider) Model() string { return "m" }

func (p *scriptedToolProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	p.mu.Lock()
	i := p.calls
	p.calls++
	p.mu.Unlock()
	msg := core.Message{Role: "assistant", Content: "done"}
	if i < len(p.msgs) {
		msg = p.msgs[i]
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: msg}}}, nil
}

func (p *scriptedToolProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// deviceToolCallRound 构造一轮对设备工具的 tool call 消息。
func deviceToolCallRound(id string) core.Message {
	return core.Message{Role: "assistant", ToolCalls: []core.ToolCall{{
		ID:       id,
		Type:     "function",
		Function: core.ToolCallFunction{Name: deviceToolName, Arguments: `{"duration":1}`},
	}}}
}

// newDeviceToolPlugin 构造本文件共用的插件编排:纯内存会话(Session.Enabled=false,
// 与 newAgentPluginP 的既有用法一致)+ scripted provider(fake caller 经
// ClientToolSource 注入工具,CollectTools 恒 nil)。等待预算由调用方经
// cfg.Session.CapabilityWaitTimeout 设定。
func newDeviceToolPlugin(t *testing.T, waitTimeoutSecs int, pv core.Provider) *AgentPlugin {
	t.Helper()
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	cfg.Session.CapabilityWaitTimeout = waitTimeoutSecs
	hub, _ := testHub(cfg)
	return newAgentPluginP(hub, pv)
}

// waitRunState 轮询句柄直至到达 want 状态(onWait(true) 的迁移在执行 goroutine
// 上,观察点异步可达,有界等待防挂死;形态对齐 SDK 侧 waitRunState)。
func waitRunState(t *testing.T, h runtimeagent.RunHandle, want runtimeagent.RunState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for h.State() != want {
		if time.Now().After(deadline) {
			t.Fatalf("run state never reached %q (now %q)", want, h.State())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitToolResultCollect 返回 EmitToken 回调与断连通知通道:首个本工具 tool_call
// token 到达时执行 disconnect 并关闭 done;tool_result 文本收集进 toolResults。
// 形态对齐 SDK 侧 waitToolCallUnbound。时序警示:ai-core 的 tool_call token 发送
// 是执行 goroutine 到 streamTokens 消费 goroutine 的通道交接,交接完成即返回——
// disconnect 的实际执行时刻相对工具 Execute 无保证(满载下消费面可晚于 Invoke
// 被调度)。因此**需要严格先于 Execute 的编排(如断连)不得放进本钩子**,先验
// 完成;钩子只承载无时序要求的通知/收集面(见
// TestDeviceToolBindingRebindAfterDisconnect 的竞态修正注)。
func waitToolResultCollect(disconnect func(), toolResults *[]string) (cb runtimeagent.RunCallbacks, done chan struct{}) {
	done = make(chan struct{})
	fired := false // 仅执行 goroutine 读写(EmitToken 单调用面)
	cb = runtimeagent.RunCallbacks{
		EmitToken: func(tok core.Token) {
			if tok.Type == core.TokenTypeToolCall && tok.ToolCall.Function.Name == deviceToolName && !fired {
				fired = true
				disconnect()
				close(done)
			}
			if tok.Type == core.TokenTypeToolResult {
				*toolResults = append(*toolResults, tok.Content)
			}
		},
	}
	return cb, done
}

// TestDeviceToolSourceBindsProxyAndExecutes(P5-9 构造面):binder 闭包经
// SpecDeviceTools 探测 → Bind(rt.ID, connID, 注册名, ttl=2*wait, x-device/call
// 执行通道) → NewClientToolProxy;注册名与门控元数据与原始中继同面(sys_ 前缀
// + benign 透传),另带 goworker.capability_wait 内部标记;直通执行(绑定在)经
// x-device/call 返回客户端结果。
func TestDeviceToolSourceBindsProxyAndExecutes(t *testing.T) {
	p := newDeviceToolPlugin(t, 0, fixedProvider{content: "unused"})
	sid := p.currentSession().ID // 纯内存形态 = "current"(Bind/Lookup 同键)

	caller := &scriptedDeviceCaller{toolsJSON: deviceToolsPayload, callRes: `{"result":"buzzed"}`}
	src := p.ClientToolSource("conn-A", caller)
	if src == nil {
		t.Fatal("binder must return the lazy tool source closure")
	}
	tools := src(context.Background())
	if len(tools) != 1 {
		t.Fatalf("tool source returned %d tools, want 1", len(tools))
	}
	tool := tools[0]
	if tool.Name != deviceToolName {
		t.Fatalf("tool name = %q, want %q (注册名与原始中继同名)", tool.Name, deviceToolName)
	}
	if tool.Metadata["risk_level"] != "benign" || tool.Metadata["requires_permission"] != "true" || tool.Metadata["allow_always_key"] != deviceToolName {
		t.Fatalf("metadata = %#v, want benign + requires_permission + allow_always_key(门控面透传)", tool.Metadata)
	}
	if tool.Metadata["goworker.capability_wait"] != "true" {
		t.Fatalf("metadata missing goworker.capability_wait marker: %#v", tool.Metadata)
	}

	// 绑定登记面:注册表按 (rt.ID, 注册名) 可见,执行通道即 x-device/call 适配。
	if _, ok := p.manager.Capabilities().Lookup(sid, deviceToolName); !ok {
		t.Fatalf("binding for (%q, %q) must be registered after a successful probe", sid, deviceToolName)
	}

	out, err := tool.Execute(context.Background(), map[string]any{"duration": 1.0})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != "buzzed" {
		t.Fatalf("Execute = %q, want %q (结果来自 x-device/call)", out, "buzzed")
	}
	if got := caller.callCount(); got != 1 {
		t.Fatalf("x-device/call forwarded %d time(s), want 1", got)
	}
}

// TestDeviceToolSourceProbeFailureCollapses:探测失败(method-not-found/断连/
// 超时)→ 现状收缩语义「无能力即无工具」:工具源闭包返回 nil,不登记任何绑定。
func TestDeviceToolSourceProbeFailureCollapses(t *testing.T) {
	p := newDeviceToolPlugin(t, 0, fixedProvider{content: "unused"})
	sid := p.currentSession().ID

	caller := &scriptedDeviceCaller{toolsErr: errors.New("method not found")}
	src := p.ClientToolSource("conn-A", caller)
	if src == nil {
		t.Fatal("binder must return the lazy closure even when the probe will fail")
	}
	if got := src(context.Background()); got != nil {
		t.Fatalf("probe failure must collapse to a nil tool source, got %d tools", len(got))
	}
	if _, ok := p.manager.Capabilities().Lookup(sid, deviceToolName); ok {
		t.Fatal("collapsed probe must not register any binding")
	}
}

// TestDeviceToolBindingRebindAfterDisconnect(P5 显式可见变化 #1 的 daemon 集成
// 证明):绑定 conn-A → 断连(UnbindCapabilities + dead executor 陷阱,先于
// StartRun 的确定性编排)→ run 执行设备工具时 lookup 必然 miss → run 迁移
// RunWaitingCapability → 新连接 conn-B 显式重绑(经 binder 的 ClientToolSource
// 探测+Bind)→ 工具在 conn-B 执行完成 → RunCompleted。dead executor 的
// x-device/call 计数==0:等待重绑绝不把请求静默路由到已断开(或任何未显式
// 重绑)的连接。
//
// 为什么断连先于 StartRun(而非 tool_call token 消费钩子):token 发送是 ai-core
// 执行 goroutine 到 streamTokens 消费 goroutine 的通道交接(agent.go emitToken),
// 交接完成执行面即继续跑中间件与 Execute,消费面何时被调度无保证——满载
// -race 全量组合下 Invoke 可先命中绑定成功返回,run 直接 completed,
// callCount==0 断言闪失败(goworker_no_agent 全量组合实测复现)。「在途 Invoke
// 失败→即时 Invalidate→等待」路径由 SDK 侧单测钉死
// (TestCapabilityInvokeFailureClassification「plain error invalidates and
// waits」),本测试聚焦断连→等待→重绑→新连接执行的 daemon 集成面。
func TestDeviceToolBindingRebindAfterDisconnect(t *testing.T) {
	p := newDeviceToolPlugin(t, 0, &scriptedToolProvider{msgs: []core.Message{
		deviceToolCallRound("call-dev-1"),
		{Role: "assistant", Content: "buzzed the device"},
	}})
	sid := p.currentSession().ID

	callerA := &scriptedDeviceCaller{toolsJSON: deviceToolsPayload, callRes: `{"result":"from-A"}`}
	srcA := p.ClientToolSource("conn-A", callerA)
	tools := srcA(context.Background()) // 绑定 conn-A

	// 断连编排:死执行器陷阱(若被静默路由,Invoke 必失败留下计数证据)+
	// Unbind——handler.Close 的断连收口等价物,先于 StartRun(见上注)。
	callerA.failCalls(errors.New("connection dead"))
	p.UnbindCapabilities("conn-A")

	var toolResults []string
	cb, _ := waitToolResultCollect(func() {}, &toolResults) // 只收集 tool_result

	h, err := p.manager.StartRun(sid, runtimeagent.RunRequest{
		Input:      "vibrate the device",
		ToolSource: func(context.Context) []core.Tool { return tools },
	}, cb)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	waitRunState(t, h, runtimeagent.RunWaitingCapability) // 断连 → waiting_capability

	// 重绑:新连接 conn-B 声明同一工具(binder 探测 + Bind,就绪通知唤醒等待者)。
	callerB := &scriptedDeviceCaller{toolsJSON: deviceToolsPayload, callRes: `{"result":"from-B"}`}
	srcB := p.ClientToolSource("conn-B", callerB)
	if got := srcB(context.Background()); len(got) != 1 {
		t.Fatalf("rebind probe returned %d tools, want 1", len(got))
	}

	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := h.Wait(wctx) // 工具在 conn-B 执行完成 → RunCompleted
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if got := callerA.callCount(); got != 0 {
		t.Fatalf("dead executor forwarded %d x-device/call(s), want 0(绝不静默路由到已断开连接)", got)
	}
	if got := callerB.callCount(); got != 1 {
		t.Fatalf("conn-B forwarded %d x-device/call(s), want 1(重绑后恰执行一次)", got)
	}
	if len(toolResults) != 1 || toolResults[0] != "from-B" {
		t.Fatalf("tool results = %v, want [from-B](结果来自 conn-B)", toolResults)
	}
	if res.State != runtimeagent.RunCompleted {
		t.Fatalf("state = %q, want RunCompleted", res.State)
	}
}

// TestDeviceToolBindingTimeoutFails:绑定 → 断连(Unbind,无重绑)→ 短窗等待
// (capability_wait_timeout=1s)→ 超时返回明确失败文本交 Agent → run 正常
// RunCompleted(失败是工具结果不是执行异常;有界,不挂死)。
func TestDeviceToolBindingTimeoutFails(t *testing.T) {
	p := newDeviceToolPlugin(t, 1, &scriptedToolProvider{msgs: []core.Message{
		deviceToolCallRound("call-dev-t"),
		{Role: "assistant", Content: "handled unavailable tool"},
	}})
	sid := p.currentSession().ID

	caller := &scriptedDeviceCaller{toolsJSON: deviceToolsPayload, callRes: `{"result":"from-A"}`}
	src := p.ClientToolSource("conn-A", caller)
	tools := src(context.Background())

	p.UnbindCapabilities("conn-A") // 断连:等待预算耗尽是唯一出口(无重绑)

	var toolResults []string
	cb, _ := waitToolResultCollect(func() {}, &toolResults) // 只收集 tool_result

	h, err := p.manager.StartRun(sid, runtimeagent.RunRequest{
		Input:      "vibrate the device",
		ToolSource: func(context.Context) []core.Tool { return tools },
	}, cb)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := h.Wait(wctx)
	if err != nil {
		t.Fatalf("Wait: %v(超时必须收敛,不挂死)", err)
	}

	if len(toolResults) != 1 {
		t.Fatalf("tool results = %v, want exactly 1", toolResults)
	}
	if !strings.Contains(toolResults[0], "device tool "+deviceToolName+" unavailable: connection lost") ||
		!strings.Contains(toolResults[0], "waited") {
		t.Fatalf("tool result %q missing explicit failure text", toolResults[0])
	}
	if got := caller.callCount(); got != 0 {
		t.Fatalf("unbound executor forwarded %d x-device/call(s), want 0", got)
	}
	if res.State != runtimeagent.RunCompleted {
		t.Fatalf("state = %q, want RunCompleted(失败结果交给 Agent,run 不失败)", res.State)
	}
}
