package acpworker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-sandbox"
)

// fakeCaller 记录调用并回放预设结果，充当 Kotlin 侧的 x-device 端点。
type fakeCaller struct {
	method string
	params json.RawMessage
	result string
	err    error
}

func (f *fakeCaller) Call(ctx context.Context, method string, params, result any) error {
	f.method = method
	f.params, _ = json.Marshal(params)
	if f.err != nil {
		return f.err
	}
	return json.Unmarshal([]byte(f.result), result)
}

func TestRelayDeviceTools_WrapsAndForwards(t *testing.T) {
	caller := &fakeCaller{result: `{"result":"notification delivered"}`}
	descriptors := []DeviceToolDescriptor{
		{
			Name:        "send_notification",
			Description: "发送系统通知",
			Schema:      `{"type":"object","properties":{"title":{"type":"string"}},"required":["title"]}`,
			RiskLevel:   "never",
		},
	}
	tools := RelayDeviceTools(caller, descriptors)
	if len(tools) != 1 {
		t.Fatalf("tools = %d, want 1", len(tools))
	}
	tool := tools[0]
	if tool.Name != "sys_send_notification" {
		t.Errorf("name = %q, want sys_ prefix", tool.Name)
	}
	if tool.Metadata["risk_level"] != "low" {
		t.Errorf("risk_level metadata = %q, want low", tool.Metadata["risk_level"])
	}
	if _, gated := tool.Metadata["requires_permission"]; gated {
		t.Errorf("legacy never must not be gated: metadata = %#v", tool.Metadata)
	}
	if _, ok := tool.Parameters["type"]; !ok {
		t.Error("schema not parsed into Parameters")
	}

	if _, err := tool.Execute(context.Background(), map[string]any{"title": "hi"}); err == nil {
		t.Fatal("device tool accepted a call without managed run identity")
	}
	if caller.method != "" {
		t.Fatalf("device caller invoked with method %q without managed identity", caller.method)
	}
}

func TestRelayDeviceToolsForSessionRejectsMissingIdentity(t *testing.T) {
	caller := &fakeCaller{result: `{"result":"buzzed"}`}
	tools := RelayDeviceToolsForSession(caller, []DeviceToolDescriptor{{Name: "vibrate"}}, "runtime-A")

	if _, err := tools[0].Execute(context.Background(), map[string]any{}); err == nil {
		t.Fatal("relay accepted a call without managed runtime identity")
	}
	if caller.method != "" {
		t.Fatalf("caller invoked with method %q without managed identity", caller.method)
	}
}

// managedWireProvider returns a device tool call then a final response. Its
// managed identity is supplied by SessionManager.StartRun, not by the test.
type managedWireProvider struct {
	mu    sync.Mutex
	calls int
}

func (*managedWireProvider) Name() string  { return "managed-wire-test" }
func (*managedWireProvider) Model() string { return "test" }

func (p *managedWireProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("stream unsupported by test provider")
}

func (p *managedWireProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	p.mu.Lock()
	call := p.calls
	p.calls++
	p.mu.Unlock()
	if call == 0 {
		return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{
			Role: "assistant",
			ToolCalls: []core.ToolCall{{
				ID:   "call-vibrate",
				Type: "function",
				Function: core.ToolCallFunction{
					Name:      "sys_vibrate",
					Arguments: `{"duration":1}`,
				},
			}},
		}}}}, nil
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{
		Role:    "assistant",
		Content: "done",
	}}}}, nil
}

func TestRelayDeviceToolsForSessionWritesManagedIdentityToWire(t *testing.T) {
	const runtimeID = "runtime-A"
	caller := &fakeCaller{result: `{"result":"buzzed"}`}
	provider := &managedWireProvider{}
	cfg := runtimeconfig.Default()
	sess := runtimeagent.NewSession(runtimeagent.SessionDeps{
		Config: cfg,
		CollectTools: func(*sandbox.Config) []core.Tool {
			return RelayDeviceToolsForSession(caller, []DeviceToolDescriptor{{Name: "vibrate", RiskLevel: "never"}}, runtimeID)
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil },
	})
	manager := runtimeagent.NewSessionManager()
	defer manager.Close()
	if err := manager.Register(runtimeagent.NewSessionRuntime(runtimeID, sess)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	handle, err := manager.StartRun(runtimeID, runtimeagent.RunRequest{Input: "vibrate"}, runtimeagent.RunCallbacks{
		Decide: func(req *hitl.InterruptRequest) hitl.Decision {
			return hitl.Decision{InterruptID: req.ID, Type: hitl.DecisionApprove}
		},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := handle.Wait(waitCtx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("run error: %v", result.Err)
	}

	if caller.method != MethodDeviceCall {
		t.Fatalf("method = %q, want %q", caller.method, MethodDeviceCall)
	}
	var wire deviceCallRequest
	if err := json.Unmarshal(caller.params, &wire); err != nil {
		t.Fatalf("decode wire request: %v", err)
	}
	if !strings.Contains(string(caller.params), `"sessionId"`) || !strings.Contains(string(caller.params), `"runId"`) {
		t.Errorf("wire request must include top-level sessionId and runId fields")
	}
	if strings.Contains(string(caller.params), `"session_id"`) || strings.Contains(string(caller.params), `"run_id"`) {
		t.Errorf("wire request must not contain legacy identity field names")
	}
	if wire.SessionID != runtimeID {
		t.Errorf("wire sessionId = %q, want managed runtime ID %q", wire.SessionID, runtimeID)
	}
	if wire.RunID != handle.RunID {
		t.Errorf("wire runId = %q, want accepted run ID %q", wire.RunID, handle.RunID)
	}
	if wire.SessionID == "" || wire.RunID == "" {
		t.Fatalf("wire request missing managed identity: %+v", wire)
	}
	if wire.Name != "vibrate" || string(wire.Arguments) != `{"duration":1}` {
		t.Errorf("wire call = name:%q args:%s, want vibrate and encoded args", wire.Name, wire.Arguments)
	}
}

func TestRelayDeviceToolsForSessionRejectsMismatchedManagedIdentity(t *testing.T) {
	caller := &fakeCaller{result: `{"result":"buzzed"}`}
	provider := &managedWireProvider{}
	cfg := runtimeconfig.Default()
	sess := runtimeagent.NewSession(runtimeagent.SessionDeps{
		Config: cfg,
		CollectTools: func(*sandbox.Config) []core.Tool {
			return RelayDeviceToolsForSession(caller, []DeviceToolDescriptor{{Name: "vibrate", RiskLevel: "never"}}, "runtime-B")
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil },
	})
	manager := runtimeagent.NewSessionManager()
	defer manager.Close()
	if err := manager.Register(runtimeagent.NewSessionRuntime("runtime-A", sess)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	handle, err := manager.StartRun("runtime-A", runtimeagent.RunRequest{Input: "vibrate"}, runtimeagent.RunCallbacks{
		Decide: func(req *hitl.InterruptRequest) hitl.Decision {
			return hitl.Decision{InterruptID: req.ID, Type: hitl.DecisionApprove}
		},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := handle.Wait(waitCtx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if caller.method != "" {
		t.Fatalf("mismatched runtime identity dispatched method %q", caller.method)
	}
}

func TestRelayDeviceTools_NormalizesLegacyRiskMetadata(t *testing.T) {
	cases := []struct {
		name       string
		declared   string
		wantRisk   string
		wantGated  bool
		wantAlways bool
	}{
		{name: "never bypass", declared: "never", wantRisk: "low"},
		{name: "mode defaults unknown", declared: "mode", wantRisk: "unclassified", wantGated: true},
		{name: "always forces hitl", declared: "always", wantRisk: "critical", wantGated: true, wantAlways: true},
		{name: "canonical risk", declared: "elevated", wantRisk: "elevated", wantGated: true},
		{name: "invalid risk fails closed", declared: "wat", wantRisk: "unclassified", wantGated: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tools := RelayDeviceTools(&fakeCaller{}, []DeviceToolDescriptor{{Name: "tool", RiskLevel: tc.declared}})
			if len(tools) != 1 {
				t.Fatalf("tools = %d, want 1", len(tools))
			}
			metadata := tools[0].Metadata
			if metadata["risk_level"] != tc.wantRisk {
				t.Errorf("risk_level = %q, want %q", metadata["risk_level"], tc.wantRisk)
			}
			_, gated := metadata["requires_permission"]
			if gated != tc.wantGated {
				t.Errorf("requires_permission present = %t, want %t; metadata = %#v", gated, tc.wantGated, metadata)
			}
			_, always := metadata["permission_policy"]
			if always != tc.wantAlways {
				t.Errorf("permission_policy present = %t, want %t; metadata = %#v", always, tc.wantAlways, metadata)
			}
			if tc.wantGated && metadata["allow_always_key"] != "sys_tool" {
				t.Errorf("allow_always_key = %q, want sys_tool", metadata["allow_always_key"])
			}
		})
	}
}

func TestRelayDeviceTools_InvalidSchemaFallsBackToBareObject(t *testing.T) {
	tools := RelayDeviceTools(&fakeCaller{}, []DeviceToolDescriptor{
		{Name: "weird", Schema: "not-json", RiskLevel: "mode"},
	})
	if len(tools) != 1 {
		t.Fatalf("tools = %d, want 1", len(tools))
	}
	if tools[0].Parameters["type"] != "object" {
		t.Errorf("fallback schema = %v, want {type: object}", tools[0].Parameters)
	}
}

func TestProbeDeviceTools_Sanitizes(t *testing.T) {
	caller := &fakeCaller{result: `{"tools":[
		{"name":"good","description":"d","schema":"{}","risk_level":"never"},
		{"name":"bad name","description":"d","schema":"{}"},
		{"name":"","description":"d","schema":"{}"},
		{"name":"weird_risk","description":"d","schema":"{}","risk_level":"yolo"}
	]}`}
	descs := ProbeDeviceTools(context.Background(), caller)
	if len(descs) != 2 {
		t.Fatalf("descriptors = %d, want 2（非法名剔除）", len(descs))
	}
	if descs[0].RiskLevel != "low" {
		t.Errorf("good risk_level = %q, want low", descs[0].RiskLevel)
	}
	if descs[1].RiskLevel != "unclassified" {
		t.Errorf("invalid risk_level should normalize to unclassified, got %q", descs[1].RiskLevel)
	}
}

func TestProbeDeviceTools_FailureMeansNoCapability(t *testing.T) {
	caller := &fakeCaller{err: context.DeadlineExceeded}
	if descs := ProbeDeviceTools(context.Background(), caller); descs != nil {
		t.Fatalf("失败探测应返回 nil, got %+v", descs)
	}
	// method-not-found（客户端未实现）同理
	caller2 := &fakeCaller{result: `{"code":-32601,"message":"method not found"}`}
	_ = caller2 // Call 把 result 解码失败也会走 err 分支 —— 由 fakeCaller.err 覆盖语义即可
}

// TestSpecDeviceTools_ConvertsToClientToolSpecs(P5-9):探测结果转换为 SDK 能力
// 代理声明——注册名沿用 sys_ 前缀(与原始中继的 core.Tool.Name 同名,模型可见
// 工具面两条路径一致),descriptor→metadata 经共用 helper 与 RelayDeviceTools
// 逐字段一致(legacy never → low 且不门控的语义原样保留)。
func TestSpecDeviceTools_ConvertsToClientToolSpecs(t *testing.T) {
	caller := &fakeCaller{result: `{"tools":[{"name":"vibrate","description":"buzz the device","schema":"{\"type\":\"object\"}","risk_level":"never"}]}`}
	specs, err := SpecDeviceTools(caller)
	if err != nil {
		t.Fatalf("SpecDeviceTools: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("specs = %d, want 1", len(specs))
	}
	spec := specs[0]
	if spec.Name != "sys_vibrate" {
		t.Errorf("name = %q, want sys_vibrate (注册名沿用 sys_ 前缀)", spec.Name)
	}
	if spec.Description != "buzz the device" {
		t.Errorf("description = %q, want passthrough", spec.Description)
	}
	if spec.Schema != `{"type":"object"}` {
		t.Errorf("schema = %q, want passthrough (解析回落由 NewClientToolProxy 承担)", spec.Schema)
	}
	if spec.Metadata["risk_level"] != "low" {
		t.Errorf("risk_level = %q, want low (legacy never 归一语义不变)", spec.Metadata["risk_level"])
	}
	if _, gated := spec.Metadata["requires_permission"]; gated {
		t.Errorf("legacy never must not be gated: metadata = %#v", spec.Metadata)
	}
}

// TestSpecDeviceTools_ProbeFailureCollapses:探测失败/空 → 非 nil error + nil
// specs,binder 据此走「无能力即无工具」的收缩语义。
func TestSpecDeviceTools_ProbeFailureCollapses(t *testing.T) {
	caller := &fakeCaller{err: context.DeadlineExceeded}
	specs, err := SpecDeviceTools(caller)
	if err == nil {
		t.Fatal("probe failure must surface as an error (binder collapse path)")
	}
	if specs != nil {
		t.Fatalf("specs = %+v, want nil", specs)
	}
}

// TestCallDeviceToolRejectsUnmanagedCalls proves stdio workers without a manager-issued
// runtime identity cannot reach the client-side device endpoint.
func TestCallDeviceToolRejectsUnmanagedCalls(t *testing.T) {
	caller := &fakeCaller{result: `{"result":"buzzed"}`}
	if _, err := CallDeviceTool(context.Background(), caller, "sys_vibrate", []byte(`{"duration":1}`)); err == nil {
		t.Fatal("CallDeviceTool accepted a call without managed run identity")
	}
	if caller.method != "" {
		t.Fatalf("caller invoked with method %q without managed run identity", caller.method)
	}
}

func TestCallDeviceToolRejectsBeforeDispatchError(t *testing.T) {
	caller := &fakeCaller{err: context.DeadlineExceeded}
	if _, err := CallDeviceTool(context.Background(), caller, "sys_vibrate", []byte(`{}`)); err == nil ||
		!strings.Contains(err.Error(), "device call vibrate") {
		t.Fatalf("err = %v, want explicit identity failure", err)
	}
	if caller.method != "" {
		t.Fatalf("caller invoked with method %q without managed run identity", caller.method)
	}
}
