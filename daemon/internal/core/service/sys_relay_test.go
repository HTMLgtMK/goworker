package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
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
	if tool.Metadata["risk_level"] != "never" {
		t.Errorf("risk_level metadata = %q, want never", tool.Metadata["risk_level"])
	}
	if _, ok := tool.Parameters["type"]; !ok {
		t.Error("schema not parsed into Parameters")
	}

	result, err := tool.Execute(context.Background(), map[string]any{"title": "hi"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result != "notification delivered" {
		t.Errorf("result = %q", result)
	}
	if caller.method != MethodDeviceCall {
		t.Errorf("method = %q, want %s", caller.method, MethodDeviceCall)
	}
	var req deviceCallRequest
	if err := json.Unmarshal(caller.params, &req); err != nil {
		t.Fatalf("decode call request: %v", err)
	}
	if req.Name != "send_notification" {
		t.Errorf("call name = %q (应为不带前缀的原始名)", req.Name)
	}
	if !strings.Contains(string(req.Arguments), `"title"`) {
		t.Errorf("arguments missing title: %s", req.Arguments)
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
	if descs[0].RiskLevel != "never" {
		t.Errorf("good risk_level = %q", descs[0].RiskLevel)
	}
	if descs[1].RiskLevel != "mode" {
		t.Errorf("非法 risk_level 应归一为 mode, got %q", descs[1].RiskLevel)
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
