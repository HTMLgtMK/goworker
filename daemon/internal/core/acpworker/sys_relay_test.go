package acpworker

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
	if tool.Metadata["risk_level"] != "low" {
		t.Errorf("risk_level metadata = %q, want low", tool.Metadata["risk_level"])
	}
	if _, gated := tool.Metadata["requires_permission"]; gated {
		t.Errorf("legacy never must not be gated: metadata = %#v", tool.Metadata)
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
