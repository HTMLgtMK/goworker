package protocol

import (
	"encoding/json"
	"testing"
)

// 这组测试钉住 session/request_permission 应答的**线上 JSON 形状**，不是 Go 结构。
//
// 为什么必须逐字比对：permission 的两端（daemon 与 ACP client）各自按 ACP 规范
// 实现，只有线上字节是共同契约。曾经 PermissionResponse 是扁平的
// {"optionId":...}，而规范要求嵌套的 {"outcome":{"outcome":"selected",...}}；
// 用同一个 Go struct 做两端的测试永远发现不了 —— 自己跟自己聊当然对得上，
// 真对端却 schema 校验不过、把"允许"读成拒绝。
func TestPermissionResponse_WireShapeMatchesACP(t *testing.T) {
	tests := []struct {
		name string
		resp PermissionResponse
		want string
	}{
		{
			name: "selected",
			resp: SelectedPermissionOutcome("allow_once"),
			want: `{"outcome":{"outcome":"selected","optionId":"allow_once"}}`,
		},
		{
			name: "cancelled",
			resp: CancelledPermissionOutcome(),
			want: `{"outcome":{"outcome":"cancelled"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.resp)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("wire shape\n got: %s\nwant: %s", got, tt.want)
			}
		})
	}
}

// 反方向：解析对端发来的规范形状，selected 必须读得出 optionId。
func TestPermissionResponse_DecodesACPShape(t *testing.T) {
	var resp PermissionResponse
	raw := `{"outcome":{"outcome":"selected","optionId":"reject_once"}}`
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Outcome.Outcome != "selected" {
		t.Errorf("outcome = %q, want selected", resp.Outcome.Outcome)
	}
	if resp.Outcome.OptionID != "reject_once" {
		t.Errorf("optionId = %q, want reject_once", resp.Outcome.OptionID)
	}

	var cancelled PermissionResponse
	if err := json.Unmarshal([]byte(`{"outcome":{"outcome":"cancelled"}}`), &cancelled); err != nil {
		t.Fatalf("unmarshal cancelled: %v", err)
	}
	if cancelled.Outcome.Outcome != "cancelled" {
		t.Errorf("outcome = %q, want cancelled", cancelled.Outcome.Outcome)
	}
}

// 旧的扁平形状必须解析不出 optionId —— 防止有人"兼容性"地把扁平字段加回来，
// 那会重新引入静默把允许读成拒绝的 bug。
func TestPermissionResponse_FlatShapeIsNotAccepted(t *testing.T) {
	var resp PermissionResponse
	if err := json.Unmarshal([]byte(`{"optionId":"allow_once"}`), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Outcome.OptionID != "" {
		t.Errorf("扁平形状读出了 optionId %q，规范形状被破坏", resp.Outcome.OptionID)
	}
	if resp.Outcome.Outcome != "" {
		t.Errorf("扁平形状读出了 outcome %q，规范形状被破坏", resp.Outcome.Outcome)
	}
}
