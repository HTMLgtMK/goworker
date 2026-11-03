package acp

import (
	"encoding/json"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// mapping_test.go 钉 SessionInfoFromDomain 的 wire 形状纪律（Plan A3 三态上 wire）：
// State 透传；domain 的 Active/Archived 是业务标记，绝不进 wire DTO（ACP
// SessionInfo 保持纯协议语义）；state 为空（降级形态）时 omitempty 收窄报文，
// 旧客户端按无字段组收下。

func TestSessionInfoFromDomainStatePassthrough(t *testing.T) {
	info := SessionInfoFromDomain(model.SessionSummary{
		SessionID: "session_abc",
		Cwd:       "/repo",
		Title:     "标题",
		UpdatedAt: "2026-01-01T00:00:00Z",
		Active:    true,  // domain 便利标记：wire 不透传
		Archived:  false, // domain 直读面：wire 不透传（wire 走 State）
		State:     model.SessionStateActive,
	})
	if info.SessionID != "session_abc" || info.Cwd != "/repo" || info.Title != "标题" || info.UpdatedAt != "2026-01-01T00:00:00Z" {
		t.Fatalf("SessionInfo = %+v, want identity fields mapped", info)
	}
	if info.State != model.SessionStateActive {
		t.Fatalf("state = %q, want %q (三态透传)", info.State, model.SessionStateActive)
	}

	// wire 形状：state 在场，Active/Archived 无键。
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if fields["state"] != model.SessionStateActive {
		t.Fatalf("wire state = %v, want %q", fields["state"], model.SessionStateActive)
	}
	for _, banned := range []string{"active", "archived"} {
		if _, has := fields[banned]; has {
			t.Errorf("wire carries %q key, want none (domain 业务标记不进 wire DTO)", banned)
		}
	}

	// 空状态（清单方未判定的降级形态）：omitempty 收窄报文。
	bare := SessionInfoFromDomain(model.SessionSummary{SessionID: "session_abc"})
	rawBare, err := json.Marshal(bare)
	if err != nil {
		t.Fatalf("marshal bare: %v", err)
	}
	if string(rawBare) != `{"sessionId":"session_abc","cwd":""}` {
		t.Fatalf("bare wire = %s, want state omitted (omitempty 兼容旧客户端)", rawBare)
	}
}
