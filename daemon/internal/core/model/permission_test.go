package model

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// stubAsker 是 PermissionAsker 的测试替身：记录收到的请求，按预设应答。
type stubAsker struct {
	optionID string
	err      error
	got      []protocol.PermissionRequest
}

func (s *stubAsker) RequestPermission(_ context.Context, sessionID string, req protocol.PermissionRequest) (string, error) {
	req.SessionID = sessionID
	s.got = append(s.got, req)
	return s.optionID, s.err
}

func TestPermissionFromInterrupt_UsesCommandAsTitle(t *testing.T) {
	req := &hitl.InterruptRequest{
		ID:         "req-1",
		ToolName:   "bash",
		Command:    "rm -rf build/",
		RiskReason: "destructive command",
	}
	got := PermissionFromInterrupt(req)

	if got.ToolCall.ToolCallID != "req-1" {
		t.Errorf("toolCallId = %q, want req-1", got.ToolCall.ToolCallID)
	}
	// 标题必须带命令本身：用户判断的是"这条命令能不能跑"，裸工具名等于盲签。
	if !strings.Contains(got.ToolCall.Title, "rm -rf build/") {
		t.Errorf("title %q 未包含命令", got.ToolCall.Title)
	}
	if !strings.Contains(got.ToolCall.Title, "destructive command") {
		t.Errorf("title %q 未包含风险原因", got.ToolCall.Title)
	}
	// sessionID 留给 RequestPermission 填，翻译层不该自作主张。
	if got.SessionID != "" {
		t.Errorf("sessionID = %q, want empty (由 Reporter 填)", got.SessionID)
	}
}

func TestPermissionFromInterrupt_FallsBackToToolName(t *testing.T) {
	// 非 bash 工具没有 command 字段，退到工具名而不是给出空标题。
	got := PermissionFromInterrupt(&hitl.InterruptRequest{ID: "req-2", ToolName: "mcp_write"})
	if got.ToolCall.Title != "mcp_write" {
		t.Errorf("title = %q, want mcp_write", got.ToolCall.Title)
	}
}

func TestPermissionFromInterrupt_OffersOnceAndAlwaysForBash(t *testing.T) {
	got := PermissionFromInterrupt(&hitl.InterruptRequest{ID: "req-3", ToolName: "bash", Command: "ls"})
	if len(got.Options) != 3 {
		t.Fatalf("options = %d, want 3", len(got.Options))
	}
	if got.Options[0].Kind != "allow_once" || got.Options[1].Kind != "reject_once" || got.Options[2].Kind != "allow_always" {
		t.Errorf("option order = %+v, want allow_once, reject_once, allow_always", got.Options)
	}
}

func TestPermissionFromInterrupt_OffersNoAlwaysForMCP(t *testing.T) {
	got := PermissionFromInterrupt(&hitl.InterruptRequest{ID: "req-3b", ToolName: "mcp_write"})
	if len(got.Options) != 2 {
		t.Fatalf("options = %d, want 2", len(got.Options))
	}
	for _, opt := range got.Options {
		if opt.Kind == "allow_always" {
			t.Fatalf("MCP option %q must not offer command-scoped always", opt.OptionID)
		}
	}
}

func TestDecisionFromOption_AllowAlwaysApprovesAlways(t *testing.T) {
	got := DecisionFromOption("req-4a", hitlOptionAllowAlways)
	if got.Type != hitl.DecisionApproveAlways {
		t.Errorf("type = %q, want approve_always", got.Type)
	}
}

func TestDecisionFromOption_AllowApproves(t *testing.T) {
	got := DecisionFromOption("req-4", hitlOptionAllow)
	if got.Type != hitl.DecisionApprove {
		t.Errorf("type = %q, want approve", got.Type)
	}
	if got.InterruptID != "req-4" {
		t.Errorf("interruptID = %q, want req-4", got.InterruptID)
	}
}

func TestDecisionFromOption_RejectsAndNeverFailsOpen(t *testing.T) {
	// 这张表是安全边界：任何非 allow 的输入都必须落到明确的 reject。
	// hitl 中间件的 switch 没有 default 分支，零值 DecisionType("") 会穿透
	// 全部 case 落到末尾的空 MiddlewareResponse —— 那是放行。
	for _, optionID := range []string{hitlOptionReject, "", "unknown-option", "reject_always"} {
		t.Run(optionID, func(t *testing.T) {
			got := DecisionFromOption("req-5", optionID)
			if got.Type != hitl.DecisionReject {
				t.Errorf("optionID %q → type %q, want reject（fail-closed）", optionID, got.Type)
			}
		})
	}
}

func TestDecideViaACP_AsksAndTranslates(t *testing.T) {
	asker := &stubAsker{optionID: hitlOptionAllow}
	decide := DecideViaACP(context.Background(), asker, "sess-1")

	got := decide(&hitl.InterruptRequest{ID: "req-6", ToolName: "bash", Command: "ls"})

	if got.Type != hitl.DecisionApprove {
		t.Errorf("type = %q, want approve", got.Type)
	}
	if len(asker.got) != 1 {
		t.Fatalf("请求数 = %d, want 1", len(asker.got))
	}
	// sessionID 必须由 asker 收到，否则请求发不出去。
	if asker.got[0].SessionID != "sess-1" {
		t.Errorf("sessionID = %q, want sess-1", asker.got[0].SessionID)
	}
}

func TestDecideViaACP_ErrorRejectsInsteadOfAllowing(t *testing.T) {
	// 问不到人（连接断开/session 取消）绝不能默认放行。
	asker := &stubAsker{err: errors.New("connection closed")}
	decide := DecideViaACP(context.Background(), asker, "sess-2")

	got := decide(&hitl.InterruptRequest{ID: "req-7", ToolName: "bash", Command: "rm -rf /"})

	if got.Type != hitl.DecisionReject {
		t.Errorf("type = %q, want reject（出错即拒绝）", got.Type)
	}
}
