package dispatcher

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	dispatch "github.com/tinguo/goworker/ai-dispatch"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

// stubTaskHandler 是 dispatch.Server 需要的最小 TaskHandler（本文件不驱动 prompt）。
type stubTaskHandler struct{}

func (stubTaskHandler) Run(context.Context, string, string, dispatch.Reporter) (string, error) {
	return protocol.StopEndTurn, nil
}

// newACPPeer 起一对 net.Pipe：一端是 dispatch.Server（daemon 侧），另一端扮演
// ACP client，按 answer 应答 session/request_permission。
func newACPPeer(t *testing.T, answer func(protocol.PermissionRequest) (any, error)) *dispatch.Server {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()
	server := dispatch.ServeConn(serverEnd, stubTaskHandler{})

	peer := protocol.NewConn(clientEnd)
	peer.Handle(protocol.MethodSessionRequestPermission, func(_ context.Context, params json.RawMessage) (any, error) {
		var req protocol.PermissionRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, err
		}
		return answer(req)
	})
	go func() { _ = peer.Serve() }()

	t.Cleanup(func() {
		_ = peer.Close()
		_ = server.Close()
	})
	return server
}

func testPermissionRequest() protocol.PermissionRequest {
	return protocol.PermissionRequest{
		ToolCall: protocol.ToolCallInfo{ToolCallID: "req-1", Title: "rm -rf build/"},
		Options: []protocol.PermissionOption{
			{OptionID: "allow_once", Name: "Allow once", Kind: "allow_once"},
			{OptionID: "reject_once", Name: "Reject", Kind: "reject_once"},
		},
	}
}

// 无订阅者时必须立刻拒绝，而不是挂起等一个永远不来的应答。
func TestAskPermissionPolicy_NoSubscriberDenies(t *testing.T) {
	p, _ := newTestPlugin(t)

	optionID, err := p.askPermissionPolicy("task_nobody")(context.Background(), testPermissionRequest())

	if err == nil {
		t.Fatal("无订阅者时应报错拒绝")
	}
	if optionID != "" {
		t.Errorf("optionID = %q, want empty", optionID)
	}
}

func TestAskPermissionPolicy_ForwardsUserChoice(t *testing.T) {
	p, _ := newTestPlugin(t)
	server := newACPPeer(t, func(protocol.PermissionRequest) (any, error) {
		return protocol.SelectedPermissionOutcome("allow_once"), nil
	})
	p.addACPRoute("task_ask", acpRoute{server: server, sessionID: "sess-1"})

	optionID, err := p.askPermissionPolicy("task_ask")(context.Background(), testPermissionRequest())

	if err != nil {
		t.Fatalf("askPermissionPolicy: %v", err)
	}
	if optionID != "allow_once" {
		t.Errorf("optionID = %q, want allow_once", optionID)
	}
}

// 对端应答为空串不能当成"没意见"，必须显式失败（失败方向 = 拒绝）。
func TestAskPermissionPolicy_EmptyAnswerDenies(t *testing.T) {
	p, _ := newTestPlugin(t)
	server := newACPPeer(t, func(protocol.PermissionRequest) (any, error) {
		return protocol.SelectedPermissionOutcome(""), nil
	})
	p.addACPRoute("task_empty", acpRoute{server: server, sessionID: "sess-1"})

	if _, err := p.askPermissionPolicy("task_empty")(context.Background(), testPermissionRequest()); err == nil {
		t.Fatal("空应答应报错拒绝")
	}
}

// newUnsupportedPeer 起一个**没有**注册 request_permission handler 的对端：
// dispatch.Server 会收到 -32601（method not found），即"这个客户端不会答"。
func newUnsupportedPeer(t *testing.T) *dispatch.Server {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()
	server := dispatch.ServeConn(serverEnd, stubTaskHandler{})
	peer := protocol.NewConn(clientEnd)
	go func() { _ = peer.Serve() }()
	t.Cleanup(func() {
		_ = peer.Close()
		_ = server.Close()
	})
	return server
}

// 观察者路由（--attach）必须参与扇出：异步任务（detach / 重启恢复）只有它
// 有订阅者，漏了它等于 ask 在这些路径上静默退化成 deny。
func TestAskPermissionPolicy_AsksLiveObservers(t *testing.T) {
	p, _ := newTestPlugin(t)
	server := newACPPeer(t, func(protocol.PermissionRequest) (any, error) {
		return protocol.SelectedPermissionOutcome("allow_once"), nil
	})
	p.addLiveRoute("task_live_ask", acpRoute{server: server, sessionID: "sess-live"})

	optionID, err := p.askPermissionPolicy("task_live_ask")(context.Background(), testPermissionRequest())

	if err != nil {
		t.Fatalf("askPermissionPolicy: %v", err)
	}
	if optionID != "allow_once" {
		t.Errorf("optionID = %q, want allow_once", optionID)
	}
}

// 关键回归：不支持权限的观察者**不能**抢先拒绝。它只说明自己帮不上忙，
// 请求必须继续问下一个订阅者。
func TestAskPermissionPolicy_SkipsUnsupportedPeerAndAsksNext(t *testing.T) {
	p, _ := newTestPlugin(t)
	p.addLiveRoute("task_mixed", acpRoute{server: newUnsupportedPeer(t), sessionID: "sess-unsupported"})
	p.addLiveRoute("task_mixed", acpRoute{server: newACPPeer(t, func(protocol.PermissionRequest) (any, error) {
		return protocol.SelectedPermissionOutcome("allow_once"), nil
	}), sessionID: "sess-capable"})

	optionID, err := p.askPermissionPolicy("task_mixed")(context.Background(), testPermissionRequest())

	if err != nil {
		t.Fatalf("应跳过不会答的对端继续问：%v", err)
	}
	if optionID != "allow_once" {
		t.Errorf("optionID = %q, want allow_once", optionID)
	}
}

// 全部订阅者都不会答时才算失败，且错误信息要点明是客户端能力缺口。
func TestAskPermissionPolicy_AllUnsupportedDenies(t *testing.T) {
	p, _ := newTestPlugin(t)
	p.addLiveRoute("task_none_capable", acpRoute{server: newUnsupportedPeer(t), sessionID: "sess-1"})

	_, err := p.askPermissionPolicy("task_none_capable")(context.Background(), testPermissionRequest())

	if err == nil {
		t.Fatal("全部不支持时应拒绝")
	}
	if !strings.Contains(err.Error(), "do not implement") {
		t.Errorf("错误信息应点明能力缺口，实际: %v", err)
	}
}

// 同一 route 重复 attach 不应让路由表膨胀。
func TestAddLiveRoute_DeduplicatesSameRoute(t *testing.T) {
	p, _ := newTestPlugin(t)
	route := acpRoute{server: &dispatch.Server{}, sessionID: "sess-dup"}

	p.addLiveRoute("task_dup", route)
	p.addLiveRoute("task_dup", route)

	if routes := p.snapshotLiveRoutes("task_dup"); len(routes) != 1 {
		t.Errorf("routes = %d, want 1（同一 route 去重）", len(routes))
	}
}

// 多个订阅者时先到先得：不应因为并发扇出而卡住或串号。
func TestAskPermissionPolicy_FirstAnswerWins(t *testing.T) {
	p, _ := newTestPlugin(t)
	first := newACPPeer(t, func(protocol.PermissionRequest) (any, error) {
		return protocol.SelectedPermissionOutcome("allow_once"), nil
	})
	second := newACPPeer(t, func(protocol.PermissionRequest) (any, error) {
		// 慢应答：让 first 抢到
		time.Sleep(50 * time.Millisecond)
		return protocol.SelectedPermissionOutcome("reject_once"), nil
	})
	p.addACPRoute("task_race", acpRoute{server: first, sessionID: "sess-1"})
	p.addACPRoute("task_race", acpRoute{server: second, sessionID: "sess-2"})

	optionID, err := p.askPermissionPolicy("task_race")(context.Background(), testPermissionRequest())

	if err != nil {
		t.Fatalf("askPermissionPolicy: %v", err)
	}
	if optionID != "allow_once" {
		t.Errorf("optionID = %q, want allow_once（先到先得）", optionID)
	}
}

// ask 需要任务身份定位订阅者；无 taskID 时退化为默认拒绝（runOptions 不装策略）。
func TestRunOptions_AskWithoutTaskIDFallsBackToDeny(t *testing.T) {
	p, _ := newTestPlugin(t)
	p.cfg.Dispatch.Workers = []runtimeconfig.WorkerConfig{{Name: "fake", Command: "fake", OnPermission: "ask"}}

	if opts := p.runOptions("fake", ""); len(opts) != 0 {
		t.Errorf("opts = %d, want 0（无 taskID 时不装策略 = 拒绝）", len(opts))
	}
	if opts := p.runOptions("fake", "task_1"); len(opts) != 1 {
		t.Errorf("opts = %d, want 1（有 taskID 时装 ask 策略）", len(opts))
	}
}
