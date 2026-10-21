package app

import (
	"context"
	"net"
	"testing"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// stubSessionSource 只为门控测试提供「非 nil 数据源」：initialize 不应触碰它，
// embedded nil interface 上任何方法被调到都会 panic，门控若失效即当场暴露。
type stubSessionSource struct{ model.SessionSource }

// TestNewACPServiceAdvertisesSessionCapabilities 钉住装配层的会话能力门控：
// 有 SessionSource 才通告 loadSession；没有 SessionSource（goworker_no_agent
// 类构建）时 initialize 不得让客户端协商一个调用必然失败的能力。
func TestNewACPServiceAdvertisesSessionCapabilities(t *testing.T) {
	tests := []struct {
		name            string
		sessions        model.SessionSource
		wantLoadSession bool
	}{
		{name: "nil session source gates loadSession", sessions: nil, wantLoadSession: false},
		{name: "session source advertises loadSession", sessions: stubSessionSource{}, wantLoadSession: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			serverConn, clientConn := net.Pipe()
			defer clientConn.Close()

			svc, err := newACPService(acpServiceOptions{
				evaluate:   func(*model.Context, string) error { return nil },
				connection: serverConn,
				sessions:   tc.sessions,
			})
			if err != nil {
				t.Fatalf("newACPService: %v", err)
			}
			if err := svc.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer func() { _ = svc.Stop() }()

			client, err := aiacp.NewClient("gating-test", clientConn, nil)
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			resp, err := client.Initialize(ctx)
			if err != nil {
				t.Fatalf("Initialize: %v", err)
			}
			if got := resp.AgentCapabilities.Has(protocol.LoadSessionCapability{}.Key()); got != tc.wantLoadSession {
				t.Fatalf("agentCapabilities loadSession = %v, want %v", got, tc.wantLoadSession)
			}
		})
	}
}
