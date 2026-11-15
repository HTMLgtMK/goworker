package app

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	"github.com/tinguo/goworker/daemon/internal/providercontrol"
	xgwprotocol "github.com/tinguo/goworker/daemon/xgoworker/protocol"
)

type appProviderSessionSource struct {
	stubSessionSource
	service *providercontrol.Service
}

func (s appProviderSessionSource) ProviderControl() *providercontrol.Service { return s.service }

type appProviderBackend struct {
	cfg      *runtimeconfig.Config
	revision string
}

func (b *appProviderBackend) ConfigSnapshotWithRevision() (*runtimeconfig.Config, string) {
	return b.cfg, b.revision
}

func (b *appProviderBackend) UpdateConfigAtRevision(expected string, mutate func(*runtimeconfig.Config) error) (*runtimeconfig.Config, string, error) {
	if expected != b.revision {
		return nil, b.revision, providercontrol.ErrRevisionConflict
	}
	if err := mutate(b.cfg); err != nil {
		return nil, b.revision, err
	}
	b.revision += "-next"
	return b.cfg, b.revision, nil
}

func newAppProviderSessionSource() (appProviderSessionSource, *appProviderBackend) {
	backend := &appProviderBackend{cfg: &runtimeconfig.Config{LLM: runtimeconfig.DefaultLLM()}, revision: "rev-1"}
	return appProviderSessionSource{service: providercontrol.New(backend)}, backend
}

func TestACPProviderWriteAuthorizationFollowsTransportTrust(t *testing.T) {
	tests := []struct {
		name       string
		unix       bool
		wantWrites bool
	}{
		{name: "private local unix socket", unix: true, wantWrites: true},
		{name: "injected connection without identity", wantWrites: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sessions, backend := newAppProviderSessionSource()
			var (
				clientConn net.Conn
				options    acpServiceOptions
			)
			if tc.unix {
				dir, err := os.MkdirTemp("/tmp", "acp-")
				if err != nil {
					t.Fatalf("create short socket directory: %v", err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(dir) })
				options.socketPath = filepath.Join(dir, "acp.sock")
			} else {
				serverConn, peerConn := net.Pipe()
				clientConn = peerConn
				options.connection = serverConn
			}
			options.evaluate = func(*model.Context, string) error { return nil }
			options.sessions = sessions
			service, err := newACPService(options)
			if err != nil {
				t.Fatalf("newACPService: %v", err)
			}
			if err := service.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer service.Stop()
			if tc.unix {
				clientConn, err = net.Dial("unix", options.socketPath)
				if err != nil {
					t.Fatalf("dial local ACP socket: %v", err)
				}
			}

			client, err := aiacp.NewClient("provider-transport-test", clientConn, nil)
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := client.Initialize(ctx); err != nil {
				t.Fatalf("Initialize: %v", err)
			}
			var capabilities xgwprotocol.CapabilitiesResult
			if err := client.Call(ctx, xgwprotocol.MethodCapabilities, nil, &capabilities); err != nil {
				t.Fatalf("x-goworker/capabilities: %v", err)
			}
			writeMethod := xgwprotocol.ProviderMethodCurrentSet
			advertised := false
			for _, method := range capabilities.Methods {
				if method == writeMethod {
					advertised = true
				}
			}
			if advertised != tc.wantWrites {
				t.Fatalf("provider write advertised = %v, want %v; methods=%v", advertised, tc.wantWrites, capabilities.Methods)
			}
			var result xgwprotocol.ProviderCurrentSetResult
			callErr := client.Call(ctx, writeMethod, xgwprotocol.ProviderCurrentSetParams{
				Revision: backend.revision, Provider: "openai", Model: "gpt-4o",
			}, &result)
			if tc.wantWrites {
				if callErr != nil {
					t.Fatalf("trusted local provider write: %v", callErr)
				}
				if backend.revision == "rev-1" {
					t.Fatal("trusted local provider write did not update revision")
				}
			} else {
				if callErr == nil {
					t.Fatal("unknown injected connection performed a provider write")
				}
				if backend.revision != "rev-1" {
					t.Fatalf("denied connection changed revision to %q", backend.revision)
				}
			}
		})
	}
}

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
