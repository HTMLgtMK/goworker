package aiacp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-acp/protocol"
	aisession "github.com/tinguo/goworker/ai-session"
)

// TestInitializeReadyRunsAfterResponseWrite checks that the ready hook issues a reverse call.
// Frame ordering is checked independently by TestInitializeResponseFramePrecedesReverseRequest.
func TestInitializeReadyRunsAfterResponseWrite(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	handler := &initializeReadyHandler{ready: make(chan struct{})}
	server := ServeConn(serverEnd, handler)
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient("test-agent", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	called := make(chan struct{})
	client.Handle("x-device/tools", func(context.Context, json.RawMessage) (any, error) {
		select {
		case <-handler.ready:
		default:
			t.Error("reverse call arrived before the ready hook")
		}
		close(called)
		return map[string]any{"tools": []any{}}, nil
	})

	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("initialize ready hook did not issue reverse call")
	}
}

type initializeReadyHandler struct {
	ready chan struct{}
}

func (*initializeReadyHandler) Prompt(context.Context, string, []protocol.ContentBlock, Reporter) (string, error) {
	return protocol.StopEndTurn, nil
}

func (h *initializeReadyHandler) InitializeReady(ctx context.Context, reporter Reporter) {
	close(h.ready)
	caller, ok := reporter.(interface {
		Call(context.Context, string, any, any) error
	})
	if !ok {
		return
	}
	var response map[string]any
	_ = caller.Call(ctx, "x-device/tools", struct{}{}, &response)
}

type sessionNewEventHandler struct{}

func (sessionNewEventHandler) Prompt(context.Context, string, []protocol.ContentBlock, Reporter) (string, error) {
	return protocol.StopEndTurn, nil
}

func (sessionNewEventHandler) CreateSession(_ context.Context, _ protocol.NewSessionRequest, reporter Reporter) (aisession.ID, error) {
	id := aisession.ID("session-events")
	reporter.Update(id.String(), protocol.SessionUpdateBody{Raw: json.RawMessage(`{"sessionUpdate":"available_commands_update","availableCommands":[{"name":"/help"}]}`)})
	return id, nil
}

func TestSessionNewWritesResponseBeforeCreatorUpdates(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, sessionNewEventHandler{})
	t.Cleanup(func() { _ = server.Close() })
	t.Cleanup(func() { _ = clientEnd.Close() })
	_ = clientEnd.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := clientEnd.Write([]byte(`{"jsonrpc":"2.0","id":7,"method":"session/new","params":{"cwd":"/repo","mcpServers":[]}}` + "\n")); err != nil {
		t.Fatalf("write session/new: %v", err)
	}
	reader := bufio.NewReader(clientEnd)
	type frame struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Result  json.RawMessage `json:"result"`
		Params  struct {
			SessionID string                     `json:"sessionId"`
			Update    protocol.SessionUpdateBody `json:"update"`
		} `json:"params"`
	}
	read := func() frame {
		t.Helper()
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		var got frame
		if err := json.Unmarshal(line, &got); err != nil {
			t.Fatalf("decode frame %s: %v", line, err)
		}
		return got
	}

	response := read()
	if string(response.ID) != "7" || response.Method != "" {
		t.Fatalf("first frame = %+v, want session/new response before notifications", response)
	}
	var created protocol.NewSessionResponse
	if err := json.Unmarshal(response.Result, &created); err != nil {
		t.Fatalf("decode session/new result: %v", err)
	}
	if created.SessionID != "session-events" {
		t.Fatalf("session/new ID = %q", created.SessionID)
	}

	update := read()
	if update.Method != protocol.MethodSessionUpdate || update.Params.SessionID != created.SessionID ||
		update.Params.Update.SessionUpdate != protocol.UpdateAvailableCommands {
		t.Fatalf("second frame = %+v, want available_commands_update for returned session", update)
	}
}

func TestSessionNewRequiresApplicationCreator(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, &initializeReadyHandler{ready: make(chan struct{})})
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient("protocol-only", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.NewSession(context.Background(), "/repo"); err == nil {
		t.Fatal("session/new succeeded without an application creator")
	}
}

func TestSessionNewReporterForwardsOptionalServerInterfaces(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	creator := &reporterSurfaceCreator{}
	server := ServeConn(serverEnd, creator)
	t.Cleanup(func() { _ = server.Close() })

	client := protocol.NewConn(clientEnd)
	client.Handle("x-test/reverse", func(_ context.Context, params json.RawMessage) (any, error) {
		var request struct {
			Probe string `json:"probe"`
		}
		if err := json.Unmarshal(params, &request); err != nil {
			return nil, err
		}
		return map[string]any{"accepted": request.Probe == "from-creator"}, nil
	})
	t.Cleanup(func() { _ = client.Close() })
	go func() { _ = client.Serve() }()

	var clientCaps protocol.ClientCapabilities
	if err := json.Unmarshal([]byte(`{"x-device":{"tools":true}}`), &clientCaps); err != nil {
		t.Fatalf("decode client capabilities: %v", err)
	}
	var initialized protocol.InitializeResponse
	if err := client.Call(context.Background(), protocol.MethodInitialize,
		protocol.InitializeRequest{ProtocolVersion: protocol.Version, ClientCapabilities: clientCaps}, &initialized); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	var created protocol.NewSessionResponse
	if err := client.Call(context.Background(), protocol.MethodSessionNew,
		protocol.NewSessionRequest{Cwd: "/repo"}, &created); err != nil {
		t.Fatalf("session/new: %v", err)
	}
	if created.SessionID != "reporter-surface-session" {
		t.Fatalf("session ID = %q", created.SessionID)
	}
	if !creator.sawClientCapability || !creator.reverseCallAccepted {
		t.Fatalf("creator reporter interfaces: capabilities=%v reverseCall=%v err=%v",
			creator.sawClientCapability, creator.reverseCallAccepted, creator.err)
	}
}

type reporterSurfaceCreator struct {
	sawClientCapability bool
	reverseCallAccepted bool
	err                 error
}

func (*reporterSurfaceCreator) Prompt(context.Context, string, []protocol.ContentBlock, Reporter) (string, error) {
	return protocol.StopEndTurn, nil
}

func (c *reporterSurfaceCreator) CreateSession(ctx context.Context, _ protocol.NewSessionRequest, reporter Reporter) (aisession.ID, error) {
	capabilityProvider, ok := reporter.(ClientCapabilitiesProvider)
	if !ok {
		c.err = errors.New("reporter lost ClientCapabilities interface")
		return "", c.err
	}
	c.sawClientCapability = capabilityProvider.ClientCapabilities().Has("x-device")

	reverseCaller, ok := reporter.(ReverseCaller)
	if !ok {
		c.err = errors.New("reporter lost reverse Call interface")
		return "", c.err
	}
	var response struct {
		Accepted bool `json:"accepted"`
	}
	if err := reverseCaller.Call(ctx, "x-test/reverse", map[string]string{"probe": "from-creator"}, &response); err != nil {
		c.err = err
		return "", err
	}
	c.reverseCallAccepted = response.Accepted
	return aisession.ID("reporter-surface-session"), nil
}

func TestServerStoresClientCapabilities(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, &lifecycleHandler{})
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient("test-agent", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if server.ClientCapabilities().Has("x-device") {
		t.Fatal("capabilities must be empty before initialize")
	}

	var caps protocol.ClientCapabilities
	if err := json.Unmarshal([]byte(`{"x-device": {"tools": true}}`), &caps); err != nil {
		t.Fatalf("decode clientCapabilities: %v", err)
	}
	if _, err := client.InitializeWithRequest(context.Background(), protocol.InitializeRequest{
		ProtocolVersion:    protocol.Version,
		ClientCapabilities: caps,
	}); err != nil {
		t.Fatalf("InitializeWithRequest: %v", err)
	}
	if !server.ClientCapabilities().Has("x-device") {
		t.Fatal("server did not store clientCapabilities.x-device from initialize")
	}
}

type testAgentCapability struct{}

func (testAgentCapability) Key() string { return "x-test" }

func (testAgentCapability) MarshalJSON() ([]byte, error) {
	return []byte(`{"enabled":true}`), nil
}

func TestServerInjectsTypedAgentCapability(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, &lifecycleHandler{}, WithAgentCapability(testAgentCapability{}))
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient("test-agent", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	response, err := client.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	capability, ok := response.AgentCapabilities.Get("x-test")
	if !ok {
		t.Fatal("initialize response does not advertise the injected capability")
	}
	wire, err := json.Marshal(capability)
	if err != nil {
		t.Fatalf("marshal injected capability: %v", err)
	}
	if string(wire) != `{"enabled":true}` {
		t.Fatalf("injected capability wire = %s, want {\"enabled\":true}", wire)
	}
}

func TestServerKeepsInitializeWireWithoutInjectedCapabilities(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, &lifecycleHandler{})
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient("test-agent", clientEnd, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	response, err := client.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if response.AgentCapabilities.Has("x-test") {
		t.Fatal("default server unexpectedly advertises an application capability")
	}
}
