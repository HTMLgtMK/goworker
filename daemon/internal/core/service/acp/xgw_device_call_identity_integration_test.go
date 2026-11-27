package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

type identityToolProvider struct {
	mu        sync.Mutex
	calls     int
	toolCalls []core.ToolCall
}

func (p *identityToolProvider) Name() string  { return "identity-test" }
func (p *identityToolProvider) Model() string { return "deterministic" }

func (p *identityToolProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	message := core.Message{Role: "assistant", Content: "done"}
	if p.calls == 1 {
		message = core.Message{Role: "assistant", ToolCalls: append([]core.ToolCall(nil), p.toolCalls...)}
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: message}}}, nil
}

func (*identityToolProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("streaming is not used by this test")
}

type deviceCallIdentity struct {
	SessionID string          `json:"sessionId"`
	RunID     string          `json:"runId"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func TestXGWDeviceCallCarriesRuntimeSessionAndRunIdentity(t *testing.T) {
	providerA := &identityToolProvider{toolCalls: []core.ToolCall{{
		ID:       "call-a",
		Type:     "function",
		Function: core.ToolCallFunction{Name: "sys_vibrate", Arguments: `{"duration":1}`},
	}}}
	providerB := &identityToolProvider{toolCalls: []core.ToolCall{
		{ID: "call-b-1", Type: "function", Function: core.ToolCallFunction{Name: "sys_vibrate", Arguments: `{"duration":1}`}},
		{ID: "call-b-2", Type: "function", Function: core.ToolCallFunction{Name: "sys_vibrate", Arguments: `{"duration":2}`}},
	}}
	control := newPR2DualControl(t, map[string]core.Provider{"runtime-a": providerA, "runtime-b": providerB}, "runtime-a", "runtime-b")

	evaluate := func(ctx *model.Context, input string) error {
		binding := agentcontrol.BindingFromCtx(ctx.Ctx)
		if binding == nil || binding.Get() == "" {
			return errors.New("ACP prompt has no runtime session binding")
		}
		handle, err := control.m.StartRun(binding.Get(), runtimeagent.RunRequest{
			Input:      input,
			ToolSource: ctx.FrontendContext.ToolSource,
		}, runtimeagent.RunCallbacks{
			Write:     ctx.Writer,
			EmitToken: ctx.FrontendContext.EmitToken,
		})
		if err != nil {
			return fmt.Errorf("start runtime run: %w", err)
		}

		result, waitErr := handle.Wait(ctx.Ctx)
		if errors.Is(waitErr, context.Canceled) {
			if err := control.m.CancelRun(handle.SessionID, handle.RunID); err != nil && !errors.Is(err, runtimeagent.ErrRunNotFound) {
				return fmt.Errorf("cancel runtime run: %w", err)
			}
			waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, waitErr = handle.Wait(waitCtx)
		}
		if waitErr != nil {
			return fmt.Errorf("wait runtime run: %w", waitErr)
		}
		return result.Err
	}

	client, _ := newPR2Stack(t, evaluate, control, NewSessionGate())
	caps := protocol.ClientCapabilities{}
	if err := json.Unmarshal([]byte(`{"x-device":{"tools":true}}`), &caps); err != nil {
		t.Fatalf("decode client capabilities: %v", err)
	}
	if _, err := client.InitializeWithRequest(context.Background(), protocol.InitializeRequest{
		ProtocolVersion:    protocol.Version,
		ClientCapabilities: caps,
	}); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	client.Handle(acpworker.MethodDeviceTools, func(context.Context, json.RawMessage) (any, error) {
		return json.RawMessage(xgwDeviceToolsPayload), nil
	})
	releaseB := make(chan struct{})
	callA := make(chan deviceCallIdentity, 2)
	callB := make(chan deviceCallIdentity, 4)
	client.Handle(acpworker.MethodDeviceCall, func(ctx context.Context, params json.RawMessage) (any, error) {
		var request deviceCallIdentity
		if err := json.Unmarshal(params, &request); err != nil {
			return nil, fmt.Errorf("decode x-device/call: %w", err)
		}
		if !strings.Contains(string(params), `"sessionId"`) || !strings.Contains(string(params), `"runId"`) {
			return nil, fmt.Errorf("x-device/call request missing sessionId or runId: %s", params)
		}
		if strings.Contains(string(params), `"session_id"`) || strings.Contains(string(params), `"run_id"`) {
			return nil, fmt.Errorf("x-device/call request contains legacy identity field names")
		}
		switch request.SessionID {
		case "runtime-a":
			callA <- request
			<-ctx.Done()
			return nil, ctx.Err()
		case "runtime-b":
			callB <- request
			select {
			case <-releaseB:
				return json.RawMessage(`{"result":"ok"}`), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		default:
			return nil, fmt.Errorf("unexpected runtime session id %q", request.SessionID)
		}
	})

	newSession := func(cwd string) string {
		t.Helper()
		var response protocol.NewSessionResponse
		if err := client.Call(context.Background(), protocol.MethodSessionNew, protocol.NewSessionRequest{Cwd: cwd}, &response); err != nil {
			t.Fatalf("session/new(%s): %v", cwd, err)
		}
		if response.SessionID == "" {
			t.Fatalf("session/new(%s) returned an empty canonical session ID", cwd)
		}
		return response.SessionID
	}
	runtimeA := newSession("/repo-a")
	runtimeB := newSession("/repo-b")
	if runtimeA != "runtime-a" || runtimeB != "runtime-b" {
		t.Fatalf("session/new IDs = %q, %q; want canonical runtime IDs runtime-a, runtime-b", runtimeA, runtimeB)
	}

	type promptResult struct {
		stop string
		err  error
	}
	promptA := make(chan promptResult, 1)
	promptB := make(chan promptResult, 1)
	go func() {
		stop, err := client.Prompt(context.Background(), runtimeA, []protocol.ContentBlock{protocol.TextBlock("cancel this run")})
		promptA <- promptResult{stop: stop, err: err}
	}()
	go func() {
		stop, err := client.Prompt(context.Background(), runtimeB, []protocol.ContentBlock{protocol.TextBlock("keep this run alive")})
		promptB <- promptResult{stop: stop, err: err}
	}()

	awaitCall := func(label string, calls <-chan deviceCallIdentity) deviceCallIdentity {
		t.Helper()
		select {
		case call := <-calls:
			return call
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s x-device/call", label)
			return deviceCallIdentity{}
		}
	}
	firstA := awaitCall("runtime-a", callA)
	firstB := awaitCall("runtime-b", callB)
	if firstA.SessionID != runtimeA || firstB.SessionID != runtimeB {
		t.Fatalf("x-device/call sessionId values = %q, %q; want runtime IDs %q, %q", firstA.SessionID, firstB.SessionID, runtimeA, runtimeB)
	}
	if firstA.RunID == "" || firstB.RunID == "" {
		t.Fatalf("x-device/call runId must be set: A=%q B=%q", firstA.RunID, firstB.RunID)
	}
	if firstA.RunID == firstB.RunID {
		t.Fatalf("different runtime sessions share runId %q", firstA.RunID)
	}
	activeA := control.m.Get(runtimeA).Active()
	activeB := control.m.Get(runtimeB).Active()
	if activeA == nil || activeA.ID != firstA.RunID || activeB == nil || activeB.ID != firstB.RunID {
		t.Fatalf("x-device/call run IDs do not match the active manager runs: A=%q B=%q activeA=%v activeB=%v", firstA.RunID, firstB.RunID, activeA, activeB)
	}

	if err := client.Notify(protocol.MethodSessionCancel, protocol.CancelNotification{SessionID: runtimeA}); err != nil {
		t.Fatalf("session/cancel(runtime-a transport): %v", err)
	}
	select {
	case result := <-promptA:
		if result.err == nil {
			t.Fatalf("cancelled prompt unexpectedly succeeded with stop reason %q", result.stop)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled prompt did not return")
	}
	select {
	case result := <-promptB:
		t.Fatalf("runtime-b prompt returned before its device tool was released: %+v", result)
	default:
	}

	close(releaseB)
	secondB := awaitCall("second runtime-b tool", callB)
	if secondB.SessionID != runtimeB {
		t.Fatalf("second runtime-b x-device/call sessionId = %q, want %q", secondB.SessionID, runtimeB)
	}
	if secondB.RunID != firstB.RunID {
		t.Fatalf("same Run produced different x-device/call runId values %q and %q", firstB.RunID, secondB.RunID)
	}
	if secondB.Name != "vibrate" {
		t.Fatalf("x-device/call name = %q, want client-side tool name vibrate", secondB.Name)
	}

	select {
	case result := <-promptB:
		if result.err != nil {
			t.Fatalf("runtime-b prompt after runtime-a cancellation: %v", result.err)
		}
		if result.stop != protocol.StopEndTurn {
			t.Fatalf("runtime-b stop reason = %q, want %q", result.stop, protocol.StopEndTurn)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime-b prompt did not finish after releasing its device calls")
	}
}
