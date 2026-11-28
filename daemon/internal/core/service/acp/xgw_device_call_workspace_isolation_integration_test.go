package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

const workspaceDeviceToolsPayload = `{"tools":[{"name":"workspace_write","description":"write a file in this workspace","schema":"{\"type\":\"object\",\"properties\":{\"fileName\":{\"type\":\"string\"},\"content\":{\"type\":\"string\"}},\"required\":[\"fileName\",\"content\"]}","risk_level":"benign"},{"name":"workspace_read","description":"read a file in this workspace","schema":"{\"type\":\"object\",\"properties\":{\"fileName\":{\"type\":\"string\"}},\"required\":[\"fileName\"]}","risk_level":"benign"}]}`

type workspaceCreateSpec struct {
	runtimeID string
	provider  core.Provider
}

type workspaceSessionSource struct {
	*pr2DualControl
	mu      sync.Mutex
	created int
	specs   []workspaceCreateSpec
}

func (s *workspaceSessionSource) CreateBoundSession(cwd string) (string, error) {
	s.mu.Lock()
	if s.created >= len(s.specs) {
		s.mu.Unlock()
		return "", errors.New("workspace test: no session creation spec remains")
	}
	spec := s.specs[s.created]
	s.created++
	s.mu.Unlock()

	deps := runtimeagent.SessionDeps{
		Config: s.cfg,
		CollectTools: func(*sandbox.Config, runtimeagent.ToolCollectionOptions) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return spec.provider, nil
		},
		RunEvents: &memEventJournal{},
	}
	runtime := runtimeagent.NewSessionRuntime(spec.runtimeID, runtimeagent.NewSession(deps))
	runtime.DeclareCWD(cwd, nil)
	runtime.Session().SetCWD(cwd)
	if err := s.m.Register(runtime); err != nil {
		return "", fmt.Errorf("register workspace runtime %q: %w", spec.runtimeID, err)
	}
	return spec.runtimeID, nil
}

func (s *workspaceSessionSource) CreateBoundSessionWithRollback(cwd string) (string, func() error, error) {
	id, err := s.CreateBoundSession(cwd)
	if err != nil {
		return "", nil, err
	}
	return id, func() error {
		runtime := s.m.Get(id)
		if runtime == nil {
			return nil
		}
		if err := runtime.Retire(); err != nil {
			return err
		}
		s.m.RemoveRuntime(runtime)
		runtime.CloseEvents()
		return nil
	}, nil
}

type workspaceToolProvider struct {
	mu        sync.Mutex
	calls     []core.ToolCall
	responses int
}

func (p *workspaceToolProvider) Name() string  { return "workspace-isolation-test" }
func (p *workspaceToolProvider) Model() string { return "deterministic" }

func (p *workspaceToolProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.responses++
	message := core.Message{Role: "assistant", Content: "done"}
	if p.responses == 1 {
		message = core.Message{Role: "assistant", ToolCalls: append([]core.ToolCall(nil), p.calls...)}
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: message}}}, nil
}

func (*workspaceToolProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("streaming is not used by this test")
}

type workspaceToolArguments struct {
	FileName string `json:"fileName"`
	Content  string `json:"content"`
}

type workspaceDeviceCall struct {
	SessionID string          `json:"sessionId"`
	RunID     string          `json:"runId"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type workspaceObservation struct {
	SessionID string
	RunID     string
	Name      string
	FileName  string
	Result    string
}

func workspaceToolCall(id, name, arguments string) core.ToolCall {
	return core.ToolCall{
		ID:   id,
		Type: "function",
		Function: core.ToolCallFunction{
			Name:      "sys_" + name,
			Arguments: arguments,
		},
	}
}

// TestXGWDeviceToolsKeepRuntimeWorkspaceIsolation models the client's runtime-ID-to-workspace
// routing over the real ACP call path. It proves the Go wire identity can support isolation and
// shared-workspace behavior; it does not validate Android filesystem permissions or symlink safety.
func TestXGWDeviceToolsKeepRuntimeWorkspaceIsolation(t *testing.T) {
	workspaceA := t.TempDir()
	workspaceB := t.TempDir()
	sharedWorkspace := t.TempDir()
	providers := map[string]*workspaceToolProvider{
		"runtime-a": {calls: []core.ToolCall{
			workspaceToolCall("write-a", "workspace_write", `{"fileName":"same.txt","content":"from-a"}`),
			workspaceToolCall("read-a", "workspace_read", `{"fileName":"same.txt"}`),
		}},
		"runtime-b": {calls: []core.ToolCall{
			workspaceToolCall("write-b", "workspace_write", `{"fileName":"same.txt","content":"from-b"}`),
			workspaceToolCall("read-b", "workspace_read", `{"fileName":"same.txt"}`),
		}},
		"runtime-shared-writer": {calls: []core.ToolCall{
			workspaceToolCall("write-shared", "workspace_write", `{"fileName":"same.txt","content":"shared-session-value"}`),
		}},
		"runtime-shared-reader": {calls: []core.ToolCall{
			workspaceToolCall("read-shared", "workspace_read", `{"fileName":"same.txt"}`),
		}},
	}
	base := newPR2DualControl(t, map[string]core.Provider{
		"runtime-a":             providers["runtime-a"],
		"runtime-b":             providers["runtime-b"],
		"runtime-shared-writer": providers["runtime-shared-writer"],
		"runtime-shared-reader": providers["runtime-shared-reader"],
	}, "runtime-a", "runtime-b")
	source := &workspaceSessionSource{
		pr2DualControl: base,
		specs: []workspaceCreateSpec{
			{runtimeID: "runtime-a", provider: providers["runtime-a"]},
			{runtimeID: "runtime-b", provider: providers["runtime-b"]},
			{runtimeID: "runtime-shared-writer", provider: providers["runtime-shared-writer"]},
			{runtimeID: "runtime-shared-reader", provider: providers["runtime-shared-reader"]},
		},
	}

	evaluate := func(ctx *model.Context, input string) error {
		binding := agentcontrol.BindingFromCtx(ctx.Ctx)
		if binding == nil || binding.Get() == "" {
			return errors.New("ACP prompt has no runtime session binding")
		}
		handle, err := base.control.StartRunSync(binding.Get(), runtimeagent.RunRequest{
			Input:      input,
			ToolSource: ctx.FrontendContext.ToolSource,
		}, runtimeagent.RunCallbacks{
			Write:     ctx.Writer,
			EmitToken: ctx.FrontendContext.EmitToken,
		})
		if err != nil {
			return fmt.Errorf("start workspace run: %w", err)
		}
		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := handle.Wait(waitCtx)
		if err != nil {
			return fmt.Errorf("wait workspace run: %w", err)
		}
		return result.Err
	}

	client, _ := newPR2Stack(t, evaluate, source, NewSessionGate())
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
		return json.RawMessage(workspaceDeviceToolsPayload), nil
	})

	var workspaceMu sync.Mutex
	workspaceByRuntime := make(map[string]string)
	var observationMu sync.Mutex
	observations := make([]workspaceObservation, 0, 8)
	client.Handle(acpworker.MethodDeviceCall, func(_ context.Context, params json.RawMessage) (any, error) {
		var call workspaceDeviceCall
		if err := json.Unmarshal(params, &call); err != nil {
			return nil, fmt.Errorf("decode x-device/call: %w", err)
		}
		if call.RunID == "" {
			return nil, errors.New("x-device/call is missing top-level run_id")
		}
		workspaceMu.Lock()
		workspace := workspaceByRuntime[call.SessionID]
		workspaceMu.Unlock()
		if workspace == "" {
			return nil, fmt.Errorf("no client workspace mapped for runtime %q", call.SessionID)
		}
		var args workspaceToolArguments
		if err := json.Unmarshal(call.Arguments, &args); err != nil {
			return nil, fmt.Errorf("decode workspace tool arguments: %w", err)
		}
		if args.FileName == "" || filepath.Base(args.FileName) != args.FileName {
			return nil, fmt.Errorf("workspace tool must use a simple fileName, got %q", args.FileName)
		}
		path := filepath.Join(workspace, args.FileName)
		var result string
		switch call.Name {
		case "workspace_write":
			if err := os.WriteFile(path, []byte(args.Content), 0o600); err != nil {
				return nil, fmt.Errorf("write workspace file for %q: %w", call.SessionID, err)
			}
			result = "written"
		case "workspace_read":
			contents, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("read workspace file for %q: %w", call.SessionID, err)
			}
			result = string(contents)
		default:
			return nil, fmt.Errorf("unexpected workspace tool %q", call.Name)
		}
		observationMu.Lock()
		observations = append(observations, workspaceObservation{
			SessionID: call.SessionID,
			RunID:     call.RunID,
			Name:      call.Name,
			FileName:  args.FileName,
			Result:    result,
		})
		observationMu.Unlock()
		response, err := json.Marshal(map[string]string{"result": result})
		if err != nil {
			return nil, fmt.Errorf("encode workspace tool result: %w", err)
		}
		return json.RawMessage(response), nil
	})

	newSession := func(cwd string) string {
		t.Helper()
		var response protocol.NewSessionResponse
		if err := client.Call(context.Background(), protocol.MethodSessionNew, protocol.NewSessionRequest{Cwd: cwd}, &response); err != nil {
			t.Fatalf("session/new(%s): %v", cwd, err)
		}
		sessionID := response.SessionID
		if sessionID == "" {
			t.Fatalf("session/new(%s) returned an empty canonical session ID", cwd)
		}
		if runtime, err := base.m.Get(sessionID).DeclaredCWD(); err != nil || runtime != cwd {
			t.Fatalf("session %q declared cwd = %q, %v; want %q", sessionID, runtime, err, cwd)
		}
		workspaceMu.Lock()
		workspaceByRuntime[sessionID] = cwd
		workspaceMu.Unlock()
		return sessionID
	}

	sessionA := newSession(workspaceA)
	sessionB := newSession(workspaceB)
	if sessionA == sessionB {
		t.Fatalf("different workspace sessions received the same session ID %q", sessionA)
	}
	promptResults := make(chan error, 2)
	for _, sessionID := range []string{sessionA, sessionB} {
		sessionID := sessionID
		go func() {
			_, err := client.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("write then read same.txt")})
			promptResults <- err
		}()
	}
	for range 2 {
		select {
		case err := <-promptResults:
			if err != nil {
				t.Fatalf("isolated workspace prompt: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("isolated workspace prompts did not finish")
		}
	}
	assertFileContents := func(workspace, want string) {
		t.Helper()
		contents, err := os.ReadFile(filepath.Join(workspace, "same.txt"))
		if err != nil {
			t.Fatalf("read %s/same.txt: %v", workspace, err)
		}
		if got := string(contents); got != want {
			t.Fatalf("%s/same.txt = %q, want %q", workspace, got, want)
		}
	}
	assertFileContents(workspaceA, "from-a")
	assertFileContents(workspaceB, "from-b")

	sessionSharedWriter := newSession(sharedWorkspace)
	sessionSharedReader := newSession(sharedWorkspace)
	if sessionSharedWriter == sessionSharedReader {
		t.Fatalf("two sessions in one workspace received the same session ID %q", sessionSharedWriter)
	}
	if runtime, err := base.m.Get(sessionSharedWriter).DeclaredCWD(); err != nil || runtime != sharedWorkspace {
		t.Fatalf("shared writer session cwd = %q, %v; want %q", runtime, err, sharedWorkspace)
	}
	if runtime, err := base.m.Get(sessionSharedReader).DeclaredCWD(); err != nil || runtime != sharedWorkspace {
		t.Fatalf("shared reader session cwd = %q, %v; want %q", runtime, err, sharedWorkspace)
	}
	if _, err := client.Prompt(context.Background(), sessionSharedWriter, []protocol.ContentBlock{protocol.TextBlock("write same.txt")}); err != nil {
		t.Fatalf("shared workspace writer prompt: %v", err)
	}
	if _, err := client.Prompt(context.Background(), sessionSharedReader, []protocol.ContentBlock{protocol.TextBlock("read same.txt")}); err != nil {
		t.Fatalf("shared workspace reader prompt: %v", err)
	}
	assertFileContents(sharedWorkspace, "shared-session-value")

	observationMu.Lock()
	defer observationMu.Unlock()
	if len(observations) != 6 {
		t.Fatalf("observed %d device calls, want 6 across isolated and shared workspaces", len(observations))
	}
	readResults := make(map[string]string)
	for _, observation := range observations {
		if observation.RunID == "" || observation.FileName != "same.txt" {
			t.Fatalf("unexpected workspace tool observation: %+v", observation)
		}
		if observation.Name == "workspace_read" {
			readResults[observation.SessionID] = observation.Result
		}
	}
	if readResults[sessionA] != "from-a" || readResults[sessionB] != "from-b" {
		t.Fatalf("isolated workspace reads = A:%q B:%q, want from-a/from-b", readResults[sessionA], readResults[sessionB])
	}
	if readResults[sessionSharedReader] != "shared-session-value" {
		t.Fatalf("session in shared workspace read %q, want shared-session-value", readResults[sessionSharedReader])
	}
	if readResults[sessionSharedWriter] != "" {
		t.Fatalf("writer-only shared session unexpectedly read %q", readResults[sessionSharedWriter])
	}
}
