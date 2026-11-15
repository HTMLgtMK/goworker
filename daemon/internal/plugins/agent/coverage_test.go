package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-memory"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

type deviceToolCaller struct {
	call func(context.Context, string, any, any) error
}

func (c deviceToolCaller) Call(ctx context.Context, method string, params, result any) error {
	return c.call(ctx, method, params, result)
}

func TestNegotiatedClientToolSpecs(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.CapabilityWaitTimeout = 1
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	caller := deviceToolCaller{call: func(_ context.Context, method string, params, result any) error {
		if method != "x-device/call" {
			return errors.New("unexpected device method: " + method)
		}
		request, err := json.Marshal(params)
		if err != nil {
			return err
		}
		var got struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(request, &got); err != nil {
			return err
		}
		if got.Name != "echo" || string(got.Arguments) != `{"text":"hello"}` {
			return errors.New("unexpected device call request")
		}
		return json.Unmarshal([]byte(`{"result":"device reply"}`), result)
	}}

	if p.BindClientToolSpecs("session", "connection", nil, caller) {
		t.Fatal("empty spec snapshot should not bind")
	}
	specs := []runtimeagent.ClientToolSpec{{
		Name:        "sys_echo",
		Description: "Echo text",
		Schema:      `{"type":"object","properties":{"text":{"type":"string"}}}`,
		Metadata:    map[string]string{"risk_level": "low", "requires_permission": "true"},
	}}
	if p.BindClientToolSpecs("session", "connection", specs, nil) {
		t.Fatal("nil caller should not bind")
	}
	if p.BindClientToolSpecs("session", "connection", specs, caller) {
		t.Fatal("first negotiated binding should not report a refresh")
	}
	if _, ok := p.manager.Capabilities().Lookup("session", "sys_echo"); !ok {
		t.Fatal("valid negotiated descriptor was not registered")
	}
	if source := p.ClientToolSourceForSpecs("session", nil); source != nil {
		t.Fatal("empty specs should produce no source")
	}

	source := p.ClientToolSourceForSpecs("session", specs)
	// The source must retain the initialize-time contract, not caller-owned mutable maps.
	specs[0].Name = "changed"
	specs[0].Metadata["risk_level"] = "critical"
	tools := source(context.Background())
	if len(tools) != 1 || tools[0].Name != "sys_echo" {
		t.Fatalf("source tools = %#v, want one frozen sys_echo proxy", tools)
	}
	if tools[0].Metadata["risk_level"] != "low" || tools[0].Metadata["requires_permission"] != "true" {
		t.Fatalf("proxy metadata = %#v, want frozen policy metadata", tools[0].Metadata)
	}
	got, err := tools[0].Execute(context.Background(), map[string]any{"text": "hello"})
	if err != nil {
		t.Fatalf("execute negotiated device tool: %v", err)
	}
	if got != "device reply" {
		t.Fatalf("device tool result = %q, want %q", got, "device reply")
	}
}

func TestProfileToolValidationAndMutations(t *testing.T) {
	root := t.TempDir()
	userPath := filepath.Join(root, "USER.md")
	if err := os.WriteFile(userPath, []byte("Prefers concise answers.\nLikes Go."), 0o600); err != nil {
		t.Fatal(err)
	}
	instructions, err := memory.LoadInstructions(root, t.TempDir(), memory.Cap{UserMaxChars: 100})
	if err != nil {
		t.Fatalf("LoadInstructions: %v", err)
	}
	p := &AgentPlugin{deps: runtimeagent.SessionDeps{Instruction: instructions}}
	tool := p.profileTool()
	if tool.Name != "profile" || tool.Execute == nil {
		t.Fatalf("profile tool = %#v, expected executable profile tool", tool)
	}

	for _, args := range []map[string]any{
		{},
		{"action": "unknown"},
		{"action": "add", "content": "  "},
		{"action": "replace", "content": "new preference", "old_text": "  "},
		{"action": "remove", "old_text": "missing"},
		{"action": "replace", "content": "new preference", "old_text": "missing"},
	} {
		if _, err := tool.Execute(context.Background(), args); err == nil {
			t.Errorf("Execute(%#v) succeeded, want validation/match error", args)
		}
	}

	result, err := tool.Execute(context.Background(), map[string]any{"action": "add", "content": "Uses table-driven tests."})
	if err != nil || !strings.Contains(result, "Saved to disk") {
		t.Fatalf("add profile entry = (%q, %v), want persisted success", result, err)
	}
	result, err = tool.Execute(context.Background(), map[string]any{
		"action": "replace", "old_text": "Likes Go", "content": "Prefers Go",
	})
	if err != nil || !strings.Contains(result, "USER.md") {
		t.Fatalf("replace profile entry = (%q, %v), want success and capacity", result, err)
	}
	result, err = tool.Execute(context.Background(), map[string]any{"action": "remove", "old_text": "Uses table-driven"})
	if err != nil || !strings.Contains(result, "removed") {
		t.Fatalf("remove profile entry = (%q, %v), want success", result, err)
	}
	if _, err := os.Stat(userPath); err != nil {
		t.Fatalf("profile file should remain persisted: %v", err)
	}

	withoutInstructions := (&AgentPlugin{}).profileTool()
	if _, err := withoutInstructions.Execute(context.Background(), map[string]any{"action": "add", "content": "x"}); err == nil {
		t.Fatal("profile write without loaded instructions should fail")
	}
}

func TestRulesCommandAndInstructionLoading(t *testing.T) {
	root := t.TempDir()
	project := filepath.Join(root, "project")
	if err := os.Mkdir(project, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "AGENTS.md"), []byte("global rule"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(project, "AGENTS.md"), []byte("project rule"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "USER.md"), []byte("user preference"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldWorkingDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(project); err != nil {
		t.Fatalf("Chdir to project: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldWorkingDir); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})
	p := &AgentPlugin{cfg: runtimeconfig.Default(), paths: runtimeconfig.Paths{ConfigDir: root}}
	instructions := p.loadInstructions()
	if instructions == nil {
		t.Fatal("loadInstructions returned nil for readable instruction files")
	}
	p.deps.Instruction = instructions

	var output strings.Builder
	ctx := &model.Context{FrontendContext: model.FrontendContext{Writer: func(text string) { _, _ = output.WriteString(text) }}}
	if err := p.handleRules(ctx); err != nil {
		t.Fatalf("handleRules with instructions: %v", err)
	}
	for _, want := range []string{"global rule", "project rule", "user preference"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("rules output %q does not contain %q", output.String(), want)
		}
	}

	output.Reset()
	p.deps.Instruction = nil
	if err := p.handleRules(ctx); err != nil {
		t.Fatalf("handleRules with instructions disabled: %v", err)
	}
	if !strings.Contains(output.String(), "instructions disabled") {
		t.Fatalf("disabled rules output = %q", output.String())
	}

	emptyDir := t.TempDir()
	if err := os.Chdir(emptyDir); err != nil {
		t.Fatalf("Chdir to empty directory: %v", err)
	}
	p.paths.ConfigDir = emptyDir
	p.deps.Instruction = p.loadInstructions()
	output.Reset()
	if err := p.handleRules(ctx); err != nil {
		t.Fatalf("handleRules with empty snapshot: %v", err)
	}
	if !strings.Contains(output.String(), "no active instructions") {
		t.Fatalf("empty rules output = %q", output.String())
	}

	p.cfg = nil
	if p.ProviderControl() != nil {
		t.Fatal("ProviderControl before Init should be nil")
	}
	if (*AgentPlugin)(nil).ProviderControl() != nil {
		t.Fatal("ProviderControl on nil receiver should be nil")
	}
	if got := p.Name(); got != "agent" {
		t.Fatalf("Name() = %q, want agent", got)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
}
