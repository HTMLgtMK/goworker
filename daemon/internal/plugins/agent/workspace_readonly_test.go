package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

func TestMissingWorkspaceAllowsHistoryButRejectsMutations(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "original")
	replacement := filepath.Join(root, "replacement")
	for _, dir := range []string{workspace, replacement} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = true
	cfg.Session.Dir = t.TempDir()
	cfg.Memory.Enabled = false
	cfg.Sandbox.AllowedWorkDir = root
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(initHub(cfg)); err != nil {
		t.Fatal(err)
	}
	id, err := p.CreateBoundSession(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(workspace); err != nil {
		t.Fatal(err)
	}
	p = NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(initHub(cfg)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Stop() })
	if err := p.LoadBoundSession(id, replacement); err != nil {
		t.Fatalf("load unavailable workspace for viewing: %v", err)
	}
	if _, err := p.SessionHistory(id); err != nil {
		t.Fatalf("read history: %v", err)
	}
	if _, err := p.StartRunSync(id, runtimeagent.RunRequest{Input: "hello"}, runtimeagent.RunCallbacks{}); err == nil {
		t.Fatal("run on unavailable workspace must fail")
	}
	p.AcquireControl(id, "client")
	if !p.HoldControl(id, "client") {
		t.Fatal("historical session control was not acquired")
	}
	if _, _, err := p.StartRun(id, "client", "hello", "request-1", model.RunToolOptions{}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("x-goworker run on unavailable workspace = %v, want read-only error", err)
	}
	binding := &agentcontrol.SessionBinding{}
	binding.Set(id)
	if err := p.handleAgent(&model.Context{
		Ctx:  agentcontrol.WithBinding(context.Background(), binding),
		Args: []string{"hello"},
	}); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("ACP prompt execution on unavailable workspace = %v, want unavailable-workspace error", err)
	}
	if _, err := p.GenerateSessionTitle(id, "client"); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("generating title for unavailable workspace = %v, want read-only error", err)
	}
	if err := p.SetSessionTitle(id, "client", "changed"); err == nil {
		t.Fatal("editing an unavailable workspace session must fail")
	}
	if err := p.ArchiveSession(id, "client"); err == nil {
		t.Fatal("archiving an unavailable workspace session must fail")
	}
	if st := p.storeFor(p.RouteSession(id)); st == nil || st.Workspace() != workspace {
		t.Fatalf("workspace after load with replacement cwd = %v, want original declaration %q", st, workspace)
	}
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := p.LoadBoundSession(id, replacement); err != nil {
		t.Fatalf("load restored workspace: %v", err)
	}
	if reason := p.SessionReadOnlyReason(id); reason != nil {
		t.Fatalf("restored workspace still read-only: %v", reason)
	}
	if cwd, err := p.RouteSession(id).DeclaredCWD(); err != nil || cwd != workspace {
		t.Fatalf("restored workspace = %q, %v; want %q", cwd, err, workspace)
	}
}

func TestWorkspaceRemovedAfterLoadFencesPromptAndRunStarts(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = true
	cfg.Session.Dir = t.TempDir()
	cfg.Memory.Enabled = false
	cfg.Sandbox.AllowedWorkDir = root
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(initHub(cfg)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	id, err := p.CreateBoundSession(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.LoadBoundSession(id, workspace); err != nil {
		t.Fatalf("load session: %v", err)
	}
	if err := os.Remove(workspace); err != nil {
		t.Fatal(err)
	}

	if _, err := p.StartRunSync(id, runtimeagent.RunRequest{Input: "hello"}, runtimeagent.RunCallbacks{}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("synchronous start after workspace removal = %v, want read-only error", err)
	}

	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := p.LoadBoundSession(id, workspace); err != nil {
		t.Fatalf("reload workspace before background-start check: %v", err)
	}
	if err := os.Remove(workspace); err != nil {
		t.Fatal(err)
	}
	p.AcquireControl(id, "client")
	if !p.HoldControl(id, "client") {
		t.Fatal("session control was not acquired")
	}
	if _, _, err := p.StartRun(id, "client", "hello", "request-removed-workspace", model.RunToolOptions{}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("background start after workspace removal = %v, want read-only error", err)
	}
	if rt := p.RouteSession(id); rt == nil || rt.Active() != nil {
		t.Fatalf("workspace removal admitted a run: runtime=%v", rt)
	}

	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := p.LoadBoundSession(id, workspace); err != nil {
		t.Fatalf("reload workspace before prompt check: %v", err)
	}
	if err := os.Remove(workspace); err != nil {
		t.Fatal(err)
	}
	binding := &agentcontrol.SessionBinding{}
	binding.Set(id)
	if err := p.handleAgent(&model.Context{
		Ctx:  agentcontrol.WithBinding(context.Background(), binding),
		Args: []string{"hello"},
	}); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("prompt after workspace removal = %v, want unavailable workspace error", err)
	}
}

func TestWorkspaceReadOnlyReason(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := runtimeconfig.Default()
	cfg.Sandbox.AllowedWorkDir = ""
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	for _, tc := range []struct {
		name, workspace string
		wantError       bool
	}{
		{"undeclared", "", false},
		{"valid", root, false},
		{"missing", filepath.Join(root, "missing"), true},
		{"file", file, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := p.workspaceReadOnlyReason(tc.workspace)
			if (err != nil) != tc.wantError {
				t.Fatalf("workspaceReadOnlyReason(%q) = %v, want error %v", tc.workspace, err, tc.wantError)
			}
		})
	}
}
