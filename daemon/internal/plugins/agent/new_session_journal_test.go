package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

func TestNewSessionRunStatusUsesNewJournal(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = t.TempDir()
	cfg.Session.Enabled = true
	cfg.Memory.Enabled = false
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(initHub(cfg)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })
	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) {
		return fixedProvider{content: "ok"}, nil
	}
	oldID := p.currentSession().ID
	newCtx, _ := newContext("")
	if err := p.handleNew(newCtx); err != nil {
		t.Fatalf("/new: %v", err)
	}
	newID := p.currentSession().ID
	if newID == oldID {
		t.Fatal("/new did not select a new session")
	}
	prompt, _ := newContext("hello")
	if err := p.handleAgent(prompt); err != nil {
		t.Fatalf("run: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(cfg.Session.Dir, newID, "runs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("new session run files = %v, %v", entries, err)
	}
	runID := strings.TrimSuffix(entries[0].Name(), ".jsonl")
	state, _, _, err := p.RunStatus(newID, runID)
	if err != nil || state != "completed" {
		t.Fatalf("RunStatus(%s, %s) = %q, %v; want completed", newID, runID, state, err)
	}
}
