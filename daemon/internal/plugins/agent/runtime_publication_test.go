package service

import (
	"context"
	"testing"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
)

func TestBuildRuntimeDoesNotPublishBeforeWorkspaceInitialization(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = t.TempDir()
	cfg.Session.Enabled = true
	cfg.Memory.Enabled = false
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(initHub(cfg)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Stop() })
	st, err := p.repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := p.buildRuntime(st)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.manager.Get(rt.ID); got != nil {
		t.Fatalf("runtime %s published before workspace initialization", rt.ID)
	}
	p.declareSessionCWD(rt, t.TempDir())
	if err := p.manager.Register(rt); err != nil {
		t.Fatal(err)
	}
	if p.manager.Get(rt.ID) != rt {
		t.Fatal("initialized runtime was not published")
	}
}
