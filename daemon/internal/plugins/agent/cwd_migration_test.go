package service

import (
	"os"
	"path/filepath"
	"testing"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-sandbox"
)

// 本文件是 P1-4 状态绑定收口的 daemon 侧验收：cwd 声明随会话边界走 runtime 级
// 迁移——旧 current runtime 的有效声明在 /new（startSession 重建）后迁入新
// runtime 并应用到新会话；无效声明作废，不毒化新会话的 prompt 门。
// SDK 层的 CWD/Usage 会话隔离（含 Session().cwd() 断言）由 ai-runtime/agent 的
// TestSessionStateIsolation 覆盖；本文件只锁 daemon 的迁移接线（Session.cwd 为
// SDK 内部态，daemon 侧以 runtime 声明与插件读面为断言对象）。

// TestCWDDeclarationSurvivesNew：legacy 槽 runtime 上有效声明 → /new → 新 runtime
// 继承声明且插件读面（currentSessionCwd）不变；无效声明 → /new → 作废（新
// runtime 零值起步，cwdErr 复位）。
func TestCWDDeclarationSurvivesNew(t *testing.T) {
	boundary := t.TempDir()
	inside := filepath.Join(boundary, "project")
	if err := os.MkdirAll(inside, 0o700); err != nil {
		t.Fatalf("mkdir inside: %v", err)
	}
	outside := t.TempDir()

	dir := t.TempDir()
	cfg := &runtimeconfig.Config{
		Session: runtimeconfig.SessionConfig{Enabled: true, Dir: dir},
		Sandbox: sandbox.SandboxConfig{AllowedWorkDir: boundary},
	}
	mountID := seedLegacySlot(t, dir, nil)
	p, _ := wireLegacyStore(t, cfg, mountID)

	// legacy 槽 runtime 上的有效声明
	p.SetSession("t1", inside)
	old := p.currentSession()
	if old == nil {
		t.Fatal("no current runtime after wiring")
	}
	if cwd, err := old.DeclaredCWD(); err != nil || cwd != inside {
		t.Fatalf("legacy runtime DeclaredCWD = (%q, %v), want (%q, nil)", cwd, err, inside)
	}

	// /new → startSession：有效声明迁移到新 runtime，清单读面回显不变
	ctx, _ := newContext()
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}
	fresh := p.currentSession()
	if fresh == nil || fresh == old {
		t.Fatal("current did not swap to a new runtime after /new")
	}
	if cwd, err := fresh.DeclaredCWD(); err != nil || cwd != inside {
		t.Errorf("fresh runtime DeclaredCWD = (%q, %v), want inherited (%q, nil)", cwd, err, inside)
	}
	if got := p.currentSessionCwd(); got != inside {
		t.Errorf("currentSessionCwd after /new = %q, want %q (有效声明跨会话保留)", got, inside)
	}
	if err := p.sessionCWDError(); err != nil {
		t.Errorf("sessionCWDError after /new = %v, want nil", err)
	}

	// 无效声明 → /new：作废。新 runtime 零值起步（cwd 空、cwdErr nil），prompt
	// 门复位，新会话无 cwd
	p.SetSession("t2", outside)
	if err := p.sessionCWDError(); err == nil {
		t.Fatal("invalid declaration not recorded on current runtime")
	}
	old = p.currentSession()
	ctx, _ = newContext()
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("second handleNew: %v", err)
	}
	fresh = p.currentSession()
	if fresh == nil || fresh == old {
		t.Fatal("current did not swap after second /new")
	}
	if cwd, err := fresh.DeclaredCWD(); cwd != "" || err != nil {
		t.Errorf("fresh runtime DeclaredCWD = (%q, %v), want zero value (无效声明作废)", cwd, err)
	}
	if got := p.currentSessionCwd(); got != "" {
		t.Errorf("currentSessionCwd after invalid /new = %q, want empty", got)
	}
	if err := p.sessionCWDError(); err != nil {
		t.Errorf("sessionCWDError after invalid /new = %v, want nil (无效标记随旧 runtime 作废)", err)
	}
}
