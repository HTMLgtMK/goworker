package service

import (
	"os"
	"path/filepath"
	"strings"
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
	// Plan A4（定案 #5）：无效标记作废（cwdErr 复位），cwd 注入本会话的缺省
	// 执行域——「作废」丢弃的是无效标记而非执行域本身，无声明会话的执行域有
	// 确定答案（tmp 每会话目录），不再回退 cfg.AllowedWorkDir/进程 cwd。
	cwd, err := fresh.DeclaredCWD()
	if err != nil {
		t.Fatalf("fresh runtime DeclaredCWD = (_, %v), want nil (无效标记作废)", err)
	}
	if !isTmpWorkspacePath(cwd) || filepath.Base(cwd) != fresh.ID {
		t.Errorf("fresh runtime cwd = %q, want injected default domain for this session (%s)", cwd, fresh.ID)
	}
	if got := p.currentSessionCwd(); got != cwd {
		t.Errorf("currentSessionCwd after invalid /new = %q, want injected %q", got, cwd)
	}
	if err := p.sessionCWDError(); err != nil {
		t.Errorf("sessionCWDError after invalid /new = %v, want nil (无效标记随旧 runtime 作废)", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan A4（定案 #5）：缺省执行域注入 —— 空 cwd 分支的语义反转与沙箱第二边界。
// ─────────────────────────────────────────────────────────────────────────────

// TestDefaultWorkspaceInjection 验证 SetSession 空 cwd 分支的注入：执行域落
// <tmp>/goworker-workspaces/<uid>/<sessionID>/（MkdirAll 0700、沙箱经第二
// allowed root 放行）、清单回显注入域（A7⑤ current 特例读声明）；注入的是
// 执行域不是声明——workspace 记录保持空（记录只落用户显式声明，A7 设计点 5
// 的「清单 cwd / load 恢复持久事实源」不变）；显式声明不受影响且照常落记录。
func TestDefaultWorkspaceInjection(t *testing.T) {
	boundary := t.TempDir()
	inside := filepath.Join(boundary, "project")
	if err := os.MkdirAll(inside, 0o700); err != nil {
		t.Fatalf("mkdir inside: %v", err)
	}
	dir := t.TempDir()
	cfg := &runtimeconfig.Config{
		Session: runtimeconfig.SessionConfig{Enabled: true, Dir: dir},
		Sandbox: sandbox.SandboxConfig{AllowedWorkDir: boundary}, // 主边界不含 tmp 根
	}
	mountID := seedLegacySlot(t, dir, nil)
	p, st := wireLegacyStore(t, cfg, mountID)
	sid := st.SessionID()

	// 空 cwd = 未声明 → 注入缺省执行域（执行域不是声明：不落 workspace 记录）
	p.SetSession("t-inject", "")
	rt := p.currentSession()
	cwd, err := rt.DeclaredCWD()
	if err != nil {
		t.Fatalf("injected DeclaredCWD = (_, %v), want nil (第二 allowed root 放行)", err)
	}
	if !isTmpWorkspacePath(cwd) || filepath.Base(cwd) != sid {
		t.Fatalf("injected cwd = %q, want tmp domain of session %s", cwd, sid)
	}
	fi, err := os.Stat(cwd)
	if err != nil || !fi.IsDir() {
		t.Fatalf("injected exec domain %s = (%v, %v), want an existing dir", cwd, fi, err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Errorf("injected exec domain perm = %o, want 0700", fi.Mode().Perm())
	}
	if got := st.Workspace(); got != "" {
		t.Errorf("workspace record after injection = %q, want empty (执行域不是声明)", got)
	}
	for _, info := range p.ListSessions() {
		if info.SessionID == sid && info.Cwd != cwd {
			t.Errorf("list cwd for current session = %q, want injected %q (A7⑤ current 读声明)", info.Cwd, cwd)
		}
	}

	// 显式声明不受影响：cwd 落用户值、workspace 记录照常写（声明的持久事实源）
	p.SetSession("t-explicit", inside)
	if got, derr := rt.DeclaredCWD(); derr != nil || got != inside {
		t.Errorf("explicit DeclaredCWD = (%q, %v), want (%q, nil)", got, derr, inside)
	}
	if got := st.Workspace(); got != inside {
		t.Errorf("workspace record after explicit declaration = %q, want %q (显式声明落记录)", got, inside)
	}
}

// TestDefaultWorkspaceInjectionOnLoad 验证 declareSessionCWD 空 cwd 分支
// （session/load 链）：从未声明过 cwd 的会话（无 workspace 记录）重开 →
// 注入本会话的缺省执行域，而非回退 cfg.AllowedWorkDir/进程 cwd。
func TestDefaultWorkspaceInjectionOnLoad(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ok"})
	// 旁路 per-id 会话：无 workspace 记录（Create CWD 为空）、不挂 runtime
	id := makeSideSession(t, p, "", "从未声明 cwd 的会话")

	if err := p.LoadBoundSession(id, ""); err != nil {
		t.Fatalf("LoadBoundSession: %v", err)
	}
	rt := p.manager.Get(id)
	if rt == nil {
		t.Fatal("reopened runtime not registered")
	}
	cwd, err := rt.DeclaredCWD()
	if err != nil {
		t.Fatalf("reopened DeclaredCWD = (_, %v), want nil (注入域经第二根放行)", err)
	}
	if !isTmpWorkspacePath(cwd) || filepath.Base(cwd) != id {
		t.Errorf("reopened cwd = %q, want injected default domain of %s", cwd, id)
	}
}

// TestTmpRootSecondAllowedRoot 验证沙箱第二边界（Plan A4）：缺省执行域根与
// cfg.AllowedWorkDir 并列为放行边界——显式声明 tmp 域内目录放行（用户主动
// 在缺省域内工作合法）；两个边界之外仍拒（第二根不放宽越界语义）。
func TestTmpRootSecondAllowedRoot(t *testing.T) {
	boundary := t.TempDir()
	outside := t.TempDir()
	dir := t.TempDir()
	cfg := &runtimeconfig.Config{
		Session: runtimeconfig.SessionConfig{Enabled: true, Dir: dir},
		Sandbox: sandbox.SandboxConfig{AllowedWorkDir: boundary},
	}
	mountID := seedLegacySlot(t, dir, nil)
	p, _ := wireLegacyStore(t, cfg, mountID)

	// 显式声明缺省域内的目录：第二根放行
	underTmp := tmpWorkspaceDir("manually-declared")
	if err := os.MkdirAll(underTmp, 0o700); err != nil {
		t.Fatalf("mkdir under tmp root: %v", err)
	}
	p.SetSession("t-tmp", underTmp)
	if err := p.sessionCWDError(); err != nil {
		t.Errorf("tmp-root declaration rejected: %v, want accepted via the second allowed root", err)
	}

	// 两边界之外仍拒：第二根只放行自己的命名空间
	p.SetSession("t-outside", outside)
	if err := p.sessionCWDError(); err == nil || !strings.Contains(err.Error(), "outside sandbox boundary") {
		t.Errorf("out-of-boundary declaration = %v, want outside sandbox boundary", err)
	}
}
