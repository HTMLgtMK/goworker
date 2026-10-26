package service

import (
	"context"
	"strings"
	"testing"
	"time"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

// P0-4 接线测试：plugin 的会话持有从「裸 Session 指针」升级为「manager 注册的
// SessionRuntime」，current 为唯一解析点。本文件锁死 P0-4 验收要求的两条语义：
//   - /new（经 startSession）后 current 指向新 runtime，manager 内旧 runtime 仍可
//     Get（归档固化窗口内 CheckpointSync 仍持有旧会话）；
//   - current 切换后 withSession 的互斥落在新 runtime 上（解析与互斥同源）。

// TestCurrentSessionSwapsOnNew 模拟 /new：current 换到新 runtime，旧 runtime 保留
// 在 manager 里。走真实 startSession 装配首会话（memory/持久化/MCP 全关，零外部
// 依赖）——这是唯一一次注册；随后真实 handleNew → startSession 重建，会话切换
// 后的 current/manager 状态即生产接线形态。
func TestCurrentSessionSwapsOnNew(t *testing.T) {
	p := NewPlugin(&runtimeconfig.Config{}, runtimeconfig.Paths{})
	p.startSession()
	old := p.currentSession()
	if old == nil {
		t.Fatal("no current runtime after startSession")
	}
	oldSession := old.Session()
	if oldSession == nil {
		t.Fatal("current runtime has no bound session after startSession")
	}
	if got := p.manager.Get(old.ID); got != old {
		t.Fatalf("manager.Get(%q) = %v, want the initially registered runtime", old.ID, got)
	}

	ctx, buf := newContext()
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}
	if !strings.Contains(buf.String(), "New session started") {
		t.Errorf("handleNew output = %q, want success message", buf.String())
	}

	fresh := p.currentSession()
	if fresh == nil {
		t.Fatal("no current runtime after /new")
	}
	if fresh == old {
		t.Fatal("current still points at the pre-/new runtime")
	}
	if fresh.Session() == nil || fresh.Session() == oldSession {
		t.Fatal("fresh runtime did not bind a new session object")
	}

	// manager 内旧 runtime 仍可 Get：同 id "current" 注册冲突时旧条目保留在册，
	// 归档固化窗口（old.CheckpointSync）期间旧会话可经 manager 触达。
	if got := p.manager.Get(old.ID); got != old {
		t.Fatalf("manager.Get(%q) = %v, want the pre-/new runtime still registered", old.ID, got)
	}
}

// TestGateFollowsCurrent 验证「解析与互斥同源」：current 切换后，withSession 的
// 互斥落在新 runtime 的 gate 上——旧 gate 被占住不再阻塞命令，新 gate 被占住
// 则命令排队等待。
func TestGateFollowsCurrent(t *testing.T) {
	p := newGatedPlugin()
	old := p.currentSession()
	if old == nil {
		t.Fatal("no current runtime after construction")
	}

	// 模拟 /new 的换轨：current 切到新 runtime（id 取不同值以便 Register 成功，
	// gate 跟随 current 的语义与 id 无关）
	fresh := runtimeagent.NewSessionRuntime("current-2", nil)
	if err := p.manager.Register(fresh); err != nil {
		t.Fatalf("register fresh runtime: %v", err)
	}
	p.current.Store(fresh)

	// 1) 旧 runtime 的 gate 被占住：withSession 不受它阻塞（互斥已随 current 迁移）
	releaseOld, err := old.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("hold old gate: %v", err)
	}
	defer releaseOld()

	first := make(chan error, 1)
	go func() {
		first <- p.withSession(context.Background(), func(context.Context) error { return nil })
	}()
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("withSession should not serialize on the superseded runtime: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("withSession blocked on the old runtime's gate")
	}

	// 2) 新 runtime 的 gate 被占住：withSession 排队等待，release 后进入
	releaseFresh, err := fresh.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("hold fresh gate: %v", err)
	}
	second := make(chan error, 1)
	go func() {
		second <- p.withSession(context.Background(), func(context.Context) error { return nil })
	}()
	select {
	case err := <-second:
		t.Fatalf("withSession entered while fresh gate held: %v", err)
	case <-time.After(50 * time.Millisecond):
		// 预期：仍阻塞在新 runtime 的 gate 上
	}
	releaseFresh()
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("withSession after fresh release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("withSession did not enter after fresh gate release")
	}
}
