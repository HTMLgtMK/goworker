package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// repo_wiring_test.go 验证 repository 持久化接线：/new 创建独立会话，旧会话数据
// 保留并可重开，current 换轨后历史互不串扰。覆盖真实插件装配路径，不依赖外部资源。

// seedLegacySession 在 root 播种带一条 user 消息的 legacy 根槽：一次性 Open（生成
// metadata）→ Commit → Close 释放文件锁，供 startSession 的 repo.Open 原地挂载。
// 返回槽内会话 id（文件 metadata 的稳定逻辑 id）。
func seedLegacySession(t *testing.T, root, userText string) string {
	t.Helper()
	seed, err := session.Open(root)
	if err != nil {
		t.Fatalf("seed legacy slot: %v", err)
	}
	if _, err := seed.Commit([]core.Message{{Role: "user", Content: userText, MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	id := seed.SessionID()
	if err := seed.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}
	return id
}

// TestNewCreatesIndependentStore：临时 Session.Dir 造 legacy 槽 → startSession
// 首周期迁移到 per-id，current 是 Create 的新会话 → /new 后断言：① 新会话目录
// <root>/<新id>/<新id>.jsonl 存在且 id ≠ 旧 id；② 旧数据原位保留不轮转，因
// 引用归零被生命周期收口（runtime 注销、repo 缓存逐出，重开走 flock 慢路径）；
// ③ 新 store 的 ActiveView 为空，旧历史不串入。
func TestNewCreatesIndependentStore(t *testing.T) {
	root := t.TempDir()
	oldID := seedLegacySession(t, root, "旧会话的问题")

	cfg := &runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: root}}
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	// 首周期（Plan A4）：legacy 槽迁移入 per-id（数据不丢、可 load），current 换成
	// Create 的新会话——「迁移数据保留 + 新会话独立」两个意图同一夹具覆盖。
	p.startSession()

	migrated, err := os.Stat(filepath.Join(root, oldID, oldID+".jsonl"))
	if err != nil || migrated.IsDir() {
		t.Fatalf("legacy slot not migrated to per-id layout: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "current.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("legacy slot still at root after migration (err=%v), want moved", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	ctx, buf := newContext()
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}
	if !strings.Contains(buf.String(), "New session started") {
		t.Errorf("handleNew output = %q, want success message", buf.String())
	}

	// ① 新会话独立目录：Create 产出 <root>/<新id>/<新id>.jsonl（Plan A1 v2 文件名，
	// 目录 basename 是合法 session id），id 与旧会话不同
	fresh := p.currentStore()
	if fresh == nil {
		t.Fatal("no current store after /new")
	}
	newID := fresh.SessionID()
	if newID == oldID {
		t.Fatalf("new session id %q equals migrated legacy id", newID)
	}
	if _, err := os.Stat(filepath.Join(root, newID, newID+".jsonl")); err != nil {
		t.Fatalf("new session dir %s missing: %v", filepath.Join(root, newID), err)
	}

	// ② 旧会话不轮转（定案需求 #2/#9：/new 纯新建，归档是显式命令）：迁移后的
	// 数据原样保留在 per-id 目录；同时单连接引用归零 → 生命周期收口（runtime
	// 注销 + store 关闭并逐出 repo 缓存），重开经 flock 慢路径拿到活实例、历史
	// 可读（A2.1①）。
	if entries, err := os.ReadDir(filepath.Join(root, "archive")); err == nil && len(entries) > 0 {
		t.Errorf("/new must not rotate sessions (Plan A2): archive entries = %d", len(entries))
	}
	if p.manager.Get(oldID) != nil {
		t.Error("old session runtime must be released after /new (引用归零即收口)")
	}
	reopened, err := p.repo.Open(oldID)
	if err != nil {
		t.Fatalf("repo.Open(migrated legacy id) after /new teardown: %v", err)
	}
	view := reopened.ActiveView()
	if len(view) != 1 || view[0].Content != "旧会话的问题" {
		t.Fatalf("reopened legacy view = %d msgs, want the seeded history (迁移数据不丢)", len(view))
	}

	// ③ 新 store ActiveView 为空：旧历史不串入新会话
	if view := fresh.ActiveView(); len(view) != 0 {
		t.Errorf("new store active view = %d messages, want empty (no legacy bleed-through)", len(view))
	}
}

// TestCurrentStoreTracksSession：ReloadCurrentSession / CurrentSessionHistory 在
// /new 前后返回各自会话的数据——取数随 current 换轨，新会话写入只落新 store。
// Plan A4 起首周期不再挂载 legacy 槽（迁移退役该行为），首会话是 Create 的新
// 会话：夹具改为对 current store 直接造数（迁移本身由 migrate_test 与
// TestNewCreatesIndependentStore 覆盖）。
func TestCurrentStoreTracksSession(t *testing.T) {
	root := t.TempDir()

	cfg := &runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: root}}
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	p.startSession()
	t.Cleanup(func() { _ = p.Stop() })

	// /new 前：current 是首周期 Create 的会话，对它造数（构造即水合，
	// ReloadCurrentSession 只做视图刷新）
	if err := p.ReloadCurrentSession(); err != nil {
		t.Fatalf("ReloadCurrentSession before /new: %v", err)
	}
	firstID := p.CurrentSessionID()
	if firstID == "" {
		t.Fatal("CurrentSessionID before /new = empty, want the first Create session")
	}
	if _, err := p.currentStore().Commit([]core.Message{{Role: "user", Content: "旧会话的问题", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit into first store: %v", err)
	}
	before := p.CurrentSessionHistory()
	if len(before) != 1 || before[0].Content == nil || before[0].Content.Text != "旧会话的问题" {
		t.Fatalf("history before /new = %+v, want single user chunk 旧会话的问题", before)
	}

	ctx, _ := newContext()
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}

	// /new 后：current 换轨到独立目录新会话，取数随之切换（旧数据不回流）
	if got := p.CurrentSessionID(); got == "" || got == firstID {
		t.Fatalf("CurrentSessionID after /new = %q, want the fresh session id", got)
	}
	if after := p.CurrentSessionHistory(); len(after) != 0 {
		t.Fatalf("history after /new = %+v, want empty (fresh session)", after)
	}
	if err := p.ReloadCurrentSession(); err != nil {
		t.Fatalf("ReloadCurrentSession after /new: %v", err)
	}

	// 新会话写入只落新 store：再次取数读到新数据、且不含旧会话内容
	fresh := p.currentStore()
	if fresh == nil {
		t.Fatal("no current store after /new")
	}
	if _, err := fresh.Commit([]core.Message{{Role: "user", Content: "新会话的问题", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit into fresh store: %v", err)
	}
	after := p.CurrentSessionHistory()
	if len(after) != 1 || after[0].Content == nil || after[0].Content.Text != "新会话的问题" {
		t.Fatalf("history after fresh commit = %+v, want single user chunk 新会话的问题", after)
	}
}
