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

// P1-3 接线测试：plugin 的持久化从全局单 Store 换轨 Repository（每会话独立目录
// + legacy 根槽兼容挂载）。本文件锁死 P1-3 验收要求的两条语义：
//   - /new 在 legacy 槽上归档后，新会话经 Repository.Create 产出独立目录
//     （<root>/<新id>/current.jsonl），与旧会话数据互不串扰；
//   - ReloadCurrentSession / CurrentSessionHistory 的取数随 current 换轨，
//     /new 前后各自返回所属会话的数据。
//
// 走真实装配路径：NewPlugin + startSession（首周期 legacy 槽挂载）+ handleNew
// （归档 + Create 换轨），除持久化外全部资源关闭（memory/MCP），零外部依赖。

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

// TestNewCreatesIndependentStore：临时 Session.Dir 造 legacy 槽 → /new（经 handler）
// → ① 新会话目录 <root>/<新id>/current.jsonl 存在且 id ≠ 旧 id；② 旧会话归档文件
// 仍在（legacy 槽内轮转）；③ 新 store ActiveView 为空（旧历史不串入）。
func TestNewCreatesIndependentStore(t *testing.T) {
	root := t.TempDir()
	oldID := seedLegacySession(t, root, "旧会话的问题")

	cfg := &runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: root}}
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	p.startSession() // 首周期：root 下有 legacy 槽 → 原地挂载（维持旧版启动行为）

	legacy := p.currentStore()
	if legacy == nil {
		t.Fatal("no current store after startSession (legacy mount failed)")
	}
	if got := legacy.SessionID(); got != oldID {
		t.Fatalf("current store id = %q, want mounted legacy id %q", got, oldID)
	}
	t.Cleanup(func() { _ = p.Stop() })

	ctx, buf := newContext()
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}
	if !strings.Contains(buf.String(), "New session started") {
		t.Errorf("handleNew output = %q, want success message", buf.String())
	}

	// ① 新会话独立目录：Create 产出 <root>/<新id>/current.jsonl，id 与旧会话不同
	fresh := p.currentStore()
	if fresh == nil {
		t.Fatal("no current store after /new")
	}
	if fresh == legacy {
		t.Fatal("current store still the pre-/new legacy instance")
	}
	newID := fresh.SessionID()
	if newID == oldID {
		t.Fatalf("new session id %q equals legacy id", newID)
	}
	if _, err := os.Stat(filepath.Join(root, newID, "current.jsonl")); err != nil {
		t.Fatalf("new session dir %s missing: %v", filepath.Join(root, newID), err)
	}

	// ② 旧会话归档仍在：legacy 槽内轮转（root/archive/<ts>.jsonl，metadata 旧 id）
	entries, err := os.ReadDir(filepath.Join(root, "archive"))
	if err != nil {
		t.Fatalf("read legacy archive dir: %v", err)
	}
	archivedOld := false
	for _, entry := range entries {
		view, viewErr := session.OpenArchiveView(filepath.Join(root, "archive", entry.Name()))
		if viewErr == nil && view.SessionID() == oldID {
			archivedOld = true
		}
	}
	if !archivedOld {
		t.Errorf("no archive under %s carries the old session id %q", filepath.Join(root, "archive"), oldID)
	}

	// ③ 新 store ActiveView 为空：旧历史不串入新会话
	if view := fresh.ActiveView(); len(view) != 0 {
		t.Errorf("new store active view = %d messages, want empty (no legacy bleed-through)", len(view))
	}
}

// TestCurrentStoreTracksSession：ReloadCurrentSession / CurrentSessionHistory 在
// /new 前后返回各自会话的数据——取数随 current 换轨，新会话写入只落新 store。
func TestCurrentStoreTracksSession(t *testing.T) {
	root := t.TempDir()
	oldID := seedLegacySession(t, root, "旧会话的问题")

	cfg := &runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: root}}
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	p.startSession()
	t.Cleanup(func() { _ = p.Stop() })

	// /new 前：取数落在 legacy 会话上（构造即水合，ReloadCurrentSession 只做视图刷新）
	if err := p.ReloadCurrentSession(); err != nil {
		t.Fatalf("ReloadCurrentSession before /new: %v", err)
	}
	if got := p.CurrentSessionID(); got != oldID {
		t.Fatalf("CurrentSessionID before /new = %q, want %q", got, oldID)
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
	if got := p.CurrentSessionID(); got == "" || got == oldID {
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
