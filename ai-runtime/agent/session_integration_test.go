package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
)

// testSessionWithStore 构造一个带 store 的 Session，用于集成测试。
func testSessionWithStore(t *testing.T, pv core.Provider) (*Session, *session.Store, *runtimeconfig.Config, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = true
	cfg.Session.Dir = dir

	st, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	s := NewSession(SessionDeps{
		Config:       cfg,
		AuditDir:     "",
		Memory:       nil,
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store: st,
	})
	return s, st, cfg, dir
}

func TestSessionIntegration_RunPersistsToFile(t *testing.T) {
	s, _, _, dir := testSessionWithStore(t, &captureProvider{})

	var buf strings.Builder
	if err := s.Run(context.Background(), RunRequest{Input: "hello"}, testRunCallbacks(&buf)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 断言 current.jsonl 出现消息行
	data, err := os.ReadFile(filepath.Join(dir, "current.jsonl"))
	if err != nil {
		t.Fatalf("read current.jsonl: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, `"kind":"msg"`) {
		t.Errorf("current.jsonl should contain msg records, got:\n%s", content)
	}
	if !strings.Contains(content, `"role":"user"`) {
		t.Errorf("current.jsonl should contain user message, got:\n%s", content)
	}
	if !strings.Contains(content, `"role":"assistant"`) {
		t.Errorf("current.jsonl should contain assistant message, got:\n%s", content)
	}
}

func TestSessionIntegration_CommitIdempotent(t *testing.T) {
	dir := t.TempDir()
	st, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session.Open: %v", err)
	}
	defer st.Close()

	// 预盖章消息
	m1 := core.Message{Role: "user", Content: "hi", MsgID: "m_test01"}
	m2 := core.Message{Role: "assistant", Content: "hey", MsgID: "m_test02"}

	// 第一次提交
	committed, err := st.Commit([]core.Message{m1, m2})
	if err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if len(committed) != 2 {
		t.Fatalf("first commit returned %d msgs, want 2", len(committed))
	}

	// 同一批消息再次提交 —— 幂等，不重复
	committed2, err := st.Commit([]core.Message{m1, m2})
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	if len(committed2) != 0 {
		t.Errorf("second commit should return 0 (all dupes), got %d", len(committed2))
	}

	// 文件内容不应重复
	data, _ := os.ReadFile(filepath.Join(dir, "current.jsonl"))
	content := string(data)
	count := strings.Count(content, `"id":"m_test01"`)
	if count > 2 { // msg 行 + head 行各一次
		t.Errorf("duplicate message in file (count=%d), want <=2", count)
	}
}

func TestSessionIntegration_RestartRecovery(t *testing.T) {
	dir := t.TempDir()

	// 第一次打开并提交
	st1, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session.Open: %v", err)
	}
	m1 := core.Message{Role: "user", Content: "remember me", MsgID: "m_rec01"}
	m2 := core.Message{Role: "assistant", Content: "ok", MsgID: "m_rec02"}
	_, err = st1.Commit([]core.Message{m1, m2})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	st1.Close()

	// 重新打开（模拟重启），ActiveView 应返回上次消息
	st2, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session.Open (reopen): %v", err)
	}
	defer st2.Close()

	view := st2.ActiveView()
	if len(view) != 2 {
		t.Fatalf("ActiveView len=%d, want 2", len(view))
	}
	if view[0].Content != "remember me" {
		t.Errorf("view[0].Content = %q, want 'remember me'", view[0].Content)
	}
	if view[1].Content != "ok" {
		t.Errorf("view[1].Content = %q, want 'ok'", view[1].Content)
	}
}

func TestSessionIntegration_StoreDisabledFallback(t *testing.T) {
	// deps.Store=nil 时 Run 行为与旧版一致（纯内存）
	s, _ := testSession(&captureProvider{})

	var buf strings.Builder
	if err := s.Run(context.Background(), RunRequest{Input: "hello"}, testRunCallbacks(&buf)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(s.conversation) == 0 {
		t.Fatal("conversation should not be empty (store disabled fallback)")
	}
	// 验证 conversation 内没有 compact 摘要混入
	for _, m := range s.conversation {
		if m.Role == "system" {
			t.Errorf("unexpected system message in conversation (store disabled): %+v", m)
		}
	}
}

func TestSessionIntegration_StoreDisabledCompact(t *testing.T) {
	// deps.Store=nil 时 /compact 回退旧逻辑
	s, _ := testSession(&captureProvider{})

	// 先注入一些 history 然后跑 compact
	s.conversation = []core.Message{
		{Role: "user", Content: "long chat 1"},
		{Role: "assistant", Content: "long chat 2"},
		{Role: "user", Content: "long chat 3"},
		{Role: "assistant", Content: "long chat 4"},
	}
	var buf strings.Builder
	if err := s.Compact(context.Background(), testRunCallbacks(&buf)); err != nil {
		t.Fatalf("Compact (store disabled): %v", err)
	}
	// 纯内存模式下 compact 无 provider 会走 compress 失败，但应回退
	// 这里只验证不 panic
}

func TestSessionIntegration_NewSessionRestoresFromStore(t *testing.T) {
	dir := t.TempDir()
	st, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session.Open: %v", err)
	}
	defer st.Close()

	// 写入一些消息
	m1 := core.Message{Role: "user", Content: "previous", MsgID: "m_ns01"}
	m2 := core.Message{Role: "assistant", Content: "response", MsgID: "m_ns02"}
	_, err = st.Commit([]core.Message{m1, m2})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// 构造 NewSession 带 store，应恢复对话
	cfg := runtimeconfig.Default()

	s := NewSession(SessionDeps{
		Config:       cfg,
		AuditDir:     "",
		Memory:       nil,
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return &captureProvider{}, nil
		},
		Store: st,
	})

	conv := s.Conversation()
	if len(conv) != 2 {
		t.Fatalf("NewSession restored %d msgs, want 2", len(conv))
	}
	if conv[0].Content != "previous" {
		t.Errorf("first msg = %q, want 'previous'", conv[0].Content)
	}
	if conv[1].Content != "response" {
		t.Errorf("second msg = %q, want 'response'", conv[1].Content)
	}
}
