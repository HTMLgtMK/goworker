package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
)

// 本文件是 P0-3 的 Run 生命周期验收测试（总方案第十一节 → 本步）：
//   - TestConcurrentSessions：manager 注册的两个 runtime 并行执行互不阻塞，
//     各 fake store 只收到自己的提交；
//   - TestSameSessionBusy：同一 runtime 第二个 Run 在 gate 上排队、第一个完成后
//     执行，两轮提交串行、历史无覆盖；
//   - TestCommitFailureNotCompleted：Commit/Compact 失败注入 → RunFailed+Persisted=false，
//     绝不出现 RunCompleted（「模型成功 ≠ 持久化成功」的硬门槛）。
//
// 并行性用「双方 provider 都已开工」的屏障断言代替计时测量：若存在跨 runtime 的
// 全局串行，B 的 provider 永远不会在 A 释放前开工，waitStarted 超时即失败——
// 比耗时近似更确定，也不引入 flaky 时钟。

// 编译期钉死接口契约：*session.Store 天然满足 ConversationStore（plugin 传参零改动）。
var (
	_ ConversationStore = (*fakeConversationStore)(nil)
	_ ConversationStore = (*session.Store)(nil)
)

// fakeConversationStore 是 ConversationStore 的测试桩：view/commits 记内存，
// commitErr/compactErr 注入持久化失败。语义对齐 *session.Store：Commit 追加进
// view，ActiveView 返回快照。
type fakeConversationStore struct {
	mu           sync.Mutex
	view         []core.Message
	commits      [][]core.Message
	compactCalls int
	checkpoints  []string
	commitErr    error
	compactErr   error
}

func (f *fakeConversationStore) ActiveView() []core.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]core.Message(nil), f.view...)
}

func (f *fakeConversationStore) Commit(msgs []core.Message) ([]core.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitErr != nil {
		return nil, f.commitErr
	}
	// Run 每轮提交的是累积视图（含历史轮消息）；*session.Store 按 MsgID 幂等去重，
	// fake 对齐该语义：已入库的 id 跳过，只落新消息。
	seen := make(map[string]bool, len(f.view))
	for _, m := range f.view {
		if m.MsgID != "" {
			seen[m.MsgID] = true
		}
	}
	var fresh []core.Message
	for _, m := range msgs {
		if m.MsgID != "" && seen[m.MsgID] {
			continue
		}
		fresh = append(fresh, m)
		f.view = append(f.view, m)
	}
	f.commits = append(f.commits, append([]core.Message(nil), fresh...))
	return fresh, nil
}

func (f *fakeConversationStore) Compact(from, to, summary string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.compactCalls++
	if f.compactErr != nil {
		return f.compactErr
	}
	return nil
}

func (f *fakeConversationStore) Checkpoint(preview string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checkpoints = append(f.checkpoints, preview)
	return nil
}

func (f *fakeConversationStore) PendingAfterCursor() []core.Message { return nil }

func (f *fakeConversationStore) AdvanceCursor(string) error { return nil }

// gatedProvider 按调用序返回预置 assistant 回复；第 i 次调用可挂在
// starts[i]/release[i] 上（nil = 不挂），用于编排「第 1 轮进行中、第 2 轮排队」
// 与「双会话同时进行中」的场景。ChatStream 返回错误，Session 回退非流式。
type gatedProvider struct {
	replies []string
	starts  []chan struct{}
	release []chan struct{}

	mu    sync.Mutex
	calls int
}

func (p *gatedProvider) Name() string  { return "gated" }
func (p *gatedProvider) Model() string { return "m" }

func (p *gatedProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	p.mu.Lock()
	i := p.calls
	p.calls++
	p.mu.Unlock()
	if i < len(p.starts) && p.starts[i] != nil {
		close(p.starts[i])
	}
	if i < len(p.release) && p.release[i] != nil {
		<-p.release[i]
	}
	content := "final"
	if i < len(p.replies) {
		content = p.replies[i]
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: content}}}}, nil
}

func (p *gatedProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// newFakeStoreSession 构造带 fake store 与固定 provider 的会话（其余依赖同 testSession）。
func newFakeStoreSession(st ConversationStore, cfg *runtimeconfig.Config, pv core.Provider) *Session {
	if cfg == nil {
		cfg = runtimeconfig.Default()
	}
	return NewSession(SessionDeps{
		Config:       cfg,
		AuditDir:     "",
		Memory:       nil,
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store: st,
	})
}

// waitStarted 等待 provider 开工；超时说明会话被别的执行挡住（并行性被破坏）。
func waitStarted(t *testing.T, started chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("provider call did not start in time — sessions are blocking each other")
	}
}

// waitRunResult 等待一轮 RecordRun 收尾并断言成功完成。
func waitRunResult(t *testing.T, done <-chan RunResult) RunResult {
	t.Helper()
	select {
	case res := <-done:
		if res.State != RunCompleted || !res.Persisted {
			t.Fatalf("run result = %+v, want RunCompleted+Persisted=true", res)
		}
		if res.Err != nil {
			t.Fatalf("run result err = %v, want nil", res.Err)
		}
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("run did not finish in time")
		return RunResult{}
	}
}

// assertSingleCommit 断言 fake store 恰好收到一轮提交，且内容为本会话自己的
// user/assistant 消息（互不串话）。
func assertSingleCommit(t *testing.T, st *fakeConversationStore, wantUser, wantAssistant string) {
	t.Helper()
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.commits) != 1 {
		t.Fatalf("commits = %d, want 1", len(st.commits))
	}
	commit := st.commits[0]
	if len(commit) != 2 ||
		commit[0].Role != "user" || commit[0].Content != wantUser ||
		commit[1].Role != "assistant" || commit[1].Content != wantAssistant {
		t.Fatalf("commit = %+v, want [user %q, assistant %q]", commit, wantUser, wantAssistant)
	}
	if len(st.checkpoints) != 1 {
		t.Errorf("checkpoints = %d, want 1 (auto checkpoint after commit)", len(st.checkpoints))
	}
}

// TestConcurrentSessions：manager 注册 A/B 两个 runtime，各自独立
// SessionDeps（fake store + gatedProvider）并行 RecordRun。断言：
// 双方 provider 同时处于运行中（互不阻塞）、各自 store 只收自己的提交、
// 双双 RunCompleted+Persisted=true。
func TestConcurrentSessions(t *testing.T) {
	mgr := NewSessionManager()

	type harness struct {
		rt      *SessionRuntime
		store   *fakeConversationStore
		started chan struct{}
		release chan struct{}
		done    chan RunResult
	}
	newHarness := func(id, reply string) *harness {
		started := make(chan struct{})
		release := make(chan struct{})
		st := &fakeConversationStore{}
		pv := &gatedProvider{
			replies: []string{reply},
			starts:  []chan struct{}{started},
			release: []chan struct{}{release},
		}
		rt := NewSessionRuntime(id, newFakeStoreSession(st, nil, pv))
		if err := mgr.Register(rt); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		return &harness{rt: rt, store: st, started: started, release: release, done: make(chan RunResult, 1)}
	}

	a := newHarness("session-A", "reply-A")
	b := newHarness("session-B", "reply-B")

	runOne := func(h *harness, input string) {
		go func() {
			rel, err := h.rt.AcquireRun(context.Background())
			if err != nil {
				h.done <- RunResult{State: RunFailed, Err: err}
				return
			}
			defer rel()
			h.done <- h.rt.RecordRun(func(s *Session) (RunResult, error) {
				return s.Run(context.Background(), RunRequest{Input: input}, testRunCallbacks(&strings.Builder{}))
			})
		}()
	}
	runOne(a, "q-A")
	runOne(b, "q-B")

	// 关键断言：A、B 的 provider 同时处于运行中 —— 两个 runtime 各持各的 gate，
	// 谁也挡不住谁。若存在跨 runtime 的全局串行，这里 waitStarted 超时失败。
	waitStarted(t, a.started)
	waitStarted(t, b.started)
	close(a.release)
	close(b.release)

	waitRunResult(t, a.done)
	waitRunResult(t, b.done)

	// 各 fake store 只收到自己的提交：A 的 store 不见 B 的内容，反之亦然。
	assertSingleCommit(t, a.store, "q-A", "reply-A")
	assertSingleCommit(t, b.store, "q-B", "reply-B")

	a.store.mu.Lock()
	b.store.mu.Lock()
	aHasB, bHasA := len(a.store.view) != 2, len(b.store.view) != 2
	a.store.mu.Unlock()
	b.store.mu.Unlock()
	if aHasB || bHasA {
		t.Fatalf("store views polluted across sessions: lenA ok=%v, lenB ok=%v", !aHasB, !bHasA)
	}
}

// TestSameSessionBusy：同一 runtime 第二个 Run 在 gate 上排队，第一个完成后执行；
// 两轮提交串行、历史无覆盖（两次 Commit 内容完整、view 累积 4 条）。
func TestSameSessionBusy(t *testing.T) {
	st := &fakeConversationStore{}
	started1 := make(chan struct{})
	started2 := make(chan struct{})
	release1 := make(chan struct{})
	release2 := make(chan struct{})
	close(release2) // 第 2 轮不需要再挂起，等 gate 即可
	pv := &gatedProvider{
		replies: []string{"reply-one", "reply-two"},
		starts:  []chan struct{}{started1, started2},
		release: []chan struct{}{release1, release2},
	}
	rt := NewSessionRuntime("busy", newFakeStoreSession(st, nil, pv))

	runWithGate := func(input string) RunResult {
		rel, err := rt.AcquireRun(context.Background())
		if err != nil {
			return RunResult{State: RunFailed, Err: err}
		}
		defer rel()
		return rt.RecordRun(func(s *Session) (RunResult, error) {
			return s.Run(context.Background(), RunRequest{Input: input}, testRunCallbacks(&strings.Builder{}))
		})
	}

	done1 := make(chan RunResult, 1)
	go func() { done1 <- runWithGate("q-one") }()
	waitStarted(t, started1) // 第 1 轮进行中，gate 被持

	// 第 2 轮启动后在 gate 上排队：第 1 轮未完成时不得开工。
	done2 := make(chan RunResult, 1)
	go func() { done2 <- runWithGate("q-two") }()
	select {
	case <-started2:
		t.Fatal("second run started while first still holds the gate")
	case <-time.After(100 * time.Millisecond):
	}

	close(release1) // 放行第 1 轮
	waitRunResult(t, done1)

	// 第 2 轮从队列继续执行完成 —— 排队语义成立。
	waitRunResult(t, done2)

	// 提交串行无覆盖：两次 Commit 内容完整，view 累积两轮共 4 条。
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.commits) != 2 {
		t.Fatalf("commits = %d, want 2", len(st.commits))
	}
	c1, c2 := st.commits[0], st.commits[1]
	if len(c1) != 2 || c1[0].Content != "q-one" || c1[1].Content != "reply-one" {
		t.Fatalf("commit 1 = %+v, want [user q-one, assistant reply-one]", c1)
	}
	if len(c2) != 2 || c2[0].Content != "q-two" || c2[1].Content != "reply-two" {
		t.Fatalf("commit 2 = %+v, want [user q-two, assistant reply-two]", c2)
	}
	if len(st.view) != 4 {
		t.Fatalf("view = %d msgs, want 4 (two rounds accumulated, no overwrite)", len(st.view))
	}
}

// TestCommitFailureNotCompleted：持久化失败注入 → RunFailed+Persisted=false，
// 绝不出现 RunCompleted。Commit 失败走普通提交路径；Compact 失败走「运行中
// 自动压缩触发 DetectCompact」的压缩提交路径（fake store 预置带 id 历史 +
// 小窗口配置，使压缩中间件在本轮内真实触发）。
func TestCommitFailureNotCompleted(t *testing.T) {
	t.Run("commit failure", func(t *testing.T) {
		commitErr := errors.New("disk full")
		st := &fakeConversationStore{commitErr: commitErr}
		s := newFakeStoreSession(st, nil, &captureProvider{})
		rt := NewSessionRuntime("fail-commit", s)

		res := rt.RecordRun(func(s *Session) (RunResult, error) {
			return s.Run(context.Background(), RunRequest{Input: "hello"}, testRunCallbacks(&strings.Builder{}))
		})
		if res.State != RunFailed {
			t.Fatalf("state = %q, want RunFailed (never RunCompleted)", res.State)
		}
		if res.Persisted {
			t.Fatal("Persisted = true, want false on commit failure")
		}
		if !errors.Is(res.Err, commitErr) {
			t.Fatalf("err = %v, want wrapped %v", res.Err, commitErr)
		}
		if got := rt.Active(); got != nil {
			t.Fatalf("active after failed run = %+v, want nil (记账已终态化并清理)", got)
		}
	})

	t.Run("compact failure", func(t *testing.T) {
		compactErr := errors.New("compact boom")
		// 预置 8 条带 MsgID 的历史（4 轮）：DetectCompact 需要 covered 段消息有 id。
		var view []core.Message
		for i := 0; i < 8; i++ {
			role := "user"
			if i%2 == 1 {
				role = "assistant"
			}
			view = append(view, core.Message{
				Role:    role,
				Content: "history filler for compaction test",
				MsgID:   "m_seed_" + string(rune('a'+i)),
			})
		}
		st := &fakeConversationStore{compactErr: compactErr, view: view}

		// 小窗口 + 低阈值 + 小 keepLast：首轮模型调用前压缩中间件必然触发，
		// 使 Run 的提交块走 DetectCompact → Store.Compact 失败注入点。
		cfg := runtimeconfig.Default()
		prov := cfg.LLM.Providers[cfg.LLM.DefaultProvider]
		prov.ContextWindow = 100
		cfg.LLM.Providers[cfg.LLM.DefaultProvider] = prov
		cfg.LLM.CompressAt = 0.1
		cfg.LLM.CompactKeep = 2

		s := newFakeStoreSession(st, cfg, &captureProvider{})
		rt := NewSessionRuntime("fail-compact", s)

		res := rt.RecordRun(func(s *Session) (RunResult, error) {
			return s.Run(context.Background(), RunRequest{Input: "hello"}, testRunCallbacks(&strings.Builder{}))
		})
		st.mu.Lock()
		calls := st.compactCalls
		st.mu.Unlock()
		if calls == 0 {
			t.Fatal("Store.Compact was never attempted — injection point not reached")
		}
		if res.State != RunFailed {
			t.Fatalf("state = %q, want RunFailed (never RunCompleted)", res.State)
		}
		if res.Persisted {
			t.Fatal("Persisted = true, want false on compact failure")
		}
		if !errors.Is(res.Err, compactErr) {
			t.Fatalf("err = %v, want wrapped %v", res.Err, compactErr)
		}
	})
}
