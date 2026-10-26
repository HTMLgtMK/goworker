package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
)

// 本文件是 P2-3 的 Run journal 接线验收测试(RecordRun → RunStateJournal):
//   - TestRecordRunJournalsStartAndFinish:一轮 RecordRun 恰好两条 state 记录
//     (running + 终态 completed),RunID/SessionID 同源、终态带 Persisted=true;
//   - TestRecordRunJournalCommitFailureInjectsFailed:Commit 失败注入
//     (fakeConversationStore.commitErr,总方案第十一节的 journal 对偶)→ 终态
//     记录 state=="failed" && !persisted,记录流中绝不出现 completed;
//   - TestRecordRunJournalAppendErrorNonFatal:AppendState 失败只 warn 不阻塞,
//     Run 照常完成(journal 可用性低于执行可用性)。
//
// 编译期断言 `var _ RunStateJournal = (*session.RunJournal)(nil)` 按计划推迟到
// P2-7(session 侧聚合类型 RunJournalStore 引入时补),本步测试用本地 stub。

// stubRunJournal 是 RunStateJournal 的内存测试桩:记录按序收进 recs;appendErr
// 非 nil 时注入 AppendState 失败(记录不收)。
type stubRunJournal struct {
	mu        sync.Mutex
	recs      []session.RunStateRecord
	appendErr error
}

func (j *stubRunJournal) AppendState(rec session.RunStateRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.appendErr != nil {
		return j.appendErr
	}
	j.recs = append(j.recs, rec)
	return nil
}

func (j *stubRunJournal) snapshot() []session.RunStateRecord {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]session.RunStateRecord(nil), j.recs...)
}

// newJournaledSession 构造带 fake store + capture provider + journal stub 的会话
// (其余依赖同 newFakeStoreSession,run_lifecycle_test.go)。
func newJournaledSession(st ConversationStore, jr *stubRunJournal) *Session {
	return NewSession(SessionDeps{
		Config:   runtimeconfig.Default(),
		AuditDir: "",
		CollectTools: func(*sandbox.Config) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return &captureProvider{}, nil
		},
		Store:      st,
		RunJournal: jr,
	})
}

// runOnce 在 rt 上同步执行一轮 Run(fn 直接内联,调用方已持 gate 的形态同
// run_lifecycle_test.go 的 runWithGate——单轮无需 AcquireRun 也成立,RecordRun
// 自身不再 AcquireRun)。
func runOnce(rt *SessionRuntime) RunResult {
	return rt.RecordRun(func(s *Session) (RunResult, error) {
		return s.Run(context.Background(), RunRequest{Input: "hello"}, testRunCallbacks(&strings.Builder{}))
	})
}

// TestRecordRunJournalsStartAndFinish:成功一轮 → journal 恰好两条记录
// (running + completed),kind/state/RunID/SessionID/Persisted 逐项断言。
func TestRecordRunJournalsStartAndFinish(t *testing.T) {
	jr := &stubRunJournal{}
	s := newJournaledSession(&fakeConversationStore{}, jr)
	rt := NewSessionRuntime("journaled", s)

	res := runOnce(rt)
	if res.State != RunCompleted || !res.Persisted {
		t.Fatalf("run result = %+v, want RunCompleted+Persisted=true", res)
	}

	recs := jr.snapshot()
	if len(recs) != 2 {
		t.Fatalf("journal records = %d, want 2 (running + terminal)", len(recs))
	}
	start, finish := recs[0], recs[1]
	if start.Kind != "state" || start.State != string(RunRunning) {
		t.Fatalf("record 1 = {kind:%q state:%q}, want kind=state state=running", start.Kind, start.State)
	}
	if start.RunID == "" {
		t.Fatal("record 1 RunID = empty")
	}
	if start.SessionID != "journaled" {
		t.Fatalf("record 1 SessionID = %q, want runtime id %q", start.SessionID, "journaled")
	}
	if start.At.IsZero() {
		t.Fatal("record 1 At = zero time")
	}
	if finish.Kind != "state" || finish.State != string(RunCompleted) {
		t.Fatalf("record 2 = {kind:%q state:%q}, want kind=state state=completed", finish.Kind, finish.State)
	}
	if finish.RunID != start.RunID {
		t.Fatalf("record 2 RunID = %q, want same as record 1 %q", finish.RunID, start.RunID)
	}
	if !finish.Persisted || finish.Err != "" {
		t.Fatalf("record 2 = {persisted:%v err:%q}, want persisted=true err empty", finish.Persisted, finish.Err)
	}
}

// TestRecordRunJournalCommitFailureInjectsFailed:Commit 失败注入 → 终态记录
// failed && !persisted,记录流中绝不出现 completed(「模型成功 ≠ 持久化成功」
// 在 journal 面的对偶钉死)。
func TestRecordRunJournalCommitFailureInjectsFailed(t *testing.T) {
	jr := &stubRunJournal{}
	st := &fakeConversationStore{commitErr: errors.New("disk full")}
	s := newJournaledSession(st, jr)
	rt := NewSessionRuntime("journaled-fail", s)

	res := runOnce(rt)
	if res.State != RunFailed || res.Persisted {
		t.Fatalf("run result = %+v, want RunFailed+Persisted=false on commit failure", res)
	}

	recs := jr.snapshot()
	if len(recs) != 2 {
		t.Fatalf("journal records = %d, want 2 (running + terminal)", len(recs))
	}
	terminal := recs[len(recs)-1]
	if terminal.State != string(RunFailed) {
		t.Fatalf("terminal record state = %q, want %q", terminal.State, string(RunFailed))
	}
	if terminal.Persisted {
		t.Fatal("terminal record persisted = true, want false on commit failure")
	}
	if terminal.Err == "" {
		t.Fatal("terminal record err = empty, want commit failure surfaced")
	}
	for i, rec := range recs {
		if rec.State == string(RunCompleted) {
			t.Fatalf("record %d state = completed in failing-commit journal stream: %+v", i, rec)
		}
	}
}

// TestRecordRunJournalAppendErrorNonFatal:AppendState 注入失败 → Run 照常完成
// (warn 路径,不阻塞执行),stub 因注入失败不收任何记录。
func TestRecordRunJournalAppendErrorNonFatal(t *testing.T) {
	jr := &stubRunJournal{appendErr: errors.New("journal io failure")}
	s := newJournaledSession(&fakeConversationStore{}, jr)
	rt := NewSessionRuntime("journaled-err", s)

	res := runOnce(rt)
	if res.State != RunCompleted || !res.Persisted {
		t.Fatalf("run result = %+v, want RunCompleted+Persisted=true (journal failure must not block execution)", res)
	}
	if recs := jr.snapshot(); len(recs) != 0 {
		t.Fatalf("journal records = %d, want 0 (append failed, nothing recorded)", len(recs))
	}
	if got := rt.Active(); got != nil {
		t.Fatalf("active after run = %+v, want nil (记账已终态化并清理)", got)
	}
}
