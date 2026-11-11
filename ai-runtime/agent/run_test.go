package agent

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

// 本文件验收 RunID 唯一性、取消/普通错误的终态映射，以及持久化失败优先级。
// AC-1 provider stub 断言由 runErrorCapture 捕获错误，确保用户可见 note 和终态一致。

// TestRunIDUniqueAcrossNewInstances:两次 newRunID() 不等(进程内 seq 后缀兜底
// 唯一,UnixNano 单调性不做强假设);格式为 run-<十进制>-<十进制>。
func TestRunIDUniqueAcrossNewInstances(t *testing.T) {
	id1 := newRunID()
	id2 := newRunID()
	if id1 == id2 {
		t.Fatalf("two newRunID() calls returned the same id %q", id1)
	}
	pattern := regexp.MustCompile(`^run-\d+-\d+$`)
	if !pattern.MatchString(id1) || !pattern.MatchString(id2) {
		t.Fatalf("run id format = %q / %q, want run-<decimal>-<decimal>", id1, id2)
	}
}

// TestSessionRun_CancelledProviderErrorMapsToCancelled 覆盖取消与普通错误的映射：
// 前者为 RunCancelled，后者为 RunFailed，并保留错误、清理 active 且不标记持久化。
func TestSessionRun_CancelledProviderErrorMapsToCancelled(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want RunState
	}{
		{"context.Canceled maps to RunCancelled", context.Canceled, RunCancelled},
		{"context.DeadlineExceeded maps to RunCancelled", context.DeadlineExceeded, RunCancelled},
		{"ordinary error stays RunFailed", errors.New("boom"), RunFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := NewSessionRuntime("cancel-map", newFakeStoreSession(nil, nil, &captureProvider{}))
			res := rt.RecordRun(func(*Session) (RunResult, error) {
				return RunResult{}, tc.err
			})
			if res.State != tc.want {
				t.Fatalf("state = %q, want %q", res.State, tc.want)
			}
			if !errors.Is(res.Err, tc.err) {
				t.Fatalf("err = %v, want preserved %v", res.Err, tc.err)
			}
			if res.Persisted {
				t.Fatal("Persisted = true, want false on non-completed run")
			}
			if got := rt.Active(); got != nil {
				t.Fatalf("active after run = %+v, want nil (记账已终态化并清理)", got)
			}
		})
	}

	// provider 错误经 note token 和 AfterAgentEvent.Err 进入 runErrorCapture。
	// fireMiddlewareEvent 必须先于 msgCh 发送，否则 capture 可能读不到错误并误记 completed；
	// Persisted 与终态断言确保错误 note 已提交且顺序保持正确。
	for _, tc := range []struct {
		name string
		err  error
		want RunState
	}{
		{"provider Chat returning context.Canceled", context.Canceled, RunCancelled},
		{"provider Chat returning ordinary error", errors.New("boom"), RunFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertProviderErrorViaCapture(t, tc.err, tc.want)
		})
	}
}

// assertProviderErrorViaCapture 验证 provider 失败后的终态、持久化错误 note、历史记录
// 与 Done token 均保持可见。Persisted 与终态断言也覆盖 capture/msgCh 的先后顺序。
func assertProviderErrorViaCapture(t *testing.T, chatErr error, want RunState) {
	t.Helper()
	var tokens []core.Token
	var output strings.Builder
	cb := RunCallbacks{
		Write:      func(s string) { output.WriteString(s) },
		WriteToken: func(_ RenderKind, s string, _ bool) { output.WriteString(s) },
		EmitToken:  func(tok core.Token) { tokens = append(tokens, tok) },
	}
	st := &fakeConversationStore{}
	rt := NewSessionRuntime("errcapture", newFakeStoreSession(st, nil, &captureProvider{chatErr: chatErr}))

	res := rt.RecordRun(func(s *Session) (RunResult, error) {
		return s.Run(context.Background(), RunRequest{Input: "hello"}, cb)
	})
	if res.State != want {
		t.Fatalf("state = %q, want %q", res.State, want)
	}
	if !errors.Is(res.Err, chatErr) {
		t.Fatalf("err = %v, want preserved %v", res.Err, chatErr)
	}
	if !res.Persisted {
		t.Fatal("Persisted = false, want true (error note committed with messages; " +
			"capture/msgCh ordering likely broken)")
	}

	// note 仍进 ActiveView(note 随 messages 提交成功的事实面)。
	foundNote := false
	for _, m := range st.ActiveView() {
		if strings.Contains(m.Content, "⚠ [harness] ✘ agent error") {
			foundNote = true
			break
		}
	}
	if !foundNote {
		t.Fatalf("active view has no harness error note; view = %+v", st.ActiveView())
	}

	// cb.EmitToken 仍收到 Done note token(用户可见 token 流不变)。
	foundDone := false
	for _, tok := range tokens {
		if tok.Done && strings.Contains(tok.Content, "⚠ [harness] ✘ agent error") {
			foundDone = true
			break
		}
	}
	if !foundDone {
		t.Fatalf("EmitToken received %d token(s), none is the Done note token: %+v", len(tokens), tokens)
	}
}

// TestSessionRun_CommitFailureWithProviderErrorStaysFailed:provider 失败(chatErr
// 注入,错误经 capture 走 AC-1 主映射)+ Commit 失败注入 → 持久化分支优先:
// RunFailed+Persisted=false(commit 块提前返回先于最终 return 的 capture 映射,
// 绝不 RunCompleted、绝不借 Persisted=true 掩盖持久化失败)。
func TestSessionRun_CommitFailureWithProviderErrorStaysFailed(t *testing.T) {
	commitErr := errors.New("disk full")
	st := &fakeConversationStore{commitErr: commitErr}
	rt := NewSessionRuntime("fail-commit-provider-err",
		newFakeStoreSession(st, nil, &captureProvider{chatErr: errors.New("boom")}))

	res := rt.RecordRun(func(s *Session) (RunResult, error) {
		return s.Run(context.Background(), RunRequest{Input: "hello"}, testRunCallbacks(&strings.Builder{}))
	})
	if res.State != RunFailed {
		t.Fatalf("state = %q, want RunFailed (persistence branch wins over provider-error mapping)", res.State)
	}
	if res.Persisted {
		t.Fatal("Persisted = true, want false on commit failure")
	}
	if !errors.Is(res.Err, commitErr) {
		t.Fatalf("err = %v, want commit failure %v", res.Err, commitErr)
	}
}
