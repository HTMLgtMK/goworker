package agent

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

// 本文件是 P2-1 的 run 语义验收测试:
//   - TestRunIDUniqueAcrossNewInstances:RunID v2(run-<unixnano>-<seq>)跨实例
//     唯一,格式为两个十进制数字段(P2-2 Journal 的 validRunID 与 P3-1 的
//     「文件名字符序=创建序」都以该格式为前提);
//   - TestSessionRun_CancelledProviderErrorMapsToCancelled:ctx 取消/超时归一为
//     RunCancelled、其余维持 RunFailed(总方案第九节「取消不记失败」)。
//
// AC-1 追加:provider-stub 场景的真实断言(错误经 runErrorCapture 捕获,
// 取代 P2-1 留下的 skip 子测试)与 TestSessionRun_CommitFailureWithProviderErrorStaysFailed
// (持久化分支优先于 provider 失败映射)。

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

// TestSessionRun_CancelledProviderErrorMapsToCancelled:ctx 取消/超时 →
// RunCancelled、普通错误 → RunFailed,Err 保留、Persisted=false、active 清理。
//
// 历史记录(P2-1 → AC-1):plan 设想的「stub provider Chat 返回 context.Canceled →
// Session.Run 返回 RunCancelled」在 P2-1 时不可达——coreagent.Agent.Run 的同步
// error 返回恒为 nil(ai-core/agent/agent.go:175),provider 错误在 goroutine 内
// 经 emitAgentNote(:82-107,:309-312)转 Done note token 走历史提交路径(实测:
// captureProvider{chatErr: context.Canceled} → Session.Run 返回 {completed,
// Persisted:true})。AC-1 落地后该场景经错误捕获中间件(runErrorCapture)真实
// 可达,见本测试尾部两个 provider-stub 子测试的真实断言;本测试主体的 call-layer
// seam(RecordRun 对 fn error 的同型映射,runtime.go)保留不变,钉死同一套归一
// 逻辑——session.go 注释「第二返回值 error 仅保留给调用层错误」,该 error 正是
// call-layer 防御映射的预期输入源。
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

	// plan 原文的 provider-stub 场景:AC-1 错误捕获中间件落地后真实可达——
	// provider 错误经 ai-core 的 note token(agent.go:82-107)与 AfterAgentEvent.Err
	// (:55)双通道漏出,runErrorCapture 捕获后由 Session.Run 在 <-msgCh 后映射终态。
	//
	// 本组断言同时钉死「fire 先于 msgCh 发送」的次序依赖(plan §6 风险 1):
	// capture 的写入在 agent goroutine defer 内 fireMiddlewareEvent(agent.go:55)
	// 处,先于同一 defer 的 `msgCh <- messages`(:57);若未来重构把发送挪出 defer
	// 或挪到 fire 之前,runErr 将读不到、provider 失败被静默记回 RunCompleted ——
	// 断言 helper 里的 Persisted==true(note 已提交)与终态断言即在该回归下报警。
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

// assertProviderErrorViaCapture 驱动一轮 provider 失败的 Session.Run(错误经
// runErrorCapture 捕获,AC-1 主映射)并断言「用户可见面不变」的机器证明:
//   - 终态 == want(ctx 取消/超时归一 RunCancelled,其余 RunFailed);
//   - Persisted==true:错误 note 已随 messages 提交成功(「持久化成功」与
//     「模型成功」独立记账)——该断言与终态断言共同钉死 capture/msgCh 的次序
//     依赖(见 TestSessionRun_CancelledProviderErrorMapsToCancelled 尾部注释);
//   - note 仍进 ActiveView:历史含 harness 错误行(文案对齐 ai-core emitAgentNote,
//     agent.go:82),用户可见历史面不变;
//   - cb.EmitToken 仍收到 Done note token:三渲染面的 note 流逐字节不变
//     (plan AC-1 行为面=零可见变化的机器证明)。
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
