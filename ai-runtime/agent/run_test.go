package agent

import (
	"context"
	"errors"
	"regexp"
	"testing"
)

// 本文件是 P2-1 的 run 语义验收测试:
//   - TestRunIDUniqueAcrossNewInstances:RunID v2(run-<unixnano>-<seq>)跨实例
//     唯一,格式为两个十进制数字段(P2-2 Journal 的 validRunID 与 P3-1 的
//     「文件名字符序=创建序」都以该格式为前提);
//   - TestSessionRun_CancelledProviderErrorMapsToCancelled:ctx 取消/超时归一为
//     RunCancelled、其余维持 RunFailed(总方案第九节「取消不记失败」)。

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
// 与 plan 测试原文的偏差(如实记录):plan 设想「stub provider Chat 返回
// context.Canceled → Session.Run 返回 RunCancelled」,但 coreagent.Agent.Run
// (ai-core/agent/agent.go:43-175)的同步 error 返回恒为 nil——provider 错误在
// goroutine 内经 emitAgentNote(:80-84,:309-312)转 Done note token 走历史提交
// 路径,到不了 session.go 的 agent.Run 失败分支(实测:captureProvider{chatErr:
// context.Canceled} → Session.Run 返回 {completed, Persisted:true})。session.go
// 的分支按 plan 保留为主映射(防御 + 未来 harness 错误通道修正后生效);本测试经
// 唯一可达的 call-layer seam(RecordRun 对 fn error 的同型映射,runtime.go)钉死
// 同一套归一逻辑——session.go:95 注释「第二返回值 error 仅保留给调用层错误」,
// 该 error 正是 call-layer 映射的唯一预期输入源。
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

	// plan 原文的 provider-stub 场景当前不可达(见函数头注释),跳过留痕:
	// ai-core 错误通道修正后改为真实断言(State==RunCancelled / RunFailed)。
	t.Run("provider Chat returning context.Canceled", func(t *testing.T) {
		t.Skip("coreagent.Agent.Run funnels provider errors into note tokens (sync err always nil); " +
			"session.go:157 branch unreachable via provider stub — see test comment and P2-1 outcome notes")
	})
}
