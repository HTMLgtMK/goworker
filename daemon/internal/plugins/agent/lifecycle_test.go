package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// lifecycle_test.go 是 Plan A2 生命周期半边的验收(定案需求 #2/#8):
//   - /new 单绑定归零即收口:发起连接是唯一引用 → runtime 注销 + store 关闭并
//     逐出 repo 缓存,重开经 flock 慢路径拿活实例(A2.1①);
//   - run 在飞解绑 → 延迟到 run 终态才收口(挂起 + WithRunDoneHook 唤醒重判);
//   - 挂起期间的新绑定 → 取消挂起收口(收口执行点的原子复查);
//
// 多连接绑定的存活语义(另一连接仍绑旧会话 → 不收口)在 binding_test.go 的
// TestHandleNewKeepsSessionWhileOtherConnectionBound 覆盖;BindingRegistry 的
// 记账契约在 agentcontrol/bindings_test.go 覆盖。
//
// 挂起面的在飞 run 用 rt.RecordRun 直接占位(manager.StartRun 的 queued/running
// 同样落在 rt.active,finalizeRun 是两条路径共享的终态化点——run-done 通知在
// 两形态下同源触发,无需 provider 参与即可确定性断言)。

// waitActive 等待 rt 的 active 记账就位(RecordRun 在独立 goroutine 上推进)。
func waitActive(t *testing.T, rt *runtimeagent.SessionRuntime) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rt.Active() != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("run did not occupy rt.active in time")
}

// newBoundSession 复用 newPersistedPlugin 的装配(真实 NewPlugin/Init,manager
// 带 WithRunDoneHook、control 已接线),再经 newPersistedSession 建一个独立目录
// 新会话(生产 /new 后的拓扑:current 与 manager 条目同一 runtime),登记一条
// 连接绑定并返回插件与会话 id。不直接用 Init 的首会话:refreshSession 的同 id
// 重建会在 manager 里留旧条目(current 不在册),那是纯内存降级的夹具形态,
// 与本文件断言的持久化拓扑无关。
func newBoundSession(t *testing.T, connID string) (*AgentPlugin, string) {
	t.Helper()
	p := newPersistedPlugin(t, fixedProvider{content: "x"})
	sid := newPersistedSession(t, p, fixedProvider{content: "x"})
	if rt := p.manager.Get(sid); rt == nil || rt != p.currentSession() {
		t.Fatal("setup: current runtime must be the registered manager entry")
	}
	p.BindConnectionSession(connID, sid)
	if got := p.control.SessionBindings(sid); len(got) != 1 || got[0] != connID {
		t.Fatalf("setup: bindings(%s) = %v, want [%s]", sid, got, connID)
	}
	return p, sid
}

// TestNewReleasesSessionWhenLastBindingDrops(/new 单绑定归零即收口):唯一绑定的
// 连接发起 /new → 旧会话引用归零 → runtime 注销 + store 关闭并逐出缓存;store
// 数据保留,repo.Open 重开拿活实例、历史可读(A2.1①:死实例不再留缓存)。
func TestNewReleasesSessionWhenLastBindingDrops(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")

	// 旧会话先落一条历史,重开断言数据不丢。
	if st := p.currentStore(); st == nil {
		t.Fatal("setup: no current store")
	} else if _, err := st.Commit([]core.Message{{Role: "user", Content: "旧会话历史", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("setup commit: %v", err)
	}

	b := &agentcontrol.SessionBinding{}
	b.Set(sidA)
	mctx, buf := newContext()
	mctx.Ctx = agentcontrol.WithClientID(agentcontrol.WithBinding(context.Background(), b), "conn-1")
	if err := p.handleNew(mctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}
	if !strings.Contains(buf.String(), "New session started") {
		t.Fatalf("handleNew output = %q, want success message", buf.String())
	}

	// 收口断言:runtime 注销、发起方 Binding 清空(回落 current 落到新会话)、
	// 登记解除。
	if p.manager.Get(sidA) != nil {
		t.Fatal("session A runtime still registered after /new (引用归零必须收口)")
	}
	if got := b.Get(); got != "" {
		t.Fatalf("initiator binding = %q, want empty", got)
	}
	if got := p.control.SessionBindings(sidA); len(got) != 0 {
		t.Fatalf("bindings after /new = %v, want empty (登记已摘除)", got)
	}

	// 重开:flock 已释放,慢路径拿活实例(A2.1①),旧历史可读、可继续写。
	st, err := p.repo.Open(sidA)
	if err != nil {
		t.Fatalf("repo.Open(released session): %v", err)
	}
	view := st.ActiveView()
	if len(view) != 1 || view[0].Content != "旧会话历史" {
		t.Fatalf("reopened view = %d msgs, want the seeded history (数据不丢)", len(view))
	}
	if _, err := st.Commit([]core.Message{{Role: "user", Content: "reopened", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit on reopened store: %v", err)
	}
}

// TestRunInFlightDelaysReleaseUntilTerminal(run 在飞解绑 → 延迟到 run 终态才收口,
// 定案需求 #8 的收口时点半边):引用归零时有 run 在飞 → 挂起(runtime/store 保留,
// A3 的 state=active 语义依赖此刻仍可观测);run 终态通知(WithRunDoneHook →
// onSessionRunDone)触发重判 → 收口。
func TestRunInFlightDelaysReleaseUntilTerminal(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")
	// current 换到 B:被评估的 A 不是 current(否则触发 current 隐式引用守卫,
	// 那是 CLI 形态的保护,与本场景无关)。
	sidB := newPersistedSession(t, p, fixedProvider{content: "x"})
	if sidB == sidA {
		t.Fatal("setup: distinct sessions required")
	}

	rt := p.manager.Get(sidA)
	if rt == nil {
		t.Fatal("setup: session A runtime missing")
	}
	// 在飞 run:RecordRun 阻塞在 fn(测试持 gate,fn 释放后终态化触发 hook)。
	gate, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("AcquireRun: %v", err)
	}
	block := make(chan struct{})
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		rt.RecordRun(func(*runtimeagent.Session) (runtimeagent.RunResult, error) {
			<-block
			return runtimeagent.RunResult{State: runtimeagent.RunCompleted}, nil
		})
	}()
	waitActive(t, rt)

	// 解绑(断连同款):引用归零但 run 在飞 → 挂起,不收口。
	p.ReleaseConnection("conn-1")
	if p.manager.Get(sidA) == nil {
		t.Fatal("session A must stay registered while its run is in flight (挂起收口)")
	}

	// run 终态:finalizeRun 清 active 后触发 hook → 重判 → 收口;hook 在
	// RecordRun 返回前同步执行,runDone 关闭即收口已完成。
	close(block)
	<-runDone
	gate()
	if p.manager.Get(sidA) != nil {
		t.Fatal("session A must be released after its run reached terminal state (延迟收口)")
	}
	// store 数据保留:重开活实例。
	if _, err := p.repo.Open(sidA); err != nil {
		t.Fatalf("repo.Open after delayed release: %v", err)
	}
}

// TestSuspendedReleaseCancelledByRebind(挂起期间的新绑定 → 取消挂起收口):
// 收口执行点的原子复查——run 终态触发的重判在执行点读最新绑定集合,挂起期间
// session/load 重开把连接绑回该会话(BindConnectionSession 登记)时,重判读到
// 非空集合,挂起收口被取消,绝不关闭仍有引用的会话。
func TestSuspendedReleaseCancelledByRebind(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")
	sidB := newPersistedSession(t, p, fixedProvider{content: "x"})
	if sidB == sidA {
		t.Fatal("setup: distinct sessions required")
	}

	rt := p.manager.Get(sidA)
	gate, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("AcquireRun: %v", err)
	}
	block := make(chan struct{})
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		rt.RecordRun(func(*runtimeagent.Session) (runtimeagent.RunResult, error) {
			<-block
			return runtimeagent.RunResult{State: runtimeagent.RunCompleted}, nil
		})
	}()
	waitActive(t, rt)

	// 解绑 → 挂起;挂起期间重开路径把新连接绑回该会话。
	p.ReleaseConnection("conn-1")
	if p.manager.Get(sidA) == nil {
		t.Fatal("session A must stay registered while its run is in flight (挂起收口)")
	}
	p.BindConnectionSession("conn-2", sidA)

	// run 终态 → 重判读到非空绑定 → 取消收口,会话存活。
	close(block)
	<-runDone
	gate()
	if p.manager.Get(sidA) == nil {
		t.Fatal("session A must survive: a new binding arrived during suspension (原子复查取消收口)")
	}
	if st := p.storeFor(p.manager.Get(sidA)); st == nil {
		t.Fatal("session A store must stay alive after suspension cancellation")
	} else if _, err := st.Commit([]core.Message{{Role: "user", Content: "still bound", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit on surviving store: %v", err)
	}
}

// TestRunDoneHookIgnoresUnsuspendedSessions(钩子消费闸门):普通 run 收尾(无挂起
// 台账)零动作——零绑定的 current 会话(CLI 形态)不能被 run 终态通知误收口,
// current 隐式引用守卫与台账闸门双保险。
func TestRunDoneHookIgnoresUnsuspendedSessions(t *testing.T) {
	p, sidA := newBoundSession(t, "conn-1")
	p.ReleaseConnection("conn-1") // 先解除登记,制造「零绑定 current」形态

	rt := p.currentSession()
	if rt == nil || rt.ID != sidA {
		t.Fatal("setup: current session must be A")
	}
	gate, err := rt.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("AcquireRun: %v", err)
	}
	rt.RecordRun(func(*runtimeagent.Session) (runtimeagent.RunResult, error) {
		return runtimeagent.RunResult{State: runtimeagent.RunCompleted}, nil
	})
	gate()

	// run 终态已发生(hook 同步触发),但 A 从未挂起 → 不收口。
	if p.manager.Get(sidA) == nil {
		t.Fatal("zero-binding current session must not be reaped by run-done hook (便利指针隐式引用)")
	}
	if st := p.currentStore(); st == nil {
		t.Fatal("current store must stay alive for the CLI fallback path")
	}
}

// 编译期守卫:model.ConnectionBinder 断言面与本包生命周期方法的对接点。
var _ model.ConnectionBinder = (*AgentPlugin)(nil)
