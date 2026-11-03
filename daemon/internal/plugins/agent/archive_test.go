package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// archive_test.go 是 Plan A3 归档执行面（x-goworker/session/archive 的插件侧
// 实现，boundsession.go ArchiveSession）的验收：fencing（control lease，run/cancel
// 同款）、run 在飞 busy（定案需求 #8）、挂起收口 busy（reaper 竞态窗口封闭）、
// 终态落盘与清单立即反映、幂等、不在册会话的 repo 路径归档，以及 A7 设计点 4
// 的 prompt 只读执法（store.Archived() 为唯一事实源）。

// TestAgentPlugin_ArchiveSessionFencingAndTerminalState 验证 fencing 与终态：
//   - 未知会话诚实未知（ErrSessionUnknown 契约，handler 映射 -32000）；
//   - 未持本会话 control lease → ErrControlLeaseLost（fencing 先于任何突变）；
//   - 持照归档成功：store.Archived() 为真（持久事实）、ListSessions 立即反映
//     state=archived（xgw 响应前可见）、重复归档幂等 no-op。
func TestAgentPlugin_ArchiveSessionFencingAndTerminalState(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ok"})
	sid := p.CurrentSessionID()

	// 未知会话：寻址先于 fence，诚实未知。
	if err := p.ArchiveSession("session_00000000000000000000000000000000", "conn-A"); !errors.Is(err, model.ErrSessionUnknown) {
		t.Fatalf("ArchiveSession(unknown) = %v, want ErrSessionUnknown", err)
	}

	// 未持照：fencing 拒绝，store 未被标记。
	if err := p.ArchiveSession(sid, "conn-A"); !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("ArchiveSession(without lease) = %v, want ErrControlLeaseLost", err)
	}
	if st := p.currentStore(); st != nil && st.Archived() {
		t.Fatal("store marked archived by a lease-lost call, want untouched")
	}

	// 持照归档：终态落盘 + 清单立即反映 + 幂等。
	p.AcquireControl(sid, "conn-A")
	if err := p.ArchiveSession(sid, "conn-A"); err != nil {
		t.Fatalf("ArchiveSession: %v", err)
	}
	if st := p.currentStore(); st == nil || !st.Archived() {
		t.Fatal("store.Archived() = false after archive, want true (持久事实)")
	}
	list := p.ListSessions()
	if len(list) != 1 || list[0].State != model.SessionStateArchived || !list[0].Archived {
		t.Fatalf("ListSessions after archive = %+v, want single archived entry", list)
	}
	if err := p.ArchiveSession(sid, "conn-A"); err != nil {
		t.Fatalf("re-archive = %v, want nil (幂等终态)", err)
	}
}

// TestAgentPlugin_ArchiveSessionBusyWhileRunInFlight 验证 run 在飞拒绝（定案
// 需求 #8「在飞即活跃」）：终态标记与在飞 run 并存会让只读执法与事件面失真
// ——瞬态 busy，等 run 终态后重试即成功。
func TestAgentPlugin_ArchiveSessionBusyWhileRunInFlight(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()
	p.AcquireControl(sid, "conn-A")

	out := make(chan runtimeagent.RunHandle, 1)
	go func() {
		h, err := p.StartRunSync(sid, runtimeagent.RunRequest{Input: "q"}, runtimeagent.RunCallbacks{Write: func(string) {}})
		if err != nil {
			t.Errorf("StartRunSync: %v", err)
			out <- runtimeagent.RunHandle{}
			return
		}
		out <- h
	}()
	var h runtimeagent.RunHandle
	select {
	case h = <-out:
	case <-time.After(5 * time.Second):
		t.Fatal("StartRunSync did not return in time")
	}
	<-gated.started // run 真实开工

	if err := p.ArchiveSession(sid, "conn-A"); err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("ArchiveSession while run in flight = %v, want busy", err)
	}
	if st := p.currentStore(); st != nil && st.Archived() {
		t.Fatal("store marked archived while a run was in flight, want untouched")
	}

	// run 终态后重试：busy 解除，归档成功。
	close(gated.release)
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.Wait(waitCtx); err != nil {
		t.Fatalf("run Wait: %v", err)
	}
	waitActiveCleared(t, p)
	if err := p.ArchiveSession(sid, "conn-A"); err != nil {
		t.Fatalf("ArchiveSession after run terminal = %v, want nil", err)
	}
}

// TestAgentPlugin_ArchiveSessionBusyWhileSuspended 验证挂起收口窗口的 busy：
// run 刚终态（active 已清）而 reaper 尚未执行的瞬间，归档会与收口的 store 关闭
// 并发——挂起台账（isSuspendedRelease）命中即拒。测试直接置台账模拟该窗口
// （生产中由 tryReleaseSession 的挂起分支写入，窗口为微秒级，无法可靠定时）。
func TestAgentPlugin_ArchiveSessionBusyWhileSuspended(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ok"})
	sid := p.CurrentSessionID()
	p.AcquireControl(sid, "conn-A")

	p.markSuspendedRelease(sid)
	if err := p.ArchiveSession(sid, "conn-A"); err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("ArchiveSession while suspended = %v, want busy", err)
	}
	if st := p.currentStore(); st != nil && st.Archived() {
		t.Fatal("store marked archived during suspended release, want untouched")
	}

	// 收口完成（台账摘除）：归档路径恢复。
	p.clearSuspendedRelease(sid)
	if err := p.ArchiveSession(sid, "conn-A"); err != nil {
		t.Fatalf("ArchiveSession after settle = %v, want nil", err)
	}
}

// TestAgentPlugin_ArchiveSessionUnregistered 验证不在册会话的 repo 路径归档：
// 已收口（引用归零注销）的会话仍可归档——repo.Open 打开落标记、用后即释
// （CloseSession 逐出探测缓存，flock 不残留）；lease 残留有效（LeaseRegistry
// 条目只增不删，曾 attach 的连接 fencing 照常）。
func TestAgentPlugin_ArchiveSessionUnregistered(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ok"})
	id := makeSideSession(t, p, "", "不在册的会话") // 落盘即 CloseSession：不在册

	// 未持照的不在册会话同样被 fencing 拒（lease 判定不区分在册与否）。
	if err := p.ArchiveSession(id, "conn-A"); !errors.Is(err, agentcontrol.ErrControlLeaseLost) {
		t.Fatalf("ArchiveSession(unregistered, no lease) = %v, want ErrControlLeaseLost", err)
	}

	// lease 残留 + 归档：repo 路径落标记。
	p.AcquireControl(id, "conn-A")
	if err := p.ArchiveSession(id, "conn-A"); err != nil {
		t.Fatalf("ArchiveSession(unregistered) = %v, want nil", err)
	}
	// 探测缓存已释放：重新打开是全新实例，archived 事实持久。
	st, err := p.repo.Open(id)
	if err != nil {
		t.Fatalf("reopen after archive: %v", err)
	}
	if !st.Archived() {
		t.Error("reopened store.Archived() = false, want true (终态持久)")
	}
	if p.RouteSession(id) != nil {
		t.Error("unregistered session became routed by archiving, want still unregistered")
	}
}

// TestHandleAgentArchivedSessionReadOnly 验证 A7 设计点 4 的 prompt 只读执法：
// archived 终态会话拒绝 prompt，执法数据源是 store.Archived()（持久事实，
// 重启后依然拒）而非 handler 的连接级 readOnly 集合（只覆盖「load 过归档的
// 连接」——归档一个仍有绑定的会话后，未 load 过它的连接 prompt 也必须被挡）。
// 错误文案与 handler 归档拒写同形（read-only），客户端按同一契约分流。
func TestHandleAgentArchivedSessionReadOnly(t *testing.T) {
	p, st, _ := sessionFixture(t, 0)
	if err := st.MarkArchived(); err != nil {
		t.Fatalf("MarkArchived: %v", err)
	}
	before := len(p.boundSession().Conversation())

	err := p.handleAgent(&model.Context{Args: []string{"hi"}})
	if err == nil || !strings.Contains(err.Error(), "archived and read-only") {
		t.Fatalf("handleAgent on archived session = %v, want archived and read-only", err)
	}
	// prompt 被挡 = run 从未提交：conversation 不动（夹具种子 1 条 user 消息）。
	if after := len(p.boundSession().Conversation()); after != before {
		t.Errorf("conversation = %d after rejected prompt, want unchanged %d", after, before)
	}
}

// TestStartRunArchivedSessionRejected 验证后台提交面的 A7④ 同源执法（返工，
// 定案 #1「archived = 只读保留终态」的 run 半边）：xgw run/start 经
// runcontrol.go StartRun 的委托前检查拒——堵「prompt 拒了、后台 run 仍可往
// archived store 追加 run 记录」的旁路；错误文案与 handleAgent 同形，客户端按
// 同一契约分流。对照面：cancel（终态管理面）在归档会话上照常触达 manager——
// 未知 run 诚实 ErrRunNotFound 而非被 archived 拦截，归档语义是只读保留，
// 不是下线不可触达（run/get/attach/detach 同理不查）。
func TestStartRunArchivedSessionRejected(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ok"})
	sid := p.CurrentSessionID()
	p.AcquireControl(sid, "conn-A")
	if err := p.ArchiveSession(sid, "conn-A"); err != nil {
		t.Fatalf("ArchiveSession: %v", err)
	}

	_, _, err := p.StartRun(sid, "conn-A", "hello", "req-arch-run-1")
	if err == nil || !strings.Contains(err.Error(), "archived and read-only") {
		t.Fatalf("StartRun on archived session = %v, want archived and read-only", err)
	}
	// 拒绝发生在委托前：无 run 落地（active 记账为空、store 无新写入）。
	if rt := p.manager.Get(sid); rt != nil {
		if run := rt.Active(); run != nil {
			t.Errorf("run %s started on archived session, want rejected before submission", run.ID)
		}
	}

	// 对照：cancel 不查 archived——未知 run 的取消诚实触达 manager（ErrRunNotFound）。
	if err := p.CancelRun(sid, "conn-A", "run-unknown"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("CancelRun on archived session = %v, want honest not-found (control surface not blocked)", err)
	}
}
