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

// archive_test.go 验证会话归档的 fencing、运行中拒绝、持久终态、清单更新和
// prompt/run/维护写入只读策略。

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

// TestAgentPlugin_ArchiveSessionBusyWhileRunInFlight 验证运行中的会话不能归档；
// run 结束后重试应成功。
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

// TestArchiveConcurrentWithStartRun verifies that archive and both run submission paths
// have one admission order: archive wins and the run is rejected, or the run reserves
// first and archive reports busy.
func TestArchiveConcurrentWithStartRun(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()
	p.AcquireControl(sid, "conn-A")

	start := make(chan struct{})
	runResult := make(chan struct {
		h   runtimeagent.RunHandle
		err error
	}, 1)
	archiveResult := make(chan error, 1)
	go func() {
		<-start
		h, err := p.StartRunSync(sid, runtimeagent.RunRequest{Input: "q"}, runtimeagent.RunCallbacks{})
		runResult <- struct {
			h   runtimeagent.RunHandle
			err error
		}{h: h, err: err}
	}()
	go func() {
		<-start
		archiveResult <- p.ArchiveSession(sid, "conn-A")
	}()
	close(start)

	run := <-runResult
	archiveErr := <-archiveResult
	if run.err == nil {
		if archiveErr == nil || !strings.Contains(archiveErr.Error(), "busy") {
			t.Fatalf("archive after run admission = %v, want busy", archiveErr)
		}
		close(gated.release)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := run.h.Wait(ctx); err != nil {
			t.Fatalf("run Wait: %v", err)
		}
		return
	}
	if archiveErr != nil || !strings.Contains(run.err.Error(), "archived and read-only") {
		t.Fatalf("archive/run outcome = archive=%v run=%v, want archive success and archived run rejection", archiveErr, run.err)
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

// TestHandleAgentArchivedSessionReadOnly 验证归档会话拒绝 prompt，且拒绝依据是
// 持久化的 store 标记，而非连接本地状态。
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

// TestStartRunArchivedSessionRejected 验证后台提交面与 prompt 面同源执行归档只读：
// xgw run/start 在 StartRun 的委托前检查处被拒，堵住「prompt 已拒、后台 run 仍可
// 向 archived store 追加记录」的旁路。错误文案与 handleAgent 同形，客户端按同一
// 契约分流。对照面：cancel 等终态管理操作在归档会话上仍可触达 manager，未知 run
// 返回 ErrRunNotFound，体现归档是只读保留而非下线。
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

// TestArchivedSessionMaintenanceWritesRejected（评审收尾，定案需求 #1）：写型
// 维护命令与 prompt/run 提交同源拒 archived——/compact 与 /rewind <n> 在落
// store 写面前被拒；/rewind 无参的检查点列表是只读观察面，保留对 archived
// 会话可用（查看历史不是写）。
func TestArchivedSessionMaintenanceWritesRejected(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ok"})
	sid := p.CurrentSessionID()
	rt := p.manager.Get(sid)
	if rt == nil {
		t.Fatal("current runtime not registered")
	}
	st := p.storeFor(rt)
	if st == nil {
		t.Fatal("no store for current session")
	}
	// 造写面前置：一轮消息 + checkpoint（/rewind <n> 需要可回溯的锚点）。
	committed, err := st.Commit(commMsgs([]string{"user", "assistant"}))
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Checkpoint("before archive"); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	p.AcquireControl(sid, "conn-A")
	if err := p.ArchiveSession(sid, "conn-A"); err != nil {
		t.Fatalf("ArchiveSession: %v", err)
	}

	// /compact 拒写（错误先于 Session.Compact，不触 provider）。
	compactCtx, _ := newContext()
	if err := p.handleCompact(compactCtx); err == nil || !strings.Contains(err.Error(), "archived and read-only") {
		t.Fatalf("handleCompact on archived session = %v, want archived and read-only", err)
	}

	// /rewind 列表（无参）仍可用：只读观察面不查。
	listCtx, listBuf := newContext()
	if err := p.handleRewind(listCtx); err != nil {
		t.Fatalf("handleRewind list on archived session: %v", err)
	}
	if !strings.Contains(listBuf.String(), "#1") {
		t.Fatalf("archived session checkpoint list should render, got:\n%s", listBuf.String())
	}

	// /rewind 1 拒写：SetHead 不落地（活跃视图停在归档前的 head）。
	rwCtx, _ := newContext("1")
	if err := p.handleRewind(rwCtx); err == nil || !strings.Contains(err.Error(), "archived and read-only") {
		t.Fatalf("handleRewind write on archived session = %v, want archived and read-only", err)
	}
	view := st.ActiveView()
	if view[len(view)-1].MsgID != committed[len(committed)-1].MsgID {
		t.Fatalf("head moved on archived session despite rejection")
	}
}
