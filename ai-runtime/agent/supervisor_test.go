package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
)

// 本文件是 P2-5 的后台执行验收测试(总方案第十一节 → 本步),全部模拟 Provider
// (复用 gatedProvider/newFakeStoreSession/fakeConversationStore,run_lifecycle_test.go):
//   - TestRunSurvivesDisconnect:提交方 goroutine 不 Wait、直接返回(模拟断连),
//     后台 Run 不受影响跑完并持久化——宿主 ctx 是 runCtx 的父,调用方 ctx 不参与;
//   - TestStartRunQueuedOnGlobalSlot:全局槽占满时后续 StartRun 立即返回且保持
//     RunQueued(同步段零阻塞、排队发生在全局槽一级);
//   - TestStartRunPendingBounded:pending 有界,排队满即拒(ErrTooManyPending);
//   - TestStartRunSecondRunRejected:同会话第二 StartRun 被拒(ErrRunActive);
//   - TestStartRunAtomicOccupy:同会话并发 StartRun 的检查/占位原子性(C1 修复
//     验收)——成功数严格 == 1,其余全部 ErrRunActive/ErrTooManyPending,成功
//     run 可被 CancelRun 定位(Active() 即该 run);
//   - TestStartRunJournalsQueuedBeforeReturn:StartRun 返回时 queued 记录已在
//     (durable-first,总方案第七节);
//   - TestStartRunRejectsWhenQueuedJournalFails:queued 记录持久化失败 → 拒绝
//     启动(I1 修复验收,failing journal stub 注错:错误链上抛、零句柄、journal/
//     active/pending/provider 四面无残留)。
//
// 并行性断言沿用 run_lifecycle_test.go 的「屏障代替计时」风格:唯一挂钟等待是
// waitStarted(开工宣告)与终态 Wait 的超时上界,其余为确定性断言。

// startOutcome 承载提交方 goroutine 的 StartRun 结果(提交方不 Wait 的形态)。
type startOutcome struct {
	h   RunHandle
	err error
}

// newSupRuntime 构造带 gatedProvider 的 runtime 并注册进 manager:starts[0] 在
// provider 开工时关闭;hang 为真时第 1 轮挂起至 release 通道关闭,否则 provider
// 被调用即返回(release 返回 nil,调用方不得关闭)。
func newSupRuntime(t *testing.T, m *SessionManager, id, reply string, hang bool) (st *fakeConversationStore, started, release chan struct{}) {
	t.Helper()
	started = make(chan struct{})
	release = make(chan struct{})
	pv := &gatedProvider{
		replies: []string{reply},
		starts:  []chan struct{}{started},
		release: []chan struct{}{release},
	}
	if !hang {
		pv.release = nil
		release = nil
	}
	st = &fakeConversationStore{}
	rt := NewSessionRuntime(id, newFakeStoreSession(st, nil, pv))
	if err := m.Register(rt); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
	return st, started, release
}

// waitHandle 等待句柄到达终态并断言 RunCompleted+Persisted=true。
func waitHandle(t *testing.T, h RunHandle) RunResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := h.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v (state=%s)", err, h.State())
	}
	if res.State != RunCompleted || !res.Persisted {
		t.Fatalf("run result = %+v, want RunCompleted+Persisted=true", res)
	}
	return res
}

// assertHostCtxAlive 断言 manager 宿主 ctx 未被取消(when 说明断言时机)。
func assertHostCtxAlive(t *testing.T, m *SessionManager, when string) {
	t.Helper()
	select {
	case <-m.contextOf().Done():
		t.Fatalf("manager host context canceled %s", when)
	default:
	}
}

// TestRunSurvivesDisconnect(总方案第十一节 → 本步):提交方 goroutine 在
// StartRun 返回后直接退出、不 Wait(模拟连接断开——StartRun 无 ctx 参数,API
// 形状即保证调用方 ctx 不成 run 父);provider 挂起期间宿主 ctx 未取消;release
// 后 Wait 得 RunCompleted+Persisted,store 提交完整。
func TestRunSurvivesDisconnect(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	st, started, release := newSupRuntime(t, m, "disc", "reply-A", true)

	// 提交方 goroutine:StartRun 返回即退出——不 Wait、不再触碰 cb(契约①)。
	out := make(chan startOutcome, 1)
	go func() {
		h, err := m.StartRun("disc", RunRequest{Input: "q-A"}, testRunCallbacks(&strings.Builder{}))
		out <- startOutcome{h: h, err: err}
	}()
	var h RunHandle
	select {
	case got := <-out:
		if got.err != nil {
			t.Fatalf("StartRun: %v", got.err)
		}
		h = got.h
	case <-time.After(5 * time.Second):
		t.Fatal("StartRun did not return in time")
	}
	if h.SessionID != "disc" || h.RunID == "" {
		t.Fatalf("handle = {SessionID:%q RunID:%q}, want session id + non-empty run id", h.SessionID, h.RunID)
	}

	waitStarted(t, started) // provider 挂起中:run 在跑,提交方 goroutine 已返回
	assertHostCtxAlive(t, m, "while the submitter is gone and the run is in flight")

	close(release) // 放行 provider → run 走到终态
	waitHandle(t, h)
	assertHostCtxAlive(t, m, "after the run completed")
	assertSingleCommit(t, st, "q-A", "reply-A")
}

// TestStartRunQueuedOnGlobalSlot:WithRunLimits(1, 8)——A 占住唯一全局槽挂起
// 中,B StartRun 立即返回(同步段零阻塞)且 State()==RunQueued(排队发生在全局
// 槽一级);release A 后 B 执行完成,两 fake store 各自提交完整。
func TestStartRunQueuedOnGlobalSlot(t *testing.T) {
	m := NewSessionManager(WithRunLimits(1, 8))
	defer m.Close()
	stA, startedA, releaseA := newSupRuntime(t, m, "session-A", "reply-A", true)
	stB, startedB, _ := newSupRuntime(t, m, "session-B", "reply-B", false)

	hA, err := m.StartRun("session-A", RunRequest{Input: "q-A"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun A: %v", err)
	}
	waitStarted(t, startedA) // A 占住唯一 runSlots 槽

	hB, err := m.StartRun("session-B", RunRequest{Input: "q-B"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun B while slot held: %v", err)
	}
	if got := hB.State(); got != RunQueued {
		t.Fatalf("B state right after StartRun = %q, want RunQueued (同步段零阻塞,排队在全局槽一级)", got)
	}
	// 确定性断言:槽容量 1 且被 A 持有,B 不得开工。
	select {
	case <-startedB:
		t.Fatal("B started while A still holds the only global slot")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseA) // 放行 A → B 获得槽执行
	waitHandle(t, hA)
	waitStarted(t, startedB)
	waitHandle(t, hB)

	assertSingleCommit(t, stA, "q-A", "reply-A")
	assertSingleCommit(t, stB, "q-B", "reply-B")
}

// TestStartRunPendingBounded:槽 1、pending 2——A running + B/C queued(不同
// 会话)后,第 4 个 StartRun 因排队满被拒(ErrTooManyPending,有界排队不无限
// 堆积);放行 A 后 B/C 依次完成。
func TestStartRunPendingBounded(t *testing.T) {
	m := NewSessionManager(WithRunLimits(1, 2))
	defer m.Close()
	_, startedA, releaseA := newSupRuntime(t, m, "s-A", "reply-A", true)
	_, startedB, _ := newSupRuntime(t, m, "s-B", "reply-B", false)
	_, startedC, _ := newSupRuntime(t, m, "s-C", "reply-C", false)
	newSupRuntime(t, m, "s-D", "reply-D", false)

	hA, err := m.StartRun("s-A", RunRequest{Input: "q-A"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun A: %v", err)
	}
	waitStarted(t, startedA)

	hB, err := m.StartRun("s-B", RunRequest{Input: "q-B"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun B: %v", err)
	}
	hC, err := m.StartRun("s-C", RunRequest{Input: "q-C"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun C: %v", err)
	}
	if got := hB.State(); got != RunQueued {
		t.Fatalf("B state = %q, want RunQueued", got)
	}
	if got := hC.State(); got != RunQueued {
		t.Fatalf("C state = %q, want RunQueued", got)
	}

	// pending 容量 2 已被 B/C 的预留占满(预留发生在 StartRun 同步段):第 4 个
	// 提交立即拒绝。
	if _, err := m.StartRun("s-D", RunRequest{Input: "q-D"}, testRunCallbacks(&strings.Builder{})); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("4th StartRun = %v, want ErrTooManyPending", err)
	}
	// 被拒的 D 不得占位/开工(拒绝发生在记账占位之前)。
	if rt := m.Get("s-D"); rt.Active() != nil {
		t.Fatalf("rejected StartRun left active accounting on s-D: %+v", rt.Active())
	}

	close(releaseA)
	waitHandle(t, hA)
	waitStarted(t, startedB)
	waitHandle(t, hB)
	waitStarted(t, startedC)
	waitHandle(t, hC)
}

// TestStartRunSecondRunRejected:同会话第二 StartRun 被拒(ErrRunActive)——
// 会话级不排队,排队只发生在全局槽一级(总方案第四节)。
func TestStartRunSecondRunRejected(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	_, started, release := newSupRuntime(t, m, "solo", "reply", true)

	h1, err := m.StartRun("solo", RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("first StartRun: %v", err)
	}
	waitStarted(t, started)

	if _, err := m.StartRun("solo", RunRequest{Input: "q2"}, testRunCallbacks(&strings.Builder{})); !errors.Is(err, ErrRunActive) {
		t.Fatalf("second StartRun = %v, want ErrRunActive", err)
	}

	close(release)
	waitHandle(t, h1)
}

// TestStartRunAtomicOccupy(C1 修复验收):200 个 goroutine 并发对同一 Session
// StartRun——「检查 active」与「占位 active」必须原子(同一 rt.mu 临界区,
// supervisor.go ②),否则并发提交可双双通过检查、后占位者覆盖先占位者的
// rt.active(C1 的检查/占位 TOCTOU)。断言:
//   - 成功数严格 == 1,其余全部 ErrRunActive/ErrTooManyPending(排队面满即拒
//     与会话占用拒都是合法拒绝面——③ 预留先于 ②,满载风暴下两者都会出现);
//   - 成功 run 的记账可被 CancelRun 定位:Active() 即该 run、CancelRun 成功
//     (active 被覆盖时 CancelRun 撞 ErrRunNotFound,正是 C1 的危害面);
//   - provider 恰被调用一次、store 恰一轮提交(被拒者从未执行)。
//
// provider 挂起(hang)而非正常返回:终态化会清 active,正常返回时 Active()/
// CancelRun 断言与 run 完成存在竞态——挂起把成功 run 钉在在途,断言窗口确定;
// 取消 + release 后经 execute ⑤ 的取消归一收敛 RunCancelled。
func TestStartRunAtomicOccupy(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	st, started, release := newSupRuntime(t, m, "atomic", "reply", true)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) }) // 失败路径放行挂起的 provider,防 m.Close 死等在途 run

	const submitters = 200
	outcomes := make(chan startOutcome, submitters)
	var wg sync.WaitGroup
	for i := 0; i < submitters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var buf strings.Builder // 每 goroutine 独立 cb:成功者唯一(契约①),拒绝者的 cb 不会被触碰
			h, err := m.StartRun("atomic", RunRequest{Input: fmt.Sprintf("q-%d", i)}, testRunCallbacks(&buf))
			outcomes <- startOutcome{h: h, err: err}
		}(i)
	}
	wg.Wait()
	close(outcomes)

	var winners []RunHandle
	for out := range outcomes {
		if out.err == nil {
			winners = append(winners, out.h)
			continue
		}
		if !errors.Is(out.err, ErrRunActive) && !errors.Is(out.err, ErrTooManyPending) {
			t.Fatalf("concurrent StartRun error = %v, want ErrRunActive or ErrTooManyPending", out.err)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("concurrent StartRun successes = %d, want exactly 1(检查/占位原子性被破坏)", len(winners))
	}
	winner := winners[0]

	waitStarted(t, started) // 成功 run 真实在跑(provider 挂起中)

	rt := m.Get("atomic")
	if rt == nil {
		t.Fatal("runtime deregistered unexpectedly")
	}
	if act := rt.Active(); act == nil || act.ID != winner.RunID {
		t.Fatalf("Active() = %+v, want the winning run %s(CancelRun 的定位面被覆盖)", act, winner.RunID)
	}
	if err := m.CancelRun("atomic", winner.RunID); err != nil {
		t.Fatalf("CancelRun winning run = %v, want nil(成功 run 必须可定位取消)", err)
	}

	releaseOnce.Do(func() { close(release) }) // 放行 provider → 取消归一收敛终态
	waitCancelled(t, winner)

	st.mu.Lock()
	commits := len(st.commits)
	st.mu.Unlock()
	if commits != 1 {
		t.Fatalf("store commits = %d, want 1(被拒的提交不得执行)", commits)
	}
}

// TestStartRunJournalsQueuedBeforeReturn:sink stub 断言 StartRun 返回时 queued
// 记录已在(durable-first:「run/start 响应在 Run 被接受并持久化登记后立即返回」,
// 总方案第七节);完成后记录流为 queued → running → completed,RunID 同源。
func TestStartRunJournalsQueuedBeforeReturn(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	jr := &stubRunJournal{}
	pv := &gatedProvider{replies: []string{"reply"}} // 不挂起、不宣告开工
	s := NewSession(SessionDeps{
		Config:   runtimeconfig.Default(),
		AuditDir: "",
		CollectTools: func(*sandbox.Config) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store:      &fakeConversationStore{},
		RunJournal: jr,
	})
	rt := NewSessionRuntime("jq", s)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}

	h, err := m.StartRun("jq", RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// StartRun 已返回:queued 记录必须在(同步段写,先于 go execute)。
	recs := jr.snapshot()
	if len(recs) < 1 {
		t.Fatal("no journal record at StartRun return, want queued record (durable-first)")
	}
	first := recs[0]
	if first.Kind != "state" || first.State != string(RunQueued) {
		t.Fatalf("first record = {kind:%q state:%q}, want kind=state state=queued", first.Kind, first.State)
	}
	if first.RunID != h.RunID || first.SessionID != "jq" {
		t.Fatalf("first record = {run:%q session:%q}, want run=%q session=jq", first.RunID, first.SessionID, h.RunID)
	}

	waitHandle(t, h)

	recs = jr.snapshot()
	wantStates := []string{string(RunQueued), string(RunRunning), string(RunCompleted)}
	if len(recs) != len(wantStates) {
		t.Fatalf("journal records = %d, want %d (queued/running/completed)", len(recs), len(wantStates))
	}
	for i, rec := range recs {
		if rec.RunID != h.RunID {
			t.Fatalf("record %d RunID = %q, want %q", i, rec.RunID, h.RunID)
		}
		if rec.State != wantStates[i] {
			t.Fatalf("record %d state = %q, want %q", i, rec.State, wantStates[i])
		}
	}
}

// TestStartRunRejectsWhenQueuedJournalFails(I1 修复验收,failing journal stub
// 故障注入):queued 接受记录持久化失败 → StartRun 拒绝启动——返回包裹
// ErrRunJournalWrite 与底层错误的链、零值 RunHandle,绝不产生「已接受但无法
// 追踪」的 Run。无痕断言四件套:
//   - journal 零记录(stub 注错不收;真 journal 形态下恢复扫描对无 state 记录的
//     run 亦跳过——失败发生在任何记录之前);
//   - active 已清退:同会话后续提交不再撞 ErrRunActive;
//   - pending 令牌已归还:WithRunLimits(1,1) 下 pending 容量恰为 1,若 ⑤ 失败
//     路径未归还会令牌,恢复后的下一次提交必撞 ErrTooManyPending——两项清退由
//     「恢复后同会话重提成功」联合证明(确定性,无计时断言);
//   - provider 未开工:被拒 run 的 execute 从未启动(goroutine 未创建,确定性),
//     恢复后的 run 才开工。
func TestStartRunRejectsWhenQueuedJournalFails(t *testing.T) {
	m := NewSessionManager(WithRunLimits(1, 1)) // pending 容量 1:令牌守恒的探针
	defer m.Close()

	jr := &stubRunJournal{}
	cause := errors.New("disk full")
	jr.appendErr = cause
	started := make(chan struct{})
	pv := &gatedProvider{
		replies: []string{"reply"},
		starts:  []chan struct{}{started},
		release: nil, // 不挂起:被调用即返回
	}
	s := NewSession(SessionDeps{
		Config:   runtimeconfig.Default(),
		AuditDir: "",
		CollectTools: func(*sandbox.Config) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store:      &fakeConversationStore{},
		RunJournal: jr,
	})
	rt := NewSessionRuntime("journal-reject", s)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 注错提交:拒绝 + 错误链(sentinel 与底层因果都可 errors.Is 判)+ 零句柄。
	h, err := m.StartRun("journal-reject", RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
	if !errors.Is(err, ErrRunJournalWrite) {
		t.Fatalf("StartRun with failing journal = %v, want ErrRunJournalWrite", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("StartRun error chain = %v, want wrap of underlying %v", err, cause)
	}
	if h != (RunHandle{}) {
		t.Fatalf("rejected StartRun returned handle %+v, want zero value(拒绝绝不携带句柄)", h)
	}

	// 无痕:journal 零记录、active 清退、provider 未开工(execute 未启动)。
	if recs := jr.snapshot(); len(recs) != 0 {
		t.Fatalf("journal records after rejected StartRun = %d, want 0(失败发生在任何记录之前)", len(recs))
	}
	if act := rt.Active(); act != nil {
		t.Fatalf("active after rejected StartRun = %+v, want nil(记账已无痕清退)", act)
	}
	select {
	case <-started:
		t.Fatal("provider started for a rejected run(execute 不得已启动)")
	case <-time.After(100 * time.Millisecond):
	}

	// 恢复 journal 后同会话重提成功 = active 清退(不撞 ErrRunActive)与 pending
	// 令牌归还(容量 1 不撞 ErrTooManyPending)的联合证明;随后走完正常生命周期。
	jr.appendErr = nil
	h2, err := m.StartRun("journal-reject", RunRequest{Input: "q2"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun after journal recovery = %v, want nil(failed write left residue: pending token or active)", err)
	}
	waitStarted(t, started)
	waitHandle(t, h2)
}

// TestStartRunRejectsWhenEventHubSeedingFails(第三轮评审 I3 修复验收,I1 断言
// 形态):EventJournal.SeqRange 播种(Journal 读取)失败 → ⑥ bindEvents/
// NewEventHub 失败 → StartRun 显式拒绝,pending/active 无痕清退——绝不带着
// 「看似健康」的 hub(旧形态把读取失败当空历史:replayIntegrity 报 complete 且
// seq 从 1 重起,与既有记录冲突)进入执行。无痕断言四件套(同
// TestStartRunRejectsWhenQueuedJournalFails):
//   - 错误链包裹底层原因(errors.Is),零值 RunHandle;
//   - active 已清退 + pending 令牌已归还:恢复播种后同会话重提成功(确定性,
//     容量 1 探针);
//   - provider 未开工:被拒 run 的 execute 从未启动;
//   - journal 恰含 ⑤ 的 queued 接受记录、无终态/事件记录(接受点在先,残留的
//     queued 记录由恢复扫描按「无终态」诚实标 interrupted)。
func TestStartRunRejectsWhenEventHubSeedingFails(t *testing.T) {
	m := NewSessionManager(WithRunLimits(1, 1)) // pending 容量 1:令牌守恒的探针
	defer m.Close()

	sj := &stubRunJournal{}
	cause := errors.New("runs dir unreadable")
	ej := &stubEventJournal{seqRangeErr: cause}
	started := make(chan struct{})
	pv := &gatedProvider{
		replies: []string{"reply"},
		starts:  []chan struct{}{started},
		release: nil, // 不挂起:被调用即返回
	}
	s := NewSession(SessionDeps{
		Config:   runtimeconfig.Default(),
		AuditDir: "",
		CollectTools: func(*sandbox.Config) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store:      &fakeConversationStore{},
		RunJournal: sj,
		RunEvents:  ej,
	})
	rt := NewSessionRuntime("hub-seed-reject", s)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 注错提交:拒绝 + 错误链 + 零句柄。
	h, err := m.StartRun("hub-seed-reject", RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
	if err == nil {
		t.Fatal("StartRun with failing hub seeding = nil, want error(I3:绝不返回看似健康的 run)")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("StartRun error chain = %v, want wrap of underlying %v", err, cause)
	}
	if h != (RunHandle{}) {
		t.Fatalf("rejected StartRun returned handle %+v, want zero value(拒绝绝不携带句柄)", h)
	}

	// 无痕:active 清退、provider 未开工(execute 未启动)、journal 无终态/事件
	// 记录(queued 接受记录已在,见上)。
	if act := rt.Active(); act != nil {
		t.Fatalf("active after rejected StartRun = %+v, want nil(记账已无痕清退)", act)
	}
	select {
	case <-started:
		t.Fatal("provider started for a rejected run(execute 不得已启动)")
	case <-time.After(100 * time.Millisecond):
	}
	recs := sj.snapshot()
	if len(recs) != 1 || recs[0].State != string(RunQueued) {
		t.Fatalf("journal records after rejected StartRun = %+v, want exactly the queued accept record(无终态/事件记录)", recs)
	}

	// 恢复播种后同会话重提成功 = active 清退(不撞 ErrRunActive)与 pending 令牌
	// 归还(容量 1 不撞 ErrTooManyPending)的联合证明;随后走完正常生命周期。
	ej.seqRangeErr = nil
	h2, err := m.StartRun("hub-seed-reject", RunRequest{Input: "q2"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun after seeding recovery = %v, want nil(failed wiring left residue: pending token or active)", err)
	}
	waitStarted(t, started)
	waitHandle(t, h2)
}

// ---------------------------------------------------------------------------
// P2-6:CancelRun + 排队取消路径(总方案第十一节 TestCancelIsolation → 本步)。
//
// plan P2-6「取消相关 stub provider 必须 select ctx.Done 及时返回」:gatedProvider
// 的挂起不感知 ctx,取消后仍要等 release 才返回,无法证明取消及时生效——取消
// 测试专用 cancelAwareProvider(单轮使用,每测试独立实例)。
// ---------------------------------------------------------------------------

// cancelAwareProvider 是取消测试专用的单轮 stub provider:开工即宣告 started,
// 随后 select 挂起在 release 与 ctx.Done 上——release 关闭则正常返回(内容为
// reply,空则回落 "reply"),ctx 取消则立即返回 ctx.Err()。每个实例只服务一轮
// Chat(关闭已关闭的 started 会 panic,单轮构造即契约)。
type cancelAwareProvider struct {
	started chan struct{}
	release chan struct{}
	reply   string
}

func (p *cancelAwareProvider) Name() string  { return "cancel-aware" }
func (p *cancelAwareProvider) Model() string { return "m" }

func (p *cancelAwareProvider) Chat(ctx context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	close(p.started)
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	content := p.reply
	if content == "" {
		content = "reply"
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: content}}}}, nil
}

func (p *cancelAwareProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// newCancelRuntime 构造带 cancelAwareProvider 的 runtime 并注册进 manager:started
// 在 provider 开工时关闭;release 关闭后 provider 正常返回,ctx 取消则立即返回。
func newCancelRuntime(t *testing.T, m *SessionManager, id, reply string) (st *fakeConversationStore, started, release chan struct{}) {
	t.Helper()
	started = make(chan struct{})
	release = make(chan struct{})
	pv := &cancelAwareProvider{started: started, release: release, reply: reply}
	st = &fakeConversationStore{}
	rt := NewSessionRuntime(id, newFakeStoreSession(st, nil, pv))
	if err := m.Register(rt); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
	return st, started, release
}

// waitCancelled 等待句柄到达终态并断言 RunCancelled(与 waitHandle 的完成断言
// 对偶;Err/Persisted 由各测试按路径自行断言)。
func waitCancelled(t *testing.T, h RunHandle) RunResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := h.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v (state=%s)", err, h.State())
	}
	if res.State != RunCancelled {
		t.Fatalf("run result = %+v, want RunCancelled", res)
	}
	return res
}

// TestCancelIsolation(总方案第十一节 → 本步):WithRunLimits(1, 8)——A 挂起
// 占住唯一全局槽,B StartRun 排队(queued)后取消 B:B.Wait = RunCancelled、B 的
// provider 从未被调用(store 零提交、排队取消直接释放占用权);A release 后照常
// RunCompleted+Persisted 且 store 提交完整——A 不受 B 取消影响。
func TestCancelIsolation(t *testing.T) {
	m := NewSessionManager(WithRunLimits(1, 8))
	defer m.Close()
	stA, startedA, releaseA := newCancelRuntime(t, m, "session-A", "reply-A")
	stB, _, _ := newCancelRuntime(t, m, "session-B", "reply-B") // B 排队取消:provider 不会被调用

	hA, err := m.StartRun("session-A", RunRequest{Input: "q-A"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun A: %v", err)
	}
	waitStarted(t, startedA) // A 占住唯一 runSlots 槽

	hB, err := m.StartRun("session-B", RunRequest{Input: "q-B"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun B while slot held: %v", err)
	}
	if got := hB.State(); got != RunQueued {
		t.Fatalf("B state right after StartRun = %q, want RunQueued", got)
	}

	if err := m.CancelRun("session-B", hB.RunID); err != nil {
		t.Fatalf("CancelRun queued B: %v", err)
	}
	resB := waitCancelled(t, hB)
	if resB.Persisted {
		t.Fatalf("cancelled queued run B persisted = true, want false: %+v", resB)
	}
	// 排队取消,B 从未执行:store 零提交。
	stB.mu.Lock()
	bCommits := len(stB.commits)
	stB.mu.Unlock()
	if bCommits != 0 {
		t.Fatalf("cancelled queued run B committed %d time(s), want 0 (B 未执行)", bCommits)
	}

	close(releaseA) // A 不受 B 取消影响,放行后照常完成
	waitHandle(t, hA)
	assertSingleCommit(t, stA, "q-A", "reply-A")
}

// TestCancelRunningRun:运行中(A 的 provider 挂起中)Cancel → RunCancelled;
// journal 记录流为 queued → running → cancelled(终态记录 state=="cancelled",
// 取消绝不记 failed/completed)。
func TestCancelRunningRun(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	jr := &stubRunJournal{}
	started := make(chan struct{})
	release := make(chan struct{}) // 不关闭:取消是本测试唯一出口
	pv := &cancelAwareProvider{started: started, release: release}
	s := NewSession(SessionDeps{
		Config:   runtimeconfig.Default(),
		AuditDir: "",
		CollectTools: func(*sandbox.Config) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store:      &fakeConversationStore{},
		RunJournal: jr,
	})
	rt := NewSessionRuntime("cancel-running", s)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}

	h, err := m.StartRun("cancel-running", RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitStarted(t, started) // provider 挂起中:run 正在运行

	if err := m.CancelRun("cancel-running", h.RunID); err != nil {
		t.Fatalf("CancelRun running: %v", err)
	}
	waitCancelled(t, h)

	recs := jr.snapshot()
	wantStates := []string{string(RunQueued), string(RunRunning), string(RunCancelled)}
	if len(recs) != len(wantStates) {
		t.Fatalf("journal records = %d, want %d (%s → %s → %s)", len(recs), len(wantStates), wantStates[0], wantStates[1], wantStates[2])
	}
	for i, rec := range recs {
		if rec.RunID != h.RunID {
			t.Fatalf("record %d RunID = %q, want %q", i, rec.RunID, h.RunID)
		}
		if rec.State != wantStates[i] {
			t.Fatalf("record %d state = %q, want %q (终态必须是 cancelled)", i, rec.State, wantStates[i])
		}
	}
}

// TestCancelUnknownRun:ErrRunNotFound 的三个产生分支——未知会话、已知会话但
// RunID 不匹配、无在途 Run(空闲会话)。
func TestCancelUnknownRun(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	_, started, release := newCancelRuntime(t, m, "known", "reply")
	newCancelRuntime(t, m, "idle", "reply") // 从未运行:无在途 Run

	h, err := m.StartRun("known", RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitStarted(t, started)

	if err := m.CancelRun("no-such-session", h.RunID); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("CancelRun unknown session = %v, want ErrRunNotFound", err)
	}
	// 已知会话但在途 Run 的 RunID 不匹配。
	if err := m.CancelRun("known", "run-bogus"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("CancelRun mismatched run id = %v, want ErrRunNotFound", err)
	}
	// 空闲会话(Active() 为 nil)。
	if err := m.CancelRun("idle", "run-whatever"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("CancelRun idle session = %v, want ErrRunNotFound", err)
	}

	close(release)
	waitHandle(t, h)
}

// TestCancelSyncRunRejected:同步路径(RecordRun)在途时 CancelRun 拒绝
// (ErrRunNotCancellable)——同步 Run 无 cancel,取消入口是调用方请求 ctx,
// CancelRun 不得误伤;随后同步 Run 照常完成。
func TestCancelSyncRunRejected(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	pv := &gatedProvider{
		replies: []string{"reply"},
		starts:  []chan struct{}{started},
		release: []chan struct{}{release},
	}
	rt := NewSessionRuntime("sync", newFakeStoreSession(&fakeConversationStore{}, nil, pv))
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}

	done := make(chan RunResult, 1)
	go func() {
		done <- rt.RecordRun(func(s *Session) (RunResult, error) {
			return s.Run(context.Background(), RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
		})
	}()
	waitStarted(t, started)

	run := rt.Active()
	if run == nil {
		t.Fatal("no active run while sync RecordRun in flight")
	}
	if err := m.CancelRun("sync", run.ID); !errors.Is(err, ErrRunNotCancellable) {
		t.Fatalf("CancelRun sync run = %v, want ErrRunNotCancellable", err)
	}

	close(release) // 同步 Run 不受影响,照常完成
	waitRunResult(t, done)
}

// ---------------------------------------------------------------------------
// P3-4:supervisor 事件接线(总方案第十一节 TestSlowSubscriber → 本步)。
//
// 测试装置:scriptedStreamProvider(流式脚本)+ eventRunFixture(真 Repository
// → 真 *session.Store → RunJournalStore 同时充当 RunJournal 与 RunEvents 注入
// SessionDeps)——「StartRun 全链」= manager → runtime → Session → ai-core
// agent(kernel 消费流式 provider)→ streamTokens/中间件 → tee → EventHub →
// RunJournalStore 落盘。
// ---------------------------------------------------------------------------

// scriptedStreamProvider 是流式 stub provider:第 i 次模型调用回放 rounds[i]
// 脚本(向 ch 发增量;流式重组契约:末 token 携带 Response,ai-core agent 的
// chat() 据此取最终响应并剥掉增量上的 Done,agent.go:191-208)。Chat 恒报错:
// ChatStream 可用即不回退非流式。单轮脚本是普通函数,可闭包测试通道做编排
// (TestSlowSubscriber 的 started/proceed)。
type scriptedStreamProvider struct {
	mu     sync.Mutex
	calls  int
	rounds []func(ch chan<- core.Token)
}

func (p *scriptedStreamProvider) Name() string  { return "scripted-stream" }
func (p *scriptedStreamProvider) Model() string { return "m" }

func (p *scriptedStreamProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("not implemented")
}

func (p *scriptedStreamProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	p.mu.Lock()
	i := p.calls
	p.calls++
	p.mu.Unlock()
	ch := make(chan core.Token)
	go func() {
		defer close(ch)
		if i < len(p.rounds) {
			p.rounds[i](ch)
		}
	}()
	return ch, nil
}

// eventRunFixture 是「真 journal 全链」测试装置:临时目录 Repository → Create
// 出真 *session.Store → RunJournalStore 同时作为 RunJournal(state 记录)与
// RunEvents(事件记录)注入 SessionDeps——两类记录同文件混存
// (runs/<runID>.jsonl)。runtime 以 store 的 SessionID 注册,StartRun 按它寻址。
type eventRunFixture struct {
	root      string
	sessionID string
	journal   *session.RunJournalStore
}

// runJournalPath 返回一个 run 的 journal 文件路径(<root>/<sessionID>/runs/<runID>.jsonl)。
func (fx eventRunFixture) runJournalPath(runID string) string {
	return filepath.Join(fx.root, fx.sessionID, "runs", runID+".jsonl")
}

// newEventRunRuntime 构造装置并注册进 manager;repo 经 t.Cleanup 收尾。
func newEventRunRuntime(t *testing.T, m *SessionManager, pv core.Provider) eventRunFixture {
	t.Helper()
	root := t.TempDir()
	repo, err := session.OpenRepository(root)
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	st, err := repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatalf("create session store: %v", err)
	}
	jr := session.NewRunJournalStore(repo, st)
	s := NewSession(SessionDeps{
		Config:   runtimeconfig.Default(),
		AuditDir: "",
		CollectTools: func(*sandbox.Config) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store:      st,
		RunJournal: jr,
		RunEvents:  jr,
	})
	rt := NewSessionRuntime(st.SessionID(), s)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}
	return eventRunFixture{root: root, sessionID: st.SessionID(), journal: jr}
}

// eventRunCallbacks 是启用事件面的最小 cb:EmitToken/Publish 非 nil(tee 只
// 包装非 nil 入口,未提供的入口不产生事件),输出丢弃。
func eventRunCallbacks() RunCallbacks {
	return RunCallbacks{
		Write:     func(string) {},
		EmitToken: func(core.Token) {},
		Publish:   func(string, any) {},
	}
}

// TestSlowSubscriber(总方案第十一节 → 本步):StartRun 全链,provider 挂起中
// 注册订阅者、放行后灌大量 EmitToken,慢订阅者(Subscribe 后不读)被背压断开:
//
//	① 该订阅者通道最终被关闭——其 ctx 从不取消、hub 从不 Close,关闭只能来自
//	  fan-out 满缓冲断开(pump 经 hubSub.dead 从阻塞投递中感知断开并退出);
//	② Publish/execute 不阻塞:run 正常 RunCompleted+Persisted,且耗时在上界内
//	  (若 tee/hub 阻塞执行,Wait 超时或耗时断言失败);
//	③ 其他正常消费的订阅者收全事件:seq 恰为 1..N 无缺口无重复,且含
//	  assistant.message_finalized(key 事件经真 journal durable-first 后广播)。
func TestSlowSubscriber(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	const flood = 800 // > 投递管线总缓冲(ch 256 + live 256 + 在手):断开必然触发
	started := make(chan struct{})
	proceed := make(chan struct{})
	var openGate sync.Once
	defer openGate.Do(func() { close(proceed) }) // 失败路径先放行 provider,防 m.Close 死等(后注册先执行)
	pv := &scriptedStreamProvider{rounds: []func(ch chan<- core.Token){func(ch chan<- core.Token) {
		close(started)
		<-proceed // 挂起:订阅窗口(订阅者先于洪流注册,洪流前至多 1 条 iteration delta)
		for i := 0; i < flood; i++ {
			ch <- core.Token{Type: core.TokenTypeText, Content: fmt.Sprintf("delta-%04d", i)}
		}
		ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
			Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "reply"}}},
		}}
	}}}
	fx := newEventRunRuntime(t, m, pv)

	h, err := m.StartRun(fx.sessionID, RunRequest{Input: "q"}, eventRunCallbacks())
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitStarted(t, started) // provider 挂起中:run 在跑

	hub := m.Get(fx.sessionID).eventHub()
	if hub == nil {
		t.Fatal("event hub not created for RunEvents-enabled session")
	}

	// ① 前置:慢订阅者 ctx 恒不取消(Background)——通道关闭只能来自断开。
	slowCh, err := hub.Subscribe(context.Background(), 0)
	if err != nil {
		t.Fatalf("Subscribe slow: %v", err)
	}
	// ③ 前置:健康订阅者 + 并发读 goroutine(洪流期间持续消费,收集进大缓冲;
	// 无读者一样会被背压断开——那正是慢订阅者路径)。4096 缓冲远大于事件总量,
	// 读者永不阻塞,健康订阅者不得被断开。
	healthyCtx, cancelHealthy := context.WithCancel(context.Background())
	defer cancelHealthy()
	healthyCh, err := hub.Subscribe(healthyCtx, 0)
	if err != nil {
		t.Fatalf("Subscribe healthy: %v", err)
	}
	healthyColl := make(chan session.SessionEvent, 4096)
	healthyDone := make(chan struct{})
	go func() {
		defer close(healthyDone)
		for ev := range healthyCh {
			healthyColl <- ev
		}
		close(healthyColl)
	}()

	begin := time.Now()
	openGate.Do(func() { close(proceed) }) // 放行洪流
	waitHandle(t, h)                       // ② run 正常终态(5s 上界,阻塞即失败)
	if elapsed := time.Since(begin); elapsed > 10*time.Second {
		t.Fatalf("run took %s with a non-reading subscriber, want < 10s(Publish/execute 不得被慢订阅者阻塞)", elapsed)
	}

	// ① 排干慢订阅者:见到关闭才算断开送达消费方。缓冲上界 = subscriberBuffer
	// (pump 投递通道),收到更多即断开未生效。
	var slowSeqs []uint64
	slowClosed := false
	slowDeadline := time.After(5 * time.Second)
	for !slowClosed {
		select {
		case ev, ok := <-slowCh:
			if !ok {
				slowClosed = true
				break
			}
			slowSeqs = append(slowSeqs, ev.Seq)
		case <-slowDeadline:
			t.Fatalf("slow subscriber channel never closed (got %d events)", len(slowSeqs))
		}
	}
	if len(slowSeqs) == 0 || len(slowSeqs) > subscriberBuffer {
		t.Fatalf("slow subscriber received %d events before close, want (0, %d](断开前缓冲上界)", len(slowSeqs), subscriberBuffer)
	}
	for i := 1; i < len(slowSeqs); i++ {
		if slowSeqs[i] <= slowSeqs[i-1] {
			t.Fatalf("slow subscriber seqs not strictly increasing at #%d: %v", i, slowSeqs)
		}
	}

	// ③ 健康订阅者收全事件:发布在 run 终态前全部完成(done 关闭的 happens-before),
	// 此刻 LatestSeq 即最终 N。
	want := hub.LatestSeq()
	if want <= flood {
		t.Fatalf("LatestSeq = %d, want > flood %d(事件面未生效)", want, flood)
	}
	var healthy []session.SessionEvent
	healthyDeadline := time.After(5 * time.Second)
	for uint64(len(healthy)) < want {
		select {
		case ev := <-healthyColl:
			healthy = append(healthy, ev)
		case <-healthyDeadline:
			t.Fatalf("healthy subscriber got %d/%d events", len(healthy), want)
		}
	}
	hasFinalized := false
	for i, ev := range healthy {
		if ev.Seq != uint64(i+1) {
			t.Fatalf("healthy subscriber seq at #%d = %d, want %d(恰为 1..%d 无缺口无重复)", i, ev.Seq, i+1, want)
		}
		if ev.Type == session.EventAssistantMessageFinalized {
			hasFinalized = true
		}
	}
	if !hasFinalized {
		t.Fatalf("healthy subscriber missed %s event", session.EventAssistantMessageFinalized)
	}

	// 健康订阅者收尾:ctx 取消 → pump 退出 → 通道关闭 → 读 goroutine 收尾
	// (无多余事件)。
	cancelHealthy()
	select {
	case ev, ok := <-healthyColl:
		if ok {
			t.Fatalf("unexpected extra event after cancel: seq %d", ev.Seq)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("healthy subscriber collector not closed after ctx cancel")
	}
	select {
	case <-healthyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("healthy reader goroutine did not exit after cancel")
	}
}

// TestStartRunEmitsJournalEvents(P3-4):RunEvents=真 RunJournalStore → 跑一轮
// 带工具循环的流式 StartRun → ReadEvents(0) 断言:
//   - key 事件 durable-first 落盘:tool_call/tool_result/assistant.message_finalized
//     各恰一条、FromSeq=0(逐事件)、RunID/SessionID 同源;
//   - delta 呈合并记录:每条 delta 记录带 [FromSeq..Seq] 合并范围,且持久化
//     记录数严格少于实时事件数(同型增量收进单条合并记录);
//   - Seq 全局连续无重复:全部记录的 [FromSeq..Seq](逐条即 [Seq..Seq])区间
//     恰好铺满 [1..N],无缺口、无重叠。
//   - state 与事件同文件混存:raw 文件恰 3 条 state(queued/running/completed)
//   - 全部事件记录,首行 queued、末行 completed(事件先于终态落盘的次序铁律)。
func TestStartRunEmitsJournalEvents(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	pv := &scriptedStreamProvider{rounds: []func(ch chan<- core.Token){
		// 第 0 轮:思考增量 + 文本增量 + 携带 tool call 的重组响应。工具未注册
		// → kernel 产出 "unknown tool" 结果,tool_call/tool_result token 照常
		// 产出(ai-core agent.go:123-160),无 HITL 参与(未知工具不进审批)。
		func(ch chan<- core.Token) {
			ch <- core.Token{Type: core.TokenTypeThinking, Content: "ponder"}
			ch <- core.Token{Type: core.TokenTypeText, Content: "need a tool"}
			ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
				Choices: []core.ResponseChoice{{Message: core.Message{
					Role: "assistant",
					ToolCalls: []core.ToolCall{{
						ID:       "call-1",
						Type:     "function",
						Function: core.ToolCallFunction{Name: "missing_tool", Arguments: `{"x":"1"}`},
					}},
				}}},
			}}
		},
		// 第 1 轮:40 条背靠背文本增量(相邻发布间隔 µs 级,合并窗必然收进
		// 同一条记录)→ 纯文本终答(流结束 → kernel 收尾 Done token)。
		func(ch chan<- core.Token) {
			for i := 0; i < 40; i++ {
				ch <- core.Token{Type: core.TokenTypeText, Content: fmt.Sprintf("chunk-%02d", i)}
			}
			ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
				Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "final answer"}}},
			}}
		},
	}}
	fx := newEventRunRuntime(t, m, pv)

	h, err := m.StartRun(fx.sessionID, RunRequest{Input: "q"}, eventRunCallbacks())
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitHandle(t, h)

	if rt := m.Get(fx.sessionID); rt.eventHub() == nil {
		t.Fatal("event hub not bound after StartRun with RunEvents enabled")
	}

	recs, err := fx.journal.ReadEvents(0)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no event records persisted")
	}
	var (
		keyCounts    = map[string]int{}
		deltaEvents  int // 合并范围展开后的实时事件数
		deltaRecords int
	)
	type seqRange struct {
		lo, hi uint64
		typ    string
	}
	ranges := make([]seqRange, 0, len(recs))
	for _, rec := range recs {
		if rec.RunID != h.RunID {
			t.Fatalf("record seq %d RunID = %q, want %q", rec.Seq, rec.RunID, h.RunID)
		}
		if rec.SessionID != fx.sessionID {
			t.Fatalf("record seq %d SessionID = %q, want %q", rec.Seq, rec.SessionID, fx.sessionID)
		}
		lo := rec.Seq
		if rec.FromSeq > 0 && rec.FromSeq < lo {
			lo = rec.FromSeq
		}
		ranges = append(ranges, seqRange{lo: lo, hi: rec.Seq, typ: rec.Type})
		if session.IsKeyEvent(rec.Type) {
			if rec.FromSeq != 0 {
				t.Fatalf("key event %s seq %d has FromSeq %d, want 0(key 事件逐事件落盘)", rec.Type, rec.Seq, rec.FromSeq)
			}
			keyCounts[rec.Type]++
			continue
		}
		if rec.FromSeq == 0 {
			t.Fatalf("delta %s seq %d persisted without merge range (delta 必须呈合并记录)", rec.Type, rec.Seq)
		}
		deltaRecords++
		deltaEvents += int(rec.Seq - rec.FromSeq + 1)
	}
	// Seq 全局连续无重复(P3-4 验收口径,记录面的可观测不变量):
	//   - 连续:全部记录的 [lo..Seq] 区间并集覆盖 [1..N] 无缺口;
	//   - 无重复:记录 Seq(=ToSeq,回放侧实际投递的游标值)两两不同。
	// 注意**不**断言区间两两不交:合并按 Type 分桶(plan P3-2「deltaBuf key=
	// type,同型合并」),同型合并范围可横跨其他类型事件的 seq(如 usage 合并
	// [1..6] 覆盖了 seq 2/3 的 thinking/text)——FromSeq 是范围声明而非所有权
	// (P3-1 契约「FromSeq 只作范围声明」;TestAttachReplayNoGap 的覆盖记账即
	// 同款语义)。每 seq 的所有权唯一由 hub 单调分配保证,不可从记录面观测。
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].lo < ranges[j].lo })
	seenSeq := map[uint64]string{}
	var cursor uint64
	for _, rg := range ranges {
		if rg.lo > cursor+1 {
			t.Fatalf("seq gap: range [%d..%d] (%s) starts after cursor %d(Seq 全局连续)", rg.lo, rg.hi, rg.typ, cursor)
		}
		if prev, dup := seenSeq[rg.hi]; dup {
			t.Fatalf("record seq %d duplicated (%s and %s)(Seq 无重复)", rg.hi, prev, rg.typ)
		}
		seenSeq[rg.hi] = rg.typ
		if rg.hi > cursor {
			cursor = rg.hi
		}
	}
	for typ, want := range map[string]int{
		session.EventToolCall:                  1,
		session.EventToolResult:                1,
		session.EventAssistantMessageFinalized: 1,
	} {
		if keyCounts[typ] != want {
			t.Fatalf("key event %s count = %d, want %d", typ, keyCounts[typ], want)
		}
	}
	if cursor < 45 {
		t.Fatalf("final seq = %d, want >= 45(41 text + 1 thinking + usage/iteration + 3 key,delta 面未生效)", cursor)
	}
	if deltaRecords >= deltaEvents {
		t.Fatalf("delta records = %d, delta events = %d, want records < events(合并窗收拢失败)", deltaRecords, deltaEvents)
	}

	// state 与事件同文件混存:恰 3 条 state(queued/running/completed)+ 全部
	// 事件记录;首行 queued、末行 completed——事件收口(显式 EndRun)先于终态
	// 落盘的次序铁律在文件序上可见。
	data, err := os.ReadFile(fx.runJournalPath(h.RunID))
	if err != nil {
		t.Fatalf("read run journal: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3+len(recs) {
		t.Fatalf("journal lines = %d, want %d (3 state + %d event records)", len(lines), 3+len(recs), len(recs))
	}
	var probe struct {
		Kind  string `json:"kind"`
		State string `json:"state"`
	}
	decodeKind := func(line string) (string, string) {
		if err := json.Unmarshal([]byte(line), &probe); err != nil {
			t.Fatalf("decode journal line %q: %v", line, err)
		}
		return probe.Kind, probe.State
	}
	if k, s := decodeKind(lines[0]); k != "state" || s != string(RunQueued) {
		t.Fatalf("first line = {kind:%s state:%s}, want {state, queued}", k, s)
	}
	if k, s := decodeKind(lines[len(lines)-1]); k != "state" || s != string(RunCompleted) {
		t.Fatalf("last line = {kind:%s state:%s}, want {state, completed}(终态必须最后落盘)", k, s)
	}
	hasRunning, hasEvent := false, false
	for _, line := range lines[1 : len(lines)-1] {
		k, s := decodeKind(line)
		switch {
		case k == "state" && s == string(RunRunning):
			hasRunning = true
		case k == "event":
			hasEvent = true
		}
	}
	if !hasRunning || !hasEvent {
		t.Fatalf("middle lines missing running state (%v) or event records (%v) — 同文件混存不成立", hasRunning, hasEvent)
	}
}

// TestStartRunJournalsRealTokens(C2 修复验收):daemon run/start 的生产提交形态
// ——RunCallbacks{} 全 nil(runcontrol.go StartRun)——下,真实 Token 流仍进事件
// journal。C2 修复前 teeRunCallbacks 只包装非 nil 入口,streamTokens/中间件对
// nil 回调直接跳过(session.go 的 nil 检查),后台 Run 的 text.delta/tool_call/
// assistant.message_finalized 全部丢失,Re-Attach 只见 run.* 生命周期事件;修复
// 后 tee 在 hub 非 nil 时对 nil 的 EmitToken/Publish 主动装上事件映射版。
//
// 装置与 TestStartRunEmitsJournalEvents 同款真 journal 全链(manager → runtime →
// Session → ai-core kernel → streamTokens → tee → hub → RunJournalStore 落盘),
// 差异仅在提交回调:全 nil。断言(ReadEvents(0) 全量回放):
//   - 含 text.delta(两轮模型的文本增量)、tool_call(未注册工具的调用,ai-core
//     kernel 产出)与 assistant.message_finalized(kernel 收尾空 content Done
//     token,恰 1 条——ai-core 剥掉增量 Done,agent.go chat());
//   - tool_call 载荷可还原 token 全貌(payload=整个 core.Token 的 JSON,P3-4 表,
//     含结构化 ToolCall 关联);
//   - 全部记录 Seq 连续无重复:记录的 [FromSeq..Seq] 区间并集铺满 [1..N]
//     (FromSeq 是范围声明非所有权,区间可横跨其他类型事件的 seq——与
//     TestStartRunEmitsJournalEvents 的覆盖记账同款语义,不断言两两不交)。
func TestStartRunJournalsRealTokens(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	pv := &scriptedStreamProvider{rounds: []func(ch chan<- core.Token){
		// 第 0 轮:文本增量 + 携带 tool call 的重组响应(工具未注册 → kernel 产
		// 出 tool_call/tool_result token,无 HITL 参与——未知工具不进审批)。
		func(ch chan<- core.Token) {
			ch <- core.Token{Type: core.TokenTypeText, Content: "need a tool"}
			ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
				Choices: []core.ResponseChoice{{Message: core.Message{
					Role: "assistant",
					ToolCalls: []core.ToolCall{{
						ID:       "call-1",
						Type:     "function",
						Function: core.ToolCallFunction{Name: "missing_tool", Arguments: `{"x":"1"}`},
					}},
				}}},
			}}
		},
		// 第 1 轮:终答增量 + 纯文本响应(kernel 收尾发空 content Done)。
		func(ch chan<- core.Token) {
			ch <- core.Token{Type: core.TokenTypeText, Content: "final answer"}
			ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
				Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "final answer"}}},
			}}
		},
	}}
	fx := newEventRunRuntime(t, m, pv)

	// daemon run/start 的提交形态:全 nil 回调(C2 的生产入口形状——修复前本
	// 测试失败,三类 token 派生事件全部缺席)。
	h, err := m.StartRun(fx.sessionID, RunRequest{Input: "q"}, RunCallbacks{})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitHandle(t, h)

	recs, err := fx.journal.ReadEvents(0)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no event records persisted")
	}
	var (
		hasTextDelta    bool
		hasToolCall     bool
		hasToolResult   bool
		finalizedCount  int
		toolCallPayload []byte
	)
	for _, rec := range recs {
		if rec.RunID != h.RunID {
			t.Fatalf("record seq %d RunID = %q, want %q", rec.Seq, rec.RunID, h.RunID)
		}
		switch rec.Type {
		case session.EventTextDelta:
			hasTextDelta = true
		case session.EventToolCall:
			hasToolCall = true
			toolCallPayload = rec.Payload
		case session.EventToolResult:
			hasToolResult = true
		case session.EventAssistantMessageFinalized:
			finalizedCount++
		}
	}
	if !hasTextDelta {
		t.Fatal("journal missing text.delta(后台 Run 的真实 token 未进事件流,C2)")
	}
	if !hasToolCall || !hasToolResult {
		t.Fatalf("journal tool_call=%v tool_result=%v, want both(后台 Run 的工具调用未进事件流,C2)", hasToolCall, hasToolResult)
	}
	if finalizedCount != 1 {
		t.Fatalf("assistant.message_finalized count = %d, want 1(kernel 收尾 Done token 恰一条)", finalizedCount)
	}
	// tool_call 载荷还原 token 全貌:结构化 ToolCall 关联可回放(P3-4 表)。
	var tok core.Token
	if err := json.Unmarshal(toolCallPayload, &tok); err != nil {
		t.Fatalf("decode tool_call payload %s: %v", toolCallPayload, err)
	}
	if tok.ToolCall.Function.Name != "missing_tool" || tok.ToolCall.ID != "call-1" {
		t.Fatalf("tool_call payload = %s, want structured ToolCall(missing_tool/call-1)", toolCallPayload)
	}

	// Seq 连续无重复(记录面的可观测不变量,TestStartRunEmitsJournalEvents 同款
	// 覆盖记账):记录的 [FromSeq..Seq] 区间并集铺满 [1..N],记录 Seq 两两不同。
	type seqRange struct {
		lo, hi uint64
		typ    string
	}
	ranges := make([]seqRange, 0, len(recs))
	for _, rec := range recs {
		lo := rec.Seq
		if rec.FromSeq > 0 && rec.FromSeq < lo {
			lo = rec.FromSeq
		}
		ranges = append(ranges, seqRange{lo: lo, hi: rec.Seq, typ: rec.Type})
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].lo < ranges[j].lo })
	seenSeq := map[uint64]string{}
	var cursor uint64
	for _, rg := range ranges {
		if rg.lo > cursor+1 {
			t.Fatalf("seq gap: range [%d..%d] (%s) starts after cursor %d(Seq 全局连续)", rg.lo, rg.hi, rg.typ, cursor)
		}
		if prev, dup := seenSeq[rg.hi]; dup {
			t.Fatalf("record seq %d duplicated (%s and %s)(Seq 无重复)", rg.hi, prev, rg.typ)
		}
		seenSeq[rg.hi] = rg.typ
		if rg.hi > cursor {
			cursor = rg.hi
		}
	}
	if cursor < 6 {
		t.Fatalf("final seq = %d, want >= 6(2 轮 token + tool_call/result + 收尾 + usage/iteration + 生命周期,token 面未生效)", cursor)
	}
}

// ---------------------------------------------------------------------------
// P5-1:WithRunContext 注入机制(总方案 §6 风险 2,sessionGateKey 死锁契约的
// SDK 侧透传证明)。
// ---------------------------------------------------------------------------

// TestStartRunInjectsRunContextValues:WithRunContext 声明的键值对随 StartRun
// 派生的 runCtx 透传,在 Session.Run 内的两个读点可见:
//   - ToolSource 探测点(session.go:128,Session.Run 以 runCtx 直接调用
//     req.ToolSource(ctx));
//   - 工具 Execute 读点(ai-core 的 toolCtx := context.WithTimeout(agentCtx,
//     60s)(ai-core/agent/agent.go:147),值沿 runCtx → agentCtx(WithCancel)→
//     toolCtx 的 ctx 链传播)——后者是 P5-1 死锁契约的关键读点:hub 工具的嵌套
//     Eval(daemon commands.go withSession 的 ctx.Value(sessionGateKey{}))就
//     发生在 Execute ctx 上。
func TestStartRunInjectsRunContextValues(t *testing.T) {
	type probeKeyA struct{}
	type probeKeyB struct{}

	m := NewSessionManager(WithRunContext(
		RunContextValue{Key: probeKeyA{}, Value: "value-A"},
		RunContextValue{Key: probeKeyB{}, Value: "value-B"},
	))
	defer m.Close()

	// scripted provider:第 0 轮发起 probe_tool 工具调用,第 1 轮终答(与
	// TestStartRunEmitsJournalEvents 同型——工具调用经流式重组响应携带)。
	pv := &scriptedStreamProvider{rounds: []func(ch chan<- core.Token){
		func(ch chan<- core.Token) {
			ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
				Choices: []core.ResponseChoice{{Message: core.Message{
					Role: "assistant",
					ToolCalls: []core.ToolCall{{
						ID:       "call-1",
						Type:     "function",
						Function: core.ToolCallFunction{Name: "probe_tool", Arguments: `{}`},
					}},
				}}},
			}}
		},
		func(ch chan<- core.Token) {
			ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
				Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "final"}}}},
			}
		},
	}}
	s := NewSession(SessionDeps{
		Config:   runtimeconfig.Default(),
		AuditDir: "",
		CollectTools: func(*sandbox.Config) []core.Tool {
			return nil
		},
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store: &fakeConversationStore{},
	})
	rt := NewSessionRuntime("ctx-inject", s)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 两个读点的命中记录:写点分别在 execute goroutine(ToolSource 探测)与
	// agent goroutine(工具 Execute),读取在 waitHandle(run.done 关闭)之后
	// ——happens-before 齐备,mu 仅为 -race 明示。
	type probeHit struct {
		where string
		a, b  any
	}
	var (
		mu   sync.Mutex
		hits []probeHit
	)
	record := func(where string, ctx context.Context) {
		mu.Lock()
		defer mu.Unlock()
		hits = append(hits, probeHit{where: where, a: ctx.Value(probeKeyA{}), b: ctx.Value(probeKeyB{})})
	}

	req := RunRequest{Input: "q"}
	req.ToolSource = func(ctx context.Context) []core.Tool {
		record("toolsource", ctx)
		return []core.Tool{{
			Name:        "probe_tool",
			Description: "ctx probe",
			Parameters:  NormalizeSchema(nil),
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				record("execute", ctx)
				return "probe-ok", nil
			},
		}}
	}

	h, err := m.StartRun("ctx-inject", req, testRunCallbacks(&strings.Builder{}))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitHandle(t, h)

	mu.Lock()
	defer mu.Unlock()
	if len(hits) != 2 {
		t.Fatalf("probe hits = %d (%+v), want 2 (toolsource + execute)", len(hits), hits)
	}
	for _, hit := range hits {
		if hit.a != "value-A" || hit.b != "value-B" {
			t.Fatalf("probe hit at %s = {a:%v b:%v}, want {value-A value-B}(runCtx 注入值未透传)", hit.where, hit.a, hit.b)
		}
	}
}

// ---------------------------------------------------------------------------
// P5-2:run.* 生命周期事件产出(仅 hub 非 nil 路径,plan §4)。
// ---------------------------------------------------------------------------

// runLifecycleEvent 在 ReadEvents 全量回放中取某事件类型的唯一记录:缺失或多条
// 都 fatal(每 run 的 started/finished/error 均应恰产出一次)。
func runLifecycleEvent(t *testing.T, recs []session.EventRecord, typ string) *session.EventRecord {
	t.Helper()
	var hit *session.EventRecord
	count := 0
	for i := range recs {
		if recs[i].Type == typ {
			hit = &recs[i]
			count++
		}
	}
	if count != 1 {
		t.Fatalf("event %s persisted %d times, want exactly 1 (records: %d)", typ, count, len(recs))
	}
	return hit
}

// assertEventPayload 解码事件载荷并断言键值(只校验 want 中列出的键;json 反序列
// 化后 string/bool 与 any 的直接比较成立)。
func assertEventPayload(t *testing.T, rec *session.EventRecord, want map[string]any) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(rec.Payload, &got); err != nil {
		t.Fatalf("decode %s payload %s: %v", rec.Type, rec.Payload, err)
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("%s payload %v missing key %q, want %v", rec.Type, got, k, w)
		}
		if g != w {
			t.Fatalf("%s payload[%q] = %v, want %v", rec.Type, k, g, w)
		}
	}
}

// assertEventBeforeTerminalState 断言原始 journal 文件(事件与 state 同文件混存)
// 中某事件类型的行先于终态 state 行——次序铁律(事件先于终态 state 记录,
// journal_store.go 句柄逐出约束)的文件序可见面,与 TestStartRunEmitsJournalEvents
// 的「末行 completed」口径同源。
func assertEventBeforeTerminalState(t *testing.T, fx eventRunFixture, runID, typ, termState string) {
	t.Helper()
	data, err := os.ReadFile(fx.runJournalPath(runID))
	if err != nil {
		t.Fatalf("read run journal: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var probe struct {
		Kind  string `json:"kind"`
		Type  string `json:"type"`
		State string `json:"state"`
	}
	eventLine, termLine := -1, -1
	for i, line := range lines {
		if err := json.Unmarshal([]byte(line), &probe); err != nil {
			t.Fatalf("decode journal line %q: %v", line, err)
		}
		switch {
		case probe.Kind == "event" && probe.Type == typ && eventLine == -1:
			eventLine = i
		case probe.Kind == "state" && probe.State == termState:
			termLine = i
		}
	}
	if eventLine == -1 {
		t.Fatalf("journal file has no %s event line", typ)
	}
	if termLine == -1 {
		t.Fatalf("journal file has no terminal state %s line", termState)
	}
	if eventLine > termLine {
		t.Fatalf("%s event at line %d is after terminal state %s at line %d(次序铁律:事件先于终态 state 记录)", typ, eventLine, termState, termLine)
	}
}

// TestStartRunEmitsRunLifecycleEvents(P5-2):RunEvents=真 RunJournalStore →
// ReadEvents 断言 run.* 四类的产出面与次序铁律:
//   - 正常 run:run.started 先于 run.finished(Seq 序),finished 的行先于终态
//     completed state 记录(文件序);
//   - failed run(captureProvider.chatErr 注入,AC-1 归一 RunFailed):含
//     run.error{err} 且先于 run.finished,两者都先于终态 state 记录;
//   - 取消 run(CancelRun,early-exit 路径):含 run.finished{cancelled},且无
//     run.started(run 未达 running)。
//
// 全部走真 journal 全链(manager → runtime → Session → ai-core → tee → hub →
// RunJournalStore 落盘,装置同 TestStartRunEmitsJournalEvents)。
func TestStartRunEmitsRunLifecycleEvents(t *testing.T) {
	t.Run("completed run emits started then finished before terminal state", func(t *testing.T) {
		m := NewSessionManager()
		defer m.Close()
		pv := &scriptedStreamProvider{rounds: []func(ch chan<- core.Token){
			func(ch chan<- core.Token) {
				ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
					Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "final answer"}}},
				}}
			},
		}}
		fx := newEventRunRuntime(t, m, pv)

		h, err := m.StartRun(fx.sessionID, RunRequest{Input: "q"}, eventRunCallbacks())
		if err != nil {
			t.Fatalf("StartRun: %v", err)
		}
		waitHandle(t, h)

		recs, err := fx.journal.ReadEvents(0)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		started := runLifecycleEvent(t, recs, session.EventRunStarted)
		finished := runLifecycleEvent(t, recs, session.EventRunFinished)
		if started.Seq >= finished.Seq {
			t.Fatalf("started seq %d >= finished seq %d, want started strictly before finished", started.Seq, finished.Seq)
		}
		assertEventPayload(t, started, map[string]any{"state": string(RunRunning)})
		assertEventPayload(t, finished, map[string]any{"state": string(RunCompleted), "persisted": true})
		assertEventBeforeTerminalState(t, fx, h.RunID, session.EventRunFinished, string(RunCompleted))
	})

	t.Run("failed run emits error before finished", func(t *testing.T) {
		m := NewSessionManager()
		defer m.Close()
		fx := newEventRunRuntime(t, m, &captureProvider{chatErr: errors.New("boom provider")})

		h, err := m.StartRun(fx.sessionID, RunRequest{Input: "q"}, eventRunCallbacks())
		if err != nil {
			t.Fatalf("StartRun: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := h.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait: %v (state=%s)", err, h.State())
		}
		if res.State != RunFailed {
			t.Fatalf("run result = %+v, want RunFailed (provider 失败经 AC-1 capture 归一)", res)
		}

		recs, err := fx.journal.ReadEvents(0)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		started := runLifecycleEvent(t, recs, session.EventRunStarted)
		errEv := runLifecycleEvent(t, recs, session.EventRunError)
		finished := runLifecycleEvent(t, recs, session.EventRunFinished)
		if started.Seq >= errEv.Seq || errEv.Seq >= finished.Seq {
			t.Fatalf("seq order: started %d, error %d, finished %d, want strictly increasing", started.Seq, errEv.Seq, finished.Seq)
		}
		assertEventPayload(t, errEv, map[string]any{"err": "boom provider"})
		assertEventPayload(t, finished, map[string]any{"state": string(RunFailed)})
		assertEventBeforeTerminalState(t, fx, h.RunID, session.EventRunError, string(RunFailed))
		assertEventBeforeTerminalState(t, fx, h.RunID, session.EventRunFinished, string(RunFailed))
	})

	t.Run("queued cancel emits finished cancelled at early exit", func(t *testing.T) {
		m := NewSessionManager(WithRunLimits(1, 8))
		defer m.Close()
		startedA := make(chan struct{})
		releaseA := make(chan struct{})
		defer close(releaseA) // 失败路径也放行 A,防 defer 的 m.Close 死等在途 run
		hangPv := &gatedProvider{
			replies: []string{"reply-A"},
			starts:  []chan struct{}{startedA},
			release: []chan struct{}{releaseA},
		}
		fxA := newEventRunRuntime(t, m, hangPv)                                       // A:占住唯一全局槽挂起中
		fxB := newEventRunRuntime(t, m, &gatedProvider{replies: []string{"reply-B"}}) // B:排队即取消,provider 不开工

		if _, err := m.StartRun(fxA.sessionID, RunRequest{Input: "q-A"}, eventRunCallbacks()); err != nil {
			t.Fatalf("StartRun A: %v", err)
		}
		hB, err := m.StartRun(fxB.sessionID, RunRequest{Input: "q-B"}, eventRunCallbacks())
		if err != nil {
			t.Fatalf("StartRun B: %v", err)
		}
		waitStarted(t, startedA) // A 已占住唯一 runSlots 槽
		if got := hB.State(); got != RunQueued {
			t.Fatalf("B state = %q, want RunQueued (排队中)", got)
		}
		if err := m.CancelRun(fxB.sessionID, hB.RunID); err != nil {
			t.Fatalf("CancelRun queued B: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resB, err := hB.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait: %v (state=%s)", err, hB.State())
		}
		if resB.State != RunCancelled {
			t.Fatalf("B result = %+v, want RunCancelled (早退出口终态)", resB)
		}

		recs, err := fxB.journal.ReadEvents(0)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		for _, rec := range recs {
			if rec.Type == session.EventRunStarted {
				t.Fatalf("early-exit run emitted %s (run 未达 running 不应产出 started 事件)", session.EventRunStarted)
			}
		}
		finished := runLifecycleEvent(t, recs, session.EventRunFinished)
		assertEventPayload(t, finished, map[string]any{"state": string(RunCancelled)})
		assertEventBeforeTerminalState(t, fxB, hB.RunID, session.EventRunFinished, string(RunCancelled))
	})
}
