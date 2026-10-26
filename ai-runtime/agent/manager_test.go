package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	core "github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// TestManagerIndependentSessions:manager 内两个 runtime 同时持有各自 gate,
// 互不阻塞(TestConcurrentSessions 的 manager 层雏形)。
func TestManagerIndependentSessions(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	runtimes := []*SessionRuntime{
		NewSessionRuntime("a", nil),
		NewSessionRuntime("b", nil),
	}
	for _, rt := range runtimes {
		if err := m.Register(rt); err != nil {
			t.Fatalf("register %s: %v", rt.ID, err)
		}
	}

	acquired := make(chan error, len(runtimes))
	for _, rt := range runtimes {
		rt := rt
		go func() {
			rel, err := rt.AcquireRun(context.Background())
			acquired <- err // 先宣告获取结果,再谈释放
			if err == nil {
				time.Sleep(50 * time.Millisecond) // 各持自己的 gate 一小段
				rel()
			}
		}()
	}

	// 双方都尚未释放时,两个获取都必须成功——互不阻塞的直接证明;
	// 若任一方被对方卡住,将在 deadline 处失败。
	deadline := time.After(time.Second)
	for range runtimes {
		select {
		case err := <-acquired:
			if err != nil {
				t.Fatalf("acquire failed while gates independent: %v", err)
			}
		case <-deadline:
			t.Fatal("independent runtimes blocked each other")
		}
	}
}

// TestSessionManagerLifecycle:Register/Get/List/Remove/Close 行为,含同 ID
// 冲突报错与幂等 Remove/Close。
func TestSessionManagerLifecycle(t *testing.T) {
	m := NewSessionManager()

	a := NewSessionRuntime("a", nil)
	if err := m.Register(a); err != nil {
		t.Fatalf("register a: %v", err)
	}
	if err := m.Register(NewSessionRuntime("a", nil)); err == nil {
		t.Fatal("duplicate id accepted, want error")
	}
	if err := m.Register(nil); err == nil {
		t.Fatal("nil runtime accepted, want error")
	}
	if got := m.Get("a"); got != a {
		t.Fatalf("Get(\"a\") = %v, want the registered runtime", got)
	}
	if got := m.Get("missing"); got != nil {
		t.Fatalf("Get(\"missing\") = %v, want nil", got)
	}

	if err := m.Register(NewSessionRuntime("b", nil)); err != nil {
		t.Fatalf("register b: %v", err)
	}
	list := m.List()
	if len(list) != 2 || list[0].ID != "a" || list[1].ID != "b" {
		t.Fatalf("List = %s, want sorted [a b]", runtimeIDs(list))
	}

	m.Remove("a")
	if got := m.Get("a"); got != nil {
		t.Fatal("Remove did not drop a")
	}
	m.Remove("a") // 幂等
	if list := m.List(); len(list) != 1 || list[0].ID != "b" {
		t.Fatalf("List after remove = %s, want [b]", runtimeIDs(list))
	}

	m.Close()
	if list := m.List(); len(list) != 0 {
		t.Fatalf("List after close = %s, want empty", runtimeIDs(list))
	}
	if got := m.Get("b"); got != nil {
		t.Fatal("Get after close returned runtime")
	}
}

func runtimeIDs(list []*SessionRuntime) []string {
	ids := make([]string, 0, len(list))
	for _, rt := range list {
		ids = append(ids, rt.ID)
	}
	return ids
}

// TestManagerCloseCancelsContextAndRejectsRegister:P2-4 的宿主停机语义——
// Close 后宿主生命周期 ctx 取消、Register 拒绝(ErrManagerClosed)、Close 幂等。
func TestManagerCloseCancelsContextAndRejectsRegister(t *testing.T) {
	m := NewSessionManager()
	rt := NewSessionRuntime("a", nil)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register a: %v", err)
	}

	ctx := m.contextOf()
	select {
	case <-ctx.Done():
		t.Fatal("host context canceled before Close")
	default:
	}

	m.Close()

	select {
	case <-m.contextOf().Done():
	default:
		t.Fatal("contextOf().Done() not closed after Close")
	}
	if err := m.Register(NewSessionRuntime("b", nil)); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Register after Close = %v, want ErrManagerClosed", err)
	}

	m.Close() // 幂等:第二次 Close 不阻塞、不 panic
	if list := m.List(); len(list) != 0 {
		t.Fatalf("List after close = %s, want empty", runtimeIDs(list))
	}
	if got := m.Get("a"); got != nil {
		t.Fatal("Get after close returned runtime")
	}
}

// TestManagerCloseWaitsActiveRun:P2-4 的 Shutdown 等待语义——在途同步 Run
// (RecordRun,fn 用调用方 ctx)未终态前 Close 阻塞;run 终态后 Close 返回,
// run 正常 RunCompleted(证明 Close 的 cancel 不影响在途同步 prompt)。
func TestManagerCloseWaitsActiveRun(t *testing.T) {
	m := NewSessionManager()
	st := &fakeConversationStore{}
	started := make(chan struct{})
	release := make(chan struct{})
	pv := &gatedProvider{
		replies: []string{"reply"},
		starts:  []chan struct{}{started},
		release: []chan struct{}{release},
	}
	rt := NewSessionRuntime("close-wait", newFakeStoreSession(st, nil, pv))
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}

	done := make(chan RunResult, 1)
	go func() {
		rel, err := rt.AcquireRun(context.Background())
		if err != nil {
			done <- RunResult{State: RunFailed, Err: err}
			return
		}
		defer rel()
		done <- rt.RecordRun(func(s *Session) (RunResult, error) {
			return s.Run(context.Background(), RunRequest{Input: "q"}, testRunCallbacks(&strings.Builder{}))
		})
	}()
	waitStarted(t, started) // provider 挂起中:run 在途,active 已记账

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()

	// Close 必须阻塞到 run 终态:provider 释放前不得返回。
	select {
	case <-closed:
		t.Fatal("Close returned while the in-flight run was still active")
	case <-time.After(100 * time.Millisecond):
	}

	close(release) // 放行 provider → run 走到终态 → Close 解除阻塞
	res := waitRunResult(t, done)
	// run.done 关闭后 Close 仍需走清表收尾,与主 goroutine 无先后保证——
	// 有界等待其返回,而非非阻塞抢跑。
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the run reached a terminal state")
	}
	if res.State != RunCompleted || !res.Persisted {
		t.Fatalf("run result = %+v, want RunCompleted+Persisted (Close cancel must not reach sync-path run)", res)
	}
	if got := rt.Active(); got != nil {
		t.Fatalf("Active after Close = %+v, want nil (run 已终态并清理)", got)
	}
}

// ---------------------------------------------------------------------------
// P3-5:生命周期收口——Manager.Close 逐 runtime 关闭 EventHub。
//
// 装置复用 supervisor_test.go 的 P3-4 编排(eventRunFixture:真 Repository →
// 真 *session.Store → RunJournalStore 同时充当 RunJournal 与 RunEvents 注入
// SessionDeps;StartRun 经 bindEvents 创建 hub——「两个 runtime 各带 hub」的
// 唯一构造路径)。
// ---------------------------------------------------------------------------

// TestManagerCloseClosesEventHubs(P3-5):两个 runtime 各带 hub,其一在途
// (cancelAwareProvider 挂起,宿主 ctx 取消是唯一出口——与 TestCancelRunningRun
// 同款单轮契约,release 永不关闭)、其一已完成(流式脚本产事件,事件已落盘)。
// Close(等待在途 run 终态 → 逐 runtime closeEvents → 清表)后断言:
//   - Close 前两 hub 活体对照:Subscribe 成功(证明后文的 ErrHubClosed 确由本次
//     Close 触发,而非 hub 从未启用);
//   - 在途 run 已到终态(RunCancelled:宿主取消经 runCtx 传播,execute 层归一);
//   - 两 hub 均 Closed:Publish/Subscribe 一律 ErrHubClosed(P3-0 表第 5 行
//     「关掉的事件流不能再产出事件」);
//   - journal 当前句柄已关(重读文件完整):在途 run 的 state 序恰为
//     queued→running→cancelled、末行即终态(终态之后无任何追加),且该 run 的
//     句柄已被终态逐出——再追加撞 ErrRunExists(宁拒勿双写,文件已封口;若
//     句柄仍被缓存,追加会复用句柄静默成功);
//   - 已完成 run 的重读文件完整:事件记录 [FromSeq..Seq] 覆盖并集恰为
//     [1..LatestSeq] 无缺口(EndRun flush 先于终态落盘的次序铁律在文件序可见)。
func TestManagerCloseClosesEventHubs(t *testing.T) {
	m := NewSessionManager()
	t.Cleanup(m.Close) // 幂等:主体已 Close 时为空操作,失败路径也收尾

	// runtime A:在途 run。cancelAwareProvider 挂在 release(永不关闭)与
	// ctx.Done 上——Close 取消宿主 ctx 是唯一出口。
	startedA := make(chan struct{})
	pvA := &cancelAwareProvider{started: startedA}
	fxA := newEventRunRuntime(t, m, pvA)
	hA, err := m.StartRun(fxA.sessionID, RunRequest{Input: "q-A"}, eventRunCallbacks())
	if err != nil {
		t.Fatalf("StartRun A: %v", err)
	}
	waitStarted(t, startedA) // A 在途:run running

	// runtime B:正常完成的 run。流式脚本产事件(增量走合并窗,key 事件
	// durable-first),供「重读文件完整」的事件面断言。
	pvB := &scriptedStreamProvider{rounds: []func(ch chan<- core.Token){func(ch chan<- core.Token) {
		for i := 0; i < 5; i++ {
			ch <- core.Token{Type: core.TokenTypeText, Content: fmt.Sprintf("chunk-%d", i)}
		}
		ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
			Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "reply-B"}}},
		}}
	}}}
	fxB := newEventRunRuntime(t, m, pvB)
	hB, err := m.StartRun(fxB.sessionID, RunRequest{Input: "q-B"}, eventRunCallbacks())
	if err != nil {
		t.Fatalf("StartRun B: %v", err)
	}
	waitHandle(t, hB) // B 完成:全部事件已落盘(done 关闭 happens-before)

	// Close 前快照 runtime 与 hub(runtime 指针在清表后仍可用,自行持有)。
	rtA, rtB := m.Get(fxA.sessionID), m.Get(fxB.sessionID)
	if rtA == nil || rtB == nil {
		t.Fatalf("runtimes not registered: A=%v B=%v", rtA, rtB)
	}
	for name, rt := range map[string]*SessionRuntime{"A": rtA, "B": rtB} {
		if hub := rt.eventHub(); hub == nil {
			t.Fatalf("runtime %s: event hub not bound (RunEvents enabled)", name)
		} else if _, err := hub.Subscribe(context.Background(), 0); err != nil {
			t.Fatalf("runtime %s: Subscribe before Close: %v(活体对照失败)", name, err)
		}
	}
	latestB := rtB.eventHub().LatestSeq() // run 终态后不再变化
	if latestB == 0 {
		t.Fatal("runtime B produced no events (事件面未生效,完整性断言失去意义)")
	}

	m.Close() // 等待 A 终态 → 逐 runtime closeEvents → 清表

	// 在途 run 的终态:宿主取消经 runCtx 传播,execute 层归一为 RunCancelled
	// (取消绝不记 failed/completed);注册表已清空。
	if res, err := hA.Wait(context.Background()); err != nil || res.State != RunCancelled {
		t.Fatalf("in-flight run after Close = {%+v}, %v, want RunCancelled", res, err)
	}
	if got := m.Get(fxA.sessionID); got != nil {
		t.Fatalf("Get after Close = %v, want nil (注册表已清空)", got)
	}

	// 两 hub 均 Closed:Publish/Subscribe 一律拒绝(ErrHubClosed)。
	for name, rt := range map[string]*SessionRuntime{"A": rtA, "B": rtB} {
		hub := rt.eventHub()
		err := hub.Publish(IngressEvent{RunID: "post-close", Type: session.EventToolCall, Payload: json.RawMessage(`"p"`)})
		if !errors.Is(err, ErrHubClosed) {
			t.Fatalf("runtime %s: Publish after Close = %v, want ErrHubClosed", name, err)
		}
		if _, err := hub.Subscribe(context.Background(), 0); !errors.Is(err, ErrHubClosed) {
			t.Fatalf("runtime %s: Subscribe after Close = %v, want ErrHubClosed", name, err)
		}
	}

	// journal 当前句柄已关(重读文件完整)。
	assertSealedRunJournal(t, fxA, hA.RunID, []string{string(RunQueued), string(RunRunning), string(RunCancelled)})
	assertSealedRunJournal(t, fxB, hB.RunID, []string{string(RunQueued), string(RunRunning), string(RunCompleted)})
	// B 的事件记录覆盖 [1..LatestSeq] 无缺口(delta flush 先于终态落盘)。
	assertRunEventCoverage(t, fxB, hB.RunID, latestB)
}

// assertSealedRunJournal 从磁盘重读一个 run 的 journal 文件并断言「当前句柄
// 已关、文件完整」:
//   - 每行均为合法 JSON(无半行/撕裂写);
//   - state 记录恰为 wantStates 序列(queued→running→终态);
//   - 末行即终态 state 记录(事件收口先于终态落盘的次序铁律,
//     TestStartRunEmitsJournalEvents 同款断言——终态之后无任何追加);
//   - 追加探测句柄已关:终态逐出(RunJournalStore.AppendState 的句柄生命周期,
//     journal_store.go)后文件已封口,再追加必撞 ErrRunExists。
func assertSealedRunJournal(t *testing.T, fx eventRunFixture, runID string, wantStates []string) {
	t.Helper()
	data, err := os.ReadFile(fx.runJournalPath(runID))
	if err != nil {
		t.Fatalf("read run journal: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < len(wantStates) {
		t.Fatalf("journal lines = %d, want >= %d (state 序列 %v)", len(lines), len(wantStates), wantStates)
	}
	type stateLine struct {
		Kind  string `json:"kind"`
		State string `json:"state"`
	}
	var states []string
	for i, line := range lines {
		var rec stateLine
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode journal line #%d %q: %v(文件含半行/坏行,句柄未收口)", i, line, err)
		}
		if rec.Kind == "state" {
			states = append(states, rec.State)
		}
	}
	if len(states) != len(wantStates) {
		t.Fatalf("state records = %v, want %v", states, wantStates)
	}
	for i, want := range wantStates {
		if states[i] != want {
			t.Fatalf("state record #%d = %q, want %q", i, states[i], want)
		}
	}
	var last stateLine
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil ||
		last.Kind != "state" || last.State != wantStates[len(wantStates)-1] {
		t.Fatalf("last journal line = {kind:%q state:%q}, want {state %q}(终态必须最后落盘)",
			last.Kind, last.State, wantStates[len(wantStates)-1])
	}
	// 句柄已关:终态逐出后该 run 的文件已封口,任何追加都撞 ErrRunExists。
	err = fx.journal.AppendState(session.RunStateRecord{
		Kind:      "state",
		SessionID: fx.sessionID,
		RunID:     runID,
		State:     string(RunRunning), // 非终态:若句柄仍在缓存则会静默续写
		At:        time.Now(),
	})
	if !errors.Is(err, session.ErrRunExists) {
		t.Fatalf("append to sealed run journal = %v, want ErrRunExists(当前句柄未关闭/未逐出)", err)
	}
}

// assertRunEventCoverage 从磁盘重读 run 的全部事件记录(ReadEvents 扫 runs/),
// 断言 [FromSeq..Seq] 覆盖并集恰为 [1..latest] 无缺口无越界——「重读文件完整」
// 的事件面:delta 合并窗与 key 事件在终态前全部落盘。记账口径同
// TestStartRunEmitsJournalEvents:FromSeq 只作范围声明,合并范围可横跨其他类型
// 事件的 seq。
func assertRunEventCoverage(t *testing.T, fx eventRunFixture, runID string, latest uint64) {
	t.Helper()
	recs, err := fx.journal.ReadEvents(0)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	covered := make([]bool, latest+1)
	n := 0
	for _, rec := range recs {
		if rec.RunID != runID {
			t.Fatalf("event record seq %d RunID = %q, want %q", rec.Seq, rec.RunID, runID)
		}
		lo := rec.Seq
		if rec.FromSeq > 0 && rec.FromSeq < lo {
			lo = rec.FromSeq
		}
		if lo < 1 || rec.Seq > latest {
			t.Fatalf("event record range [%d..%d] outside [1..%d]", lo, rec.Seq, latest)
		}
		for x := lo; x <= rec.Seq; x++ {
			if !covered[x] {
				covered[x] = true
				n++
			}
		}
	}
	if uint64(n) != latest {
		t.Fatalf("event coverage = %d of %d seqs, want full [1..%d](重读文件缺段)", n, latest, latest)
	}
}
