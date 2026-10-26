package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-runtime/session"
)

// 本文件是 P3-2 的 EventHub 写侧验收测试(plan P3-2「新增测试」):
//   - TestEventHubSeqMonotonicAcrossRuns:BeginRun 切换后 seq 连续不重复
//     (跨 Run 游标,P3-0 契约表第 1 行);
//   - TestKeyEventDurableFirst:stub journal AppendEvent 注错 → Publish 返回
//     err 且无订阅者收到(durable-first 失败不广播);成功路径以 stub 里的
//     顺序钩子断言 journal 先于广播(plan 原文),并用 gate 阻塞把「落盘未
//     完成 ⇒ 不广播」变成可轮询的确定断言;
//   - TestDeltaMergeWindow:50ms 窗内 100 条 text.delta → journal 恰 1 条
//     合并记录(Seq=最后一条、FromSeq=第一条),订阅者仍逐条收到 100 条
//     (实时不合并,P3-0 契约表第 3 行)。
//
// 订阅注册在 P3-3 的 Subscribe(Replay+Live 原子切换)落地前只有内部结构,
// 测试经 addTestSub(同包白盒)直接进 subs 表;P3-3 落地后这些测试照旧成立。

// 编译期断言:测试桩满足 EventJournal 消费面。
var _ EventJournal = (*stubEventJournal)(nil)

// stubEventJournal 是 EventJournal 的内存测试桩:成功追加的记录按序收进 recs;
// appendErr 非 nil 时注入 AppendEvent 失败(记录不收)。另带两个顺序断言装置:
//   - order(顺序钩子,plan「以 stub 里的顺序钩子断言」):非 nil 时每次成功
//     追加向共享顺序日志记 "journal:<type>",与订阅者读侧记的 "sub:<type>"
//     同池比对——durable-first ⇒ journal 条目恒先于 sub 条目;
//   - gate/entered:gate 非 nil 时 AppendEvent 在记下 entered 后阻塞直至 gate
//     关闭,把「落盘进行中」变成测试可轮询的确定状态(而非依赖调度运气)。
//     gate/entered 构造期赋值只读,无并发写。
type stubEventJournal struct {
	mu        sync.Mutex
	recs      []session.EventRecord
	appendErr error

	order   *orderLog
	gate    chan struct{}
	entered chan struct{}
}

func (j *stubEventJournal) AppendEvent(rec session.EventRecord) error {
	if j.entered != nil {
		close(j.entered)
	}
	if j.gate != nil {
		<-j.gate
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.appendErr != nil {
		return j.appendErr
	}
	if j.order != nil {
		j.order.add("journal:" + rec.Type)
	}
	j.recs = append(j.recs, rec)
	return nil
}

func (j *stubEventJournal) ReadEvents(afterSeq uint64) ([]session.EventRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []session.EventRecord
	for _, rec := range j.recs {
		if rec.Seq > afterSeq {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (j *stubEventJournal) SeqRange() (earliest, latest uint64, ok bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.recs) == 0 {
		return 0, 0, false
	}
	return j.recs[0].Seq, j.recs[len(j.recs)-1].Seq, true
}

func (j *stubEventJournal) snapshot() []session.EventRecord {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]session.EventRecord(nil), j.recs...)
}

// orderLog 是 stub 与订阅者读侧共用的顺序日志(mutex 保护,跨 goroutine)。
type orderLog struct {
	mu    sync.Mutex
	steps []string
}

func (l *orderLog) add(step string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.steps = append(l.steps, step)
}

func (l *orderLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.steps...)
}

// addTestSub 直接向 hub.subs 注册一个缓冲订阅者(同包白盒;P3-3 的 Subscribe
// 落地前订阅注册面的唯一入口),返回收事件通道。
func (h *EventHub) addTestSub() <-chan session.SessionEvent {
	ch := make(chan session.SessionEvent, subscriberBuffer)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs[h.nextSubID] = &hubSub{live: ch}
	h.nextSubID++
	return ch
}

// recvEvent 带超时收一个订阅者事件(超时即失败,不悬挂测试)。
func recvEvent(t *testing.T, ch <-chan session.SessionEvent) session.SessionEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subscriber event")
		return session.SessionEvent{}
	}
}

// requireNoEvent 断言通道当前无事件可收(fan-out 是 Publish 内的同步非阻塞
// 投递,若已广播则此刻必然可收)。
func requireNoEvent(t *testing.T, ch <-chan session.SessionEvent) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("unexpected subscriber event: %+v", ev)
	default:
	}
}

// waitEntered 轮询等待 stub 的 AppendEvent 已进入(阻塞在 gate 上)。
func waitEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-entered:
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for journal AppendEvent to enter")
		}
		time.Sleep(500 * time.Microsecond)
	}
}

// TestEventHubSeqMonotonicAcrossRuns:BeginRun/EndRun 切换 run 前后 seq 连续
// 不重复不重置(P3-0 契约表第 1 行「Seq 是 Session 维度严格递增游标,跨 Run
// 不重复」);key 事件落盘 seq 与广播 seq 同源。
func TestEventHubSeqMonotonicAcrossRuns(t *testing.T) {
	jr := &stubEventJournal{}
	hub := NewEventHub("seq-test", jr)
	defer hub.Close()
	ch := hub.addTestSub()

	pub := func(runID, typ string) {
		t.Helper()
		if err := hub.Publish(IngressEvent{RunID: runID, Type: typ, Payload: json.RawMessage(`"p"`)}); err != nil {
			t.Fatalf("Publish %s: %v", typ, err)
		}
	}

	// run A(key+delta 混合)→ EndRun → run B:游标跨 run 续接。
	hub.BeginRun("run-a")
	pub("run-a", session.EventToolCall) // key, seq 1
	pub("run-a", session.EventTextDelta)
	pub("run-a", session.EventTextDelta)
	pub("run-a", session.EventToolResult) // key, seq 4
	hub.EndRun()
	hub.BeginRun("run-b")
	pub("run-b", session.EventTextDelta)                 // seq 5(不重置)
	pub("run-b", session.EventAssistantMessageFinalized) // key, seq 6
	hub.EndRun()

	if got := hub.LatestSeq(); got != 6 {
		t.Fatalf("LatestSeq = %d, want 6", got)
	}
	want := []struct {
		runID string
		typ   string
	}{
		{"run-a", session.EventToolCall},
		{"run-a", session.EventTextDelta},
		{"run-a", session.EventTextDelta},
		{"run-a", session.EventToolResult},
		{"run-b", session.EventTextDelta},
		{"run-b", session.EventAssistantMessageFinalized},
	}
	for i, w := range want {
		ev := recvEvent(t, ch)
		if ev.Seq != uint64(i+1) {
			t.Fatalf("event #%d seq = %d, want %d(seq 跨 run 连续不重复)", i, ev.Seq, i+1)
		}
		if ev.RunID != w.runID || ev.Type != w.typ {
			t.Fatalf("event #%d = {run:%q type:%q}, want {run:%q type:%q}", i, ev.RunID, ev.Type, w.runID, w.typ)
		}
		if ev.SessionID != "seq-test" {
			t.Fatalf("event #%d session = %q, want %q", i, ev.SessionID, "seq-test")
		}
	}
	requireNoEvent(t, ch)

	// key 事件(durable-first)的落盘 seq 与广播 seq 一致。
	wantKey := map[uint64]string{
		1: session.EventToolCall,
		4: session.EventToolResult,
		6: session.EventAssistantMessageFinalized,
	}
	gotKey := map[uint64]string{}
	for _, rec := range jr.snapshot() {
		if session.IsKeyEvent(rec.Type) {
			gotKey[rec.Seq] = rec.Type
		}
	}
	if len(gotKey) != len(wantKey) {
		t.Fatalf("persisted key events = %v, want %v", gotKey, wantKey)
	}
	for seq, typ := range wantKey {
		if gotKey[seq] != typ {
			t.Fatalf("persisted key event at seq %d = %q, want %q", seq, gotKey[seq], typ)
		}
	}
}

// TestKeyEventDurableFirst:key 事件 durable-first 语义(总方案第五节「持久化
// 写入失败不能继续把事件当作可恢复事件正常广播」)。
func TestKeyEventDurableFirst(t *testing.T) {
	t.Run("AppendErrorSuppressesBroadcast", func(t *testing.T) {
		jr := &stubEventJournal{appendErr: errors.New("fsync boom")}
		hub := NewEventHub("durable-err", jr)
		defer hub.Close()
		hub.BeginRun("run-e")
		ch := hub.addTestSub()

		err := hub.Publish(IngressEvent{RunID: "run-e", Type: session.EventToolCall, Payload: json.RawMessage(`"c"`)})
		if err == nil {
			t.Fatal("Publish key event with failing journal = nil, want error")
		}
		if !errors.Is(err, jr.appendErr) {
			t.Fatalf("Publish err = %v, want wrap of %v", err, jr.appendErr)
		}
		requireNoEvent(t, ch) // 失败不广播:无订阅者收到
		if got := hub.LatestSeq(); got != 0 {
			t.Fatalf("LatestSeq after failed key publish = %d, want 0(失败事件不推进 watermark)", got)
		}
		if recs := jr.snapshot(); len(recs) != 0 {
			t.Fatalf("journal records = %d, want 0(注错未落盘)", len(recs))
		}
	})

	t.Run("JournalCompletesBeforeBroadcast", func(t *testing.T) {
		log := &orderLog{}
		jr := &stubEventJournal{order: log, gate: make(chan struct{}), entered: make(chan struct{})}
		hub := NewEventHub("durable-ok", jr)
		defer hub.Close()
		hub.BeginRun("run-o")
		ch := hub.addTestSub()

		// 订阅者读 goroutine:收到事件即向共享顺序日志记 "sub:<type>"。
		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			for ev := range ch {
				log.add("sub:" + ev.Type)
			}
		}()

		pubErr := make(chan error, 1)
		go func() {
			pubErr <- hub.Publish(IngressEvent{RunID: "run-o", Type: session.EventToolCall, Payload: json.RawMessage(`"c"`)})
		}()

		// 落盘进行中(AppendEvent 阻塞在 gate):广播必须尚未发生——
		// durable-first 的确定断言,不依赖调度运气。
		waitEntered(t, jr.entered)
		requireNoEvent(t, ch)
		if steps := log.snapshot(); len(steps) != 0 {
			t.Fatalf("order log before journal completes = %v, want empty", steps)
		}

		close(jr.gate) // 释放落盘 → Publish 完成 → fan-out
		if err := <-pubErr; err != nil {
			t.Fatalf("Publish: %v", err)
		}

		// 等订阅者读到事件(reader 收到后记 "sub:..."),再比对顺序钩子。
		deadline := time.Now().Add(5 * time.Second)
		for len(log.snapshot()) < 2 {
			if time.Now().After(deadline) {
				t.Fatalf("order log did not fill: %v", log.snapshot())
			}
			time.Sleep(500 * time.Microsecond)
		}
		hub.Close() // 关闭 ch 让 reader 退出(defer 的幂等 Close 兜底失败路径)
		<-readerDone
		want := []string{"journal:tool_call", "sub:tool_call"}
		if got := log.snapshot(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("order log = %v, want %v(journal 先于广播)", got, want)
		}
	})
}

// TestDeltaMergeWindow:50ms 窗内 100 条 text.delta → 持久化恰 1 条合并记录
// (Seq=最后一条、FromSeq=第一条,P3-1 合并契约),订阅者仍逐条收到 100 条
// (实时广播不合并,per-event seq)。100 次发布的耗时远小于 50ms 窗,定时器
// 未及触发,EndRun 收口即唯一 flush 点。
func TestDeltaMergeWindow(t *testing.T) {
	jr := &stubEventJournal{}
	hub := NewEventHub("merge-win", jr)
	defer hub.Close()
	hub.BeginRun("run-m")
	ch := hub.addTestSub()

	const n = 100
	var wantText strings.Builder
	for i := 0; i < n; i++ {
		tok := fmt.Sprintf("t%d", i)
		payload, err := json.Marshal(tok)
		if err != nil {
			t.Fatalf("marshal token #%d: %v", i, err)
		}
		if err := hub.Publish(IngressEvent{RunID: "run-m", Type: session.EventTextDelta, Payload: payload}); err != nil {
			t.Fatalf("Publish delta #%d: %v", i, err)
		}
		wantText.WriteString(tok)
	}
	firstSeq, lastSeq := uint64(1), hub.LatestSeq()
	if lastSeq != n {
		t.Fatalf("LatestSeq = %d, want %d", lastSeq, n)
	}
	hub.EndRun() // 窗收口:合并缓冲 flush

	// 实时侧:100 条逐条到达(per-event seq,不合并)。
	for i := 0; i < n; i++ {
		ev := recvEvent(t, ch)
		if ev.Seq != uint64(i+1) || ev.Type != session.EventTextDelta || ev.RunID != "run-m" {
			t.Fatalf("event #%d = {seq:%d type:%s run:%s}, want {seq:%d text.delta run-m}", i, ev.Seq, ev.Type, ev.RunID, i+1)
		}
	}
	requireNoEvent(t, ch) // 恰 100 条,无多余投递

	// 持久化侧:恰 1 条合并记录(无任何逐条落盘)。
	recs := jr.snapshot()
	if len(recs) != 1 {
		t.Fatalf("journal records = %d, want exactly 1 merged record", len(recs))
	}
	rec := recs[0]
	if rec.Type != session.EventTextDelta {
		t.Fatalf("merged record type = %q, want %q", rec.Type, session.EventTextDelta)
	}
	if rec.Seq != lastSeq || rec.FromSeq != firstSeq {
		t.Fatalf("merged record range = [%d..%d], want [%d..%d](Seq=最后一条、FromSeq=第一条)", rec.FromSeq, rec.Seq, firstSeq, lastSeq)
	}
	if rec.RunID != "run-m" || rec.SessionID != "merge-win" {
		t.Fatalf("merged record = {session:%q run:%q}, want {merge-win, run-m}", rec.SessionID, rec.RunID)
	}
	// 聚合 payload(文档化行为):窗内 token 字符串全部拼接。
	var gotText string
	if err := json.Unmarshal(rec.Payload, &gotText); err != nil {
		t.Fatalf("merged payload %s is not a JSON string: %v", rec.Payload, err)
	}
	if gotText != wantText.String() {
		t.Fatalf("merged payload = %q, want concatenated %q", gotText, wantText.String())
	}
}
