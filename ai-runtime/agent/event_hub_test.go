package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-runtime/session"
)

// 本文件是 P3-2/P3-3 的 EventHub 验收测试。P3-2 写侧(plan P3-2「新增测试」):
//   - TestEventHubSeqMonotonicAcrossRuns:BeginRun 切换后 seq 连续不重复
//     (跨 Run 游标,P3-0 契约表第 1 行);
//   - TestKeyEventDurableFirst:stub journal AppendEvent 注错 → Publish 返回
//     err 且无订阅者收到(durable-first 失败不广播);成功路径以 stub 里的
//     顺序钩子断言 journal 先于广播(plan 原文),并用 gate 阻塞把「落盘未
//     完成 ⇒ 不广播」变成可轮询的确定断言;
//   - TestDeltaMergeWindow:50ms 窗内 100 条 text.delta → journal 恰 1 条
//     批次记录(Seq=最后一条、FromSeq=第一条,payload 为 DeltaBatch JSON,
//     per-part seq+payload 按序保留),订阅者仍逐条收到 100 条
//     (实时不合并,P3-0 契约表第 3 行)。
//
// 修复验收(CR6/CR3-I1):
//   - TestSubscribeReplayExpandsDeltaBatch(CR3-I1):回放把合并批次展开为
//     原始 per-seq 事件,实时与回放形状彻底一致(wire 无 FromSeq/批量语义,
//     SessionEvent 已无该字段——编译期钉死);
//   - TestReplayResumeInsideInterleavedDeltaBatch(CR3-I1 核心交错实例):
//     游标落在合并批次 seq 区间内部时,断点恢复不重复、不丢失;
//   - TestEventHubReplayIntegrity(I4):完整性三态——空历史/纯内存 → Complete、
//     播种自非空 journal → Unknown、key 落盘失败/合并 flush 失败 → Incomplete
//     且粘滞永不复位(含「失败后成功发布不翻回」与 Unknown → Incomplete 单向);
//   - TestNewEventHubPropagatesSeqRangeError(第三轮评审 I3):SeqRange 播种
//     注错 → NewEventHub 返回 (nil, err) 显式上抛,绝不创建「看似健康」的 hub
//     (旧形态把读取失败当空历史:complete 且 seq 从 1 重起,与旧记录冲突)。
//
// P3-3 读侧(plan P3-3「新增测试」):
//   - TestAttachReplayNoGap(总方案第十一节 → 本步):Replay+Live 原子切换的
//     无缺口无重复压测(-race);
//   - TestSubscribeCursorExpired / TestSubscribeAfterClose /
//     TestSubscribeCtxCancelCleansUp:窗口校验、关停拒绝、断线清理。
//
// P3-2 时代的订阅注册经 addTestSub(同包白盒)直接进 subs 表;P3-3 的
// Subscribe 落地后这些测试照旧成立(addTestSub 的 watermark=0 不触发水位过滤)。

// 编译期断言:测试桩满足 EventJournal 消费面。
var _ EventJournal = (*stubEventJournal)(nil)

// mustNewEventHub 是测试构造助手:NewEventHub 失败即 Fatal(成功路径 stub 的
// SeqRange 不会注错;播种失败的专测直接调 NewEventHub 断言 (nil, err),
// TestNewEventHubPropagatesSeqRangeError)。
func mustNewEventHub(t *testing.T, sessionID string, j EventJournal) *EventHub {
	t.Helper()
	hub, err := NewEventHub(sessionID, j)
	if err != nil {
		t.Fatalf("NewEventHub(%s): %v", sessionID, err)
	}
	return hub
}

// stubEventJournal 是 EventJournal 的内存测试桩:成功追加的记录按序收进 recs;
// appendErr 非 nil 时注入 AppendEvent 持续失败(记录不收);failFirst > 0 时
// 接下来 N 次 AppendEvent 注错后自愈(粘滞性验收用——验证「失败后成功发布不
// 翻回」,需要失败与成功在同一个 stub 上先后发生);seqRangeErr 非 nil 时注入
// SeqRange 播种失败(I3:NewEventHub 显式拒绝创建,TestNewEventHubPropagatesSeqRangeError)。
// 另带两个顺序断言装置:
//   - order(顺序钩子,plan「以 stub 里的顺序钩子断言」):非 nil 时每次成功
//     追加向共享顺序日志记 "journal:<type>",与订阅者读侧记的 "sub:<type>"
//     同池比对——durable-first ⇒ journal 条目恒先于 sub 条目;
//   - gate/entered:gate 非 nil 时 AppendEvent 在记下 entered 后阻塞直至 gate
//     关闭,把「落盘进行中」变成测试可轮询的确定状态(而非依赖调度运气)。
//     gate/entered 构造期赋值只读,无并发写。
type stubEventJournal struct {
	mu          sync.Mutex
	recs        []session.EventRecord
	appendErr   error
	failFirst   int
	seqRangeErr error

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
	if j.failFirst > 0 {
		j.failFirst--
		return errors.New("transient fsync boom")
	}
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

func (j *stubEventJournal) SeqRange() (earliest, latest uint64, ok bool, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.seqRangeErr != nil {
		return 0, 0, false, j.seqRangeErr
	}
	if len(j.recs) == 0 {
		return 0, 0, false, nil
	}
	return j.recs[0].Seq, j.recs[len(j.recs)-1].Seq, true, nil
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
// 落地前订阅注册面的唯一入口),返回收事件通道。dead 与 Subscribe 的构造同款
// (close-only 断开信号,Close/fanOut 关闭它)。
func (h *EventHub) addTestSub() <-chan session.SessionEvent {
	ch := make(chan session.SessionEvent, subscriberBuffer)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs[h.nextSubID] = &hubSub{live: ch, dead: make(chan struct{})}
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
	hub := mustNewEventHub(t, "seq-test", jr)
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
		hub := mustNewEventHub(t, "durable-err", jr)
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
		hub := mustNewEventHub(t, "durable-ok", jr)
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

// TestDeltaMergeWindow:50ms 窗内 100 条 text.delta → 持久化恰 1 条批次记录
// (Seq=最后一条、FromSeq=第一条,payload 为 DeltaBatch JSON 且 per-part
// seq+payload 按序保留,CR3-I1 合并契约),订阅者仍逐条收到 100 条
// (实时广播不合并,per-event seq)。100 次发布的耗时远小于 50ms 窗,定时器
// 未及触发,EndRun 收口即唯一 flush 点。
func TestDeltaMergeWindow(t *testing.T) {
	jr := &stubEventJournal{}
	hub := mustNewEventHub(t, "merge-win", jr)
	defer hub.Close()
	hub.BeginRun("run-m")
	ch := hub.addTestSub()

	const n = 100
	for i := 0; i < n; i++ {
		payload, err := json.Marshal(fmt.Sprintf("t%d", i))
		if err != nil {
			t.Fatalf("marshal token #%d: %v", i, err)
		}
		if err := hub.Publish(IngressEvent{RunID: "run-m", Type: session.EventTextDelta, Payload: payload}); err != nil {
			t.Fatalf("Publish delta #%d: %v", i, err)
		}
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

	// 持久化侧:恰 1 条批次记录(无任何逐条落盘)。
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
	// 批次 payload(DeltaBatch JSON,CR3-I1 文档化行为):窗内 100 个事件按序
	// 保留 per-part seq+payload 边界——合并只压缩记录条数,不压缩事件边界。
	var batch DeltaBatch
	if err := json.Unmarshal(rec.Payload, &batch); err != nil {
		t.Fatalf("merged payload %s is not a DeltaBatch: %v", rec.Payload, err)
	}
	if len(batch.Events) != n {
		t.Fatalf("batch events = %d, want %d", len(batch.Events), n)
	}
	for i, part := range batch.Events {
		wantTok := fmt.Sprintf("t%d", i)
		if part.Seq != uint64(i+1) {
			t.Fatalf("batch part #%d seq = %d, want %d(per-part seq 按序保留)", i, part.Seq, i+1)
		}
		var got string
		if err := json.Unmarshal(part.Payload, &got); err != nil {
			t.Fatalf("batch part #%d payload %s is not a JSON string: %v", i, part.Payload, err)
		}
		if got != wantTok {
			t.Fatalf("batch part #%d payload = %q, want %q", i, got, wantTok)
		}
	}
}

// TestSubscribeReplayExpandsDeltaBatch(CR3-I1 修复验收):回放把合并批次展开
// 为原始 per-seq 事件——实时与回放形状彻底一致:
//   - 回放侧:3 条 text.delta 的合并批次 [1..3] → 3 条逐事件(seq 1/2/3),
//     随后的 tool_call(seq 4)单条记录原样产出;
//   - 实时侧:live 投递逐事件,与回放同形状(wire 无 FromSeq/范围声明——
//     SessionEvent 已无该字段,编译期即钉死);
//   - 游标以逐事件 Seq 推进,无批量语义。
//
// 订阅晚于全部发布:无并发发布面,回放快照确定;订阅后另发一条 delta 验证
// live 侧粒度,收尾关闭由 defer 的 hub.Close 承担(pump 随 Close 退出)。
func TestSubscribeReplayExpandsDeltaBatch(t *testing.T) {
	jr := &stubEventJournal{}
	hub := mustNewEventHub(t, "batch-expand", jr)
	defer hub.Close()
	hub.BeginRun("run-fs")

	// 3 条 text.delta → EndRun 收口为一条批次记录 [1..3];再逐条 key 事件(seq 4)。
	for i := 0; i < 3; i++ {
		if err := hub.Publish(IngressEvent{RunID: "run-fs", Type: session.EventTextDelta, Payload: json.RawMessage(`"d"`)}); err != nil {
			t.Fatalf("Publish delta #%d: %v", i, err)
		}
	}
	hub.EndRun()
	if err := hub.Publish(IngressEvent{RunID: "run-fs", Type: session.EventToolCall, Payload: json.RawMessage(`"c"`)}); err != nil {
		t.Fatalf("Publish key: %v", err)
	}

	ch, err := hub.Subscribe(context.Background(), 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// 回放:恰 4 条逐事件(批次展开 3 条 + key 单条 1 条),seq 连续。
	for i := 1; i <= 4; i++ {
		ev := recvEvent(t, ch)
		wantType := session.EventTextDelta
		if i == 4 {
			wantType = session.EventToolCall
		}
		if ev.Seq != uint64(i) || ev.Type != wantType {
			t.Fatalf("replay #%d = {seq:%d type:%s}, want {seq:%d %s}(批次展开为 per-seq 事件)", i, ev.Seq, ev.Type, i, wantType)
		}
	}
	requireNoEvent(t, ch) // 回放快照止于注册点 latest=4

	// 实时侧:live 投递逐事件,与回放同形状。
	if err := hub.Publish(IngressEvent{RunID: "run-fs", Type: session.EventTextDelta, Payload: json.RawMessage(`"e"`)}); err != nil {
		t.Fatalf("Publish live delta: %v", err)
	}
	ev := recvEvent(t, ch)
	if ev.Seq != 5 || ev.Type != session.EventTextDelta {
		t.Fatalf("live event = {seq:%d type:%s}, want {seq:5 text.delta}(实时逐事件)", ev.Seq, ev.Type)
	}
	requireNoEvent(t, ch)
}

// TestReplayResumeInsideInterleavedDeltaBatch(CR3-I1 核心验收,评审 §三 I1 的
// 101-104 交错实例):key 事件落在合并窗 seq 区间内部时,游标停在批次中间的
// 断点恢复必须不重复、不丢失:
//
//	101 text.delta "A" ┐
//	102 tool_call      │ 102 逐条落盘(key);101/103/104 合并为批次记录
//	103 text.delta "B" │ [FromSeq=101..Seq=104](批次只含 text 的 3 个 part)
//	104 text.delta "C" ┘
//
// 各 afterSeq 的期望恢复面(逐事件、精确到 seq):
//   - afterSeq=100:101"A" 102 103"B" 104"C"(全量:批次展开 + key 单条);
//   - afterSeq=101:102 103"B" 104"C"(101 不重复);
//   - afterSeq=102:103"B" 104"C"(核心断言:批次头部被跳过——"A" 不重复、
//     "B"/"C" 不丢失,评审指出的重复/丢失场景);
//   - afterSeq=103:104"C"。
func TestReplayResumeInsideInterleavedDeltaBatch(t *testing.T) {
	// buildHub 构造交错流并收口(EndRun flush 批次);预置历史 seq=100(key 事件,
	// 模拟上游游标,使交错流落在 101-104)。每次调用全新 stub + 全新 hub,
	// 各 afterSeq 场景互不干扰。
	buildHub := func(t *testing.T) (*EventHub, *stubEventJournal) {
		t.Helper()
		jr := &stubEventJournal{}
		if err := jr.AppendEvent(session.EventRecord{
			SessionID: "resume-mid", RunID: "run-old",
			Type: session.EventRunStarted, Seq: 100, Time: time.Now(),
		}); err != nil {
			t.Fatalf("seed journal: %v", err)
		}
		hub := mustNewEventHub(t, "resume-mid", jr)
		t.Cleanup(func() { hub.Close() })
		hub.BeginRun("run-rr")
		pub := func(typ, payload string) {
			t.Helper()
			if err := hub.Publish(IngressEvent{RunID: "run-rr", Type: typ, Payload: json.RawMessage(payload)}); err != nil {
				t.Fatalf("Publish %s: %v", typ, err)
			}
		}
		pub(session.EventTextDelta, `"A"`)    // 101
		pub(session.EventToolCall, `"toolX"`) // 102 key:逐条落盘,打断 text 窗
		pub(session.EventTextDelta, `"B"`)    // 103
		pub(session.EventTextDelta, `"C"`)    // 104
		hub.EndRun()                          // 收口:text 批次 [101..104] 落盘
		return hub, jr
	}

	// 存储面形状断言:种子 + tool_call 单条(102)+ text 批次 [101..104],
	// 批次内按序保留 3 个 per-part {seq,payload}。
	hub, jr := buildHub(t)
	if got := hub.LatestSeq(); got != 104 {
		t.Fatalf("LatestSeq = %d, want 104", got)
	}
	recs := jr.snapshot()
	if len(recs) != 3 {
		t.Fatalf("journal records = %d, want 3(种子 + tool_call 单条 + text 批次)", len(recs))
	}
	batchRec := recs[2]
	if batchRec.Type != session.EventTextDelta || batchRec.FromSeq != 101 || batchRec.Seq != 104 {
		t.Fatalf("batch record = {type:%s from:%d seq:%d}, want {text.delta 101 104}", batchRec.Type, batchRec.FromSeq, batchRec.Seq)
	}
	var batch DeltaBatch
	if err := json.Unmarshal(batchRec.Payload, &batch); err != nil {
		t.Fatalf("batch payload %s is not a DeltaBatch: %v", batchRec.Payload, err)
	}
	wantParts := []struct {
		seq     uint64
		payload string
	}{{101, "A"}, {103, "B"}, {104, "C"}}
	if len(batch.Events) != len(wantParts) {
		t.Fatalf("batch parts = %d, want %d(102 的 tool_call 不属于 text 批次)", len(batch.Events), len(wantParts))
	}
	for i, w := range wantParts {
		var got string
		if batch.Events[i].Seq != w.seq {
			t.Fatalf("batch part #%d seq = %d, want %d", i, batch.Events[i].Seq, w.seq)
		}
		if err := json.Unmarshal(batch.Events[i].Payload, &got); err != nil || got != w.payload {
			t.Fatalf("batch part #%d payload = %s, want %q", i, batch.Events[i].Payload, w.payload)
		}
	}

	// 恢复矩阵:每个 afterSeq 一个全新 hub,订阅后只收回放(无并发发布,
	// 快照确定),逐条断言 seq/类型/payload,再断言无多余投递。
	cases := []struct {
		afterSeq uint64
		want     []struct {
			seq     uint64
			typ     string
			payload string // 仅 text.delta 断言;tool_call 填 ""
		}
	}{
		{100, []struct {
			seq     uint64
			typ     string
			payload string
		}{{101, session.EventTextDelta, "A"}, {102, session.EventToolCall, ""}, {103, session.EventTextDelta, "B"}, {104, session.EventTextDelta, "C"}}},
		{101, []struct {
			seq     uint64
			typ     string
			payload string
		}{{102, session.EventToolCall, ""}, {103, session.EventTextDelta, "B"}, {104, session.EventTextDelta, "C"}}},
		{102, []struct {
			seq     uint64
			typ     string
			payload string
		}{{103, session.EventTextDelta, "B"}, {104, session.EventTextDelta, "C"}}},
		{103, []struct {
			seq     uint64
			typ     string
			payload string
		}{{104, session.EventTextDelta, "C"}}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("afterSeq=%d", tc.afterSeq), func(t *testing.T) {
			hub, _ := buildHub(t)
			ch, err := hub.Subscribe(context.Background(), tc.afterSeq)
			if err != nil {
				t.Fatalf("Subscribe(afterSeq=%d): %v", tc.afterSeq, err)
			}
			for i, w := range tc.want {
				ev := recvEvent(t, ch)
				if ev.Seq != w.seq || ev.Type != w.typ {
					t.Fatalf("afterSeq=%d resumed #%d = {seq:%d type:%s}, want {seq:%d %s}", tc.afterSeq, i, ev.Seq, ev.Type, w.seq, w.typ)
				}
				if w.payload != "" {
					var got string
					if err := json.Unmarshal(ev.Payload, &got); err != nil || got != w.payload {
						t.Fatalf("afterSeq=%d resumed seq %d payload = %s, want %q", tc.afterSeq, ev.Seq, ev.Payload, w.payload)
					}
				}
			}
			// 无重复:恢复面恰好是期望集合(101 的 "A" 在 afterSeq>=101 时绝不重放)。
			requireNoEvent(t, ch)
		})
	}
}

// TestEventHubReplayIntegrity(I4 修复验收):回放完整性三态的播种与粘滞置位——
//   - 播种:全新空 journal / 纯内存 hub → Complete(本进程观察面即全部);
//     播种自非空 journal(既有历史)→ Unknown(跨进程纪元完整性不可验证,
//     诚实优先,绝不把不可验证的历史报告为完整);
//   - 置位:stub journal 注错 → key 落盘失败 → Incomplete;注错自愈后的成功
//     发布不翻回(粘滞,永不复位);合并 delta flush 失败同样置位;
//   - 方向单调:Unknown 播种后遇失败 → Incomplete 一去不返;成功发布不改变
//     任何三态(只有失败路径是写点)。
func TestEventHubReplayIntegrity(t *testing.T) {
	t.Run("FreshEmptyJournalComplete", func(t *testing.T) {
		hub := mustNewEventHub(t, "fresh-journal", &stubEventJournal{})
		defer hub.Close()
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityComplete {
			t.Fatalf("fresh hub on empty journal replayIntegrity = %q, want %q", got, ReplayIntegrityComplete)
		}
	})

	t.Run("PureInMemoryComplete", func(t *testing.T) {
		hub := mustNewEventHub(t, "fresh-mem", nil)
		defer hub.Close()
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityComplete {
			t.Fatalf("pure in-memory hub replayIntegrity = %q, want %q", got, ReplayIntegrityComplete)
		}
	})

	t.Run("SeededFromNonEmptyJournalUnknown", func(t *testing.T) {
		// 预置历史 [1..2](模拟上一进程纪元的落盘产物)→ 新 hub 播种 Unknown。
		jr := &stubEventJournal{}
		for seq := uint64(1); seq <= 2; seq++ {
			if err := jr.AppendEvent(session.EventRecord{SessionID: "seeded", RunID: "run-old", Type: session.EventToolCall, Seq: seq, Time: time.Now()}); err != nil {
				t.Fatalf("seed journal seq %d: %v", seq, err)
			}
		}
		hub := mustNewEventHub(t, "seeded", jr)
		defer hub.Close()
		if got := hub.LatestSeq(); got != 2 {
			t.Fatalf("LatestSeq = %d, want 2(播种自既有历史)", got)
		}
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityUnknown {
			t.Fatalf("seeded hub replayIntegrity = %q, want %q(跨纪元完整性不可验证)", got, ReplayIntegrityUnknown)
		}
		// 本纪元的成功发布不改变 Unknown(只有失败路径是写点)。
		if err := hub.Publish(IngressEvent{RunID: "run-new", Type: session.EventToolCall, Payload: json.RawMessage(`"c"`)}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityUnknown {
			t.Fatalf("replayIntegrity after healthy publish = %q, want %q(成功发布不改变三态)", got, ReplayIntegrityUnknown)
		}
	})

	t.Run("KeyEventAppendFailureStickyIncomplete", func(t *testing.T) {
		// failFirst=1:首次 AppendEvent 注错、其后自愈——验证「失败后的成功
		// 发布不翻回」的粘滞语义(I4:失败是「曾经发生」的单调事实)。
		jr := &stubEventJournal{failFirst: 1}
		hub := mustNewEventHub(t, "sticky-key", jr)
		defer hub.Close()
		hub.BeginRun("run-i")
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityComplete {
			t.Fatalf("fresh hub replayIntegrity = %q, want %q", got, ReplayIntegrityComplete)
		}
		if err := hub.Publish(IngressEvent{RunID: "run-i", Type: session.EventToolCall, Payload: json.RawMessage(`"c"`)}); err == nil {
			t.Fatal("Publish key event with failing journal = nil, want error")
		}
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityIncomplete {
			t.Fatalf("replayIntegrity after failed key publish = %q, want %q(落盘失败置位)", got, ReplayIntegrityIncomplete)
		}
		// 注错自愈后的成功发布不翻回:标记粘滞。
		if err := hub.Publish(IngressEvent{RunID: "run-i", Type: session.EventToolResult, Payload: json.RawMessage(`"r"`)}); err != nil {
			t.Fatalf("Publish after journal recovery: %v", err)
		}
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityIncomplete {
			t.Fatalf("replayIntegrity flipped after successful publish = %q, want %q(永不复位)", got, ReplayIntegrityIncomplete)
		}
	})

	t.Run("DeltaFlushFailure", func(t *testing.T) {
		jr := &stubEventJournal{appendErr: errors.New("disk full")}
		hub := mustNewEventHub(t, "incomplete-delta", jr)
		defer hub.Close()
		hub.BeginRun("run-j")
		// delta 发布不落盘(进合并缓冲):Publish 成功,标记仍 Complete。
		if err := hub.Publish(IngressEvent{RunID: "run-j", Type: session.EventTextDelta, Payload: json.RawMessage(`"d"`)}); err != nil {
			t.Fatalf("Publish delta: %v", err)
		}
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityComplete {
			t.Fatalf("replayIntegrity before flush = %q, want %q(flush 未发生,无失败证据)", got, ReplayIntegrityComplete)
		}
		hub.EndRun() // 收口 flush 撞注错:降级 warn,标记置位
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityIncomplete {
			t.Fatalf("replayIntegrity after failed delta flush = %q, want %q(重放缺 delta 段)", got, ReplayIntegrityIncomplete)
		}
		if recs := jr.snapshot(); len(recs) != 0 {
			t.Fatalf("journal records = %d, want 0(注错未落盘,缺段由三态显式暴露)", len(recs))
		}
	})

	t.Run("SeededThenFailureIncomplete", func(t *testing.T) {
		// Unknown 播种后的失败 → Incomplete(置位方向单调:Complete/Unknown →
		// Incomplete 一去不返)。种子先落(消费掉零次注错),再开 failFirst。
		jr := &stubEventJournal{}
		if err := jr.AppendEvent(session.EventRecord{SessionID: "seeded-2", RunID: "run-old", Type: session.EventToolCall, Seq: 9, Time: time.Now()}); err != nil {
			t.Fatalf("seed journal: %v", err)
		}
		jr.failFirst = 1
		hub := mustNewEventHub(t, "seeded-2", jr)
		defer hub.Close()
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityUnknown {
			t.Fatalf("seeded hub replayIntegrity = %q, want %q", got, ReplayIntegrityUnknown)
		}
		if err := hub.Publish(IngressEvent{RunID: "run-new", Type: session.EventToolCall, Payload: json.RawMessage(`"c"`)}); err == nil {
			t.Fatal("Publish key event with failing journal = nil, want error")
		}
		if got := hub.ReplayIntegrity(); got != ReplayIntegrityIncomplete {
			t.Fatalf("replayIntegrity = %q, want %q(Unknown → Incomplete 单向)", got, ReplayIntegrityIncomplete)
		}
	})
}

// TestNewEventHubPropagatesSeqRangeError(第三轮评审 I3 修复验收):SeqRange
// 播种注错(Journal 读取失败)→ NewEventHub 返回 (nil, err) 显式上抛——
//   - 错误链包裹底层原因(errors.Is 可判);
//   - hub 恒为 nil:绝不创建「看似健康」的 hub(旧形态把读取失败降级为空历史:
//     replayIntegrity 报 complete 且 seq 从 1 重起,与既有记录冲突);
//   - 失败构造零副作用:同一 stub 恢复后可正常构造,空历史诚实报 Complete。
func TestNewEventHubPropagatesSeqRangeError(t *testing.T) {
	cause := errors.New("runs dir unreadable")
	jr := &stubEventJournal{seqRangeErr: cause}
	hub, err := NewEventHub("i3-seed-err", jr)
	if err == nil {
		t.Fatal("NewEventHub with failing SeqRange = nil error, want error(读取失败绝不当作空历史)")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("NewEventHub err = %v, want wrap of %v", err, cause)
	}
	if hub != nil {
		t.Fatalf("NewEventHub with failing SeqRange returned %+v, want nil(绝不创建看似健康的 hub)", hub)
	}

	// 恢复播种后同一 stub 构造成功(失败不缓存、可重试),空历史照旧 Complete。
	jr.seqRangeErr = nil
	hub2 := mustNewEventHub(t, "i3-seed-err", jr)
	defer hub2.Close()
	if got := hub2.ReplayIntegrity(); got != ReplayIntegrityComplete {
		t.Fatalf("recovered hub replayIntegrity = %q, want %q(空历史诚实报 complete)", got, ReplayIntegrityComplete)
	}
	if got := hub2.LatestSeq(); got != 0 {
		t.Fatalf("recovered hub LatestSeq = %d, want 0(空历史从零起算)", got)
	}
}

// ── P3-3:Subscribe Replay+Live 原子切换(防 Race A/B)+ ErrCursorExpired ────

// TestAttachReplayNoGap(总方案第十一节 → P3-3):单 hub,N 轮
// BeginRun/Publish(混合 key+delta,含合并窗)/EndRun;M 个订阅者在随机时刻以
// 随机 fromSeq(含 0、含注册时 latest)Subscribe,各收集到 Close;断言每个
// 订阅者收到的 seq 严格覆盖 (fromSeq, finalLatest] 无缺口无重复。
//
// 记账口径(CR3-I1 起):回放已把合并批次展开为原始 per-seq 事件,实时与回放
// 同形状——收到的每个 SessionEvent 恰计入其自身 Seq,不再按合并记录范围展开
// (展开记账在批次时代反而可能掩盖批次内部的缺段:收到批次末 seq 即误认为
// 整窗已覆盖)。「无重复」由收到流严格递增独立钉死。
//
// 时序收口:Close 前先轮询等全部订阅者抵达 finalLatest(10s 上界)——Close
// 断开订阅者,提前 Close 会截断在途投递使覆盖断言失真;fromSeq==finalLatest
// 的订阅者期望空覆盖,轮询按此豁免。随机源按 goroutine 独立播种(固定种子,
// 失败可复现,且 rand.Rand 非并发安全)。
func TestAttachReplayNoGap(t *testing.T) {
	jr := &stubEventJournal{}
	hub := mustNewEventHub(t, "replay-nogap", jr)
	defer hub.Close()

	const (
		rounds      = 6
		perRound    = 19 // 4+4+4 text.delta + tool_call + 3 thinking.delta + 2 usage.progress + tool_result
		finalLatest = rounds * perRound
	)

	// subRec 是一个订阅者的收集记录:fromSeq、收到流(期望严格递增)、收尾
	// 信号(collector 退出即 Close 已断开订阅者)。
	type subRec struct {
		name string
		from uint64
		ch   <-chan session.SessionEvent
		mu   sync.Mutex
		seqs []uint64
		done chan struct{}
	}
	var (
		subsMu sync.Mutex
		subs   []*subRec
	)
	startSub := func(name string, from uint64) {
		ch, err := hub.Subscribe(context.Background(), from)
		if err != nil {
			t.Errorf("sub %s: Subscribe(from=%d): %v", name, from, err)
			return
		}
		rec := &subRec{name: name, from: from, ch: ch, done: make(chan struct{})}
		subsMu.Lock()
		subs = append(subs, rec)
		subsMu.Unlock()
		go func() {
			defer close(rec.done)
			for ev := range ch {
				rec.mu.Lock()
				rec.seqs = append(rec.seqs, ev.Seq)
				rec.mu.Unlock()
			}
		}()
	}

	// 发布者:N 轮混合 key+delta;delta 突发远小于 50ms 窗 ⇒ 每轮 EndRun 收口
	// 出合并记录;轮间随机小睡给订阅者留出随机订阅窗口。
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		rng := rand.New(rand.NewSource(101))
		for r := 0; r < rounds; r++ {
			runID := fmt.Sprintf("run-r%d", r)
			hub.BeginRun(runID)
			pub := func(typ string) {
				if err := hub.Publish(IngressEvent{RunID: runID, Type: typ, Payload: json.RawMessage(`"p"`)}); err != nil {
					t.Errorf("publish %s in %s: %v", typ, runID, err)
				}
			}
			for i := 0; i < 4; i++ {
				pub(session.EventTextDelta)
			}
			pub(session.EventToolCall) // key,打断 text 窗:journal 物理序 ≠ seq 序
			for i := 0; i < 4; i++ {
				pub(session.EventTextDelta)
			}
			for i := 0; i < 3; i++ {
				pub(session.EventThinkingDelta)
			}
			for i := 0; i < 2; i++ {
				pub(session.EventUsageProgress)
			}
			pub(session.EventToolResult) // key
			for i := 0; i < 4; i++ {
				pub(session.EventTextDelta)
			}
			hub.EndRun()
			time.Sleep(time.Duration(rng.Intn(3)) * time.Millisecond)
		}
	}()

	startSub("early", 0) // 发布前订阅:fromSeq=0,全程实时
	var randWG sync.WaitGroup
	for i := 0; i < 5; i++ {
		randWG.Add(1)
		go func(i int) {
			defer randWG.Done()
			rng := rand.New(rand.NewSource(int64(200 + i)))
			time.Sleep(time.Duration(rng.Intn(12)) * time.Millisecond)
			latest := hub.LatestSeq()
			var from uint64
			switch i % 3 {
			case 0:
				from = 0 // 含 0
			case 1:
				from = latest // 含注册时 latest
			case 2:
				if latest > 0 {
					from = uint64(rng.Intn(int(latest) + 1)) // [0..latest] 随机
				}
			}
			startSub(fmt.Sprintf("rand-%d", i), from)
		}(i)
	}
	<-pubDone
	if got := hub.LatestSeq(); got != finalLatest {
		t.Fatalf("LatestSeq after publisher = %d, want %d(%d 轮 × 每轮 %d 事件,发布不得失败)", got, finalLatest, rounds, perRound)
	}

	// waitAtLatest 轮询等全部已注册订阅者抵达 finalLatest(from==finalLatest
	// 者期望空覆盖,豁免)。
	waitAtLatest := func() {
		deadline := time.Now().Add(10 * time.Second)
		for {
			subsMu.Lock()
			var pending []string
			for _, s := range subs {
				if s.from == finalLatest {
					continue
				}
				s.mu.Lock()
				maxSeq := uint64(0)
				for _, sq := range s.seqs {
					if sq > maxSeq {
						maxSeq = sq
					}
				}
				n := len(s.seqs)
				s.mu.Unlock()
				if maxSeq != finalLatest {
					pending = append(pending, fmt.Sprintf("%s(from=%d, max=%d of %d events)", s.name, s.from, maxSeq, n))
				}
			}
			subsMu.Unlock()
			if len(pending) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("subscribers did not reach final latest %d: %v", finalLatest, pending)
			}
			time.Sleep(500 * time.Microsecond)
		}
	}
	waitAtLatest()
	randWG.Wait() // 全部随机订阅者已完成订阅(先于 Close,杜绝对已关 hub 订阅)

	// 收口期确定性订阅:全量回放(fromSeq=0)与恰在 latest(fromSeq=
	// finalLatest:零回放、零实时,期望零事件)。
	startSub("late-full", 0)
	startSub("late-at-latest", finalLatest)
	waitAtLatest()

	hub.Close() // 各收集到 Close:订阅通道关闭,collector 退出
	for _, s := range subs {
		select {
		case <-s.done:
		case <-time.After(10 * time.Second):
			t.Fatalf("sub %s collector did not finish after Close", s.name)
		}
	}

	// journal 非空(事件确实落盘;逐事件记账不再需要记录面的覆盖展开表)。
	if len(jr.snapshot()) == 0 {
		t.Fatal("journal has no event records")
	}

	for _, s := range subs {
		s.mu.Lock()
		seqs := append([]uint64(nil), s.seqs...)
		s.mu.Unlock()

		// 无重复:收到流严格递增(回放按 Seq 升序 + live 递增 + watermark 衔接)。
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Errorf("sub %s: seq stream not strictly increasing at #%d (%d after %d): %v", s.name, i, seqs[i], seqs[i-1], seqs)
				break
			}
		}

		// 严格覆盖 (fromSeq, finalLatest]:CR3-I1 起回放已展开为 per-seq 事件,
		// 收到的每个 SessionEvent 恰计入其自身 Seq——不再按合并记录范围展开
		// (展开记账可能掩盖批次内部缺段:收到批次末 seq 即误认为整窗覆盖)。
		covered := make([]bool, finalLatest+1)
		for _, sq := range seqs {
			if sq > finalLatest {
				t.Errorf("sub %s: received seq %d beyond final latest %d", s.name, sq, finalLatest)
				continue
			}
			covered[sq] = true
		}
		for x := s.from + 1; x <= finalLatest; x++ {
			if !covered[x] {
				t.Errorf("sub %s (from=%d): seq %d missing — coverage of (%d,%d] has a gap; got %v", s.name, s.from, x, s.from, finalLatest, seqs)
				break
			}
		}
	}

	// late-at-latest:订阅时 latest 已是 finalLatest ⇒ 无回放可读、无实时可收。
	for _, s := range subs {
		if s.name != "late-at-latest" {
			continue
		}
		s.mu.Lock()
		n := len(s.seqs)
		s.mu.Unlock()
		if n != 0 {
			t.Errorf("late-at-latest received %d events, want 0", n)
		}
	}
}

// TestSubscribeCursorExpired:窗口校验(plan P3-3 ①)——fromSeq 超前 latest
// (客户端声称收到未发事件)与落于保留历史 earliest 之前 → ErrCursorExpired;
// fromSeq=0(从保留最早开始)与区间内(含恰为 latest)→ 成功。
func TestSubscribeCursorExpired(t *testing.T) {
	t.Run("AheadOfLatest", func(t *testing.T) {
		jr := &stubEventJournal{}
		hub := mustNewEventHub(t, "cursor-ahead", jr)
		defer hub.Close()
		hub.BeginRun("run-c")
		if err := hub.Publish(IngressEvent{RunID: "run-c", Type: session.EventToolCall, Payload: json.RawMessage(`"c"`)}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if err := hub.Publish(IngressEvent{RunID: "run-c", Type: session.EventTextDelta, Payload: json.RawMessage(`"d"`)}); err != nil {
			t.Fatalf("Publish: %v", err)
		}

		for _, from := range []uint64{3, 100} { // latest=2:超前一律拒
			if _, err := hub.Subscribe(context.Background(), from); !errors.Is(err, ErrCursorExpired) {
				t.Fatalf("Subscribe(%d) err = %v, want ErrCursorExpired", from, err)
			}
		}
		for _, from := range []uint64{0, 1, 2} { // 0 与区间内(含恰为 latest)成功
			if _, err := hub.Subscribe(context.Background(), from); err != nil {
				t.Fatalf("Subscribe(%d): %v", from, err)
			}
		}
	})

	t.Run("BeforeEarliest", func(t *testing.T) {
		// 白盒预置 journal 历史 [3..5]:earliest=3。V1 无修剪,该状态仅 hub
		// 重建且历史被截短时可达,直接构造以驱动校验分支。
		jr := &stubEventJournal{}
		for seq := uint64(3); seq <= 5; seq++ {
			if err := jr.AppendEvent(session.EventRecord{SessionID: "cursor-old", RunID: "run-old", Type: session.EventToolCall, Seq: seq, Time: time.Now()}); err != nil {
				t.Fatalf("seed journal seq %d: %v", seq, err)
			}
		}
		hub := mustNewEventHub(t, "cursor-old", jr)
		defer hub.Close()
		if got := hub.EarliestSeq(); got != 3 {
			t.Fatalf("EarliestSeq = %d, want 3", got)
		}
		if got := hub.LatestSeq(); got != 5 {
			t.Fatalf("LatestSeq = %d, want 5", got)
		}

		// fromSeq=1:要的首事件 2 落于保留最早 3 之前 → 拒。
		if _, err := hub.Subscribe(context.Background(), 1); !errors.Is(err, ErrCursorExpired) {
			t.Fatalf("Subscribe(1) err = %v, want ErrCursorExpired", err)
		}
		// fromSeq=0(从保留最早开始)与 fromSeq=2(首事件恰为 earliest)成功。
		for _, from := range []uint64{0, 2} {
			if _, err := hub.Subscribe(context.Background(), from); err != nil {
				t.Fatalf("Subscribe(%d): %v", from, err)
			}
		}
	})
}

// TestSubscribeAfterClose:Close 后 Subscribe 一律拒绝(plan P3-3 ①)。
func TestSubscribeAfterClose(t *testing.T) {
	hub := mustNewEventHub(t, "sub-closed", nil) // 纯内存 hub 走同一拒绝路径
	if err := hub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := hub.Subscribe(context.Background(), 0); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("Subscribe after Close err = %v, want ErrHubClosed", err)
	}
}

// TestSubscribeCtxCancelCleansUp:断线清理(plan P3-3 ④)——ctx 取消后 pump
// 退出,hub.subs 不残留(内部计数断言,LatestSeq 看不到订阅者),订阅通道
// 关闭(消费方 range 语义的断线告知)。
func TestSubscribeCtxCancelCleansUp(t *testing.T) {
	hub := mustNewEventHub(t, "sub-cancel", nil) // 纯内存:无回放,直入 live 等待
	defer hub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := hub.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	subsCount := func() int {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return len(hub.subs)
	}
	if n := subsCount(); n != 1 {
		t.Fatalf("hub.subs after Subscribe = %d, want 1", n)
	}

	// 实时通路先验证(发布在订阅后:纯内存 hub 无回放,订阅前的收不到)。
	if err := hub.Publish(IngressEvent{RunID: "run-x", Type: session.EventTextDelta, Payload: json.RawMessage(`"d"`)}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ev := recvEvent(t, ch); ev.Seq != 1 {
		t.Fatalf("first event seq = %d, want 1", ev.Seq)
	}

	cancel() // 模拟断线:pump 必须自行退出并摘除注册
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n := subsCount(); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("hub.subs not cleaned up after ctx cancel (still %d)", subsCount())
		}
		time.Sleep(500 * time.Microsecond)
	}

	// 订阅通道最终关闭(先排干在途缓冲,再见到关闭)。
	closed := false
	for !closed {
		select {
		case _, ok := <-ch:
			if !ok {
				closed = true
			}
		case <-time.After(5 * time.Second):
			t.Fatal("subscribe channel not closed after ctx cancel")
		}
	}
	// hub 仍健康:清理不影响写侧。
	if err := hub.Publish(IngressEvent{RunID: "run-x", Type: session.EventTextDelta, Payload: json.RawMessage(`"d"`)}); err != nil {
		t.Fatalf("Publish after subscriber cleanup: %v", err)
	}
	if got := hub.LatestSeq(); got != 2 {
		t.Fatalf("LatestSeq = %d, want 2", got)
	}
}
