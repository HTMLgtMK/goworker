//go:build unix

// journal_test.go 验证 Run 状态 Journal（P2-2）。flock 语义为 unix 族专属
// （非 unix 平台是无锁桩，acquireDirLock 恒成功、永不返回 ErrStoreLocked，
// 见 store_lock_test.go 文件头），TestRunJournalRejectsLockedStore 的
// ErrStoreLocked 断言故本文件仅在 unix 构建。
package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRunJournalRoundTrip 验证 journal 的字节往返：Open→Append×2→Close→重读，
// 记录逐字段一致，且 lastGoodEnd 恰为文件大小（两条记录都完整合法）。
func TestRunJournalRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := tempDir(t)
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	s, err := repo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sid := s.SessionID()
	runID := "run-1000-1"

	j, err := repo.OpenRunJournal(ctx, sid, runID)
	if err != nil {
		t.Fatalf("OpenRunJournal: %v", err)
	}

	at := time.Now()
	want := []RunStateRecord{
		{Kind: "state", SessionID: sid, RunID: runID, State: "running", At: at},
		{Kind: "state", SessionID: sid, RunID: runID, State: "failed", Err: "boom", At: at.Add(time.Second)},
	}
	for i, rec := range want {
		if err := j.Append(rec); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(s.dir, "runs", runID+".jsonl")
	recs, lastGoodEnd, err := loadStateRecords(path)
	if err != nil {
		t.Fatalf("loadStateRecords: %v", err)
	}
	if len(recs) != len(want) {
		t.Fatalf("loaded %d records, want %d", len(recs), len(want))
	}
	for i, w := range want {
		got := recs[i]
		if got.Kind != w.Kind || got.SessionID != w.SessionID || got.RunID != w.RunID ||
			got.State != w.State || got.Err != w.Err || got.Persisted != w.Persisted || !got.At.Equal(w.At) {
			t.Errorf("record #%d = %+v, want %+v", i, got, w)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat journal file: %v", err)
	}
	if lastGoodEnd != info.Size() {
		t.Errorf("lastGoodEnd = %d, want file size %d (两条记录都完整)", lastGoodEnd, info.Size())
	}
}

// TestRunJournalRejectsLockedStore 验证 custody 与跨进程锁的两道防线：
//
//	① 会话未在本 Repository 的 open 缓存 → ErrSessionNotOpen（即使该会话存在于
//	   磁盘且被另一实例持有）；
//	② 跨进程模拟：另一 Repository（同 root）Open 同 id → ErrStoreLocked（flock
//	   宁拒勿双写，store_lock_test.go 语义延伸）——Journal 无独立锁面却仍然不可
//	   能出现第二写者（勘察修正 C）。
//	顺带钉死 validRunID 的 traversal 防线：畸形 runID 在拼路径前拒绝，runs/ 目录
//	不被创建。
func TestRunJournalRejectsLockedStore(t *testing.T) {
	ctx := context.Background()
	root := tempDir(t)
	repoA, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository A: %v", err)
	}
	t.Cleanup(func() { _ = repoA.Close() })

	s, err := repoA.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sid := s.SessionID()
	runID := "run-2000-1"

	// 同 root 的另一 Repository（跨进程模拟）：会话存在但未在本实例 open。
	repoB, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository B: %v", err)
	}
	t.Cleanup(func() { _ = repoB.Close() })

	// ① 未 Open 的 id → ErrSessionNotOpen。
	if _, err := repoB.OpenRunJournal(ctx, sid, runID); !errors.Is(err, ErrSessionNotOpen) {
		t.Fatalf("OpenRunJournal on not-open session: err = %v, want ErrSessionNotOpen", err)
	}

	// ② 另一 Repository Open 同 id → ErrStoreLocked（flock 被 repoA 的 store 持有）。
	if _, err := repoB.Open(sid); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("repoB.Open(same id): err = %v, want ErrStoreLocked", err)
	}

	// ③ validRunID 防线：空/路径穿越/非 run-<十进制>-<十进制> 形在拼路径前拒绝。
	for _, bad := range []string{"", "../evil", "runs", "run-1", "run-x-1", "run-1-x", "run-1-2-3"} {
		if j, err := repoA.OpenRunJournal(ctx, sid, bad); err == nil {
			j.Close()
			t.Errorf("OpenRunJournal with runID %q: err = nil, want rejection", bad)
		}
	}
	// 非法 runID 未创建 runs 目录（校验先于 MkdirAll）。
	if _, err := os.Stat(filepath.Join(s.dir, "runs")); !os.IsNotExist(err) {
		t.Errorf("runs dir should not exist after rejected opens, stat err = %v", err)
	}
}

// TestRunJournalDoubleCreateRejected 验证 O_EXCL：同 runID 的第二次创建被拒
// （ErrRunExists）；先例句柄关闭后文件已在磁盘，重试仍被拒——P2-1 的跨重启
// RunID 碰撞兜底：碰撞是可观测错误，不是静默覆盖。
func TestRunJournalDoubleCreateRejected(t *testing.T) {
	ctx := context.Background()
	root := tempDir(t)
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	s, err := repo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sid := s.SessionID()
	runID := "run-3000-1"

	j1, err := repo.OpenRunJournal(ctx, sid, runID)
	if err != nil {
		t.Fatalf("first OpenRunJournal: %v", err)
	}

	// 首例仍打开：第二次创建被拒。
	if _, err := repo.OpenRunJournal(ctx, sid, runID); !errors.Is(err, ErrRunExists) {
		t.Fatalf("second OpenRunJournal: err = %v, want ErrRunExists", err)
	}

	if err := j1.Append(RunStateRecord{Kind: "state", SessionID: sid, RunID: runID, State: "running", At: time.Now()}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 文件已在磁盘：关闭后的重试同样被拒。
	if _, err := repo.OpenRunJournal(ctx, sid, runID); !errors.Is(err, ErrRunExists) {
		t.Fatalf("OpenRunJournal after close: err = %v, want ErrRunExists", err)
	}

	// 已有内容未被破坏。
	recs, _, err := loadStateRecords(filepath.Join(s.dir, "runs", runID+".jsonl"))
	if err != nil {
		t.Fatalf("loadStateRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].State != "running" || recs[0].RunID != runID {
		t.Fatalf("records after double-create attempts = %+v, want 1 running record intact", recs)
	}
}

// TestRestoreInterruptedRun 完整版（总方案第十一节 → P2-2）：daemon 重启后对
// 上一进程遗留 runs/ 的恢复语义——
//
//	① 末条 state 非终态的 run（run-B）被追加恰好一条 interrupted 标记，原有记录
//	   前缀不被改写；
//	② 已终态的 run（run-A）逐字节不动；
//	③ current.jsonl（legacy 布局根槽）的 ActiveView 历史完整可读；
//	④ 除 interrupted 标记外无任何新增记录——恢复绝不执行工具/不重跑，用记录数
//	   断言钉死；
//	⑤ 幂等：重扫无新增标记。
//
// run-A/run-B 文件名不经 validRunID：恢复扫描的文件名来自 os.ReadDir 而非调用
// 方输入（validRunID 注释）。
func TestRestoreInterruptedRun(t *testing.T) {
	ctx := context.Background()
	root := tempDir(t)

	// legacy 布局 store（root 槽 current.jsonl），Commit 数轮。
	s, err := Open(root)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	for _, round := range [][]string{{"m0", "m1"}, {"m2", "m3"}, {"m4"}} {
		if _, err := s.Commit(commMsgs(round, "user")); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	sid := s.SessionID()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 遗留 runs/：run-A 已终态 completed；run-B 末记录 running（上一进程未收尾）。
	runsDir := filepath.Join(root, "runs")
	if err := os.MkdirAll(runsDir, 0o700); err != nil {
		t.Fatalf("mkdir runs: %v", err)
	}
	at := time.Now()
	writeRunJournal := func(name string, recs ...RunStateRecord) {
		t.Helper()
		var buf []byte
		for _, rec := range recs {
			line, err := json.Marshal(rec)
			if err != nil {
				t.Fatalf("marshal record for %s: %v", name, err)
			}
			buf = append(buf, line...)
			buf = append(buf, '\n')
		}
		if err := os.WriteFile(filepath.Join(runsDir, name), buf, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	writeRunJournal("run-A.jsonl",
		RunStateRecord{Kind: "state", SessionID: sid, RunID: "run-A", State: "running", At: at},
		RunStateRecord{Kind: "state", SessionID: sid, RunID: "run-A", State: "completed", Persisted: true, At: at.Add(time.Second)},
	)
	writeRunJournal("run-B.jsonl",
		RunStateRecord{Kind: "state", SessionID: sid, RunID: "run-B", State: "running", At: at.Add(2 * time.Second)},
	)

	// 基线快照：恢复前各文件的记录行数与完整字节（④ 的记录数断言基线）。
	countLines := func(path string) (lines, size int64) {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return int64(bytes.Count(data, []byte{'\n'})), int64(len(data))
	}
	runAPath := filepath.Join(runsDir, "run-A.jsonl")
	runBPath := filepath.Join(runsDir, "run-B.jsonl")
	curPath := filepath.Join(root, "current.jsonl")
	runALines, runASize := countLines(runAPath)
	runBLines, _ := countLines(runBPath)
	curLines, curSize := countLines(curPath)
	runABefore, err := os.ReadFile(runAPath)
	if err != nil {
		t.Fatalf("read run-A baseline: %v", err)
	}

	// 第一进程生命周期：repo 挂载 legacy 根槽 → repo.Close 全释放。
	repo1, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository 1: %v", err)
	}
	if _, err := repo1.Open(sid); err != nil {
		t.Fatalf("repo1.Open legacy slot: %v", err)
	}
	if err := repo1.Close(); err != nil {
		t.Fatalf("repo1.Close: %v", err)
	}

	// 重新 OpenRepository + Open（重启模拟）→ 恢复扫描。
	repo2, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository 2: %v", err)
	}
	t.Cleanup(func() { _ = repo2.Close() })
	st2, err := repo2.Open(sid)
	if err != nil {
		t.Fatalf("repo2.Open: %v", err)
	}

	marked, err := repo2.MarkInterruptedRuns(ctx, sid)
	if err != nil {
		t.Fatalf("MarkInterruptedRuns: %v", err)
	}
	if len(marked) != 1 || marked[0] != "run-B" {
		t.Fatalf("marked = %v, want [run-B]", marked)
	}

	// ① run-B 被追加恰好一条 interrupted 标记，原前缀不被改写。
	bRecs, _, err := loadStateRecords(runBPath)
	if err != nil {
		t.Fatalf("load run-B after restore: %v", err)
	}
	if len(bRecs) != int(runBLines)+1 {
		t.Fatalf("run-B records = %d, want %d (原记录 + 恰一条标记)", len(bRecs), runBLines+1)
	}
	if bRecs[0].State != "running" || bRecs[0].RunID != "run-B" {
		t.Fatalf("run-B first record = %+v, want original running record intact", bRecs[0])
	}
	marker := bRecs[len(bRecs)-1]
	if marker.Kind != "state" || marker.State != StateInterrupted || marker.RunID != "run-B" ||
		marker.SessionID != sid || marker.At.IsZero() {
		t.Fatalf("run-B marker record = %+v, want interrupted marker for run-B", marker)
	}

	// ② run-A 逐字节不动。
	runAAfter, err := os.ReadFile(runAPath)
	if err != nil {
		t.Fatalf("read run-A after restore: %v", err)
	}
	if !bytes.Equal(runABefore, runAAfter) {
		t.Fatalf("run-A modified by restore: %d bytes before, %d after", len(runABefore), len(runAAfter))
	}

	// ③ current.jsonl 的 ActiveView 历史完整可读。
	if got := viewIDs(st2.ActiveView()); !equalSlices(got, []string{"m0", "m1", "m2", "m3", "m4"}) {
		t.Fatalf("ActiveView after restore = %v, want [m0 m1 m2 m3 m4]", got)
	}

	// ④ 除 interrupted 标记外无任何新增记录（恢复绝不重跑）。
	bLines, bSize := countLines(runBPath)
	aLines, aSize := countLines(runAPath)
	curLines2, curSize2 := countLines(curPath)
	if bLines != runBLines+1 || bSize == 0 {
		t.Fatalf("run-B lines after restore = %d, want %d", bLines, runBLines+1)
	}
	if aLines != runALines || aSize != runASize {
		t.Fatalf("run-A lines/size changed: %d/%d, want %d/%d", aLines, aSize, runALines, runASize)
	}
	if curLines2 != curLines || curSize2 != curSize {
		t.Fatalf("current.jsonl grew during restore: %d/%d, want %d/%d", curLines2, curSize2, curLines, curSize)
	}

	// ⑤ 幂等：重扫无标记、无新增记录。
	marked2, err := repo2.MarkInterruptedRuns(ctx, sid)
	if err != nil {
		t.Fatalf("second MarkInterruptedRuns: %v", err)
	}
	if len(marked2) != 0 {
		t.Fatalf("second MarkInterruptedRuns = %v, want empty", marked2)
	}
	bLines2, _ := countLines(runBPath)
	if bLines2 != bLines {
		t.Fatalf("run-B lines after rescan = %d, want %d (幂等：不追加第二个标记)", bLines2, bLines)
	}
}

// ── P3-1：EventRecord 契约与跨文件事件读写 ──────────────────────────────

// mustAppendEvent 经 RunJournalStore.AppendEvent 落一条事件记录（Kind 显式置
// eventKind，对齐 agent 侧 state 记录的显式 Kind 先例；AppendEvent 内还有存储
// 边界强制，双保险）。
func mustAppendEvent(t *testing.T, jst *RunJournalStore, sid, runID, typ string, seq, fromSeq uint64, at time.Time, payload string) {
	t.Helper()
	if err := jst.AppendEvent(EventRecord{
		Kind:      eventKind,
		SessionID: sid,
		RunID:     runID,
		Type:      typ,
		Seq:       seq,
		FromSeq:   fromSeq,
		Time:      at,
		Payload:   json.RawMessage(payload),
	}); err != nil {
		t.Fatalf("AppendEvent %s seq %d: %v", typ, seq, err)
	}
}

// eventSeqs 提取记录序号序列，供有序性断言。
func eventSeqs(recs []EventRecord) []uint64 {
	out := make([]uint64, len(recs))
	for i, r := range recs {
		out[i] = r.Seq
	}
	return out
}

// uint64sEqual 判定两个 []uint64 逐元素相等（store_test.go 的 equalSlices 只收 string）。
func uint64sEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestEventRecordsCrossFileRoundTrip 钉死 P3-1 的核心不变量「文件名序=seq 序」：
// 两个 run 文件、Seq 跨文件连续（run-1: 1..3 含一条合并 [2..3] 记录；run-2:
// 4..5），ReadEvents(0) 全量有序拼接。顺带验证：
//
//	① state/event 同文件混存互不误读（loadStateRecords 只见 state、ReadEvents
//	   只见 event）；
//	② handles 缓存 state/event 共用：run-1 的 state 记录建文件后事件走缓存句柄
//	   追加（ErrRunExists 即失败）；
//	③ run-2 无 state 记录先行，AppendEvent 自行建文件（OpenRunJournal 路径）；
//	④ 终态逐出后对同一 run 的 AppendEvent 被拒（ErrRunExists）——AppendEvent
//	   注释的次序约束（事件必须先于终态 state 记录）；
//	⑤ EventRecord 逐字段往返一致（含 FromSeq 合并声明与 Payload 原字节）。
func TestEventRecordsCrossFileRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := tempDir(t)
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	s, err := repo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sid := s.SessionID()
	jst := NewRunJournalStore(repo, s)

	run1, run2 := "run-1000-1", "run-2000-1"
	at := time.Now()

	// run-1：state 记录先建文件并缓存句柄（queued → running），事件复用同一句柄。
	for _, st := range []string{"queued", "running"} {
		if err := jst.AppendState(RunStateRecord{Kind: "state", SessionID: sid, RunID: run1, State: st, At: at}); err != nil {
			t.Fatalf("AppendState %s: %v", st, err)
		}
	}
	mustAppendEvent(t, jst, sid, run1, EventToolCall, 1, 0, at, `{"name":"grep"}`)
	mustAppendEvent(t, jst, sid, run1, EventTextDelta, 3, 2, at.Add(time.Millisecond), `"he"`)

	// run-1 终态：句柄 Close+逐出（AppendState 的既有机制）。
	if err := jst.AppendState(RunStateRecord{Kind: "state", SessionID: sid, RunID: run1, State: "completed", Persisted: true, At: at.Add(2 * time.Millisecond)}); err != nil {
		t.Fatalf("AppendState completed: %v", err)
	}
	// ④ 终态逐出后的事件追加被拒：文件已在磁盘（O_EXCL），宁拒勿双写。
	err = jst.AppendEvent(EventRecord{Kind: eventKind, SessionID: sid, RunID: run1, Type: EventTextDelta, Seq: 9, Time: at})
	if !errors.Is(err, ErrRunExists) {
		t.Fatalf("AppendEvent after terminal state: err = %v, want ErrRunExists", err)
	}

	// run-2：无 state 先行，事件经 OpenRunJournal 建文件。
	mustAppendEvent(t, jst, sid, run2, EventToolResult, 4, 0, at.Add(3*time.Millisecond), `{"ok":true}`)
	mustAppendEvent(t, jst, sid, run2, EventAssistantMessageFinalized, 5, 0, at.Add(4*time.Millisecond), `"done"`)

	// ReadEvents(0)：跨文件全量有序（文件名序=创建序=nano 序 ⇒ seq 序）。
	got, err := jst.ReadEvents(0)
	if err != nil {
		t.Fatalf("ReadEvents(0): %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("ReadEvents(0) = %d records, want 4", len(got))
	}
	if seqs := eventSeqs(got); !uint64sEqual(seqs, []uint64{1, 3, 4, 5}) {
		t.Fatalf("ReadEvents(0) seqs = %v, want [1 3 4 5] (跨文件有序拼接)", seqs)
	}

	// ⑤ 逐字段往返一致。
	want := []EventRecord{
		{Kind: eventKind, SessionID: sid, RunID: run1, Type: EventToolCall, Seq: 1, Time: at, Payload: json.RawMessage(`{"name":"grep"}`)},
		{Kind: eventKind, SessionID: sid, RunID: run1, Type: EventTextDelta, Seq: 3, FromSeq: 2, Time: at.Add(time.Millisecond), Payload: json.RawMessage(`"he"`)},
		{Kind: eventKind, SessionID: sid, RunID: run2, Type: EventToolResult, Seq: 4, Time: at.Add(3 * time.Millisecond), Payload: json.RawMessage(`{"ok":true}`)},
		{Kind: eventKind, SessionID: sid, RunID: run2, Type: EventAssistantMessageFinalized, Seq: 5, Time: at.Add(4 * time.Millisecond), Payload: json.RawMessage(`"done"`)},
	}
	for i, w := range want {
		g := got[i]
		if g.Kind != w.Kind || g.SessionID != w.SessionID || g.RunID != w.RunID || g.Type != w.Type ||
			g.Seq != w.Seq || g.FromSeq != w.FromSeq || !g.Time.Equal(w.Time) ||
			!bytes.Equal(g.Payload, w.Payload) {
			t.Errorf("record #%d = %+v (payload %s), want %+v (payload %s)", i, g, g.Payload, w, w.Payload)
		}
	}

	// ① 混存互不误读：run-1 文件的 5 行都能被两个读取方解析（事件行 Unmarshal
	// 进 RunStateRecord 也合法，未知字段忽略）；恢复扫描的末条 state 判定按
	// kind 域过滤（MarkInterruptedRuns 只认 kind==stateKind），事件行不干扰
	// 终态判定——这是 interrupted 标记在混存文件上依然正确的前提。
	run1Path := filepath.Join(s.dir, "runs", run1+".jsonl")
	allRecs, _, err := loadStateRecords(run1Path)
	if err != nil {
		t.Fatalf("loadStateRecords(run-1): %v", err)
	}
	if len(allRecs) != 5 {
		t.Fatalf("run-1 lines = %d, want 5 (3 state + 2 event，逐行均合法)", len(allRecs))
	}
	var states []string
	for _, r := range allRecs {
		if r.Kind == stateKind {
			states = append(states, r.State)
		}
	}
	if !equalSlices(states, []string{"queued", "running", "completed"}) {
		t.Fatalf("run-1 state kinds filter = %v, want [queued running completed]", states)
	}
	run1Events, err := loadEventRecords(run1Path)
	if err != nil {
		t.Fatalf("loadEventRecords(run-1): %v", err)
	}
	if len(run1Events) != 2 || run1Events[0].Seq != 1 || run1Events[1].Seq != 3 {
		t.Fatalf("run-1 event records = %+v, want seq [1 3]", eventSeqs(run1Events))
	}

	// 两个 run 文件确已分别创建。
	for _, run := range []string{run1, run2} {
		if _, err := os.Stat(filepath.Join(s.dir, "runs", run+".jsonl")); err != nil {
			t.Errorf("stat %s journal: %v", run, err)
		}
	}
}

// TestReadEventsFiltersAfterSeq 钉死回放游标过滤：只返回 Seq>afterSeq 的记录
// （严格大于），afterSeq=latest 或更晚 → 空结果。
func TestReadEventsFiltersAfterSeq(t *testing.T) {
	ctx := context.Background()
	root := tempDir(t)
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	s, err := repo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sid := s.SessionID()
	jst := NewRunJournalStore(repo, s)

	run1, run2 := "run-1000-1", "run-2000-1"
	at := time.Now()
	for i, run := range []string{run1, run1, run1, run2, run2} {
		mustAppendEvent(t, jst, sid, run, EventTextDelta, uint64(i+1), 0, at, `"x"`)
	}

	cases := []struct {
		after uint64
		want  []uint64
	}{
		{after: 0, want: []uint64{1, 2, 3, 4, 5}},
		{after: 2, want: []uint64{3, 4, 5}},
		{after: 3, want: []uint64{4, 5}}, // 边界：Seq==afterSeq 不返回（严格大于）
		{after: 5, want: nil},            // 游标=最新 → 空
		{after: 99, want: nil},
	}
	for _, tc := range cases {
		got, err := jst.ReadEvents(tc.after)
		if err != nil {
			t.Fatalf("ReadEvents(%d): %v", tc.after, err)
		}
		if seqs := eventSeqs(got); !uint64sEqual(seqs, tc.want) {
			t.Errorf("ReadEvents(%d) seqs = %v, want %v", tc.after, seqs, tc.want)
		}
	}
}

// TestSeqRangeSeedsFromDisk 钉死 SeqRange 的播种语义（hub 构造用）：
//
//	① 空 runs/ → ok=false；
//	② 只有 state 记录（无事件）→ ok=false；
//	③ 非连续事件（3/5/7）→ (3,7,true)，min/max 而非「首末即边界」假设；
//	④ 新 Repository 实例 + 新 RunJournalStore（重启模拟，句柄缓存全空）仍从
//	   磁盘播种——播种不依赖进程内状态。
func TestSeqRangeSeedsFromDisk(t *testing.T) {
	ctx := context.Background()
	root := tempDir(t)
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	s, err := repo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sid := s.SessionID()
	jst := NewRunJournalStore(repo, s)

	// ① 空 runs/。
	if e, l, ok := jst.SeqRange(); ok {
		t.Fatalf("SeqRange on empty runs/ = (%d,%d,true), want ok=false", e, l)
	}

	run1 := "run-1000-1"
	at := time.Now()
	// ② 只有 state 记录：文件存在但无事件。
	if err := jst.AppendState(RunStateRecord{Kind: "state", SessionID: sid, RunID: run1, State: "running", At: at}); err != nil {
		t.Fatalf("AppendState: %v", err)
	}
	if e, l, ok := jst.SeqRange(); ok {
		t.Fatalf("SeqRange with state-only file = (%d,%d,true), want ok=false", e, l)
	}

	// ③ 非连续 seq：min/max 单遍扫描。
	for _, seq := range []uint64{5, 3, 7} {
		mustAppendEvent(t, jst, sid, run1, EventTextDelta, seq, 0, at, `"x"`)
	}
	if e, l, ok := jst.SeqRange(); !ok || e != 3 || l != 7 {
		t.Fatalf("SeqRange = (%d,%d,%v), want (3,7,true)", e, l, ok)
	}

	// ④ 重启模拟：终态收口（关句柄）→ repo.Close → 新 Repository + 新聚合实例。
	if err := jst.AppendState(RunStateRecord{Kind: "state", SessionID: sid, RunID: run1, State: "completed", At: at.Add(time.Second)}); err != nil {
		t.Fatalf("AppendState completed: %v", err)
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("repo.Close: %v", err)
	}
	repo2, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository 2: %v", err)
	}
	t.Cleanup(func() { _ = repo2.Close() })
	st2, err := repo2.Open(sid)
	if err != nil {
		t.Fatalf("repo2.Open: %v", err)
	}
	jst2 := NewRunJournalStore(repo2, st2)
	if e, l, ok := jst2.SeqRange(); !ok || e != 3 || l != 7 {
		t.Fatalf("SeqRange after restart = (%d,%d,%v), want (3,7,true) (播种来自磁盘)", e, l, ok)
	}
	got, err := jst2.ReadEvents(3)
	if err != nil {
		t.Fatalf("ReadEvents(3) after restart: %v", err)
	}
	if seqs := eventSeqs(got); !uint64sEqual(seqs, []uint64{5, 7}) {
		t.Fatalf("ReadEvents(3) after restart = %v, want [5 7]", seqs)
	}
}

// TestEventTailRecovery 钉死事件读取的坏尾行语义：事件文件尾部半行（崩溃时
// Write 被截断，无行尾 '\n'）→ ReadEvents 跳过坏行返回合法前缀，不报错、不
// 截断文件（截断修复是 MarkInterruptedRuns 的职责，读取方只容忍）。
func TestEventTailRecovery(t *testing.T) {
	ctx := context.Background()
	root := tempDir(t)
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	s, err := repo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sid := s.SessionID()
	jst := NewRunJournalStore(repo, s)

	// 手写 run 文件：两条合法事件 + 一条截断尾行（半行 json，无 '\n'）。
	runsDir := filepath.Join(s.dir, "runs")
	if err := os.MkdirAll(runsDir, 0o700); err != nil {
		t.Fatalf("mkdir runs: %v", err)
	}
	at := time.Now()
	good := []EventRecord{
		{Kind: eventKind, SessionID: sid, RunID: "run-3000-1", Type: EventToolCall, Seq: 1, Time: at, Payload: json.RawMessage(`"a"`)},
		{Kind: eventKind, SessionID: sid, RunID: "run-3000-1", Type: EventTextDelta, Seq: 2, Time: at.Add(time.Millisecond), Payload: json.RawMessage(`"b"`)},
	}
	var buf []byte
	for _, rec := range good {
		line, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("marshal event: %v", err)
		}
		buf = append(buf, line...)
		buf = append(buf, '\n')
	}
	buf = append(buf, []byte(`{"kind":"event","run_id":"run-3000-1","type":"tool_result","seq":3`)...) // 崩溃半行
	run3Path := filepath.Join(runsDir, "run-3000-1.jsonl")
	if err := os.WriteFile(run3Path, buf, 0o600); err != nil {
		t.Fatalf("write run-3000-1: %v", err)
	}

	// 合法前缀完整返回，坏尾行跳过且无错误。
	got, err := jst.ReadEvents(0)
	if err != nil {
		t.Fatalf("ReadEvents(0) with bad tail: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ReadEvents(0) = %d records, want 2 (坏尾行跳过，返回合法前缀)", len(got))
	}
	if seqs := eventSeqs(got); !uint64sEqual(seqs, []uint64{1, 2}) {
		t.Fatalf("ReadEvents(0) seqs = %v, want [1 2]", seqs)
	}
	for i, w := range good {
		if got[i].Type != w.Type || got[i].RunID != w.RunID || got[i].Seq != w.Seq ||
			!bytes.Equal(got[i].Payload, w.Payload) {
			t.Errorf("record #%d = %+v, want %+v", i, got[i], w)
		}
	}
	if got2, err := jst.ReadEvents(1); err != nil || len(got2) != 1 || got2[0].Seq != 2 {
		t.Fatalf("ReadEvents(1) = %v (err %v), want seq [2]", eventSeqs(got2), err)
	}

	// SeqRange 同样容忍坏尾行：(1,2,true)。
	if e, l, ok := jst.SeqRange(); !ok || e != 1 || l != 2 {
		t.Fatalf("SeqRange with bad tail = (%d,%d,%v), want (1,2,true)", e, l, ok)
	}

	// 文件未被读取路径截断（尾修复归 MarkInterruptedRuns，读取方只容忍）。
	info, err := os.Stat(run3Path)
	if err != nil {
		t.Fatalf("stat after read: %v", err)
	}
	if int64(len(buf)) != info.Size() {
		t.Fatalf("file size after read = %d, want %d (ReadEvents 不截断)", info.Size(), len(buf))
	}
}

// TestReadEventsFiltersBySessionID 钉死 P5-3 的 SessionID 过滤（勘察修正 I 的
// 收口）：同一 runs/ 目录混入异 SessionID 的事件记录（历史混存模拟：Archive
// 同目录换 SessionID，store.go:556）→ ReadEvents 与 SeqRange 均不含它们，本
// session 记录照常返回——attach 回放不串他 incarnation 的事件。覆盖两种混存
// 形态：
//
//	① 同文件混行：AppendEvent 只按 RunID 寻址、不校验 rec.SessionID，异 id 记录
//	   可与本 id 记录同文件混行（写入侧无拦截，过滤只在读取侧）；
//	② 独立异 id run 文件：整文件来自历史 incarnation（读路径文件名不走
//	   validRunID，TestRestoreInterruptedRun 同款手写先例）。
func TestReadEventsFiltersBySessionID(t *testing.T) {
	ctx := context.Background()
	root := tempDir(t)
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	s, err := repo.Create(ctx, CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sid := s.SessionID()
	jst := NewRunJournalStore(repo, s)

	foreign := "session_deadbeefdeadbeefdeadbeefdeadbeef" // 历史 incarnation 的异 id（构造期快照之外）
	run1, run2, foreignRun := "run-1000-1", "run-2000-1", "run-1500-9"
	at := time.Now()

	// 本 session：run-1 两条、run-2 一条（经 AppendEvent 正常建文件）。
	mustAppendEvent(t, jst, sid, run1, EventToolCall, 1, 0, at, `{"name":"grep"}`)
	mustAppendEvent(t, jst, sid, run1, EventTextDelta, 3, 2, at.Add(time.Millisecond), `"he"`)
	mustAppendEvent(t, jst, sid, run2, EventToolResult, 5, 0, at.Add(3*time.Millisecond), `{"ok":true}`)

	// ① 同文件混行：异 id 记录写进本 session 的 run-1 文件，seq 恰落在本 id 两条
	// 记录之间（过滤若失效，ReadEvents 序列立即混入 seq 2）。
	mustAppendEvent(t, jst, foreign, run1, EventTextDelta, 2, 0, at.Add(time.Millisecond), `"f1"`)

	// ② 独立异 id run 文件：文件名序在 run-1 与 run-2 之间，seq 跨在本 id 首末
	// 之外（4、6；过滤若失效，SeqRange 上界会被拉到 6）。
	foreignRecs := []EventRecord{
		{Kind: eventKind, SessionID: foreign, RunID: foreignRun, Type: EventToolResult, Seq: 4, Time: at.Add(2 * time.Millisecond), Payload: json.RawMessage(`{"ok":false}`)},
		{Kind: eventKind, SessionID: foreign, RunID: foreignRun, Type: EventAssistantMessageFinalized, Seq: 6, Time: at.Add(4 * time.Millisecond), Payload: json.RawMessage(`"f-done"`)},
	}
	var fbuf []byte
	for _, rec := range foreignRecs {
		line, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("marshal foreign event: %v", err)
		}
		fbuf = append(fbuf, line...)
		fbuf = append(fbuf, '\n')
	}
	if err := os.WriteFile(filepath.Join(s.dir, "runs", foreignRun+".jsonl"), fbuf, 0o600); err != nil {
		t.Fatalf("write %s: %v", foreignRun, err)
	}

	// ReadEvents(0)：只含本 session 的 [1,3,5]（文件名序拼接，异 id 三条剔除）。
	got, err := jst.ReadEvents(0)
	if err != nil {
		t.Fatalf("ReadEvents(0): %v", err)
	}
	if seqs := eventSeqs(got); !uint64sEqual(seqs, []uint64{1, 3, 5}) {
		t.Fatalf("ReadEvents(0) seqs = %v, want [1 3 5] (异 SessionID 记录被过滤)", seqs)
	}
	for i, rec := range got {
		if rec.SessionID != sid {
			t.Errorf("record #%d SessionID = %q, want %q (过滤不得漏异 id 记录)", i, rec.SessionID, sid)
		}
	}

	// 游标与 SessionID 两道过滤正交：ReadEvents(1) 越过本 id seq 1 与异 id seq 2。
	if got2, err := jst.ReadEvents(1); err != nil || !uint64sEqual(eventSeqs(got2), []uint64{3, 5}) {
		t.Fatalf("ReadEvents(1) = %v (err %v), want [3 5]", eventSeqs(got2), err)
	}
	// 游标越过本 id 末条后为空——异 id seq 6 不得漏出。
	if got3, err := jst.ReadEvents(5); err != nil || len(got3) != 0 {
		t.Fatalf("ReadEvents(5) = %v (err %v), want empty (异 id seq 6 不漏出)", eventSeqs(got3), err)
	}

	// SeqRange：播种游标与回放同源——(1,5,true)；异 id seq 2/4/6 不参与播种。
	if e, l, ok := jst.SeqRange(); !ok || e != 1 || l != 5 {
		t.Fatalf("SeqRange = (%d,%d,%v), want (1,5,true) (异 id seq 不参与播种)", e, l, ok)
	}

	// 前置自证：混入数据确在磁盘上（剔除是读取侧行为，不是写入失败）——
	// run-1 文件 3 条事件（2 本 id + 1 异 id 混行）、异 id 文件 2 条全异 id。
	r1Events, err := loadEventRecords(filepath.Join(s.dir, "runs", run1+".jsonl"))
	if err != nil {
		t.Fatalf("loadEventRecords(run-1): %v", err)
	}
	if len(r1Events) != 3 || r1Events[2].SessionID != foreign || r1Events[2].Seq != 2 {
		t.Fatalf("run-1 event records = %+v, want 3 records ending in the foreign seq-2 one", r1Events)
	}
	fEvents, err := loadEventRecords(filepath.Join(s.dir, "runs", foreignRun+".jsonl"))
	if err != nil {
		t.Fatalf("loadEventRecords(%s): %v", foreignRun, err)
	}
	if len(fEvents) != 2 || fEvents[0].SessionID != foreign || fEvents[1].SessionID != foreign {
		t.Fatalf("foreign file records = %+v, want 2 records of %q", fEvents, foreign)
	}
}
