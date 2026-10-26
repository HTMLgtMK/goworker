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
