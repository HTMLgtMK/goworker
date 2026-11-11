// journal_store.go 将每个 Run 的 RunJournal 聚合为会话级状态/事件存储。
// 每个 Run 的句柄在首次追加时创建并缓存，写入终态后关闭并逐出。
//
// s.mu 保护 handles；文件追加由 RunJournal.mu 串行，I/O 不持有 s.mu。所有状态与
// 事件记录共享对应 Run 的 journal 文件，因此事件必须先于终态状态记录写入。
package session

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// RunJournalStore aggregates per-run state and event journals for one session. It
// snapshots the session ID at construction because legacy directory names may differ
// from metadata IDs; the repository cache key selects the correct runs/ directory.
type RunJournalStore struct {
	repo      *Repository
	st        *Store
	sessionID string

	mu      sync.Mutex
	handles map[string]*RunJournal // runID → 打开的 journal 句柄（非终态期间持有）
}

// NewRunJournalStore 构造会话级 Run 状态 journal 聚合。st 必须是 repo 打开缓存
// 中的实例（daemon startSession 经 repo.Open/Create 解析，custody 前置天然满足，
// 见 journal.go 文件头的锁论证）；否则 OpenRunJournal 拒绝（ErrSessionNotOpen）。
func NewRunJournalStore(repo *Repository, st *Store) *RunJournalStore {
	return &RunJournalStore{
		repo:      repo,
		st:        st,
		sessionID: st.SessionID(),
		handles:   make(map[string]*RunJournal),
	}
}

// AppendState routes and durably appends one state record. Nonterminal runs retain
// their handle; terminal runs close and evict it even on append failure, leaving any
// missing tail for recovery to mark interrupted. Open/append errors are returned for
// caller logging; ErrRunExists remains visible to prevent silent second-writer reuse.
func (s *RunJournalStore) AppendState(rec RunStateRecord) error {
	if rec.RunID == "" {
		return fmt.Errorf("session: run journal store %s: append %q: empty run id", s.sessionID, rec.State)
	}

	s.mu.Lock()
	j, ok := s.handles[rec.RunID]
	if !ok {
		// OpenRunJournal 要求 store 在 repo 打开缓存中（custody）；ctx 仅用于
		// 取消检查，Background 恒通过。
		var err error
		j, err = s.repo.OpenRunJournal(context.Background(), s.sessionID, rec.RunID)
		if err != nil {
			s.mu.Unlock()
			return fmt.Errorf("session: run journal store %s: open %s: %w", s.sessionID, rec.RunID, err)
		}
		s.handles[rec.RunID] = j
	}
	s.mu.Unlock()

	// 追加在 s.mu 外（文件 I/O 不持聚合锁；单句柄串行由 j.mu 保证）。
	err := j.Append(rec)

	// 终态收口：无论追加成败都关闭并逐出——成败不改变「run 已收尾」的事实；
	// 追加失败时文件缺口由恢复扫描标记 interrupted（journalRunState 已 warn），
	// 不留半死句柄。Close 幂等（journal.go:92-101）。
	if isTerminalRunState(rec.State) {
		s.mu.Lock()
		delete(s.handles, rec.RunID)
		s.mu.Unlock()
		if cerr := j.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		return fmt.Errorf("session: run journal store %s: append %s/%s: %w", s.sessionID, rec.RunID, rec.State, err)
	}
	return nil
}

// AppendEvent routes events by RunID and shares the state handle when present; append is
// durable-first. Events must precede terminal state: terminal writes close/evict the
// handle, and reopening fails with ErrRunExists. EndRun flushes deltas first; a rejected
// late delta is an allowed gap because finalized messages provide the fallback.
func (s *RunJournalStore) AppendEvent(rec EventRecord) error {
	if rec.RunID == "" {
		return fmt.Errorf("session: run journal store %s: append event %q: empty run id", s.sessionID, rec.Type)
	}

	s.mu.Lock()
	j, ok := s.handles[rec.RunID]
	if !ok {
		// OpenRunJournal 要求 store 在 repo 打开缓存中（custody）；ctx 仅用于
		// 取消检查，Background 恒通过。
		var err error
		j, err = s.repo.OpenRunJournal(context.Background(), s.sessionID, rec.RunID)
		if err != nil {
			s.mu.Unlock()
			return fmt.Errorf("session: run journal store %s: open %s: %w", s.sessionID, rec.RunID, err)
		}
		s.handles[rec.RunID] = j
	}
	s.mu.Unlock()

	// 追加在 s.mu 外（文件 I/O 不持聚合锁；单句柄串行由 j.mu 保证）。Kind 由
	// RunJournal.AppendEvent 在存储边界强制（journal.go AppendEvent 注释）。
	if err := j.AppendEvent(rec); err != nil {
		return fmt.Errorf("session: run journal store %s: append event %s/%s: %w", s.sessionID, rec.RunID, rec.Type, err)
	}
	return nil
}

// ReadEvents returns this session's event records with Seq > afterSeq in filename order.
// Serialized run creation makes file order match nonoverlapping Seq ranges. Malformed rows
// are skipped without truncation, and SessionID filtering excludes legacy mixed-incarnation
// data; reads tolerate concurrent appends without locking the handle map.
func (s *RunJournalStore) ReadEvents(afterSeq uint64) ([]EventRecord, error) {
	var out []EventRecord
	if err := s.walkEventRecords(func(rec EventRecord) {
		if rec.SessionID != s.sessionID {
			return // 他 incarnation 段（P5-3：过滤在消费侧，扫描侧零逻辑）
		}
		if rec.Seq > afterSeq {
			out = append(out, rec)
		}
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// SeqRange scans this session's event records once and returns min/max Seq for hub
// cursor seeding; foreign SessionIDs do not widen the replay window. Empty history
// returns (0,0,false,nil). Scan errors remain explicit: callers must reject hub creation
// rather than treat I/O failure as empty history and risk restarting Seq at 1.
func (s *RunJournalStore) SeqRange() (earliest, latest uint64, ok bool, err error) {
	first := true
	err = s.walkEventRecords(func(rec EventRecord) {
		if rec.SessionID != s.sessionID {
			return // 他 incarnation 段（P5-3：过滤在消费侧，扫描侧零逻辑）
		}
		if first {
			earliest, latest = rec.Seq, rec.Seq
			first = false
			return
		}
		if rec.Seq < earliest {
			earliest = rec.Seq
		}
		if rec.Seq > latest {
			latest = rec.Seq
		}
	})
	if err != nil {
		return 0, 0, false, fmt.Errorf("session: run journal store %s: seq range: %w", s.sessionID, err)
	}
	return earliest, latest, !first, nil
}

// LastRunState returns the last kind==state record for a run by scanning its journal;
// event rows are ignored. Missing files, malformed rows, read errors, or no state record
// all yield ok=false (the RunStatus contract has no error result); errors are warned.
// Invalid run IDs are rejected before path construction. Scanning is read-only and does
// not hold s.mu, matching ReadEvents and SeqRange.
func (s *RunJournalStore) LastRunState(runID string) (RunStateRecord, bool) {
	if !validRunID(runID) {
		return RunStateRecord{}, false
	}
	path := filepath.Join(s.st.dir, "runs", runID+".jsonl")
	recs, _, err := loadStateRecords(path)
	if err != nil {
		slog.Warn("session: last run state scan failed", "session", s.sessionID, "run", runID, "err", err)
		return RunStateRecord{}, false
	}
	var last RunStateRecord
	found := false
	for i := range recs {
		if recs[i].Kind == stateKind {
			last = recs[i]
			found = true
		}
	}
	return last, found
}

// walkEventRecords 是 ReadEvents/SeqRange 共用的单遍扫描骨架：runs/ 目录按
// os.ReadDir 的文件名序（排序保证，MarkInterruptedRuns 同款遍历），逐文件
// loadEventRecords 后把事件记录依次交给 visit。runs/ 不存在 = 无 run 历史，
// 空扫描。仅访问磁盘与 visit 回调，不触碰 s.mu/handles（读路径无 custody，
// 见 ReadEvents 注释）。
func (s *RunJournalStore) walkEventRecords(visit func(rec EventRecord)) error {
	runsDir := filepath.Join(s.st.dir, "runs")
	entries, err := os.ReadDir(runsDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("session: scan runs dir %s: %w", runsDir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		path := filepath.Join(runsDir, e.Name())
		recs, err := loadEventRecords(path)
		if err != nil {
			return fmt.Errorf("session: load run journal %s: %w", path, err)
		}
		for _, rec := range recs {
			visit(rec)
		}
	}
	return nil
}
