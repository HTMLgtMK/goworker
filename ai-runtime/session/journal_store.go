// journal_store.go 是 P2-7 的 Run 状态 Journal 聚合类型：把 P2-2 的 per-run
// 存储面（RunJournal / Repository.OpenRunJournal）聚合成本包对 agent 侧
// RunStateJournal 消费接口（agent/spec.go）的会话级实现——daemon startSession
// 按「一个会话一个实例」构造并注入 SessionDeps.RunJournal。
//
// 为什么类型内必须按 runID 缓存句柄：OpenRunJournal 以 O_CREATE|O_EXCL 创建
// runs/<runID>.jsonl（P2-1 跨重启 RunID 碰撞兜底），同一 runID 的第二次 Open
// 返回 ErrRunExists——而一条 Run 的生命周期会产生多条 state 记录（queued/
// running → 终态），所以首条记录打开的 *RunJournal 必须缓存在本类型内复用，
// 不能每条记录重开。
//
// 句柄生命周期（主 agent 补充决策，兑现 P2-2「句柄随 run 终态关闭」的承诺）：
// AppendState 写入终态记录（completed/failed/cancelled，isTerminalRunState 判定）
// 后立即 Close 并逐出缓存句柄；非终态（queued/running）保留——否则 daemon 同步
// 路径每轮 /agent 泄漏一个 fd 直到 Stop，长跑进程会 fd 耗尽（P2-3 as-built 的
// RecordRun 未关闭句柄，由本类型收口）。
//
// P3-1 起本类型兼作事件 Journal 聚合：AppendEvent/ReadEvents/SeqRange（agent
// 侧 EventJournal 消费面，P3-2 进 SessionDeps.RunEvents）。事件记录与该 run 的
// state 记录同文件混存，handles 缓存为两者共用——plan 原稿「runID 变化时关闭
// 旧句柄再开新文件」的切换逻辑由既有终态逐出机制天然覆盖：上一 run 的句柄在其
// 终态 state 记录落盘时已 Close+逐出（AppendState），新 run 首条记录自然打开
// 新文件，无需 AppendEvent 感知 run 切换。
//
// 并发与锁论证：s.mu 只护 handles map 与句柄的取/存/逐出；单文件追加串行由
// RunJournal 自身的 j.mu 保证（journal.go:82-89）。同会话的调用方本就被 gate
// 串行（RecordRun 持 gate；StartRun 的 queued 记录先于 execute 获得 gate），
// 互斥只是防御性兜底，无锁环（s.mu 内不触碰 repo 锁——OpenRunJournal 在
// s.mu 内被调用，但它只取 r.mu 且不回触 s.mu，单向无环）。
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

// RunJournalStore 是一个会话的 Run 状态 journal 聚合：按 rec.RunID 把记录路由
// 到 repo.OpenRunJournal 打开的 per-run 文件句柄。持有 repo 与 store 引用；
// sessionID 在构造时从 store 快照——它是 repo 实例唯一缓存的挂载键：legacy 槽
// 被 Archive 轮转后 store 的 metadata id 会更换而缓存键不变（plugin.go P1-3
// 注释），逐次取 st.SessionID() 会在轮转后错指缓存外 id（ErrSessionNotOpen），
// 构造期快照则始终落在本会话的 runs/ 目录。
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

// AppendState 实现持久化消费面：按 rec.RunID 路由一条 state 记录（durable-first
// 追加，RunJournal.Append 内 fsync）。终态记录落盘后关闭并逐出句柄；非终态
// 保留句柄供后续记录复用。
//
// 错误语义（宁观测勿静默）：打开/追加失败原样返回——agent 侧 journalRunState
// warn 不阻塞执行，缺口由恢复扫描显式标记 interrupted。特别地，无缓存句柄时
// OpenRunJournal 撞上已存在的文件（ErrRunExists）= P2-1 的跨重启 RunID 碰撞
// 兜底或句柄被终态逐出后的异常重放，作为可观测错误上抛而非静默续写（宁拒勿
// 双写，同一文件不存在第二个合法写者）。
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

// AppendEvent 按 rec.RunID 路由一条事件记录（P3-1）：命中 handles 缓存即复用
// （与该 run 的 state 记录同句柄同文件混存）；未命中经 OpenRunJournal 打开——
// 通常 state 记录（queued/running）已先建文件，事件的追加走缓存命中路径。路由
// 按 rec.RunID 全量寻址，无「当前 run」概念：单写者下同一会话的 run 不会并发
// 产生事件（gate 串行，P3-0 契约表），跨 run 切换由终态逐出（见文件头）兜住，
// AppendEvent 无需感知。
//
// 次序约束（P3-2/P3-4 落码时注意）：事件必须落在该 run 的终态 state 记录之前
// ——终态记录会 Close+逐出句柄，此后的 AppendEvent 走 OpenRunJournal 撞上已存
// 在的文件 → ErrRunExists（custody 的「宁拒勿双写」，可观测错误而非静默续写）。
// EventHub.EndRun 的 delta flush 因此必须先于终态 state 记录落盘；若 flush 晚到
// 而 OpenRunJournal 被拒，delta 缺段可接受（P3-2 的降级语义：
// assistant.message_finalized 兜底定稿），调用方按 warn 处理。
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

// ReadEvents 回放读取（P3-1，总方案第五节）：扫 <dir>/runs/*.jsonl 按文件名序
// 拼接全部事件记录（kind=="event" 且 rec.SessionID==s.sessionID 且 Seq>afterSeq）。
// 文件名序 = 创建序 = nano
// 序（run-<unixnano>-<seq> 的 nano 定宽至 2262 年，run.go:121-123），跨文件
// Seq 区间单调不重叠（P3-0 契约表：gate 串行 ⇒ 后启动 run 的 seq 区间整体大于
// 先前的），拼接即全局有序——本不变量由 TestEventRecordsCrossFileRoundTrip 钉死。
//
// 坏行跳过（loadEventRecords，与 loadStateRecords 同策略）：并发追加下读方容忍
// 半行（写方单 Write 整行，store.go:686 同款）；不做尾截断（MarkInterruptedRuns
// 的职责）。SessionID 过滤（P5-3 起按构造期快照的 sessionID 过滤，勘察修正 I 的
// 收口）：s.sessionID 是 repo 实例唯一缓存的挂载键（类型注释），比对在 visit
// 回调（消费侧）完成而非扫描侧——walkEventRecords 对两类调用方共用且零过滤
// 逻辑，loadEventRecords 的坏行跳过语义不变。对历史上已存在的混存文件（Archive
// 同目录换 SessionID，store.go:556），attach 回放不再含他 incarnation 段；现网
// 数据流无同目录多 incarnation（/new 一律 Create 新目录，legacy 根槽仅首轮
// 挂载），本过滤对既有用户可见面=零（TestReadEventsFiltersBySessionID 钉死）。
//
// 只读扫描不持 s.mu：读不到 handles map；与追加并存的正确性由坏行跳过语义保证
// （见上），与 P2-2「Append 串行、读方容忍」的取舍一致。
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

// SeqRange 单遍扫描 runs/ 全部事件记录取 (min Seq, max Seq)，hub 构造播种用
// （P3-2：EventHub 以 latest 恢复 seq 分配、以 earliest 做游标窗口校验）。
// SessionID 过滤与 ReadEvents 同源（P5-3：播种游标与回放同源，构造期快照的
// sessionID，比对在 visit 回调消费侧完成，见 ReadEvents 注释）——混存文件中
// 他 incarnation 的 seq 不参与播种，否则游标窗口会被异 id 区间拉宽。
// 空 runs/ 或无任何事件记录 → ok=false。扫描 I/O 错误（runs/ 存在但不可读等）
// 同样返回 ok=false 并 warn——本签名无 error 出参（agent 侧 EventJournal 接口
// 形状，P3-2 定稿），播种方拿到 false 以「从零开始」处理，真实 I/O 故障会在
// 随后的 AppendEvent/ReadEvents 以错误显式上抛，此处不静默放大。
func (s *RunJournalStore) SeqRange() (earliest, latest uint64, ok bool) {
	first := true
	err := s.walkEventRecords(func(rec EventRecord) {
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
		slog.Warn("session: seq range scan failed", "session", s.sessionID, "err", err)
		return 0, 0, false
	}
	return earliest, latest, !first
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
