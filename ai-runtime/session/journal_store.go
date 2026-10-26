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
// 并发与锁论证：s.mu 只护 handles map 与句柄的取/存/逐出；单文件追加串行由
// RunJournal 自身的 j.mu 保证（journal.go:82-89）。同会话的调用方本就被 gate
// 串行（RecordRun 持 gate；StartRun 的 queued 记录先于 execute 获得 gate），
// 互斥只是防御性兜底，无锁环（s.mu 内不触碰 repo 锁——OpenRunJournal 在
// s.mu 内被调用，但它只取 r.mu 且不回触 s.mu，单向无环）。
package session

import (
	"context"
	"fmt"
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
