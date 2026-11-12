// journal.go 持久化每个 Run 的状态与事件到同一 JSONL 文件，追加落盘；恢复时
// 截断尾部坏行并将未终态标记为 interrupted，不自动重跑。写入依附 Repository
// 缓存中的 Store 目录 flock；O_EXCL 阻止重复 runID，RunJournal.mu 串行文件追加。
package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RunStateRecord is a persisted run-state entry. State is a string to avoid an
// import cycle with agent; StateInterrupted is specific to recovery.
type RunStateRecord struct {
	Kind      string    `json:"kind"`       // 恒 "state"
	SessionID string    `json:"session_id"` // 诊断字段（Archive 换 id 后的归属可辨）
	RunID     string    `json:"run_id"`
	State     string    `json:"state"` // agent.RunState 字符串化；另有 StateInterrupted
	Err       string    `json:"err,omitempty"`
	Persisted bool      `json:"persisted,omitempty"`
	At        time.Time `json:"at"`
}

// stateKind 是 RunStateRecord 的 kind 值。恢复扫描只认 kind=="state" 的记录
// （事件记录与 state 同文件混存，末条行未必是 state 记录）。
const stateKind = "state"

// StateInterrupted 是 journal 自有的恢复标记态：恢复扫描发现上一进程留下的
// 非终态 run 时追加（「Run Running → 标记 Interrupted」）。属
// journal 语义，不进 agent.RunState 枚举（无产生方）。
const StateInterrupted = "interrupted"

// eventKind 是 EventRecord 的 kind 值：事件记录与 state 记录同文件混存，逐行
// kind 域自辨（ReadEvents 只收 kind=="event"，loadStateRecords/恢复扫描只认
// kind=="state"，互不误读）。
const eventKind = "event"

// ── 事件契约 ───────────────────────────────────────────────────────────

// EventRecord is the persisted event shape. Seq is session-wide; FromSeq > 0 marks
// a stored delta batch whose payload preserves each event's sequence and content.
// Replay expands batches to the same per-event SessionEvent shape used live.
type EventRecord struct {
	Kind      string          `json:"kind"`       // 恒 "event"（AppendEvent 强制，存储层不变量）
	SessionID string          `json:"session_id"` // 诊断字段（同 RunStateRecord.SessionID）
	RunID     string          `json:"run_id"`
	Type      string          `json:"type"`               // 事件类型常量之一（下方两组）
	Seq       uint64          `json:"seq"`                // 记录末事件序号（合并批次记录=批次末 seq）
	FromSeq   uint64          `json:"from_seq,omitempty"` // >0 = 合并批次记录，payload 为 DeltaBatch JSON
	Time      time.Time       `json:"time"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// SessionEvent is the per-event wire shape. Storage-only Kind/FromSeq fields are
// omitted; replay expands stored batches, so clients advance their cursor by each
// received Seq just as they do for live delivery.
type SessionEvent struct {
	SessionID string          `json:"session_id"`
	RunID     string          `json:"run_id"`
	Seq       uint64          `json:"seq"`
	Type      string          `json:"type"`
	Time      time.Time       `json:"time"`
	Payload   json.RawMessage `json:"payload"`
}

// 关键事件类型（契约表的 9 类 key 事件）：必须持久化且 durable-first
// （journal fsync 成功后才广播；失败不广播）。tool_call/tool_result/
// permission.*/assistant.message_finalized 由 teeRunCallbacks 从
// RunCallbacks 三入口产出（源自 streamTokens 实际流 session.go:213-249）；
// run.* 四类是 run 生命周期事件的落码点（supervisor 状态迁移时产出）。
const (
	EventToolCall                  = "tool_call"
	EventToolResult                = "tool_result"
	EventPermissionRequested       = "permission.requested"
	EventPermissionResolved        = "permission.resolved"
	EventAssistantMessageFinalized = "assistant.message_finalized"
	EventRunStarted                = "run.started"
	EventRunStateChanged           = "run.state_changed"
	EventRunFinished               = "run.finished"
	EventRunError                  = "run.error"
)

// 高频 delta 事件类型（契约表的 3 类）：进合并缓冲，50ms/4KB 窗合并为
// 一条批次记录（FromSeq..Seq 范围声明 + DeltaBatch payload）落盘；
// 实时广播仍逐事件（per-event seq）。命名对齐
// runtimeconfig.Event* 既有风格（config/spec.go:251-252）。
const (
	EventTextDelta     = "text.delta"
	EventThinkingDelta = "thinking.delta"
	EventUsageProgress = "usage.progress"
)

// IsKeyEvent 判定事件类型是否为关键事件（durable-first 落盘的 9 类）；false 即
// delta 类（合并落盘，落盘失败可降级——重放缺 delta 段可接受，
// assistant.message_finalized 兜底定稿）。未知类型按非 key 处理（保守：宁缺
// 记录勿阻塞广播；产生方只用上述常量）。
func IsKeyEvent(typ string) bool {
	switch typ {
	case EventToolCall, EventToolResult,
		EventPermissionRequested, EventPermissionResolved,
		EventAssistantMessageFinalized,
		EventRunStarted, EventRunStateChanged, EventRunFinished, EventRunError:
		return true
	default:
		return false
	}
}

// isTerminalRunState 判定 state 是否为 run 终态：恢复扫描据其判定「该 run 已
// 收尾，无需处理」。completed/failed/cancelled 与 agent.RunState 的字符串化
// 一致（run.go:18-22，取消已归一为 cancelled）；StateInterrupted 是
// journal 自有的恢复标记，同属终态——这是幂等的关键：重扫不再追加第二个标记。
func isTerminalRunState(state string) bool {
	switch state {
	case "completed", "failed", "cancelled", StateInterrupted:
		return true
	default:
		return false
	}
}

// RunJournal 是单个 run 的状态 journal（每 run 一文件，runs/<runID>.jsonl）。
// 经 Repository.OpenRunJournal 创建（O_EXCL 保证全局唯一创建权，无多写者）；
// 句柄随 run 终态由调用方关闭，不登记在 Repository 上（句柄随 run 终态关闭，
// 不存在跨 /new 滞留句柄）。
type RunJournal struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// Append appends and syncs a state record. A partial trailing write is repaired during recovery.
func (j *RunJournal) Append(rec RunStateRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return fmt.Errorf("session: run journal %s: append after close", j.path)
	}
	return appendRunStateRecord(j.f, rec)
}

// AppendEvent appends and syncs an event in the run journal. Kind is set at the
// storage boundary so malformed records cannot silently disappear during replay.
func (j *RunJournal) AppendEvent(rec EventRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return fmt.Errorf("session: run journal %s: append after close", j.path)
	}
	rec.Kind = eventKind
	return appendEventRecord(j.f, rec)
}

// Close 关闭 journal 文件句柄。幂等（已关闭再 Close 返回 nil）。
func (j *RunJournal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return nil
	}
	err := j.f.Close()
	j.f = nil
	return err
}

// appendRecordLine 是 state/事件记录共用的落盘骨架：marshal json 单行 + '\n'
// （字节格式对齐 Store.appendRecord，store.go:680-693）+ fsync。单次 Write 写
// 整行：崩溃最多留半行，读方按坏行跳过、恢复扫描尾截断修复；串行由调用方保证
// （RunJournal.Append/AppendEvent 持 j.mu；MarkInterruptedRuns 单写者）。
func appendRecordLine(f *os.File, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// appendRunStateRecord 将一条 state 记录追加到文件末尾并 fsync。
func appendRunStateRecord(f *os.File, rec RunStateRecord) error {
	if err := appendRecordLine(f, rec); err != nil {
		return fmt.Errorf("session: append run state record: %w", err)
	}
	return nil
}

// appendEventRecord 将一条事件记录追加到文件末尾并 fsync（Kind 由
// RunJournal.AppendEvent 在调用前强制）。
func appendEventRecord(f *os.File, rec EventRecord) error {
	if err := appendRecordLine(f, rec); err != nil {
		return fmt.Errorf("session: append event record: %w", err)
	}
	return nil
}

// loadStateRecords 从 run journal 文件加载全部记录。解析目标不是会话 Record
// （不同 kind 域，UnmarshalJSON 会拒绝/错解），故不能直接复用 loadRecords，
// 只共用其行扫描骨架 scanJSONL（同策略：坏行跳过并 warn；返回 lastGoodEnd =
// 最后一条合法记录行末偏移）。
func loadStateRecords(path string) (recs []RunStateRecord, lastGoodEnd int64, err error) {
	lastGoodEnd, err = scanJSONL(path, func(line []byte, _ int64) bool {
		var rec RunStateRecord
		if uerr := json.Unmarshal(line, &rec); uerr != nil {
			slog.Warn("session: skip bad journal line", "err", uerr, "line_preview", truncate(string(line), 80))
			return false
		}
		recs = append(recs, rec)
		return true
	})
	if err != nil {
		return nil, 0, err
	}
	return recs, lastGoodEnd, nil
}

// loadEventRecords 只收 kind==event 的记录；合法 state 行按 kind 丢弃，避免混存时
// 误读。与 loadStateRecords 共用 scanJSONL，坏行跳过并 warn；不返回偏移、不截断，
// 尾部修复由 MarkInterruptedRuns 负责。
func loadEventRecords(path string) (recs []EventRecord, err error) {
	_, err = scanJSONL(path, func(line []byte, _ int64) bool {
		var rec EventRecord
		if uerr := json.Unmarshal(line, &rec); uerr != nil {
			slog.Warn("session: skip bad journal line", "err", uerr, "line_preview", truncate(string(line), 80))
			return false
		}
		if rec.Kind == eventKind {
			recs = append(recs, rec)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return recs, nil
}

// validRunID accepts only run-<decimal>-<decimal> IDs. It rejects traversal before
// OpenRunJournal joins the ID into a path; recovery scanning trusts only names returned
// by os.ReadDir and therefore does not use this caller-input validator.
func validRunID(id string) bool {
	if !validSessionID(id) {
		return false
	}
	rest, ok := strings.CutPrefix(id, "run-")
	if !ok {
		return false
	}
	nano, seq, ok := strings.Cut(rest, "-")
	if !ok || nano == "" || seq == "" {
		return false
	}
	for _, part := range [...]string{nano, seq} {
		for _, r := range part {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

// OpenRunJournal creates a run journal only for a Store cached by this Repository;
// otherwise it returns ErrSessionNotOpen. O_EXCL makes runID collisions observable as
// ErrRunExists rather than overwriting data; validate IDs before constructing the path.
func (r *Repository) OpenRunJournal(ctx context.Context, sessionID, runID string) (*RunJournal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validSessionID(sessionID) {
		// 非法 id 不可能进过 open 缓存，custody 前置必然不满足。
		return nil, fmt.Errorf("session: open run journal %q: %w", sessionID, ErrSessionNotOpen)
	}
	if !validRunID(runID) {
		return nil, fmt.Errorf("session: open run journal: invalid run id %q", runID)
	}

	// 与 Open 的慢路径同理（repository.go Open 头注释）：缓存查找与后续文件
	// 创建全程持 r.mu，保证「缓存命中 ⇒ 文件创建期间 store 仍在缓存（Delete
	// 必拒）」不被并发 Close 打破。
	r.mu.Lock()
	defer r.mu.Unlock()

	store, ok := r.open[sessionID]
	if !ok {
		return nil, fmt.Errorf("session: open run journal %s/%s: %w", sessionID, runID, ErrSessionNotOpen)
	}

	runsDir := filepath.Join(store.dir, "runs")
	if err := os.MkdirAll(runsDir, 0o700); err != nil {
		return nil, fmt.Errorf("session: create runs dir %s: %w", runsDir, err)
	}
	path := filepath.Join(runsDir, runID+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("session: open run journal %s: %w", runID, ErrRunExists)
		}
		return nil, fmt.Errorf("session: open run journal %s: %w", path, err)
	}
	return &RunJournal{f: f, path: path}, nil
}

// MarkInterruptedRuns scans run journals in filename order, truncates only a malformed
// tail to the last valid record (matching Store.Open), then appends one interrupted
// marker for each run whose last state is nonterminal. It is idempotent, never reruns
// work, and leaves current.jsonl untouched; recovery failures are returned to caller.
func (r *Repository) MarkInterruptedRuns(ctx context.Context, sessionID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validSessionID(sessionID) {
		return nil, fmt.Errorf("session: mark interrupted runs %q: %w", sessionID, ErrSessionNotOpen)
	}

	// 全程持 r.mu，理由同 OpenRunJournal：扫描与标记期间缓存成员资格不变。
	r.mu.Lock()
	defer r.mu.Unlock()

	store, ok := r.open[sessionID]
	if !ok {
		return nil, fmt.Errorf("session: mark interrupted runs %s: %w", sessionID, ErrSessionNotOpen)
	}

	runsDir := filepath.Join(store.dir, "runs")
	entries, err := os.ReadDir(runsDir)
	if os.IsNotExist(err) {
		return nil, nil // 无 runs 目录 = 无 run 历史，空结果
	}
	if err != nil {
		return nil, fmt.Errorf("session: scan runs dir %s: %w", runsDir, err)
	}

	var marked []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		runID := strings.TrimSuffix(e.Name(), ".jsonl")
		if runID == "" {
			continue // 防御：恰名为 ".jsonl" 的条目无从给出 runID
		}
		path := filepath.Join(runsDir, e.Name())
		recs, lastGoodEnd, err := loadStateRecords(path)
		if err != nil {
			return nil, fmt.Errorf("session: load run journal %s: %w", path, err)
		}

		// 尾部坏行截断回 lastGoodEnd。触发条件与 Store.Open 相同（store.go:106）：
		// 只有「坏行是最后一行」（size > lastGoodEnd）才截断；中部坏行之后还有
		// 合法记录时 lastGoodEnd == 文件大小，不动（纯追加原则）。
		if info, statErr := os.Stat(path); statErr == nil && info.Size() > lastGoodEnd {
			if terr := truncateFileSync(path, lastGoodEnd); terr != nil {
				return nil, fmt.Errorf("session: repair run journal tail %s: %w", path, terr)
			}
			slog.Warn("session: repaired partial run journal tail", "path", path, "offset", lastGoodEnd)
		}

		// 末条 state 记录（只认 kind=="state"；无任何 state 记录则无从判定，跳过）。
		var last *RunStateRecord
		for i := range recs {
			if recs[i].Kind == stateKind {
				last = &recs[i]
			}
		}
		if last == nil || isTerminalRunState(last.State) {
			continue
		}

		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("session: open run journal %s for marker: %w", path, err)
		}
		err = appendRunStateRecord(f, RunStateRecord{
			Kind:      stateKind,
			SessionID: sessionID,
			RunID:     runID,
			State:     StateInterrupted,
			At:        time.Now(),
		})
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, fmt.Errorf("session: append interrupted marker %s: %w", path, err)
		}
		marked = append(marked, runID)
	}
	return marked, nil
}

// truncateFileSync 将文件截断到 size 并落盘（Sync）。MarkInterruptedRuns 的
// 尾部修复专用：Store.Open 的修复在其常驻追加句柄上做（store.go:106-118），
// 此处扫描期无句柄，按需开、用完即关。
func truncateFileSync(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}
