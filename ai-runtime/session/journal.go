// journal.go 实现 Run 状态 Journal（P2-2）：每 run 一个 jsonl 文件，落点
// <会话目录>/runs/<runID>.jsonl，durable-first 追加（marshal 单行 + '\n' +
// file.Sync，字节格式对齐 Store.appendRecord，store.go:680-693）。这是总方案
// 第九节「Run 状态持久化」的存储面，加上同节「Daemon 重启后」的恢复扫描
// （MarkInterruptedRuns：尾部坏行截断 + 非终态标记 interrupted，绝不自动重跑）。
//
// 并发与锁论证（custody，勘察修正 C）：Journal 不新增锁面——<dir>/lock 的
// flock（lock_unix.go）只保护 Store 写者，Journal 若独立持锁需新增锁面。本设计
// 让 Journal 写权依附于「该会话 Store 在本 Repository 的 open 缓存」（OpenRunJournal
// ①-② 强制），而 store 进缓存必经 Store.Open 持 flock——跨进程第二写者开不了
// Store ⇒ 开不了 Journal（宁拒勿双写）；同 runID 的重复创建被 O_EXCL 拒绝
// （ErrRunExists，P2-1 跨重启 RunID 的碰撞兜底）；单文件追加由 j.mu 串行，且
// 每 run 至多一个 RunJournal 实例，无多写者。
//
// Delete 安全：open 缓存命中 ⇒ Delete 必拒（repository.go ErrSessionInUse）
// ⇒ Journal（依附在缓存 store）存在期间会话目录不可能被 RemoveAll——「删目录
// 而 Journal 仍写」不可达，无需给 Journal 建独立占用登记。
//
// P3-1 起本文件还承载事件记录契约：EventRecord/SessionEvent（总方案第五节的
// 存储面与传输形状）、事件类型常量与 IsKeyEvent。事件记录（kind=="event"）与
// state 记录（kind=="state"）同文件混存（runs/<runID>.jsonl），逐行 kind 域
// 自辨，custody 论证不变。
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

// RunStateRecord 是 run journal 中的一条 run 状态记录。kind 自立门户恒为
// "state"：P3 事件记录与它同文件混存（runs/<runID>.jsonl）零格式迁移。
// State 用 string：agent.RunState 字符串化（session 不得反向 import agent，
// 勘察修正 D），另有 journal 自有的 StateInterrupted。
type RunStateRecord struct {
	Kind      string    `json:"kind"`       // 恒 "state"
	SessionID string    `json:"session_id"` // 诊断字段（勘察修正 I：Archive 换 id 后的归属可辨）
	RunID     string    `json:"run_id"`
	State     string    `json:"state"` // agent.RunState 字符串化；另有 StateInterrupted
	Err       string    `json:"err,omitempty"`
	Persisted bool      `json:"persisted,omitempty"`
	At        time.Time `json:"at"`
}

// stateKind 是 RunStateRecord 的 kind 值。恢复扫描只认 kind=="state" 的记录
// （P3 起事件记录与 state 同文件混存，末条行未必是 state 记录）。
const stateKind = "state"

// StateInterrupted 是 journal 自有的恢复标记态：恢复扫描发现上一进程留下的
// 非终态 run 时追加（总方案第九节「Run Running → 标记 Interrupted」）。属
// journal 语义，不进 agent.RunState 枚举（P2 无产生方）。
const StateInterrupted = "interrupted"

// eventKind 是 EventRecord 的 kind 值：事件记录与 state 记录同文件混存，逐行
// kind 域自辨（ReadEvents 只收 kind=="event"，loadStateRecords/恢复扫描只认
// kind=="state"，互不误读）。
const eventKind = "event"

// ── 事件契约（P3-1，总方案第五节）──────────────────────────────────────

// EventRecord 是 run journal 中的一条事件记录（kind 恒 "event"，与 state 记录
// 同文件混存）。Seq 是会话维度严格递增游标（P3-0 契约表：EventHub 单调分配，
// 跨 Run 不重复）；Seq 记「记录末事件序号」，合并记录（FromSeq>0）声明覆盖
// [FromSeq..Seq] 的连续区间且 Seq=ToSeq——回放方推进游标到 Seq，范围声明消除
// 「缺口歧义」（总方案第五节「实时与重放相同事件序号语义」）。
type EventRecord struct {
	Kind      string          `json:"kind"`       // 恒 "event"（AppendEvent 强制，存储层不变量）
	SessionID string          `json:"session_id"` // 诊断字段（勘察修正 I，同 RunStateRecord.SessionID）
	RunID     string          `json:"run_id"`
	Type      string          `json:"type"`               // 事件类型常量之一（下方两组）
	Seq       uint64          `json:"seq"`                // 记录末事件序号（合并记录=ToSeq）
	FromSeq   uint64          `json:"from_seq,omitempty"` // >0 = 合并 [FromSeq..Seq]
	Time      time.Time       `json:"time"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// SessionEvent 是事件回放/实时投递的传输形状（总方案第五节，agent 侧 EventHub
// 消费）。与 EventRecord 的差异：无 Kind 信封（存储信封对传输方不可见），Payload
// 不 omit 空值。FromSeq 上 wire（I2lite 修复，GPT 评审）：>0 = 本条是合并记录、
// 覆盖 [FromSeq..Seq] 的连续区间（且 Seq=ToSeq）——实时逐事件投递恒为 0（省略），
// 客户端据此区分批量与单条：同一 Seq 区间在实时侧呈现逐事件粒度、回放侧呈现
// 合并粒度的差异（I2）由范围声明显式化，不再与「单条事件恰逢游标」不可辨。
// EventHub.Publish 把 IngressEvent 包成本类型后既广播又落盘（P3-2，实时侧
// FromSeq=0），Subscribe 的回放转换透传持久化记录的 FromSeq（P3-3）。
type SessionEvent struct {
	SessionID string          `json:"session_id"`
	RunID     string          `json:"run_id"`
	Seq       uint64          `json:"seq"`
	FromSeq   uint64          `json:"from_seq,omitempty"` // >0 = 合并记录，覆盖 [FromSeq..Seq]（实时逐事件恒省略）
	Type      string          `json:"type"`
	Time      time.Time       `json:"time"`
	Payload   json.RawMessage `json:"payload"`
}

// 关键事件类型（P3-0 契约表的 9 类 key 事件）：必须持久化且 durable-first
// （journal fsync 成功后才广播，P3-2；失败不广播）。tool_call/tool_result/
// permission.*/assistant.message_finalized 由 P3-4 的 teeRunCallbacks 从
// RunCallbacks 三入口产出（源自 streamTokens 实际流 session.go:213-249）；
// run.* 四类是 run 生命周期事件的落码预留（supervisor 状态迁移时产出，P3-4）。
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

// 高频 delta 事件类型（P3-0 契约表的 3 类）：进合并缓冲，50ms/4KB 窗合并为一条
// [FromSeq..Seq] 记录落盘（P3-2）；实时广播仍逐事件（per-event seq）。命名对齐
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
// 一致（run.go:18-22，P2-1 已把取消归一为 cancelled）；StateInterrupted 是
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

// Append 追加一条 state 记录：marshal 单行 + '\n'（字节格式对齐 appendRecord，
// store.go:680-693）+ file.Sync——durable-first。单次 Write 写整行，崩溃最多
// 留半行，由恢复扫描的尾截断修复（「部分写入的尾记录有明确截断修复策略」）。
func (j *RunJournal) Append(rec RunStateRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return fmt.Errorf("session: run journal %s: append after close", j.path)
	}
	return appendRunStateRecord(j.f, rec)
}

// AppendEvent 追加一条事件记录（P3-1：与 state 记录同文件混存）。串行与
// durable-first 语义同 Append（j.mu + fsync）；Kind 在存储边界强制为
// eventKind——「EventRecord.Kind 恒 event」是本层不变量，不依赖调用方自觉
// （agent 侧 state 记录由调用方显式置 Kind 的先例见 runtime.go:155，事件侧
// 改为存储层强制：kind 错误会导致回放静默丢事件，代价不对称）。
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

// loadEventRecords 从 run journal 文件加载事件记录（kind=="event"，P3-1）。
// 行扫描骨架与坏行策略同 loadStateRecords（scanJSONL：坏行跳过并 warn）；
// state 行是合法 json（Unmarshal 进 EventRecord 不报错，未知字段忽略），按
// kind 域过滤丢弃——对偶面是 MarkInterruptedRuns 的末条 state 判定同样只认
// kind==stateKind（loadStateRecords 逐行收集、由它过滤），事件行不干扰恢复
// 语义，混存互不误读。返回值无 lastGoodEnd：本读取方不做尾截断（那是
// MarkInterruptedRuns 经 loadStateRecords 的职责），坏行一律跳过即可。
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

// validRunID 拒绝空 id、路径分隔符与非 "run-<十进制>-<十进制>" 形的 run id
// （P2-1 newRunID 的格式，run.go:107-110）。OpenRunJournal 以
// <dir>/runs/<runID>.jsonl 拼路径，恶意/畸形 runID 必须在拼路径前卡死
// （防 traversal，同 validSessionID 的论证，repository.go:321-333）。
// MarkInterruptedRuns 的扫描方不经此校验：文件名来自 os.ReadDir（不可能含
// 路径分隔符），runID 取自文件名而非调用方输入。
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

// OpenRunJournal 为已打开会话的一个 run 创建状态 journal 文件（custody 入口）。
// 前置（custody，见文件头论证）：sessionID 的 Store 必须已在本 Repository 的
// open 缓存中，否则 ErrSessionNotOpen（journal 依附 Store in-cache，不新增锁）。
// 文件以 O_CREATE|O_EXCL 创建：同 runID 的 journal 已存在（含 P2-1 的 RunID
// 跨重启碰撞）→ ErrRunExists——碰撞变成可观测错误而非静默覆盖（run.go:100-104）。
// 无效 runID 在拼路径前拒绝（validRunID，防 traversal）。
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

// MarkInterruptedRuns 执行恢复扫描（总方案第九节「Daemon 重启后」）：对会话
// runs/ 下的每个 journal 文件（文件名序）：
//  1. 按 loadRecords 同策略解析（scanJSONL：坏行跳过，lastGoodEnd = 最后一条
//     合法记录行末），末尾坏行截断回 lastGoodEnd——「部分写入的尾记录有明确
//     截断修复策略」，与 Store.Open 对 current.jsonl 的修复一致（store.go:101-118）；
//  2. 末条 state 记录非终态（completed/failed/cancelled/interrupted 之外）→
//     O_APPEND 追加 {kind:"state", state:"interrupted", run_id:文件名 runID}。
//
// 幂等：已终态（含 interrupted）的 run 跳过，重扫不追加第二个标记。
// 恢复绝不自动重跑：本方法只做截断与标记，不产生任何其他记录、不触碰
// current.jsonl——「不盲目重新执行」由恢复路径纯标记的构造保证
// （TestRestoreInterruptedRun 用记录数断言钉死）。
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
