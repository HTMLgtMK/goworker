package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-runtime/session"
)

// EventHub 的回放、持久化与背压语义见 ../docs/README.md。
// hub.mu 保护事件顺序和订阅状态；Publish 与 Subscribe 在锁内执行 journal I/O，
// 以保证写入顺序和回放到实时订阅的原子衔接。journal 锁不能反向获取 hub.mu；
// 持锁时不获取 runtime、manager 或 run gate 的锁。

// EventJournal 是事件持久化的 agent 侧消费面(进 SessionDeps.RunEvents):
// 与 RunStateJournal(spec.go)同构的「消费侧最小面」先例,由
// session.RunJournalStore 实现。ReadEvents/SeqRange 由 Subscribe 回放消费,
// 接口一次定形避免再动 SessionDeps。
type EventJournal interface {
	AppendEvent(rec session.EventRecord) error
	ReadEvents(afterSeq uint64) ([]session.EventRecord, error)
	// SeqRange 返回保留历史的 [earliest..latest] 事件 seq 区间。四出参形状
	// (契约定形):ok=false 且 err=nil = 空历史(无任何事件记录,从 1
	// 起算);err 非 nil = Journal 扫描 I/O 失败——空历史与读取失败必须可辨,
	// 调用方 NewEventHub 对后者显式拒绝创建,绝不把读取失败当空历史从 Seq=1
	// 重起(与既有记录冲突)。
	SeqRange() (earliest, latest uint64, ok bool, err error)
}

// 编译期断言:session.RunJournalStore 满足 agent 侧消费面。签名漂移在
// 编译期暴露(断言在实现类型就位的那一步补)。
var _ EventJournal = (*session.RunJournalStore)(nil)

// ErrHubClosed 是 hub 停机后的拒绝 sentinel:Close 之后 Publish/Subscribe
// 一律拒绝——「关掉的事件流不能再产出事件」。
var ErrHubClosed = errors.New("session: event hub closed")

// ErrCursorExpired 是订阅游标失效 sentinel(「不允许静默
// 跳过缺失事件」):fromSeq 超前 latest(客户端声称收到了尚未发布的事件,只能
// 是客户端错误或状态错乱)或 fromSeq+1 落于保留历史 earliest 之前(要的首事件
// 已被清理;V1 无修剪,仅 hub 重建且历史被截短时可达)——两种失效都明确报错,
// 绝不静默归零或跳段续播。fromSeq=0 是「从保留最早开始」,不算失效。
var ErrCursorExpired = errors.New("session: event cursor expired")

// delta 合并窗与订阅者缓冲参数:
//   - deltaFlushInterval/deltaFlushBytes 取「50~100ms 或一定字节数」的下限:
//     50ms 窗 ⇒ 合并落盘频率上界 20 次/s;
//   - subscriberBuffer 是 fan-out 直投通道容量:256 条给慢订阅者的宽限,超出
//     即断开(不阻塞执行/他人)。
const (
	deltaFlushInterval = 50 * time.Millisecond
	deltaFlushBytes    = 4 << 10
	subscriberBuffer   = 256
)

// ReplayIntegrity values describe what this hub can prove about its replay history:
// complete means no prior history or observed write failures; unknown means history
// predates this process; incomplete means a write/flush failed and remains sticky.
const (
	ReplayIntegrityComplete   = "complete"
	ReplayIntegrityUnknown    = "unknown"
	ReplayIntegrityIncomplete = "incomplete"
)

// hubSub routes live events registered after watermark. dead is close-only so a
// pump blocked on a full output channel can detect disconnect without draining live;
// only Close/fanOutLocked close these signals, and pump closes the consumer channel.
type hubSub struct {
	live      chan session.SessionEvent // fan-out 直投(容量 subscriberBuffer,非阻塞)
	watermark uint64                    // 注册点 latest:live 广播只含 seq>watermark 的事件
	dead      chan struct{}             // 断开信号(close-only):投递阻塞时经它感知断开并退出
}

// DeltaPart 是合并批次内的一个原始事件位:Seq 为该事件自身的会话游标,
// Payload 为实时广播的原字节(不透明 RawMessage,展开时原样透传)。
type DeltaPart struct {
	Seq     uint64          `json:"seq"`
	Payload json.RawMessage `json:"payload"`
}

// DeltaBatch 是合并批次记录(EventRecord FromSeq>0)的 payload 形状:
// Events 按追加序(即 seq 序)保存窗内每个事件的 {seq,payload}。批次只存在
// 于存储面——合并只压缩落盘记录条数,不压缩事件边界;携带 DeltaBatch payload
// 的 EventRecord 从不直接上 wire,Subscribe 的回放把批次展开为逐事件
// SessionEvent,wire 上单条与批次不可区分。
type DeltaBatch struct {
	Events []DeltaPart `json:"events"`
}

// validDeltaBatch accepts only non-empty, strictly increasing parts whose first
// and last sequence numbers match the declared interval. Invalid batches are
// skipped during replay and mark integrity incomplete.
func validDeltaBatch(batch DeltaBatch, fromSeq, seq uint64) bool {
	if len(batch.Events) == 0 {
		return false
	}
	if batch.Events[0].Seq != fromSeq {
		return false
	}
	for i := 1; i < len(batch.Events); i++ {
		if batch.Events[i].Seq <= batch.Events[i-1].Seq {
			return false
		}
	}
	return batch.Events[len(batch.Events)-1].Seq == seq
}

// pendingDelta groups same-type events for one run until the time/size threshold
// or EndRun/Close. parts preserve each sequence and payload for exact replay; the
// byte count is approximate and used only for the flush threshold.
type pendingDelta struct {
	runID    string
	fromSeq  uint64
	toSeq    uint64
	lastTime time.Time
	parts    []DeltaPart
	bytes    int
}

// IngressEvent 是 Publish 的入口形状(teeRunCallbacks 从 RunCallbacks
// 三入口产出):RunID 由产生方逐事件携带(tee 捕获 run.ID);为空时回落
// curRunID(BeginRun 括起的当前 run)。Payload 已是 JSON 编码(逐事件一次
// marshal,合并时复用原字节)。
type IngressEvent struct {
	RunID   string
	Type    string
	Payload json.RawMessage
}

// EventHub 是一个会话的事件枢纽(每会话一个,挂在 SessionRuntime.events,
// 惰性创建):Publish 是写侧(分配 Seq + durable-first/合并落盘 + fan-out),
// Subscribe 是读侧。j 为 nil 合法(纯内存 hub:不落盘、delta 不进合并
// 缓冲——纯内存场景无重放,缓冲无意义),测试与未启用持久化的宿主用。
type EventHub struct {
	sessionID string

	// mu 保护以下全部字段(字段清单 + flushTimer:定时 flush 必须 mu
	// 协调——上锁判空、落锁清 nil,Close 时停止,不泄漏 timer 回调)。
	mu sync.Mutex

	// seq 是下一个待分配的会话游标;earliest/latest 是保留历史区间(构造时
	// 自 journal.SeqRange 播种,V1 无修剪故 earliest 恒定);
	// latest 随每次成功发布推进为已分配的最大 seq(= 最新广播事件的 seq)。
	// key 事件落盘失败不推进 latest 也不回退 seq:该 seq 被烧掉(见 Publish
	// 注释的失败语义论证),live 与重放两侧一致跳过,缺口显式可观测。
	seq      uint64
	earliest uint64
	latest   uint64

	// replayIntegrity seeds Complete for empty/new history and Unknown for existing history
	// (cross-epoch integrity cannot be verified). Any durable write failure sets Incomplete,
	// which is sticky; it tracks replay completeness only, not run outcomes.
	replayIntegrity string

	// subs 是订阅者表(键=nextSubID 自增号);fan-out 与断开都在 mu 内。
	subs      map[uint64]*hubSub
	nextSubID uint64

	// journal 是持久化面(nil=纯内存);curRunID 是 BeginRun/EndRun 括起的
	// 当前 run(AppendEvent 目标的兜底与 delta 分桶归属;逐事件 RunID 以
	// IngressEvent.RunID 优先)。
	journal  EventJournal
	curRunID string

	// deltaBuf 是 delta 合并缓冲(key=Type;BeginRun/EndRun 括起保证单 run,
	// 切换前 flush);flushTimer 是窗定时器(time.AfterFunc 回调经 mu 协调)。
	deltaBuf   map[string]*pendingDelta
	flushTimer *time.Timer

	closed bool

	// done 是 hub 停机广播信号(NewEventHub 创建,Close 恰好关闭一次):pump
	// 阶段一投递 select 借它感知 Close 并退出(阶段二由 Close/fanOut 关闭
	// sub.live 感知,无需重复信号)。close-only 通道,pump 无锁只读,安全。
	done chan struct{}
}

// NewEventHub seeds sequence and replay integrity from the journal. Existing history resumes
// at latest+1 and reports Unknown; empty or in-memory history starts at 1 and reports Complete.
// Seeding errors propagate uncached so no misleadingly healthy hub is created.
func NewEventHub(sessionID string, j EventJournal) (*EventHub, error) {
	h := &EventHub{
		sessionID:       sessionID,
		subs:            make(map[uint64]*hubSub),
		journal:         j,
		deltaBuf:        make(map[string]*pendingDelta),
		replayIntegrity: ReplayIntegrityComplete,
		done:            make(chan struct{}),
	}
	if j != nil {
		earliest, latest, ok, err := j.SeqRange()
		if err != nil {
			return nil, fmt.Errorf("agent: event hub: session %s: seed seq range: %w", sessionID, err)
		}
		if ok {
			h.earliest, h.latest = earliest, latest
			h.replayIntegrity = ReplayIntegrityUnknown
		}
	}
	h.seq = h.latest + 1
	return h, nil
}

// BeginRun 括起一个 run 的产生边界:设置 AppendEvent 目标兜底(curRunID),
// 并防御性 flush 上一 run 残留的合并缓冲(正常次序由 EndRun 收口,此处只兜
// 「未 EndRun 就 BeginRun」的调用形态——缓冲跨 run 混桶会破坏「同 run 内同型
// 合并」)。Close 后为幂等空操作。
func (h *EventHub) BeginRun(runID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.flushAllDeltaLocked()
	h.curRunID = runID
}

// EndRun 收口当前 run:flush 合并缓冲(delta 落盘先于该 run 的终态 state 记录
// ——RunJournalStore.AppendEvent 依赖此次序,终态落盘会 Close+逐出句柄,晚了
// 会撞 ErrRunExists,journal_store.go:125-130 的次序约束)+ 清 curRunID。flush
// 失败仅 warn(降级语义见文件头)。Close 后为幂等空操作(Close 已 flush)。
func (h *EventHub) EndRun() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.flushAllDeltaLocked()
	h.curRunID = ""
}

// Publish assigns a session-wide sequence and broadcasts while holding hub.mu.
// Key events are durable-first and rejected on append failure (the sequence is
// consumed); deltas broadcast immediately, then flush in batches with warn-only
// persistence failure. Slow subscribers are disconnected without blocking callers.
func (h *EventHub) Publish(ev IngressEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrHubClosed
	}

	// 取用后自增:seq 语义是「下一个待分配」(播种 latest+1),首个事件即取
	// 该值——新鲜 hub 的首事件 seq=1,磁盘续接的首事件 seq=latest+1,无跳号。
	seq := h.seq
	h.seq++
	runID := ev.RunID
	if runID == "" {
		runID = h.curRunID
	}
	now := time.Now()
	out := session.SessionEvent{
		SessionID: h.sessionID,
		RunID:     runID,
		Seq:       seq,
		Type:      ev.Type,
		Time:      now,
		Payload:   ev.Payload,
	}

	if session.IsKeyEvent(ev.Type) {
		// durable-first:fsync 成功后才广播;失败不广播、上抛(缺口显式)。
		// EventRecord.Kind 由存储边界强制(journal.go RunJournal.AppendEvent
		// 置 eventKind),此处不依赖其常量。
		if h.journal != nil {
			rec := session.EventRecord{
				SessionID: h.sessionID,
				RunID:     runID,
				Type:      ev.Type,
				Seq:       seq,
				Time:      now,
				Payload:   ev.Payload,
			}
			if err := h.journal.AppendEvent(rec); err != nil {
				// 落盘失败置 Incomplete(粘滞永不复位)——run 的执行结果
				// 与事件完整性是两个独立状态,本置位不改 Publish 的失败上抛
				// 语义(不广播、错误原样)。
				h.replayIntegrity = ReplayIntegrityIncomplete
				return fmt.Errorf("agent: event hub: append key event %s seq %d: %w", ev.Type, seq, err)
			}
		}
		h.latest = seq
		h.fanOutLocked(out)
		return nil
	}

	// delta:实时逐事件广播(不合并),后进合并缓冲(仅持久化面,j!=nil)。
	h.latest = seq
	h.fanOutLocked(out)
	if h.journal != nil {
		h.bufferDeltaLocked(ev.Type, ev.Payload, out)
	}
	return nil
}

// LatestSeq 返回已成功广播的最大 seq(订阅注册 watermark 语义);无事件时为
// 播种值(磁盘 latest 或 0)。
func (h *EventHub) LatestSeq() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.latest
}

// EarliestSeq 返回保留历史的最早 seq(V1 无修剪,恒为播种值;纯内存 hub 为 0)。
func (h *EventHub) EarliestSeq() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.earliest
}

// ReplayIntegrity reports whether replay history is complete, unverifiable across
// process restarts, or known incomplete. Any append/flush failure makes the state
// Incomplete permanently; this signal does not affect run results.
func (h *EventHub) ReplayIntegrity() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.replayIntegrity
}

// Subscribe atomically snapshots replay through the registration watermark and
// registers live delivery under hub.mu. Replay is sequence-sorted; live events
// must exceed the watermark, preventing gaps/duplicates. It returns cursor, closed,
// or journal-read errors; callers cancel ctx when they stop consuming.
func (h *EventHub) Subscribe(ctx context.Context, fromSeq uint64) (<-chan session.SessionEvent, error) {
	h.mu.Lock()
	// ① 拒绝面:关闭与窗口校验(见 ErrCursorExpired 注释)。
	if h.closed {
		h.mu.Unlock()
		return nil, ErrHubClosed
	}
	if fromSeq > h.latest || (fromSeq != 0 && fromSeq+1 < h.earliest) {
		h.mu.Unlock()
		return nil, ErrCursorExpired
	}

	// Replay snapshot: flush pending deltas first so the journal covers through latest and
	// live delivery begins after the watermark without a gap. Journal physical order differs
	// from seq order, so the read history is sorted by the globally unique Seq.
	var replay []session.SessionEvent
	if h.journal != nil {
		h.flushAllDeltaLocked()
		recs, err := h.journal.ReadEvents(fromSeq)
		if err != nil {
			h.mu.Unlock()
			return nil, fmt.Errorf("agent: event hub: subscribe %s read events from %d: %w", h.sessionID, fromSeq, err)
		}
		replay = make([]session.SessionEvent, 0, len(recs))
		for _, rec := range recs {
			// Expand each record to per-seq SessionEvents: single records yield one event;
			// merged batches yield one per part, skipping parts at or below fromSeq so resume
			// neither duplicates nor loses events. Parts are then sorted by Seq.
			if rec.FromSeq == 0 {
				replay = append(replay, session.SessionEvent{
					SessionID: rec.SessionID,
					RunID:     rec.RunID,
					Seq:       rec.Seq,
					Type:      rec.Type,
					Time:      rec.Time,
					Payload:   rec.Payload,
				})
				continue
			}
			// Valid JSON is insufficient: validDeltaBatch must also verify non-empty parts,
			// strictly increasing seqs, and consistent range bounds. Any failure sets sticky
			// Incomplete, warns, and skips the record rather than failing the whole attach.
			var batch DeltaBatch
			if err := json.Unmarshal(rec.Payload, &batch); err != nil {
				// Legacy fallback: pre-release data stored merged deltas as one opaque JSON
				// string. Emit it as a single event at the record's end Seq, but mark Incomplete
				// because per-seq attribution is unknown. New journals never produce this format.
				var legacy string
				lerr := json.Unmarshal(rec.Payload, &legacy)
				if lerr == nil {
					h.replayIntegrity = ReplayIntegrityIncomplete
					slog.Warn("agent: event hub replay legacy merged delta payload (per-seq attribution unknown)",
						"session", h.sessionID, "run", rec.RunID,
						"type", rec.Type, "seq", rec.Seq, "from_seq", rec.FromSeq)
					replay = append(replay, session.SessionEvent{
						SessionID: rec.SessionID,
						RunID:     rec.RunID,
						Seq:       rec.Seq,
						Type:      rec.Type,
						Time:      rec.Time,
						Payload:   rec.Payload,
					})
					continue
				}
				// 两种解析都失败 → 跳过 + Incomplete(缺口经三态显式暴露)。
				h.replayIntegrity = ReplayIntegrityIncomplete
				slog.Warn("agent: event hub replay skip undecodable delta batch",
					"err", err, "legacy_err", lerr, "session", h.sessionID, "run", rec.RunID,
					"type", rec.Type, "seq", rec.Seq, "from_seq", rec.FromSeq)
				continue
			}
			if !validDeltaBatch(batch, rec.FromSeq, rec.Seq) {
				// 结构校验失败视同解码失败:跳过 + Incomplete。此形态的
				// payload 已是 JSON 对象,旧格式(JSON 字符串)回退必然失败,直接
				// 走跳过路径。
				h.replayIntegrity = ReplayIntegrityIncomplete
				slog.Warn("agent: event hub replay skip malformed delta batch structure",
					"session", h.sessionID, "run", rec.RunID,
					"type", rec.Type, "seq", rec.Seq, "from_seq", rec.FromSeq)
				continue
			}
			for _, part := range batch.Events {
				if part.Seq <= fromSeq {
					continue
				}
				replay = append(replay, session.SessionEvent{
					SessionID: rec.SessionID,
					RunID:     rec.RunID,
					Seq:       part.Seq,
					Type:      rec.Type,
					Time:      rec.Time,
					Payload:   part.Payload,
				})
			}
		}
		sort.Slice(replay, func(i, j int) bool { return replay[i].Seq < replay[j].Seq })
	}

	// ③ 注册订阅:watermark=注册点 latest(防 Race B 的回放/实时边界,见
	// hubSub 注释)。
	sub := &hubSub{
		live:      make(chan session.SessionEvent, subscriberBuffer),
		watermark: h.latest,
		dead:      make(chan struct{}),
	}
	id := h.nextSubID
	h.nextSubID++
	h.subs[id] = sub

	// ④ pump:单线程消费两源(排定的 replay 快照 + sub.live),先 ≤watermark
	// 的 replay 后 >watermark 的 live。ch 容量 subscriberBuffer:replay/live
	// 投递只阻塞 pump 自身,慢消费不反压 hub(会满的是 fan-out 直投侧,满了
	// 断开该订阅者;pump 侧由 ctx 感知弃读)。
	ch := make(chan session.SessionEvent, subscriberBuffer)
	h.mu.Unlock()
	go h.pumpEvents(ctx, id, ch, sub, replay)
	return ch, nil
}

// pumpEvents delivers the replay snapshot before live events and exits on context
// cancellation, hub shutdown, or subscriber disconnect. On every exit it removes
// the subscription and closes the consumer channel; only Close/fanOutLocked close live.
func (h *EventHub) pumpEvents(ctx context.Context, id uint64, ch chan<- session.SessionEvent, sub *hubSub, replay []session.SessionEvent) {
	defer close(ch)
	defer h.removeSub(id)

	// 阶段一:replay 快照,已按 Seq 升序。lastSeq 是「已投递的最大 Seq」哨兵
	// (Seq 恒 ≥1,0 即「尚未投递」)。
	var lastSeq uint64
	for _, ev := range replay {
		select {
		case ch <- ev:
		case <-ctx.Done():
			return
		case <-h.done:
			return
		case <-sub.dead:
			return
		}
		lastSeq = ev.Seq
	}
	// 阶段二:live 转发。fan-out 只投 seq>watermark ≥ 快照最大 seq,序天然
	// 衔接;skip ≤ lastSeq 是 Race B 的 belt-and-braces(真重复在 mu 临界区
	// 下不可达,到达即纪律)——收到重复 Seq 去重,但不吞真实缺口(缺口处
	// seq 不重复,原样透传,由消费方游标推进语义显式可见)。
	for {
		select {
		case ev, ok := <-sub.live:
			if !ok {
				return
			}
			if ev.Seq <= lastSeq {
				continue
			}
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			case <-sub.dead: // ch 满被停读阻塞时,断开信号解锁退出(见函数头)
				return
			}
			lastSeq = ev.Seq
		case <-ctx.Done():
			return
		}
	}
}

// removeSub 从订阅表摘除一个订阅者(pump 退出路径专用)。Close/fanOutLocked
// 断开时已各自 delete,此处重复删除是 no-op。不 close(sub.live):close 的
// 所有权在 Close/fanOutLocked(仅有的两个 close 点);ctx 退出路径的 sub.live
// 随注册摘除后无人再投递,由 GC 回收。调用方必须不持 h.mu。
func (h *EventHub) removeSub(id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs, id)
}

// Close rejects future publish/subscribe calls, wakes pumps, flushes pending deltas,
// disconnects subscribers, and closes journals implementing io.Closer. It is
// idempotent; flush failures are logged and do not block shutdown.
func (h *EventHub) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	close(h.done)
	h.flushAllDeltaLocked()
	for id, sub := range h.subs {
		delete(h.subs, id)
		close(sub.live)
		close(sub.dead) // 与慢订阅者断开同款:解锁可能阻塞在投递上的 pump
	}
	var err error
	if c, ok := h.journal.(io.Closer); ok {
		if cerr := c.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// fanOutLocked broadcasts only events above each subscriber's registration watermark,
// then disconnects (delete+close) any subscriber whose buffer is full. It never blocks.
// Caller must hold h.mu.
func (h *EventHub) fanOutLocked(ev session.SessionEvent) {
	for id, sub := range h.subs {
		if ev.Seq <= sub.watermark {
			continue
		}
		select {
		case sub.live <- ev:
		default:
			delete(h.subs, id)
			close(sub.live)
			close(sub.dead) // 解锁可能阻塞在投递上的 pump(hubSub 注释)
			slog.Warn("agent: event hub disconnected slow subscriber", "session", h.sessionID, "type", ev.Type, "seq", ev.Seq)
		}
	}
}

// bufferDeltaLocked 把一条 delta 追加进合并缓冲(调用方必须持 h.mu,且广播已
// 完成):按 Type 取桶(无则开新桶,fromSeq=本事件 seq),推进 toSeq/parts/
// bytes/lastTime;首条进空缓冲时上 50ms 窗定时器(AfterFunc 回调经 mu 协调,
// 见 flushDeltaOnTimer);单桶越过 4KB 阈值立即 flush 该桶(窗提前关闭,缓冲
// 不设上界增长)。RunID/Time 取广播形状(与逐事件实时流同源)。
func (h *EventHub) bufferDeltaLocked(typ string, payload json.RawMessage, out session.SessionEvent) {
	b := h.deltaBuf[typ]
	if b == nil {
		b = &pendingDelta{runID: out.RunID, fromSeq: out.Seq}
		h.deltaBuf[typ] = b
		if h.flushTimer == nil {
			h.flushTimer = time.AfterFunc(deltaFlushInterval, h.flushDeltaOnTimer)
		}
	}
	b.toSeq = out.Seq
	b.lastTime = out.Time
	b.parts = append(b.parts, DeltaPart{Seq: out.Seq, Payload: payload})
	b.bytes += len(payload)
	if b.bytes >= deltaFlushBytes {
		h.flushDeltaTypeLocked(typ)
		if len(h.deltaBuf) == 0 {
			h.stopFlushTimerLocked()
		}
	}
}

// flushDeltaOnTimer 是窗定时器回调:50ms 窗到点,flush 全部缓冲桶。经 mu 协调
// ——与 Close 竞争时谁先拿锁谁生效,后到者见空桶/见 closed,无双重 flush、
// 无泄漏(Close 必停 timer;本回调自身也不重启 timer)。
func (h *EventHub) flushDeltaOnTimer() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.flushAllDeltaLocked()
}

// flushAllDeltaLocked flush 全部缓冲桶并在清空后停掉窗定时器(空缓冲无需
// 定时;下一条 delta 会重新上窗)。调用方必须持 h.mu。
func (h *EventHub) flushAllDeltaLocked() {
	for typ := range h.deltaBuf {
		h.flushDeltaTypeLocked(typ)
	}
	h.stopFlushTimerLocked()
}

// flushDeltaTypeLocked merges a buffer bucket into one DeltaBatch record covering
// [FromSeq..ToSeq]; replay expands it per seq. Write failures only warn. Caller holds h.mu.
func (h *EventHub) flushDeltaTypeLocked(typ string) {
	b, ok := h.deltaBuf[typ]
	if !ok {
		return
	}
	delete(h.deltaBuf, typ)
	payload, err := json.Marshal(DeltaBatch{Events: b.parts})
	if err != nil {
		// parts 全为合法 JSON 的 RawMessage,marshal 理论不可达(原始 payload
		// 均经 json.Marshal 产出);与 append 失败同款降级(warn + Incomplete,
		// 不落盘)。
		h.replayIntegrity = ReplayIntegrityIncomplete
		slog.Warn("agent: event hub delta batch marshal failed (replay may miss delta range)",
			"err", err, "session", h.sessionID, "run", b.runID, "type", typ,
			"from_seq", b.fromSeq, "to_seq", b.toSeq)
		return
	}
	rec := session.EventRecord{
		SessionID: h.sessionID,
		RunID:     b.runID,
		Type:      typ,
		Seq:       b.toSeq,   // 合并记录 Seq=批次末事件 seq(区间声明,存储层批次标记)
		FromSeq:   b.fromSeq, // >0 = 合并批次记录(payload 为 DeltaBatch JSON)
		Time:      b.lastTime,
		Payload:   payload,
	}
	if err := h.journal.AppendEvent(rec); err != nil {
		// 合并 flush 失败置 Incomplete(粘滞永不复位)——重放缺 [FromSeq..
		// Seq] 段的事实被单调记账,ReplayIntegrity 随之 Incomplete,客户端不再
		// 把缺段回放当完整历史。warn 降级语义不变(assistant.message_finalized
		// 兜底定稿,执行不受阻)。
		h.replayIntegrity = ReplayIntegrityIncomplete
		slog.Warn("agent: event hub delta flush failed (replay may miss delta range)",
			"err", err, "session", h.sessionID, "run", b.runID, "type", typ,
			"from_seq", b.fromSeq, "to_seq", b.toSeq)
	}
}

// stopFlushTimerLocked 停掉窗定时器并清引用(定时器必须 mu 协调判空/清理,
// Close 与 flush 路径共用;AfterFunc 停止/触发均不泄漏 goroutine)。调用方必须
// 持 h.mu。
func (h *EventHub) stopFlushTimerLocked() {
	if h.flushTimer != nil {
		h.flushTimer.Stop()
		h.flushTimer = nil
	}
}
