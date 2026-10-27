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

// event_hub.go 是 P3-2/P3-3 的事件枢纽(总方案 P3 第 2/3 条):写侧 Seq 分配、
// 关键事件 durable-first 落盘、高频 delta 合并落盘、fan-out 非阻塞广播(P3-2);
// 读侧 Subscribe 的 Replay+Live 原子切换、ErrCursorExpired 窗口校验、断线清理
// (P3-3)。P3-4 再把它接进 StartRun(teeRunCallbacks)。
//
// 语义契约(总方案第五节,落实为 P3-0 契约表):
//   - Seq 是 Session 维度严格递增游标,跨 Run 不重复(P3-0 表第 1 行):seq 在
//     hub.mu 内单调分配,构造时自 journal 既有 max seq 播种,BeginRun/EndRun
//     切换 run 不重置游标;
//   - 关键事件必须持久化且 durable-first(P3-0 表第 2 行):IsKeyEvent 的 9 类
//     事件在 mu 内 journal fsync 成功后才广播,失败不广播、返回错误——总方案
//     第五节「持久化写入失败不能继续把事件当作可恢复事件正常广播」;失败同时
//     把 replayIntegrity 置为 Incomplete(I4 三态,粘滞永不复位);
//   - 高频 delta 合并落盘(P3-0 表第 3 行,CR3-I1 定稿存储形状):3 类 delta
//     先逐事件实时广播(per-event seq),再进 50ms/4KB 合并窗,按 Type 分桶
//     合并为一条批次记录(EventRecord FromSeq>0、Seq=批次末 seq,payload 为
//     DeltaBatch JSON——按序保留窗内每个事件的 {seq,payload} 边界);Subscribe
//     回放把批次展开为原始 per-seq 事件,实时与回放形状彻底一致(wire 上单条/
//     批次不可区分,断点恢复精确到 seq)。合并落盘失败仅 warn——重放缺 delta
//     段可接受,assistant.message_finalized 兜底定稿(降级语义,P3-0 表第 7 行
//     的对偶面),失败同样置 Incomplete:执行结果与事件完整性是两个独立状态,
//     后者经 ReplayIntegrity()/attach 与 run/get 的 replayIntegrity 字段向
//     客户端显式暴露(I4 三态,替代旧 eventsComplete 布尔),不把不完整事件
//     当完整历史、也不能把「未观察到失败」谎报为「历史完整」;
//   - 慢订阅者背压(P3-0 表第 8 行):fan-out 非阻塞投递,缓冲满即断开该订阅者
//     (delete + close(sub.live)),绝不阻塞 AgentRun 执行与其他订阅者;重连从
//     游标补齐属 P3-3 的 Subscribe。
//
// 并发与锁论证(plan P3-2/P3-3「并发与锁论证」):hub.mu 全程保护 seq/earliest/
// latest/subs/nextSubID/journal 目标/curRunID/deltaBuf/flushTimer/closed 全部
// 可变字段(done 是 close-only 信号,构造后只关一次,无锁读安全)。Publish 的
// journal fsync 与 Subscribe 的 ReadEvents 在 mu 内都是 V1 正确性优先的取舍:
// delta 合并把写频率压到 ≤20 次/s(50ms 窗)、订阅低频;若生产不达标,P5 优化
// 为「mu 内定序号槽、mu 外写、写完补广播」(注释留档,本 Plan 不做)。fan-out
// 的 close(sub.live) 在 mu 内合法(Go channel close 本身无锁竞争)。无嵌套获取:
// hub.mu 内只做非阻塞 send 与文件读写,不触 gate/runSlots/manager.mu/rt.mu
// (journal 的 AppendEvent 走 RunJournalStore 自持的 s.mu→j.mu,单向无回边)
// ⇒ 无锁环。

// EventJournal 是事件持久化的 agent 侧消费面(P3-2 定稿,P3-4 进 SessionDeps.
// RunEvents):与 RunStateJournal(spec.go)同构的「消费侧最小面」先例,由
// session.RunJournalStore(P3-1)实现。ReadEvents/SeqRange 由 P3-3 的 Subscribe
// 回放消费,接口一次定形避免再动 SessionDeps。
type EventJournal interface {
	AppendEvent(rec session.EventRecord) error
	ReadEvents(afterSeq uint64) ([]session.EventRecord, error)
	// SeqRange 返回保留历史的 [earliest..latest] 事件 seq 区间。四出参形状
	// (第三轮评审 I3 定稿):ok=false 且 err=nil = 空历史(无任何事件记录,从 1
	// 起算);err 非 nil = Journal 扫描 I/O 失败——空历史与读取失败必须可辨,
	// 调用方 NewEventHub 对后者显式拒绝创建,绝不把读取失败当空历史从 Seq=1
	// 重起(与既有记录冲突)。
	SeqRange() (earliest, latest uint64, ok bool, err error)
}

// 编译期断言:session.RunJournalStore(P3-1)满足 agent 侧消费面。签名漂移在
// 编译期暴露(plan P2-3 的先例:断言在实现类型就位的那一步补)。
var _ EventJournal = (*session.RunJournalStore)(nil)

// ErrHubClosed 是 hub 停机后的拒绝 sentinel:Close 之后 Publish/Subscribe
// 一律拒绝——「关掉的事件流不能再产出事件」。
var ErrHubClosed = errors.New("session: event hub closed")

// ErrCursorExpired 是订阅游标失效 sentinel(P3-3,总方案第五节「不允许静默
// 跳过缺失事件」):fromSeq 超前 latest(客户端声称收到了尚未发布的事件,只能
// 是客户端错误或状态错乱)或 fromSeq+1 落于保留历史 earliest 之前(要的首事件
// 已被清理;V1 无修剪,仅 hub 重建且历史被截短时可达)——两种失效都明确报错,
// 绝不静默归零或跳段续播。fromSeq=0 是「从保留最早开始」,不算失效。
var ErrCursorExpired = errors.New("session: event cursor expired")

// delta 合并窗与订阅者缓冲参数(P3-2 常量):
//   - deltaFlushInterval/deltaFlushBytes 是总方案第五节「50~100ms 或一定字节
//     数」取下限:50ms 窗 ⇒ 合并落盘频率上界 20 次/s(plan §5 风险 2 的依据);
//   - subscriberBuffer 是 fan-out 直投通道容量:256 条给慢订阅者的宽限,超出
//     即断开(P3-0 表第 8 行,不阻塞执行/他人)。
const (
	deltaFlushInterval = 50 * time.Millisecond
	deltaFlushBytes    = 4 << 10
	subscriberBuffer   = 256
)

// ReplayIntegrity 三态常量(I4,第二轮评审:eventsComplete 布尔在进程重启后
// 失真——旧纪元的落盘失败不延续到新 hub,布尔恒 true 把「未观察到失败」谎报
// 为「历史完整」):会话事件流回放完整性的显式协议信号,daemon wire 的
// replayIntegrity 字段直接携带这三个字符串值。语义按 hub 的观察边界定义:
//
//   - Complete:本 hub 实例自创建以来未发生任何写侧失败,且没有需要跨进程纪元
//     继承的历史——空历史/纯内存 hub,本进程观察面即全部,「完整」可证;
//   - Unknown:journal 播种发现既有历史(SeqRange ok)——旧纪元的完整性事实
//     无法在本进程验证(旧 hub 的失败标记不持久化,I4 的核心批评),诚实优先,
//     绝不把不可验证的历史报告为完整;客户端按「可能缺段」消费或按 0 全量重放;
//   - Incomplete:本 hub 纪元内发生 key 事件落盘失败或合并 delta flush 失败,
//     重放确定缺段——粘滞(见 replayIntegrity 字段注释),永不复位。
const (
	ReplayIntegrityComplete   = "complete"
	ReplayIntegrityUnknown    = "unknown"
	ReplayIntegrityIncomplete = "incomplete"
)

// hubSub 是一个已注册的订阅者(live 广播直投端)。watermark 是注册点的
// latest(P3-3):fan-out 只会投递 seq>watermark 的事件——注册在 hub.mu 内,
// 晚于注册的 Publish 必然分配到更大的 seq,该不变量时序上天然成立;fanOut
// 侧的显式 skip 与 pump 的 lastSeq 去重是同一防线的 belt-and-braces(Race B:
// 回放与实时重复/乱序)。
//
// dead 是断开信号的 close-only 通道(P3-4 TestSlowSubscriber 暴露的缺口):
// 断开 = fanOutLocked/Close 关闭 live+dead 两个通道。pump 的投递若被消费方
// 停读卡死(ch 满),它阻塞在 ch 发送上、无法经「接收 live」感知 live 已关
// ——对已关闭且带缓冲的 channel 发起接收会先排干缓冲,把接收 case 加进投递
// select 等于给 pump 开一条无界虹吸,反而令 live 永不满、断开永不触发。dead
// 恒空、close-only:关闭后接收立即就绪且不消耗数据,是被阻塞投递解锁的
// 唯一干净信号。消费方的可见断开面仍是「返回的 ch 被关闭」(pump 退出时
// defer close)。
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

// DeltaBatch 是合并批次记录(EventRecord FromSeq>0)的 payload 形状(CR3-I1):
// Events 按追加序(即 seq 序)保存窗内每个事件的 {seq,payload}。批次只存在
// 于存储面——合并只压缩落盘记录条数,不压缩事件边界;携带 DeltaBatch payload
// 的 EventRecord 从不直接上 wire,Subscribe 的回放把批次展开为逐事件
// SessionEvent,wire 上单条与批次不可区分。
type DeltaBatch struct {
	Events []DeltaPart `json:"events"`
}

// pendingDelta 是 delta 合并缓冲的一个分桶(按 Type,同 run 内同型合并):
// 攒 [fromSeq..toSeq] 区间内的全部事件,窗满(50ms/4KB)或 EndRun/Close
// 时合并为一条 EventRecord 落盘。runID/lastTime 取桶内首末事件的值(合并记录
// 的归属与时间戳);parts 按序保存窗内每个事件的 {seq,payload}(bufferDelta
// Locked 追加时记录 out.Seq——批次内部边界不丢,回放展开 per-seq 事件、断点
// 恢复精确到 seq 的依据);bytes 是 payload 粗略字节数(4KB 阈值判据,不求精确)。
type pendingDelta struct {
	runID    string
	fromSeq  uint64
	toSeq    uint64
	lastTime time.Time
	parts    []DeltaPart
	bytes    int
}

// IngressEvent 是 Publish 的入口形状(P3-4 的 teeRunCallbacks 从 RunCallbacks
// 三入口产出):RunID 由产生方逐事件携带(tee 捕获 run.ID);为空时回落
// curRunID(BeginRun 括起的当前 run)。Payload 已是 JSON 编码(逐事件一次
// marshal,合并时复用原字节)。
type IngressEvent struct {
	RunID   string
	Type    string
	Payload json.RawMessage
}

// EventHub 是一个会话的事件枢纽(每会话一个,P3-4 挂在 SessionRuntime.events,
// 惰性创建):Publish 写侧(分配 Seq + durable-first/合并落盘 + fan-out),
// Subscribe 读侧属 P3-3。j 为 nil 合法(纯内存 hub:不落盘、delta 不进合并
// 缓冲——纯内存场景无重放,缓冲无意义),测试与未启用持久化的宿主用。
type EventHub struct {
	sessionID string

	// mu 保护以下全部字段(plan 的字段清单 + flushTimer:定时 flush 必须 mu
	// 协调——上锁判空、落锁清 nil,Close 时停止,不泄漏 timer 回调)。
	mu sync.Mutex

	// seq 是下一个待分配的会话游标;earliest/latest 是保留历史区间(构造时
	// 自 journal.SeqRange 播种,V1 无修剪故 earliest 恒定,plan §5 风险 6);
	// latest 随每次成功发布推进为已分配的最大 seq(= 最新广播事件的 seq)。
	// key 事件落盘失败不推进 latest 也不回退 seq:该 seq 被烧掉(见 Publish
	// 注释的失败语义论证),live 与重放两侧一致跳过,缺口显式可观测。
	seq      uint64
	earliest uint64
	latest   uint64

	// replayIntegrity 是回放完整性三态(I4,替代旧 eventsIncomplete 布尔——
	// 布尔在 hub 重建后失真:旧纪元失败不延续,新 hub 恒「完整」谎报)。构造期
	// 播种:journal 既有历史(SeqRange ok)→ Unknown(跨纪元完整性不可验证,
	// 诚实优先);空历史/纯内存 → Complete(本进程观察面即全部)。此后唯一写
	// 点是失败路径:任何 key 事件落盘失败(Publish 的 durable-first 拒绝)或
	// 合并 delta flush 失败 → Incomplete,且**永不复位**(粘滞)——「曾经失败」
	// 是单调事实,后续成功发布不翻回,置位方向单调(Complete/Unknown →
	// Incomplete 一去不返)故无条件赋值即粘滞,无需额外守卫。客户端经
	// ReplayIntegrity()/attach 与 run/get 的 replayIntegrity 字段读出,据此
	// 判断回放完整性(Complete 之外都不得把收到的事件当完整历史)。执行结果与
	// 事件完整性是两个独立状态(I6):本标记只记账后者,不改变任何终态化路径。
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

// NewEventHub 创建事件枢纽并播种游标与完整性三态:j 非 nil 时经 SeqRange 读
// 既有保留历史的 [earliest..latest](磁盘扫描),seq=latest+1 续接分配;j 为
// nil 时 latest=0、seq=1(纯内存 hub 从 1 起算)。播种失败(第三轮评审 I3):
// SeqRange 返回 err(Journal 读取/解析失败)→ 返回 (nil, err) 显式上抛——
// 持久化会话的 hub 创建失败绝不创建「看似健康」的 hub(旧形态把读取失败降级
// 为空历史:replayIntegrity 恒 complete 且 seq 从 1 重起,与既有记录冲突);
// 调用链 bindEvents/EnsureHub/StartRun 同步上抛。空历史(ok=false, err=nil)
// 与既有历史(ok=true)照旧分别播种 Complete/Unknown。完整性播种(I4):有既有
// 历史(ok)→ Unknown——历史产生于旧进程纪元,其间的落盘失败无从在本进程验证,
// 诚实报 Unknown 绝不谎报完整;空历史/纯内存 → Complete——本进程观察面即全部,
// 此后的失败由写侧置 Incomplete。
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

// Publish 发布一个事件(P3-2 核心语义,全程持 hub.mu):
//
//   - closed → ErrHubClosed;
//   - seq++ 分配会话游标(跨 Run 不重复,BeginRun/EndRun 不重置),包成
//     session.SessionEvent 广播形状(逐事件 Time,广播与落盘同源);
//   - key 事件(session.IsKeyEvent,9 类):journal.AppendEvent(fsync)成功后
//     才 fan-out(durable-first);失败 → 不广播、返回错误(总方案第五节
//     「持久化写入失败不能继续把事件当作可恢复事件正常广播」)。失败时该 seq
//     被烧掉不回退:回退会让「Sync 失败但 Write 已完整落行」的记录与下一个
//     事件共用同 seq,重放去重会丢掉后者(真事件丢失);烧号只留下一个两侧
//     一致跳过的显式缺口,是失败语义下更安全的取舍。j==nil 的纯内存 hub 跳过
//     落盘直接广播(key 语义只约束「可恢复的持久化事件」);
//   - delta(3 类高频):先 fan-out(实时逐事件,per-event seq——订阅者看到
//     的流不合并),后进合并缓冲(50ms/4KB 窗,按 Type 分桶;落盘失败仅 warn
//     ——重放缺 delta 段可接受,assistant.message_finalized 兜底定稿);
//   - fan-out:遍历 subs 非阻塞 send;缓冲满 → delete+close(sub.live) 断开该
//     订阅者(总方案第五节背压:不阻塞 AgentRun 执行与其他订阅者;重连从游标
//     补齐属 P3-3)。
//
// 成功发布(广播已发生)推进 latest 为本事件 seq——它是 P3-3 订阅注册点的
// watermark 语义来源。journal 落盘在 mu 内:V1 正确性优先取舍,P5 优化路径见
// 文件头锁论证。
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
				// I4:落盘失败置 Incomplete(粘滞永不复位)——run 的执行结果
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

// ReplayIntegrity 返回回放完整性三态(I4,eventsIncomplete 布尔/EventsComplete
// 的替代读面):构造期播种(有 journal 历史 → Unknown,空历史/纯内存 →
// Complete),此后本 hub 纪元内任何 key 事件落盘失败或合并 delta flush 失败
// 使其变为 Incomplete 且永不复位(粘滞——失败是「曾经发生」的单调事实,后续
// 成功发布不翻回)。daemon 侧 RunControlSource.RunStatus 与 attach 响应经它
// 向客户端暴露回放完整性的显式信号(I4:不能让客户端把不完整事件当完整历史,
// 也不能把不可验证的跨纪元历史谎报为完整)。
func (h *EventHub) ReplayIntegrity() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.replayIntegrity
}

// Subscribe 注册一个事件订阅者并返回只读投递通道(P3-3,总方案第五节第 3 条
// Replay+Live 原子切换):fromSeq=0 从保留最早开始;fromSeq=N 投递 Seq>N 的
// 事件——先回放 (fromSeq, 注册点 latest] 的持久化历史,再无缝续上此后实时
// 广播的 >latest 事件。游标失效(超前 / 落于保留历史之前)返回 ErrCursorExpired,
// hub 已关闭返回 ErrHubClosed,回放读取失败上抛。
//
// mu 内一口气完成 ①-③(总方案第五节「订阅注册与确定回放边界组合成一个原子
// 逻辑操作」= 防 Race A:先查历史再注册、其间事件永久丢失的窗口不存在):
//
//	① closed → ErrHubClosed;窗口校验 → ErrCursorExpired;
//	② replay := journal.ReadEvents(fromSeq)(快照止于注册点 latest,见下);
//	③ 注册 sub(watermark=latest)+ nextSubID++;
//	④ 起 pump goroutine,返回只读 chan(容量 subscriberBuffer)。
//
// 顺序论证(plan 原文):replay 快照止于注册点 latest;此后 Publish 的事件
// seq 恒 >latest 且 fan-out 只投 sub.live ⇒ pump 先发完 ≤latest 的 replay 再
// 转发 >latest 的 live:无缝、无重、无乱序。
//
// 并发与锁:①-③ 同一 mu 临界区(上文 Race A);mu 内文件读是 V1 取舍(plan
// 原文)——ReadEvents 扫 runs/ 会阻塞 Publish 数 ms,正确性优先,P5 优化路径
// 同 Publish(文件头锁论证);hub.mu→journal 读锁单向(hub.mu → stub 的 j.mu /
// RunJournalStore 读路径无锁),与 Publish 的 hub.mu → s.mu → j.mu 同向,无锁环。
// 消费方契约:不再消费必须取消 ctx(断线/关页),否则 pump 在投递上等待至
// hub Close;通道关闭(pump 退出)即订阅终结,重连以已确认游标重新 Subscribe。
func (h *EventHub) Subscribe(ctx context.Context, fromSeq uint64) (<-chan session.SessionEvent, error) {
	h.mu.Lock()
	// ① 拒绝面:关闭与窗口校验(总方案第五节,ErrCursorExpired 注释)。
	if h.closed {
		h.mu.Unlock()
		return nil, ErrHubClosed
	}
	if fromSeq > h.latest || (fromSeq != 0 && fromSeq+1 < h.earliest) {
		h.mu.Unlock()
		return nil, ErrCursorExpired
	}

	// ② 回放快照。快照前先 flush 合并缓冲:pending 窗内的 delta 已实时广播但
	// 尚未落盘,不 flush 则 ReadEvents 止于更早的 seq,而 watermark=latest 又令
	// live 侧只投 >latest ——(fromSeq, latest] 会出现两侧都不投的缺口,违背
	// 「无缝」;flush 后 journal 完整覆盖到 latest,「replay 快照止于注册点
	// latest」才成立。flush 是 50ms 窗定时路径的同款操作(mu 内、幂等),订阅
	// 是低频动作,不侵蚀 delta 合并收益。
	//
	// journal 物理序 ≠ seq 序:key 事件在发布时逐条落盘,其前后 delta 的合并
	// 记录在 flush 时才追加,故同段历史里 key 记录可排在合并记录之前。总方案
	// 第五节要求回放「按 Seq 排序的历史事件」,读出后排序;Seq 全局唯一(每
	// 事件恰落一条记录,合并记录 Seq=ToSeq),按 Seq 排序即全序。
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
			// EventRecord → SessionEvent(传输形状)展开(CR3-I1):丢 Kind 信封。
			//   - FromSeq==0 = 单条事件记录(key 事件与逐条落盘形态)→ 原样产出
			//     一条 SessionEvent;
			//   - FromSeq>0 = 合并批次记录(payload 为 DeltaBatch JSON)→ 按
			//     per-part 逐条展开产出,并跳过 part.Seq <= fromSeq:批次记录的
			//     Seq=批次末 seq 可能远大于游标(ReadEvents 只按记录末 seq 过滤),
			//     批次内早于游标的部分不得重发——断点恢复精确到 seq,不重复、
			//     不丢失(CR3-I1 的交错实例:批次 [101..104] 内含 103/104,游标
			//     102 之后的恢复面恰为 103/104,101 绝不重放);
			//   - part 顺序即 seq 序(桶内追加序),展开后统一按 Seq 升序排序。
			//
			// 展开后实时与回放形状彻底一致:wire 上均为 per-seq 单条事件,无
			// 范围声明、无批量语义,客户端游标以实际收到的 Seq 逐条推进。
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
			var batch DeltaBatch
			if err := json.Unmarshal(rec.Payload, &batch); err != nil {
				// 批次解码失败 → warn 跳过该记录(降级语义,与落盘失败对偶:
				// 重放缺 delta 段可接受,assistant.message_finalized 兜底定稿)。
				slog.Warn("agent: event hub replay skip undecodable delta batch",
					"err", err, "session", h.sessionID, "run", rec.RunID,
					"type", rec.Type, "seq", rec.Seq)
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

// pumpEvents 是单订阅者的投递协程(Subscribe ④):阶段一逐条投递 replay 快照,
// 阶段二转发 live 广播。退出路径全覆盖、不泄漏 goroutine:
//   - ctx 取消:两阶段的投递 select 都含 <-ctx.Done(),阻塞即醒;
//   - hub Close:阶段一经 h.done( Close 恰好关闭一次)唤醒——快照投递中途
//     停机即断开,与 Close「断开全部订阅者」语义一致;阶段二由 Close 关闭
//     sub.live 感知(range 收到 ok=false);
//   - 慢订阅者断开:fanOutLocked 关闭 sub.live,同阶段二路径退出。
//
// 任一出口都经 defer 做 mu 内 removeSub(断线清理,重复删除是 no-op)与
// close(ch)(消费方 range 到关闭即知断线,重连从游标补齐)。sub.live 的
// close 所有权只在 Close/fanOutLocked,pump 不碰。
//
// 断开感知(P3-4 TestSlowSubscriber):投递被消费方停读卡死(ch 满)时,
// 阻塞中的 select 必须仍能感知断开——sub.dead 是 close-only 信号,关闭后
// 接收立即就绪且不消耗数据(与「接收 live」的排干语义不同,见 hubSub 注释),
// 是被阻塞投递解锁的唯一干净出口。没有它,慢订阅者(订阅后不读)的 ch 永远
// 不会被关闭——断开的消费方可见信号(pump defer close(ch))无法送达。
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

// Close 停机:置 closed(此后 Publish/Subscribe 一律拒绝)→ 广播 done(唤醒
// 可能阻塞在阶段一 replay 投递上的 pump;阶段二 pump 由下一步的 sub.live 关闭
// 唤醒)→ 停窗定时器 → flush 残留合并缓冲 → 断开全部订阅者(delete+close
// (sub.live),与慢订阅者断开同一惯用法;P3-3 的 pump range sub.live 由此自然
// 退出)→ 关 journal 当前句柄。幂等(重复 Close 返回 nil)。
//
// 「关 journal 当前句柄」(plan P3-2 Close 语义):EventJournal 接口无 Close
// ——P3-1 的 RunJournalStore 自管句柄生命周期(终态 state 记录落盘时 Close+
// 逐出,journal_store.go 文件头),故只对实现了 io.Closer 的 journal 桥接关闭,
// RunJournalStore 不实现它,桥接为空操作。flush 失败仅 warn(降级语义)。
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

// fanOutLocked 非阻塞广播:对每个订阅者先按注册点水位过滤(P3-0 表 Race B 行
// 「只广播 seq>注册点 latest」——该不变量由 mu 内注册时序天然成立,显式 skip
// 是 belt-and-braces;watermark=0 的白盒订阅者不受影响,seq 恒 ≥1),再 select
// 直投,缓冲满即断开(delete+close)。断开语义:该订阅者此后收不到任何事件,
// 其 live 通道关闭(消费方 range 到关闭即知断线,重连从游标补齐属 P3-3 的
// Subscribe);不阻塞、不影响他人。调用方必须持 h.mu。
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

// flushDeltaTypeLocked 把一个缓冲桶合并为一条 EventRecord 落盘(Seq=ToSeq、
// FromSeq 声明 [FromSeq..Seq] 范围,payload 为 DeltaBatch JSON——窗内逐事件
// 的 {seq,payload} 按序保留,CR3-I1;回放侧展开为 per-seq 事件)。失败仅
// warn——重放缺 delta 段可接受,assistant.message_finalized 兜底定稿(降级
// 语义,文件头)。j==nil 不会到这(bufferDeltaLocked 仅在 j!=nil 时被调)。
// 调用方必须持 h.mu。
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
		// I4:合并 flush 失败置 Incomplete(粘滞永不复位)——重放缺 [FromSeq..
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
