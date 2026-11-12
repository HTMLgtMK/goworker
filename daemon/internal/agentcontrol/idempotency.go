package agentcontrol

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
)

// idempotency.go 是 Run 提交的幂等注册表:**按 Key 的 single-flight**——同 Key
// 并发提交只有一个底层 StartRun,等待同一结果;不同 Key 不因本注册表的任何锁
// 互相阻塞。

// ErrRequestConflict 是 requestId 幂等冲突 sentinel(会话作用域):同一幂等键
// (同 sessionID+requestID)重试但 Input 指纹与首提不同——诚实拒绝,绝不静默
// 复用原 Run 或另起新 run。
var ErrRequestConflict = errors.New("request id conflict: the idempotency key was already submitted with a different input")

// requestConflictErr 是幂等冲突的定位化包装(requestID 可观测)。
func requestConflictErr(requestID string) error {
	return fmt.Errorf("agent: requestID %q: %w", requestID, ErrRequestConflict)
}

// errFlightAborted 是 flight 的兜底失败态:leader 的底层提交 panic 等异常路径下,
// 等待者以明确错误唤醒而非挂死(正常路径该值必被提交结果覆盖)。
var errFlightAborted = errors.New("agentcontrol: request submission did not complete")

// maxTrackedRequestIDs 是 requestId 幂等表的容量上限(V1 进程内存量,环形逐出
// 防无界;审批 requestID 与 run/start requestID 是两个命名空间,审批表在 SDK
// 的 ApprovalManager,互不共享)。
const maxTrackedRequestIDs = 4096

// RequestKey scopes an idempotency request to a runtime session. Callers use the
// runtime ID so aliases such as an empty ID and "current" resolve to the same key;
// an empty RequestID disables idempotency.
type RequestKey struct {
	SessionID string
	RequestID string
}

// trackedRun 是幂等表的一笔记录:提交时返回的
// RunHandle + input sha256 指纹(同键重试的 Input 一致性判定依据,表内不存
// 明文输入)。RunHandle 供 State 快照(幂等重试返回原 runID 与其当前状态,
// 终态保留)与 (sessionID, runID) 反查。
type trackedRun struct {
	handle    runtimeagent.RunHandle
	inputHash [sha256.Size]byte
}

// flight 是一个幂等键的在途提交(per-key single-flight):首个到达者成为 leader
// 执行底层提交,并发到达的同键等待者在 done 上等待同一结果。inputHash 在
// flight 创建时定稿(leader 的 Input 指纹),joiner 据此在等待之前即可判定
// 冲突(不同 Input 的并发重试不等 leader 完成即被拒绝)。
type flight struct {
	inputHash [sha256.Size]byte
	done      chan struct{}
	handle    runtimeagent.RunHandle
	err       error
}

// StartFunc 是 leader 执行的底层提交闭包:一次 manager.StartRun 的同步段(含
// journal fsync 的 durable-first 接受记录)。同一幂等键的并发提交中恰好一个
// 闭包被调用;闭包返回的 RunHandle 必须来自真实提交(本注册表只做簿记,不做
// State 快照——由 Service 从句柄取状态,句柄无效的形态在真实提交路径不存在)。
type StartFunc func() (runtimeagent.RunHandle, error)

// RequestRegistry deduplicates requests per session and merges concurrent submissions
// for the same key. It stores handles only up to a bounded capacity; evicted or
// post-restart keys no longer provide an idempotency guarantee. Submissions run outside
// registry.mu, and same-key waiters never hold that lock while waiting.
type RequestRegistry struct {
	mu sync.Mutex
	// table 是已记账的幂等键 → 提交结果;order 是环形逐出队列(与 table 同键
	// 集,先入先出)。
	table map[RequestKey]trackedRun
	order []RequestKey
	// flights 是在途提交的 per-key 状态:提交完成后(成功或失败)移除,不泄漏;
	// 失败不入 table,重试重新成为 leader(与迁移前「被拒提交不入表」一致)。
	flights map[RequestKey]*flight
}

// NewRequestRegistry 构造空注册表(零值亦可惰性工作,见 Submit/recordLocked)。
func NewRequestRegistry() *RequestRegistry {
	return &RequestRegistry{
		table:   make(map[RequestKey]trackedRun),
		flights: make(map[RequestKey]*flight),
	}
}

// Submit deduplicates by (sessionID, requestID); empty RequestID bypasses the registry.
// Matching concurrent callers share one leader's result; different inputs conflict.
// A panic clears the flight, wakes waiters with errFlightAborted, then propagates.
func (r *RequestRegistry) Submit(key RequestKey, input string, start StartFunc) (h runtimeagent.RunHandle, fresh bool, err error) {
	if key.RequestID == "" {
		h, err = start()
		return h, true, err
	}
	fp := inputFingerprint(input)

	r.mu.Lock()
	// ① 已记账:同 Input 幂等短路,不同 Input 显式冲突。
	if tr, ok := r.table[key]; ok {
		r.mu.Unlock()
		if tr.inputHash != fp {
			return runtimeagent.RunHandle{}, false, requestConflictErr(key.RequestID)
		}
		return tr.handle, false, nil
	}
	// ② 在途命中(joiner):指纹一致才等待,不一致立即冲突——等待窗口内 leader
	// 在 registry.mu 之外提交,本临界区不做任何等待。
	if f, ok := r.flights[key]; ok {
		if f.inputHash != fp {
			r.mu.Unlock()
			return runtimeagent.RunHandle{}, false, requestConflictErr(key.RequestID)
		}
		r.mu.Unlock()
		<-f.done
		if f.err != nil {
			return runtimeagent.RunHandle{}, false, f.err
		}
		return f.handle, false, nil
	}
	// ③ 成为 leader:登记 flight 后在 registry.mu 之外执行底层提交(不同 Key
	// 的提交段互不阻塞的落点)。
	f := &flight{inputHash: fp, done: make(chan struct{}), err: errFlightAborted}
	if r.flights == nil {
		r.flights = make(map[RequestKey]*flight)
	}
	r.flights[key] = f
	r.mu.Unlock()

	completed := false
	defer func() {
		if !completed {
			// start() panic 等异常路径:清 flight + 唤醒等待者(errFlightAborted
			// 已在创建时置位),panic 原样上抛。
			r.mu.Lock()
			delete(r.flights, key)
			r.mu.Unlock()
			close(f.done)
		}
	}()

	h, err = start()
	r.mu.Lock()
	delete(r.flights, key)
	if err == nil {
		r.recordLocked(key, h, fp)
	}
	r.mu.Unlock()
	// 先写结果后关 done:等待者经 close 的 happens-before 读到完整结果。
	f.handle, f.err = h, err
	close(f.done)
	completed = true

	if err != nil {
		return runtimeagent.RunHandle{}, true, err
	}
	return h, true, nil
}

// recordLocked 记录一笔幂等键 → RunHandle + input 指纹,并按容量环形逐出最老
// 条目(调用方必须持 r.mu)。同键重复记录只刷新句柄与指纹、不重复入队(幂等表
// 的保守自愈;正常路径 put 前已查表,不可达)。
func (r *RequestRegistry) recordLocked(key RequestKey, h runtimeagent.RunHandle, fp [sha256.Size]byte) {
	if r.table == nil {
		r.table = make(map[RequestKey]trackedRun)
	}
	if _, ok := r.table[key]; !ok {
		r.order = append(r.order, key)
		for len(r.order) > maxTrackedRequestIDs {
			oldest := r.order[0]
			delete(r.table, oldest)
			// 环形逐出:copy-shift O(容量),仅在表满后的每次插入发生——
			// 4096 条的量级下代价可忽略,换无别名滚动的简单性(原实现同款)。
			copy(r.order, r.order[1:])
			r.order = r.order[:len(r.order)-1]
		}
	}
	r.table[key] = trackedRun{handle: h, inputHash: fp}
}

// Tracked 返回 (sessionID, requestID) 的已记账句柄(幂等表按键直查的观测面:
// 「按 requestId 观测」与测试断言「拒绝路径不入表」消费)。ok=false = 该键无
// 记账(含空 requestID——空键从不记账)。
func (r *RequestRegistry) Tracked(sessionID, requestID string) (runtimeagent.RunHandle, bool) {
	if requestID == "" {
		return runtimeagent.RunHandle{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	tr, ok := r.table[RequestKey{SessionID: sessionID, RequestID: requestID}]
	if !ok {
		return runtimeagent.RunHandle{}, false
	}
	return tr.handle, true
}

// byRun finds a tracked handle by (sessionID, runID). On journal-less in-memory hosts it is
// RunStatus's fallback; session filtering prevents cross-session mismatches. Persistent
// hosts always prefer the authoritative journal, including synchronous runs without request IDs.
func (r *RequestRegistry) byRun(sessionID, runID string) (runtimeagent.RunHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, tr := range r.table {
		if tr.handle.SessionID == sessionID && tr.handle.RunID == runID {
			return tr.handle, true
		}
	}
	return runtimeagent.RunHandle{}, false
}

// inputFingerprint 计算 input 的 sha256 指纹。
func inputFingerprint(input string) [sha256.Size]byte {
	return sha256.Sum256([]byte(input))
}
