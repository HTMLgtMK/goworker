package agentcontrol

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
)

// idempotency.go 是 Run 提交的幂等注册表(总方案 §4.3):从 plugin runcontrol.go
// 的 reqMu + runRequests + requestOrder 迁移,并按 §4.3 升级为**按 Key 的
// single-flight**——同 Key 并发提交只有一个底层 StartRun,等待同一结果;不同
// Key 不因本注册表的任何锁互相阻塞。

// ErrRequestConflict 是 requestId 幂等冲突 sentinel(I5;第二轮评审 I5 收窄到
// 会话作用域,原 runcontrol.go 迁入):同一幂等键(同 sessionID+requestID)重试
// 但 Input 指纹与首提不同——诚实拒绝,绝不静默复用原 Run 或另起新 run。
var ErrRequestConflict = errors.New("request id conflict: the idempotency key was already submitted with a different input")

// requestConflictErr 是幂等冲突的定位化包装(requestID 可观测)。
func requestConflictErr(requestID string) error {
	return fmt.Errorf("agent: requestID %q: %w", requestID, ErrRequestConflict)
}

// errFlightAborted 是 flight 的兜底失败态:leader 的底层提交 panic 等异常路径下,
// 等待者以明确错误唤醒而非挂死(正常路径该值必被提交结果覆盖)。
var errFlightAborted = errors.New("agentcontrol: request submission did not complete")

// maxTrackedRequestIDs 是 requestId 幂等表的容量上限(V1 进程内存量,环形逐出
// 防无界;审批 requestID 与 run/start requestID 是两个命名空间——§6 风险 6,
// 审批表在 SDK 的 ApprovalManager,互不共享)。原 runcontrol.go 迁入。
const maxTrackedRequestIDs = 4096

// RequestKey 是幂等存储键(§4.3 草案类型):(SessionID, RequestID) 组合寻址——
// 原全局 requestID 键在多会话路由下会跨会话错误复用(GPT 评审 I5 的「B 误返
// A 的 run」)。SessionID 取路由解析后的 rt.ID(runtime 身份,canonical):纯内存
// 降级下调用方传空串与传 "current" 寻址同一 runtime,必须折叠为同一键。
// RequestID 为空 = 无幂等键(每次新起跑,不记账)。struct 键取代迁移前的
// NUL 分隔字符串键(NUL 分隔的碰撞前提是 id 内含 NUL;struct 组合键把该前提
// 从「id 形态约定」收紧为类型系统事实)。
type RequestKey struct {
	SessionID string
	RequestID string
}

// trackedRun 是幂等表的一笔记录(I5,原 runcontrol.go 迁入):提交时返回的
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

// RequestRegistry 是 (sessionID, requestID) 组合键的幂等注册表,按 Key
// single-flight(§4.3 硬性要求「同一 RequestKey 并发提交必须只有一个底层
// StartRun,而不能先检查 Map、释放锁、再各自提交」):
//
//   - 表:map[RequestKey]trackedRun + 环形逐出队列(容量 4096,防无界),语义
//     原样迁移:命中同键,同 Input → 返回原 runID 与其当前状态快照,不重复
//     起跑;不同 Input → ErrRequestConflict 显式冲突;空 requestID 不记账;
//     进程内存量,重启清零,只提供进程内幂等(跨重启幂等需持久化该映射,
//     V1 不做——§4.3:不顺手塞进 Conversation Store)。
//
//   - single-flight 的正确性论证(为何不靠调用方的 fencing 串行):
//     1. 主路径:同别名同键的并发重试在本会话 lease(WithControl)内串行,
//     后到者查表必见首提记账——这与迁移前「guard 串行保证严格幂等」
//     (DE2-I1)一致,fencing 仍是「校验 + 提交」原子的第一道防线;
//     2. 收窄闭合:纯内存降级的跨别名形态(""与"current" 寻址同一 runtime,
//     各持各的租约,原 runcontrol.go 文件头文档化的已知收窄)不再被 lease
//     串行——迁移前后到者得 manager 的 ErrRunActive;迁移后两别名经
//     canonical rt.ID 键汇入同一 flight,joiner 等待同一结果,恰好满足
//     §4.3「重复请求等待同一个提交结果」,属方案要求的升级而非行为破坏
//     (幂等契约本就是「同键同 Input 返回原 run」);
//     3. 持久化模式会话 id 唯一 canonical,同键必经同会话 lease,行为与
//     迁移前零差异。
//
//   - 「不同 Key 不互相阻塞」的论证:registry.mu 只护表/flight 结构的读写
//     (查表/记账/登记,短临界区、无 I/O);leader 的底层提交(含 journal
//     fsync)在 registry.mu 之外执行——不同 Key 的提交段互不串行,跨会话独立
//     性由「每会话 lease + registry.mu 不覆盖提交段」联合保证(DE2-I1 语义在
//     注册表层同样成立,单一全局锁兜底的旧形态已废)。
//
//   - 与 lease 的锁序:调用方(Service)在 lease.mu 临界区内调 Submit,锁序
//     lease.mu→registry.mu→manager 单向;本注册表绝不回调任何取 lease.mu 的
//     代码(Joiner 等待的是 channel 而非锁),无环。
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

// Submit 按 (sessionID, requestID) 组合键提交一次底层 StartRun:
//   - key.RequestID 为空:无幂等键,直接执行 start(每次新起跑);
//   - 表命中:同 Input → 返回原 RunHandle(调用方取 RunID/State 快照),不重复
//     起跑;不同 Input → ErrRequestConflict 显式冲突,绝不静默复用;
//   - 在途命中(joiner):Input 与 leader 一致 → 等待并返回同一结果;不一致 →
//     立即冲突(leader 指纹在 flight 创建时定稿,无需等待);
//   - 否则成为 leader:在 registry.mu 之外执行 start(),完成后记账(仅成功)+
//     关闭 done 唤醒等待者。start() panic 等异常路径:flight 清除、等待者以
//     errFlightAborted 唤醒、panic 原样上抛——注册表不留孤儿 flight、等待者
//     不挂死(该形态与迁移前「guard 临界区内 panic 毒化 guard」的暴露面相当,
//     属进程级故障,不做恢复语义)。
//
// 被拒提交(错误路径)不入表;被拒的 joiner/leader 均不占记账。
func (r *RequestRegistry) Submit(key RequestKey, input string, start StartFunc) (runtimeagent.RunHandle, error) {
	if key.RequestID == "" {
		return start()
	}
	fp := inputFingerprint(input)

	r.mu.Lock()
	// ① 已记账:同 Input 幂等短路,不同 Input 显式冲突。
	if tr, ok := r.table[key]; ok {
		r.mu.Unlock()
		if tr.inputHash != fp {
			return runtimeagent.RunHandle{}, requestConflictErr(key.RequestID)
		}
		return tr.handle, nil
	}
	// ② 在途命中(joiner):指纹一致才等待,不一致立即冲突——等待窗口内 leader
	// 在 registry.mu 之外提交,本临界区不做任何等待。
	if f, ok := r.flights[key]; ok {
		if f.inputHash != fp {
			r.mu.Unlock()
			return runtimeagent.RunHandle{}, requestConflictErr(key.RequestID)
		}
		r.mu.Unlock()
		<-f.done
		if f.err != nil {
			return runtimeagent.RunHandle{}, f.err
		}
		return f.handle, nil
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

	h, err := start()
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
		return runtimeagent.RunHandle{}, err
	}
	return h, nil
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

// byRun 在幂等表中按 (sessionID, runID) 反查(原 lookupTrackedRun 迁入):纯内存
// 宿主(无 journal)下本表是进程内唯一 run 观测面,RunStatus 的兜底回退;会话
// 过滤防跨会话 runID 误配(C3 后 RunStatus 按会话路由,回退面同界)。持久化模式
// 不走此处(journal 权威,覆盖包括无 requestID 同步 run 在内的全部 run——该
// 优先级由 Service.RunStatus 的分支序承担)。
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

// inputFingerprint 计算 input 的 sha256 指纹(I5,原 runcontrol.go 迁入)。
func inputFingerprint(input string) [sha256.Size]byte {
	return sha256.Sum256([]byte(input))
}
