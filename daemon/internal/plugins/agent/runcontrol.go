package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// runcontrol.go 是 RunControlSource(P5-6)的 AgentPlugin 实现面:x-goworker 扩展
// 协议(P5-7/P5-8 接线)消费的后台 Run 控制、审批直转、事件 attach 与 control
// lease。放在独立文件:plugin.go 只持有会话状态与生命周期,本文件是「后台 Run 的
// daemon 面」——两个入口(同步 /agent 与后台 run/start)靠 runtime gate 互斥
// (P2P3 §6,拍板⑦:同步面不切 StartRun)。
//
// 行为变化声明(P5-6):零——接口与实现新增,无协议暴露(StartRun 仍无前端调用
// 方,P5-7 才注册 x-goworker 方法);同步 RecordRun 路径完全不经此处。
//
// 并发与锁论证:
//   - reqMu 只护 requestId 幂等表(map + 环形逐出队列);临界区覆盖
//     manager.StartRun 同步段(μs~ms 级,含一次 journal fsync)——同会话第二
//     run 本就被 manager 的 ErrRunActive 串行,跨会话并发提交亦被 reqMu 串行
//     化(C3 后多会话路由可并发到达),持锁串行的额外代价可忽略,换来同
//     requestID 并发重试的严格幂等(第二个等待者持锁后命中表,不再撞
//     ErrRunActive);
//   - leaseMu 只护 controlLeases 租约表(sessionID → connectionID,I3 修正后的
//     per-session control lease);不与本插件 mu/reqMu/manager 锁嵌套,无锁环。
//     跨层嵌套唯一一处:daemon 的 xgw 面(I2 生命周期屏障)在 handler 的 h.mu
//     临界区内调用 Acquire/ReleaseControl——锁序 h.mu→leaseMu 单向(leaseMu
//     是叶锁,绝不回调 handler、不取 h.mu),Close 侧同向,无环;
//   - 其余方法(manager.StartRun/CancelRun、rt.Active/Hub、Approvals、
//     Capabilities)的并发面由 SDK 侧既有锁序承担,本层不做新共享状态。

// RunControlSource 编译期断言(P5-6):AgentPlugin 满足可选能力接口,handler
// 以 type-assert 获取(SessionSource 先例,capability.go:61-78)。
var _ model.RunControlSource = (*AgentPlugin)(nil)

// ClientToolBinder 编译期断言(P5-9):AgentPlugin 同时满足设备工具的能力代理
// 面,ACP handler 优先 type-assert 它走 CapabilityRegistry + proxy(P5 显式
// 可见变化 #1 的生产者),断言失败回落原始 relay。
var _ model.ClientToolBinder = (*AgentPlugin)(nil)

// maxTrackedRequestIDs 是 requestId 幂等表的容量上限(V1 进程内存量,环形逐出
// 防无界;审批 requestID 与 run/start requestID 是两个命名空间——§6 风险 6,
// 审批表在 SDK 的 ApprovalManager,互不共享)。
const maxTrackedRequestIDs = 4096

// defaultCapabilityWaitTimeout 是 CapabilityWaitTimeout 的兜底值(拍板③ 45s;
// 与 SDK 的 defaultCapabilityWaitTimeout 同值——那边未导出,daemon 侧按配置
// 秒化后 <=0 回落此处,双保险不依赖 yaml 载入路径)。
const defaultCapabilityWaitTimeout = 45 * time.Second

// maxCapabilityWaitTimeout 是 CapabilityWaitTimeout 的安全钳制上限(P4 评审
// issue2,与 SDK 侧 resolveCapabilityWait 的钳制同型:硬约束 < ai-core 工具
// 执行 60s——55s 给 Invoke 留余量;超配的等待必被 toolCtx 掐断,「等待重绑→
// 明确失败文本」语义无声劣化,故显式降级 + warn)。
const maxCapabilityWaitTimeout = 55 * time.Second

// sessionRuntime 是 C3 修复后的多会话路由解析点(GPT 评审 C3:run/start|status|
// cancel、attach、approval/list 与 ActiveRun(第二轮评审 I1 修正)不再限
// current,推翻 P4P5 plan 拍板⑤的
// V1-current-only——多 Session 控制面是总方案的最终目标):按 sessionID 定位
// runtime,nil → 调用方报 sessionNotFoundErr(ErrSessionNotFound 语义;/new 已
// 收口的旧 id 与任意未注册 id 在此诚实失败,归档历史走既有 session/load 只读
// 重放)。
//
// 解析序:
//  1. current 优先:sessionID 命中当前会话的身份(runtime id,含纯内存降级的
//     空串形态)→ 取 current runtime——同 id 重建窗口(纯内存 /new、测试
//     fixture 的 refreshSession)下注册表可能仍挂旧条目,current 才是权威路由
//     目标,registerCurrentRuntime 随后把注册表拉齐。持久化模式下空串不匹配
//     任何 store id → 落到第 2 步 → nil(诚实失败,不把「未声明」静默当成
//     current)。
//  2. manager.Get(sessionID):注册表权威,覆盖非 current 的 runtime(/new 后
//     保留在册的旧 runtime、多会话注册)。
func (p *AgentPlugin) sessionRuntime(sessionID string) *runtimeagent.SessionRuntime {
	if cur := p.currentSession(); cur != nil {
		if sessionID == cur.ID || (sessionID == "" && p.CurrentSessionID() == "") {
			return cur
		}
	}
	return p.manager.Get(sessionID)
}

// sessionNotFoundErr 是 C3 多会话路由下「sessionID 未定位到 runtime」的统一
// 错误:包裹 manager 的 ErrSessionNotFound sentinel(errors.Is 可判,P5-7 协议
// 面映射 -32000)。覆盖三种形态:任意未注册 id、/new 已收口的旧 id(诚实失败)、
// 无活动 runtime(Init 未完成/失败回滚——原「no active session」并入同一语义,
// 路由面 nil 即 not found)。
func sessionNotFoundErr(sessionID string) error {
	return fmt.Errorf("agent: session %q: %w", sessionID, runtimeagent.ErrSessionNotFound)
}

// sessionJournalStore 返回 rt 会话的 Run journal 只读查询面(C3:RunStatus 的
// journal 权威分支必须用被路由会话自己的 journal——p.deps 是当前会话边界重建
// 的 deps,对跨会话路由的 runtime 是错误事实源)。解析:
//   - rt 即 current runtime → p.runJournalStore()(deps.RunEvents type-assert,
//     与该会话同源的单例,写面句柄缓存单例语义不受影响);
//   - 跨会话路由的 runtime → repo.Open(rt.ID) 命中实例唯一缓存(会话边界打开
//     的 store 在 Stop 前常驻缓存,plugin.go),构造只读查询实例——LastRunState/
//     SeqRange 是纯磁盘只读扫描(journal_store.go 读路径不触 handles 锁),第二
//     实例不参与写面、不双开句柄。
//
// 解析失败(纯内存/持久化未启用/仓库已收尾)→ nil,调用方按各自降级分支处理
// (RunStatus 回落幂等表反查或 run-not-found)。
func (p *AgentPlugin) sessionJournalStore(rt *runtimeagent.SessionRuntime) *session.RunJournalStore {
	if p.currentSession() == rt {
		return p.runJournalStore()
	}
	if p.repo == nil {
		return nil
	}
	st, err := p.repo.Open(rt.ID)
	if err != nil {
		return nil
	}
	return session.NewRunJournalStore(p.repo, st)
}

// ErrRequestConflict 是 requestId 幂等冲突 sentinel(I5;第二轮评审 I5 收窄到
// 会话作用域):同一幂等键(同 sessionID+requestID)重试但 Input 指纹与首提
// 不同——诚实拒绝,绝不静默复用原 Run 或另起新 run。
var ErrRequestConflict = errors.New("request id conflict: the idempotency key was already submitted with a different input")

// ErrApprovalOwnership 是审批越权 sentinel(第二轮评审 C1 修复):决策声明的
// sessionID 与服务端记录的审批归属会话不一致。授权以服务端记录为准、绝不信任
// 客户端声明——归属在 SDK 审批管理面 Wait 注册时落账(SessionID+RunID 非连接),
// 即便调用方真持有其他会话的 control lease 也不放行(跨会话审批越权是本轮
// C1 的攻击面:持有任意会话 lease 的连接曾可应答他会话的审批)。
var ErrApprovalOwnership = errors.New("approval ownership mismatch: the approval belongs to a different session")

func requestConflictErr(requestID string) error {
	return fmt.Errorf("agent: requestID %q: %w", requestID, ErrRequestConflict)
}

// trackedRun 是幂等表的一笔记录(I5):提交时返回的 RunHandle + input sha256
// 指纹(同键重试的 Input 一致性判定依据,表内不存明文输入)。
type trackedRun struct {
	handle    runtimeagent.RunHandle
	inputHash [sha256.Size]byte
}

// requestKey 构造幂等键(I5):(sessionID, requestID) 组合,NUL 分隔——原全局
// requestID 键在多会话路由下会跨会话错误复用(GPT 评审 I5 的「B 误返 A 的
// run」)。键的 sessionID 取路由解析后的 rt.ID(runtime 身份,canonical):纯
// 内存降级下调用方传空串与传 "current" 寻址同一 runtime,必须折叠为同一键。
// NUL 分隔的键空间碰撞前提是 id 内含 NUL,会话/请求 id 的既有形态(store id、
// crypto/rand hex)不含,组合键按此前提成立。
func requestKey(sessionID, requestID string) string {
	return sessionID + "\x00" + requestID
}

// inputFingerprint 计算 input 的 sha256 指纹(I5)。
func inputFingerprint(input string) [sha256.Size]byte {
	return sha256.Sum256([]byte(input))
}

// registerCurrentRuntime 修复「current runtime 已注册」不变量(P5-6;/new 后的
// 注册表形态处理,P5-5 报备):纯内存降级下 /new 的新旧 runtime 同 id,startSession
// 的 Register 撞旧条目保留旧值,handleNew 尾部 Remove 移除的正是旧条目——新
// runtime 此刻不在册(Get == nil);经同 id 重建的路径(如测试 fixture 的
// refreshSession)则可能有同 id 旧条目占位(Get != rt)。两者都违反不变量,
// Remove+Register 幂等重建。Register 的错误不在此放大:唯一可达失败是
// ErrManagerClosed,后续 manager.StartRun 会给出同一权威错误。
func (p *AgentPlugin) registerCurrentRuntime(rt *runtimeagent.SessionRuntime) {
	if got := p.manager.Get(rt.ID); got != rt {
		p.manager.Remove(rt.ID)
		_ = p.manager.Register(rt)
	}
}

// runJournalStore 返回当前会话的 Run journal 聚合实例:从 p.deps.RunEvents
// type-assert(P5-4 起 RunEvents 与 RunJournal 注入同一个 *session.RunJournalStore
// 实例,runjournal_wiring_test.go 编译期+运行期钉死)。这是「最简形态」取舍:
// p.deps 在每个会话边界(startSession)全量重建,恒与 current store 同源,无需
// 经 repo+currentStore 重建第二个实例(那会绕开句柄缓存的单例语义)。跨会话
// 路由(C3)的只读查询面见 sessionJournalStore——非 current 会话才按 repo 只读
// 实例解析,本函数仍是 current 会话的单例取数点。nil = 纯内存/持久化禁用
// (typed-nil 纪律,spec.go:96),调用方按 run-not-found 处理。
func (p *AgentPlugin) runJournalStore() *session.RunJournalStore {
	if js, ok := p.deps.RunEvents.(*session.RunJournalStore); ok {
		return js
	}
	return nil
}

// StartRun 实现 RunControlSource(P5-6;C3/I5 修正):多会话路由 → 幂等表命中
// 短路/冲突拒绝 → 注册表不变量修复 → 空回调后台提交。
//
//   - 路由(C3):sessionRuntime 按 sessionID 定位 runtime(不限 current),
//     StartRun 用该 rt——其 Session 自带本会话边界注入的 deps,journal/hub/
//     conversation 天然按会话隔离;nil → sessionNotFoundErr。
//   - 幂等(I5;第二轮评审 I5 收窄):requestID 非空时,幂等键是 (sessionID,
//     requestID) 组合(requestKey),表存提交时 RunHandle + input sha256 指纹。
//     命中同键:同 Input → 返回原 runID 与其当前状态(RunHandle.State() 的实时
//     快照,终态保留),不重复起跑;不同 Input → ErrRequestConflict 显式冲突,
//     绝不静默复用。requestID 的唯一性只在同一会话的幂等作用域内成立——跨会话
//     冲突扫描已删除(第二轮评审 I5:两客户端各自局部递增 requestID 不再互相
//     拒绝;组合键本身已防「B 误返 A 的 run」)。空 requestID 不记账(幂等键
//     缺失,每次新起跑)。命中/冲突判定与记录同持 reqMu,并发同 requestID 重试
//     严格幂等(见文件头)。边界:本表是进程内存量,重启清零,只提供进程内幂
//     等;跨重启幂等需持久化该映射(V1 不做)。
//   - 回调全 nil(runtimeagent.RunCallbacks{}):事件采集由 supervisor 的 tee
//     自行供给(C2 修复:teeRunCallbacks 在 hub 非 nil 时对 nil 的 EmitToken/
//     Publish 装上事件映射版,不再依赖调用方回调)——真实 token/tool_call/最终
//     消息与 usage 类事件照常进事件流/journal,后台 Run 的输出不进任何终端;
//     Decide 留 nil → P4-2 的审批回退接管(decideFunc → ApprovalManager.Wait),
//     决策经 ResolveApproval 跨连接投递。
//   - 返回的 state 是提交时快照(manager.StartRun 同步段落盘 queued 记录后
//     durable-first 返回,恒为 queued)。
func (p *AgentPlugin) StartRun(sessionID, input, requestID string) (string, string, error) {
	rt := p.sessionRuntime(sessionID)
	if rt == nil {
		return "", "", sessionNotFoundErr(sessionID)
	}

	p.reqMu.Lock()
	defer p.reqMu.Unlock()

	var (
		fp  [sha256.Size]byte
		key string
	)
	if requestID != "" {
		fp = inputFingerprint(input)
		key = requestKey(rt.ID, requestID)
		if tr, ok := p.runRequests[key]; ok {
			if tr.inputHash != fp {
				return "", "", requestConflictErr(requestID)
			}
			return tr.handle.RunID, string(tr.handle.State()), nil
		}
	}

	p.registerCurrentRuntime(rt)
	h, err := p.manager.StartRun(rt.ID, runtimeagent.RunRequest{Input: input}, runtimeagent.RunCallbacks{})
	if err != nil {
		return "", "", err
	}
	if requestID != "" {
		p.recordRequestID(key, h, fp)
	}
	return h.RunID, string(h.State()), nil
}

// recordRequestID 记录一笔幂等键 → RunHandle + input 指纹,并按容量环形逐出最
// 老条目(调用方必须持 reqMu)。同键重复记录只刷新句柄与指纹、不重复入队(幂等
// 表的保守自愈;正常路径 put 前已查表,不可达)。
func (p *AgentPlugin) recordRequestID(key string, h runtimeagent.RunHandle, fp [sha256.Size]byte) {
	if p.runRequests == nil {
		p.runRequests = make(map[string]trackedRun)
	}
	if _, ok := p.runRequests[key]; !ok {
		p.requestOrder = append(p.requestOrder, key)
		for len(p.requestOrder) > maxTrackedRequestIDs {
			oldest := p.requestOrder[0]
			delete(p.runRequests, oldest)
			// 环形逐出:copy-shift O(容量),仅在表满后的每次插入发生——
			// 4096 条的量级下代价可忽略,换无别名滚动的简单性。
			copy(p.requestOrder, p.requestOrder[1:])
			p.requestOrder = p.requestOrder[:len(p.requestOrder)-1]
		}
	}
	p.runRequests[key] = trackedRun{handle: h, inputHash: fp}
}

// lookupTrackedRun 在幂等表中按 (sessionID, runID) 反查(调用方必须持 reqMu):
// 纯内存宿主(无 journal)下本表是进程内唯一 run 观测面,RunStatus 的兜底回退;
// 会话过滤防跨会话 runID 误配(C3 后 RunStatus 按会话路由,回退面同界)。持久
// 化模式不走此处(journal 权威,覆盖包括无 requestID 同步 run 在内的全部 run)。
func (p *AgentPlugin) lookupTrackedRun(sessionID, runID string) (runtimeagent.RunHandle, bool) {
	for _, tr := range p.runRequests {
		if tr.handle.SessionID == sessionID && tr.handle.RunID == runID {
			return tr.handle, true
		}
	}
	return runtimeagent.RunHandle{}, false
}

// CancelRun 实现 RunControlSource(P5-6;C3 修正):多会话路由 → 不变量修复 →
// 直转 manager。manager 的拒绝语义原样上抛:未知 run → ErrRunNotFound;同步
// RecordRun 路径的 run 无 cancel → ErrRunNotCancellable(其取消入口是调用方
// 请求 ctx,P2-6 契约)。不变量修复的必要性:current 会话的 runtime 在同 id
// 重建窗口(纯内存 /new)下可能未在册,同步 run 活跃其上时不经修复会被误报
// ErrRunNotFound;跨会话路由的 runtime 本就来自注册表,修复为幂等空操作。
func (p *AgentPlugin) CancelRun(sessionID, runID string) error {
	rt := p.sessionRuntime(sessionID)
	if rt == nil {
		return sessionNotFoundErr(sessionID)
	}
	p.registerCurrentRuntime(rt)
	return p.manager.CancelRun(rt.ID, runID)
}

// RunStatus 实现 RunControlSource(P5-6;C3/I6lite 修正,I4 起完整性字段改三态
// replayIntegrity):多会话路由 → run 当前状态 + 该会话事件流 latest seq +
// 回放完整性三态。观测面三分支,按权威序:
//  1. rt.Active() 命中 runID(在跑/排队)→ AgentRun.State() 实时快照;latestSeq
//     取 hub.LatestSeq()(会话维度游标,P3-0 契约);
//  2. 该会话自己的 RunJournalStore.LastRunState(已终态,active 已清):journal
//     权威(sessionJournalStore:current 会话走 deps 单例,跨会话路由的会话走
//     repo 只读实例——journal 必须取被路由会话的事实源,不能拿 p.deps 的当前
//     会话 journal 冒充),读 runs/<runID>.jsonl 末条 state 记录;latestSeq 取
//     SeqRange 的磁盘 latest(hub 可能尚未经 StartRun 建立——只有同步 run 的
//     会话即此形态);
//  3. 幂等表反查(仅纯内存):无 journal 时本插件经 StartRun 记账的 RunHandle
//     是唯一观测面(按路由会话过滤);持久化模式不回退——journal 覆盖本会话
//     全部 run,miss 即未找到,不发明第二事实源。
//
// replayIntegrity(I4)三分支同源:rt.Hub() 存在时取 hub.ReplayIntegrity()
// (三态:本进程纪元内任何 key 事件落盘失败/合并 delta flush 失败 → Incomplete,
// 粘滞永不复位——hub 是会话级单例,标记覆盖其上产生的全部 run,不只被查询的
// 这一个;journal 播种发现既有历史 → Unknown,跨进程纪元的完整性不可验证,
// 诚实优先;空历史/纯内存 → Complete);hub 为 nil(未启用 RunEvents/尚无
// StartRun)→ Complete——无事件流即无「不完整回放」可言,该取值只携带失败
// 证据、不发明完整性承诺(I4 前身 I6lite 的同款语义,仅布尔改三态)。
//
// 未命中 → 包裹 runtimeagent.ErrRunNotFound 的错误(P5-7 协议面映射 -32000)。
func (p *AgentPlugin) RunStatus(sessionID, runID string) (string, uint64, string, error) {
	rt := p.sessionRuntime(sessionID)
	if rt == nil {
		return "", 0, runtimeagent.ReplayIntegrityComplete, sessionNotFoundErr(sessionID)
	}

	replayIntegrity := runtimeagent.ReplayIntegrityComplete
	if hub := rt.Hub(); hub != nil {
		replayIntegrity = hub.ReplayIntegrity()
	}

	if run := rt.Active(); run != nil && run.ID == runID {
		latest := uint64(0)
		if hub := rt.Hub(); hub != nil {
			latest = hub.LatestSeq()
		}
		return string(run.State()), latest, replayIntegrity, nil
	}

	if js := p.sessionJournalStore(rt); js != nil {
		rec, ok := js.LastRunState(runID)
		if !ok {
			return "", 0, replayIntegrity, fmt.Errorf("agent: run %q: %w", runID, runtimeagent.ErrRunNotFound)
		}
		_, latest, _ := js.SeqRange()
		return rec.State, latest, replayIntegrity, nil
	}

	p.reqMu.Lock()
	h, ok := p.lookupTrackedRun(rt.ID, runID)
	p.reqMu.Unlock()
	if ok {
		latest := uint64(0)
		if hub := rt.Hub(); hub != nil {
			latest = hub.LatestSeq()
		}
		return string(h.State()), latest, replayIntegrity, nil
	}
	return "", 0, replayIntegrity, fmt.Errorf("agent: run %q: %w", runID, runtimeagent.ErrRunNotFound)
}

// PendingApprovals 实现 RunControlSource(P5-6;C3 修正):多会话路由 → manager
// 审批快照 → 扁平化为 model.PendingApprovalInfo。内部寻址用 rt.ID 而非调用方
// sessionID:审批的 SessionID 归属是 StartRun 传入的 runtime id(纯内存下为
// "current" 而调用方寻址空串),按 rt.ID 寻址在两种形态下都正确。未知会话在
// 路由层已报 sessionNotFoundErr;已注册会话无审批返回空切片(manager.
// PendingApprovals 语义)。
func (p *AgentPlugin) PendingApprovals(sessionID string) ([]model.PendingApprovalInfo, error) {
	rt := p.sessionRuntime(sessionID)
	if rt == nil {
		return nil, sessionNotFoundErr(sessionID)
	}
	return flattenPendingApprovals(p.manager.PendingApprovals(rt.ID)), nil
}

// flattenPendingApprovals 把 agent 包的 PendingApproval 快照压成 daemon 侧扁平
// 结构(wire 最小面,见 model.PendingApprovalInfo 注释;manager 已按 RequestID
// 排序,顺序保留)。
func flattenPendingApprovals(pending []runtimeagent.PendingApproval) []model.PendingApprovalInfo {
	out := make([]model.PendingApprovalInfo, 0, len(pending))
	for _, pa := range pending {
		out = append(out, model.PendingApprovalInfo{
			RequestID: pa.RequestID,
			SessionID: pa.SessionID,
			RunID:     pa.RunID,
			ToolName:  pa.Request.ToolName,
			Command:   pa.Request.Command,
			RiskLevel: pa.Request.RiskLevel,
			ExpiresAt: pa.Request.ExpiresAt,
		})
	}
	return out
}

// ResolveApproval 实现 RunControlSource(P5-6;C1 修复:审批归属三段校验,推翻
// 旧契约「requestID 全局寻址、无需会话校验」——control lease 已按会话隔离
// (I3),审批授权必须绑定目标会话,否则持任意会话 lease 的连接可应答他会话
// 的审批):
//  1. 路由:sessionRuntime 按 sessionID 定位 runtime(与 PendingApprovals 同一
//     寻址模型:内部比较用 rt.ID 而非调用方原串——审批的 SessionID 归属是
//     StartRun 传入的 runtime id,纯内存降级下为 "current" 而调用方寻址空串),
//     nil → sessionNotFoundErr;
//  2. 归属:manager.ApprovalOwner(requestID) 查服务端记录的真实归属,不存在
//     (从未注册/已过宽限窗被清扫)→ 包裹 ErrApprovalUnknown 的明确错误;
//  3. 越权:归属会话 != rt.ID → 包裹 ErrApprovalOwnership 的越权错误(「服务端
//     记录为准,不信客户端声明」);
//  4. 匹配 → 直转 manager.ResolveApproval:首个有效决策获胜,迟到/重复 →
//     ErrApprovalResolved(sentinel 语义见 approval.go),错误原样上抛由协议面
//     映射 -32000。
func (p *AgentPlugin) ResolveApproval(sessionID, requestID string, decision hitl.Decision) error {
	rt := p.sessionRuntime(sessionID)
	if rt == nil {
		return sessionNotFoundErr(sessionID)
	}
	owner, ok := p.manager.ApprovalOwner(requestID)
	if !ok {
		return fmt.Errorf("agent: approval %q: %w", requestID, runtimeagent.ErrApprovalUnknown)
	}
	if owner != rt.ID {
		return fmt.Errorf("agent: approval %q belongs to session %q, not %q: %w", requestID, owner, rt.ID, ErrApprovalOwnership)
	}
	return p.manager.ResolveApproval(requestID, decision)
}

// Attach 实现 RunControlSource(P5-6,P5-8 的 attach 面消费;C3 修正):多会话
// 路由 → rt.EnsureHub()——遗留③「重启后 attach 时 hub 未建」的解法,attach
// 即建(含 SeqRange 磁盘播种,游标续接既有历史;hub 由被路由会话自己的
// deps.RunEvents 供给,事件流天然按会话隔离);RunEvents 未接线/纯内存 →
// 诚实错误(P5-8 起映射 -32000,客户端指引 session/load)。latest 是当前事件
// 流位置:仅描述,客户端不得跳游标(总方案 §7 :586-589,P5-10 写入 PROTOCOL.md)。
func (p *AgentPlugin) Attach(sessionID string) (*runtimeagent.EventHub, uint64, error) {
	rt := p.sessionRuntime(sessionID)
	if rt == nil {
		return nil, 0, sessionNotFoundErr(sessionID)
	}
	hub := rt.EnsureHub()
	if hub == nil {
		return nil, 0, fmt.Errorf("agent: event stream not enabled for session %q (persistence disabled)", sessionID)
	}
	return hub, hub.LatestSeq(), nil
}

// ActiveRun 实现 RunControlSource(P5-8;第二轮评审 I1 修正):sessionRuntime
// 多会话路由 → rt.Active() 快照直转,与 RunStatus 的 active 分支同源(同一观察
// 点)。语义:无 runtime(未注册//new 已收口)或无活动 run → ok=false——attach
// 响应的 currentRun=null 不是错误。I1 修正前这里经 requireCurrentSession 把非
// current 会话一律报 ok=false:attach 已按会话路由成功,响应却 currentRun=null,
// 与「该会话确有在途 run」的事实不符(重连客户端靠该快照立即得知在途 Run);
// 修正后与 run/start|status|cancel、attach、approval/list 同一身份模型——全部
// 路由入口经 sessionRuntime 定位 runtime,不再有 current-only 例外(纯内存降级
// 下空串寻址回落 current runtime,见 sessionRuntime 第 1 步)。本访问器只做
// 快照观察,Attach 才是会话路由的权威校验面。
func (p *AgentPlugin) ActiveRun(sessionID string) (string, string, bool) {
	rt := p.sessionRuntime(sessionID)
	if rt == nil {
		return "", "", false
	}
	if run := rt.Active(); run != nil {
		return run.ID, string(run.State()), true
	}
	return "", "", false
}

// AcquireControl 实现 RunControlSource 的 per-session control lease(P5-6,
// V1 宿主授权简化形,拍板②原文「每会话至多一个 control 持有者」;I3 修正:
// 全局单字符串 → 会话维度租约表,两连接各控各会话互不影响,接管只发生在
// 同一会话上):该会话空闲 → 占用并返回 false;他人持有 → 显式接管(置新
// holder,返回 true 并 slog.Info 审计——「权限检查必须在 Daemon 侧完成」,
// 总方案 §7.4 :610);同持有者重复 acquire → false(非接管)。空 connectionID
// 拒绝(P5-7 工厂恒发非空 crypto/rand hex;防御空串与「空闲」不可辨)。
// sessionID 允许空串:纯内存降级形态的会话句柄本就是空(CurrentSessionID()
// 为空串,sessionRuntime 的空串回退以 current 为寻址目标),空串是合法租约键。
func (p *AgentPlugin) AcquireControl(sessionID, connectionID string) bool {
	if connectionID == "" {
		return false
	}
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	if p.controlLeases[sessionID] == connectionID {
		return false
	}
	tookOver := p.controlLeases[sessionID] != ""
	if tookOver {
		slog.Info("x-goworker: control lease taken over", "session", sessionID, "previous", p.controlLeases[sessionID], "new", connectionID)
	}
	if p.controlLeases == nil {
		p.controlLeases = make(map[string]string)
	}
	p.controlLeases[sessionID] = connectionID
	return tookOver
}

// ReleaseControl 实现 RunControlSource(I3 后按会话释放):仅该会话的持有者
// 本人有效,他人调用为空操作(不抢、不误清他会话)。空 connectionID 不可能
// 成为 holder(Acquire 已拒)。
func (p *AgentPlugin) ReleaseControl(sessionID, connectionID string) {
	if connectionID == "" {
		return
	}
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	if p.controlLeases[sessionID] == connectionID {
		delete(p.controlLeases, sessionID)
	}
}

// HoldControl 实现 RunControlSource(I3 后按会话判定):connectionID 当前是否
// 持有 sessionID 的 control lease。nil map 取值安全(零值 "")。
func (p *AgentPlugin) HoldControl(sessionID, connectionID string) bool {
	if connectionID == "" {
		return false
	}
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	return p.controlLeases[sessionID] == connectionID
}

// ReleaseConnection 实现 RunControlSource(I3 新增断连收口):释放 connectionID
// 持有的全部会话的 control lease——handler.Close 一次调用替代逐会话归还,
// 「审批不悬死在已死连接上」的宿主授权收口。连接不持有任何租约时幂等空操作。
func (p *AgentPlugin) ReleaseConnection(connectionID string) {
	if connectionID == "" {
		return
	}
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	for sessionID, holder := range p.controlLeases {
		if holder == connectionID {
			delete(p.controlLeases, sessionID)
		}
	}
}

// UnbindCapabilities 实现 RunControlSource(P5-6):直转 manager 能力注册表的
// 连接级解绑(P4-3 的 UnbindConnection;P5-9 起 handler.Close 断连收口消费)。
// 连接不存在时幂等空操作。
func (p *AgentPlugin) UnbindCapabilities(connectionID string) {
	p.manager.Capabilities().UnbindConnection(connectionID)
}

// CapabilityWaitTimeout 实现 RunControlSource(P5-6):cfg.Session.
// CapabilityWaitTimeout(秒,P4-4 字段)秒化;<=0 → 45s 兜底(拍板③,硬约束
// < ai-core 工具执行 60s——SDK defaultCapabilityWaitTimeout 的同值镜像);
// >55s 钳到 maxCapabilityWaitTimeout 并 slog.Warn(P4 评审 issue2:硬约束
// 从注释落为代码,SDK 侧 resolveCapabilityWait 的出口镜像)。
func (p *AgentPlugin) CapabilityWaitTimeout() time.Duration {
	secs := 0
	if p.cfg != nil {
		secs = p.cfg.Session.CapabilityWaitTimeout
	}
	if secs <= 0 {
		return defaultCapabilityWaitTimeout
	}
	wait := time.Duration(secs) * time.Second
	if wait > maxCapabilityWaitTimeout {
		slog.Warn("agent: capability wait timeout exceeds ai-core toolCtx budget, clamped",
			"configured", wait, "clamped_to", maxCapabilityWaitTimeout,
			"reason", "wait runs inside ai-core toolCtx (60s), budget must leave Invoke headroom")
		wait = maxCapabilityWaitTimeout
	}
	return wait
}

// ─────────────────────────────────────────────────────────────────────────────
// ClientToolBinder(P5-9):设备工具的能力代理面(总方案 §8 的 daemon 接线,
// P5 显式可见变化 #1)。stdio worker 路径(acpworker 自持原始 relay)不经此处。
// ─────────────────────────────────────────────────────────────────────────────

// deviceToolExecutor 把 SDK 的 ClientToolExecutor 收窄面适配到 x-device/call:
// Execute 收到的 toolName 是注册名(sys_ 前缀,与原始中继的 core.Tool.Name 同
// 名),CallDeviceTool 统一剥前缀转发客户端原名(wire 形状单一事实源,
// sys_relay.go CallDeviceTool)。Invoke 失败由代理 Execute 即时 Invalidate 该
// 绑定并进入等待重绑(P4-4 语义),绝不静默路由到其他连接。
type deviceToolExecutor struct {
	caller model.DeviceCaller
}

func (e deviceToolExecutor) Invoke(ctx context.Context, toolName string, argsJSON []byte) (string, error) {
	return acpworker.CallDeviceTool(ctx, e.caller, toolName, argsJSON)
}

// ClientToolSource 实现 model.ClientToolBinder(P5-9):返回本连接的设备工具惰性
// 源。调用时机 = 每轮 prompt 工具收集(勘察结论 D),closure 内:
//
//   - 探测(acpworker.SpecDeviceTools,ProbeTimeout 2s 自计时)失败/空 → nil:
//     现状收缩语义「无能力即无工具」,不登记任何绑定(方向必须是收缩不是放行);
//   - 成功 → manager.Capabilities().Bind(会话 id, connectionID, 注册名,
//     ttl=2*CapabilityWaitTimeout, deviceToolExecutor{caller}) →
//     runtimeagent.NewClientToolProxy(reg, sessionID, specs, waitTimeout)。
//
// 键与预算(plan P5-9 表):
//   - 会话 id 用 rt.ID:持久化模式它与 currentStore().SessionID() 同源
//     (startSession/refreshSession 的会话边界以 store id 注册 runtime),纯内存
//     降级为 "current" 单例——Bind/Lookup 同键,不引入第二会话标识;
//   - ttl = 2*CapabilityWaitTimeout:等待预算内绑定不得被 TTL 判过期(过期条目
//     视同无绑定,capability.go lazy expiry),2 倍留余量;
//   - 每次 prompt 探测成功都重新 Bind:幂等刷新 TTL + 工具集(P4-3 的「每次
//     prompt 探测都会 Bind」契约)。
//
// 断连语义(P5 显式可见变化 #1):handler.Close → UnbindCapabilities(connID)
// 连接退出即解绑;在途的设备工具调用由 P4-4 的「无绑定/Invoke 失败 → 等待重绑
// (默认 45s)→ 明确失败文本或重绑成功」接管;未声明 x-device 的客户端(VS Code
// 等)不进此路径,零变化;沙箱/HITL 门控面零变化(spec.Metadata 透传,重绑后
// 照常 per-call 裁决)。无 runtime(Init 未完成/失败回滚)→ nil。
func (p *AgentPlugin) ClientToolSource(connectionID string, caller model.DeviceCaller) func(context.Context) []core.Tool {
	return func(_ context.Context) []core.Tool {
		specs, err := acpworker.SpecDeviceTools(caller)
		if err != nil || len(specs) == 0 {
			return nil
		}
		rt := p.currentSession()
		if rt == nil {
			return nil
		}
		sessionID := rt.ID
		wait := p.CapabilityWaitTimeout()
		names := make([]string, 0, len(specs))
		for _, spec := range specs {
			names = append(names, spec.Name)
		}
		reg := p.manager.Capabilities()
		reg.Bind(sessionID, connectionID, names, 2*wait, deviceToolExecutor{caller: caller})
		return runtimeagent.NewClientToolProxy(reg, sessionID, specs, wait)
	}
}
