package agentcontrol

import (
	"fmt"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
)

// runs.go 是 Run 控制操作(StartRun/StartRunSync/CancelRun/CancelRunSync/
// RunStatus)的编排面:多会话寻址(SessionRouter)→ fencing 校验(LeaseRegistry.
// WithControl)→ 幂等 single-flight(RequestRegistry)→ 直转
// runtimeagent.SessionManager。与迁移前的行为契约逐条对齐,差异仅两处且均为方案
// 要求:①幂等注册表按 Key single-flight(§4.3,跨别名同键并发重试收敛为一次底层
// 提交,见 RequestRegistry 论证);②StartRun 签名增 cb 参数(§4.1 草案允许的
// 偏差:回调由调用方传入,本 PR 的 plugin 委托传零值回调,语义与迁移前逐字一致
// ——事件采集由 supervisor 的 tee 自行供给,Decide 留 nil → P4-2 审批回退接管,
// 决策经 ResolveApproval 跨连接投递)。StartRunSync/CancelRunSync 是 PR4(§九)
// 的同步宿主入口形态:无 fencing、无幂等、返回 RunHandle,见各方法注释。R3 起
// (第四轮评审)两方法经受限接口 InternalRunner 暴露——不进 model.RunControlSource
// 或任何公共能力面,Transport Adapter 结构性触达不到。

// InternalRunner 是同步宿主入口的受限内部提交面(第四轮评审 R3):StartRunSync/
// CancelRunSync 只经本接口提供给宿主组装面注入的可信本地命令 Adapter——CLI 命令
// 与嵌入式命令(生产调用方:AgentPlugin.handleAgent,commands.go 的 prompt 提交
// 与 Legacy 取消);Transport Adapter(ACP/x-goworker 协议面及未来新增)一律走
// RunControlSource 的 fencing 面(model.RunControlSource 的 StartRun/CancelRun,
// Connection Lease 校验与提交同临界区)。接口收敛的意义:两个面的方法集分离——
// RunControlSource 刻意不含两个 Sync 方法,其消费者(acp handler 的 h.runs)静态
// 类型拿不到本接口 ⇒ ACP 面结构性无法绕过 fencing;未来 Adapter 需要同步提交时,
// 必须经宿主组装面显式注入 InternalRunner,而不是在公共能力接口上再开一个无
// fencing 入口。分离的钉死点:本断言与插件侧 var _ InternalRunner = (*AgentPlugin)
// (nil)(runcontrol.go)正向钉住 InternalRunner 面;model.RunControlSource 不含
// Sync 方法由 acp 包反射守卫钉死(Go 无编译期负断言)。
type InternalRunner interface {
	StartRunSync(sessionID string, req runtimeagent.RunRequest, cb runtimeagent.RunCallbacks) (runtimeagent.RunHandle, error)
	CancelRunSync(sessionID, runID string) error
}

// InternalRunner 编译期断言:Service 是该面的唯一实现方,方法集或签名漂移即
// 编译失败。
var _ InternalRunner = (*Service)(nil)

// ensureRegistered 修复「current runtime 已注册」不变量(原 runcontrol.go 的
// registerCurrentRuntime 迁入;修复所需的两步操作只经 Service 持有的 manager,
// 不依赖宿主状态):纯内存降级下 /new 的新旧 runtime 同 id,startSession 的
// Register 撞旧条目保留旧值,handleNew 尾部 Remove 移除的正是旧条目——新
// runtime 此刻不在册(Get == nil);经同 id 重建的路径(如测试 fixture 的
// refreshSession)则可能有同 id 旧条目占位(Get != rt)。两者都违反不变量,
// Remove+Register 幂等重建。Register 的错误不在此放大:唯一可达失败是
// ErrManagerClosed,后续 manager.StartRun/CancelRun 会给出同一权威错误。
func (s *Service) ensureRegistered(rt *runtimeagent.SessionRuntime) {
	if got := s.sessions.Get(rt.ID); got != rt {
		s.sessions.Remove(rt.ID)
		_ = s.sessions.Register(rt)
	}
}

// StartRun 提交一次后台 Run 并立即返回 (runID, 提交时状态):
//
//   - 寻址(C3):router.RouteSession 按 sessionID 定位 runtime(不限 current),
//     StartRun 用该 rt——其 Session 自带本会话边界注入的 deps,journal/hub/
//     conversation 天然按会话隔离;nil → sessionNotFoundErr(路由先于 lease
//     校验:未注册会话诚实失败,ErrSessionNotFound 语义不受 fencing 影响)。
//
//   - fencing(第三轮评审 I4;DE2-I1 后锁粒度=会话):本会话 lease 临界区内原子
//     完成「lease 校验 + 提交」——接管的 Acquire 同锁,校验通过后到提交完成之间
//     不可能被接管,旧持有者绝无法在失权后落地 run/start。未持有(从未持有/
//     已被接管/已释放)→ ErrControlLeaseLost 的明确错误;拒绝发生在幂等查表
//     之前,被拒提交不入幂等表。
//
//   - 幂等(I5;§4.3 single-flight):requestID 非空时按 (rt.ID, requestID) 组合
//     键寻址(键的 SessionID 取路由解析后的 rt.ID,canonical——纯内存降级的
//     空串/"current" 两别名折叠为同一键)。同键同 Input → 返回原 runID 与其
//     当前状态(RunHandle.State() 的实时快照,终态保留),不重复起跑;同键不同
//     Input → ErrRequestConflict 显式冲突;requestID 的唯一性只在同一会话的
//     幂等作用域内成立(第二轮评审 I5:跨会话冲突扫描已删)。空 requestID 不
//     记账(每次新起跑)。single-flight 保证同键并发提交恰好一次底层 StartRun
//     (见 RequestRegistry 注释的论证)。幂等表是进程内存量,重启清零。
//
//   - cb 所有权:manager.StartRun 返回起归 supervisor(其契约 1);同键并发下
//     仅 leader 的 cb 生效(joiner 等待同一结果,与迁移前「命中幂等表的重试
//     不再触发提交」的回调丢弃语义一致)。
//
//   - 返回的 state 是提交时快照(manager.StartRun 同步段落盘 queued 记录后
//     durable-first 返回,恒为 queued)。
func (s *Service) StartRun(sessionID, clientID, input, requestID string, cb runtimeagent.RunCallbacks) (string, string, error) {
	rt := s.route(sessionID)
	if rt == nil {
		return "", "", sessionNotFoundErr(sessionID)
	}
	var key RequestKey
	if requestID != "" {
		key = RequestKey{SessionID: rt.ID, RequestID: requestID}
	}

	var runID, state string
	err := s.leases.WithControl(sessionID, clientID, func() error {
		h, fresh, submitErr := s.requests.Submit(key, input, func() (runtimeagent.RunHandle, error) {
			s.ensureRegistered(rt)
			return s.sessions.StartRun(rt.ID, runtimeagent.RunRequest{Input: input}, cb)
		})
		if submitErr == nil {
			// 响应状态语义按提交形态区分(第四轮 TestCrossSessionRequestIdempotency
			// 的两个断言面):fresh 提交 → 接受时刻快照(SubmitState,恒 queued,
			// 确定性——h.State() 是活快照,fast provider 下 execute 可能已把 run
			// 推到 completed,会让 run/start 的接受响应非确定);幂等命中 → 活快照
			//(重试方在问「run 现在怎么样」,completed 是事实而非竞争噪声)。
			if fresh {
				runID, state = h.RunID, string(h.SubmitState())
			} else {
				runID, state = h.RunID, string(h.State())
			}
		}
		return submitErr
	})
	if err != nil {
		return "", "", err
	}
	return runID, state, nil
}

// StartRunSync 提交一次内部(同步宿主入口)Run 并返回句柄(PR4,总方案 §九「同
// 步与后台 Run 最终统一」):/agent 与 session/prompt 的统一提交面——handleAgent
// 经 plugin 委托调用,与后台路径共用 manager.StartRun 的同一套记账(per-session
// active 占位)、门禁(每会话至多一个活动 Run,第二提交 ErrRunActive)、全局
// runSlots/pending 配额、journal 记账(queued→running→终态)、事件面(hub tee)
// 与终态化(finalizeRun),不复制任何执行流程。
//
// 与 StartRun(后台/xgw 面)的三点差异,均为「同步宿主入口」的身份使然:
//   - 无 fencing:调用方是进程内同步入口(CLI 命令/嵌入式 Eval),不持有任何
//     Connection Control Lease——§4.1「内部调度可以不依赖 Connection Lease,
//     但仍需要明确它具有启动 Run 的权限」,本方法即该内部执行身份(service.go
//     ClientKindInternal 预留)的提交入口;协议面 StartRun 仍强制 lease,两不
//     混淆。R3 收敛(第四轮评审):本方法只在 InternalRunner 受限面暴露,不进
//     model.RunControlSource——其消费者 type-assert 不到 InternalRunner,ACP 面
//     结构性无法绕过 fencing;
//   - 无幂等记账:同步 prompt 无 requestID(空键从不入 RequestRegistry,每次
//     新起跑),直接 manager.StartRun;
//   - 返回 RunHandle 而非 (runID, state) 快照:同步形态的调用方需要 Wait 阻塞
//     至终态(「prompt 返回 = run 完成」),句柄是唯一等待面;Legacy 取消语义
//     (调用方 ctx 取消 → 显式取消)由 Adapter 经 CancelRunSync 落地——runCtx
//     来自宿主(StartRun 契约②),请求 ctx 不参与。
//
// 路由与不变量修复与 StartRun 同源:route(sessionID) nil → sessionNotFoundErr;
// ensureRegistered 修复「current runtime 已注册」(纯内存 /new 同 id 重建窗口,
// 提交不可落到注销的旧 runtime)。
func (s *Service) StartRunSync(sessionID string, req runtimeagent.RunRequest, cb runtimeagent.RunCallbacks) (runtimeagent.RunHandle, error) {
	rt := s.route(sessionID)
	if rt == nil {
		return runtimeagent.RunHandle{}, sessionNotFoundErr(sessionID)
	}
	s.ensureRegistered(rt)
	return s.sessions.StartRun(rt.ID, req, cb)
}

// CancelRunSync 取消一个内部(同步宿主入口)Run(PR4,总方案 §九的 Legacy 取消
// 语义):同步 Run 的 runCtx 来自宿主,调用方请求 ctx 的取消不传播——Adapter
// 在等待面(handleAgent 的 select)显式转本方法落地取消。与 CancelRun 的唯一
// 差异是无 fencing(内部执行身份不持 Connection Lease,同 StartRunSync——R3 起
// 同经 InternalRunner 受限面暴露,不入 model.RunControlSource);
// ensureRegistered 的不变量修复同源。manager 的拒绝语义原样上抛:run 已终态
// (与取消竞争落败)→ ErrRunNotFound,Adapter 视为幂等空操作。
func (s *Service) CancelRunSync(sessionID, runID string) error {
	rt := s.route(sessionID)
	if rt == nil {
		return sessionNotFoundErr(sessionID)
	}
	s.ensureRegistered(rt)
	return s.sessions.CancelRun(rt.ID, runID)
}

// CancelRun 取消一个后台 Run:多会话寻址 → 本会话 lease 临界区内原子完成
// 「lease 校验 + 取消提交」→ 直转 manager(与 StartRun 同构的 fencing:校验通过
// 后不可能被接管,未持有 → ErrControlLeaseLost)。不变量修复的必要性同迁移前
// (registerCurrentRuntime → ensureRegistered):current 会话的 runtime 在同 id
// 重建窗口(纯内存 /new)下可能未在册,同步 run 活跃其上时不经修复会被误报
// ErrRunNotFound;跨会话路由的 runtime 本就来自注册表,修复为幂等空操作。
// manager 的拒绝语义原样上抛:未知 run → ErrRunNotFound;PR4 起 daemon 全部
// 入口(sync/background)的 Run 都经 manager.StartRun 提交、均可取消,
// ErrRunNotCancellable 仅剩 RecordRun 记账形态(测试/内部细节,无 cancel)。
func (s *Service) CancelRun(sessionID, clientID, runID string) error {
	rt := s.route(sessionID)
	if rt == nil {
		return sessionNotFoundErr(sessionID)
	}
	return s.leases.WithControl(sessionID, clientID, func() error {
		s.ensureRegistered(rt)
		return s.sessions.CancelRun(rt.ID, runID)
	})
}

// RunStatus 返回 run 当前状态、该会话事件流 latest seq 与回放完整性三态
// replayIntegrity。只读观察面,不做 fencing(run/get 不要求 control lease,
// 观察者与控制者分离,总方案 §7.4)。观测面三分支,按权威序:
//  1. rt.Active() 命中 runID(在跑/排队)→ 实时快照;latestSeq 取
//     hub.LatestSeq()(会话维度游标,P3-0 契约);
//  2. 该会话自己的 journal(已终态,active 已清):journal 权威——journal 必须
//     取被路由会话的事实源(JournalSource.RunJournal:current 会话走 deps 单例、
//     跨会话路由的会话走 repo 只读实例的解析由宿主承担),读 runs/<runID>.jsonl
//     末条 state 记录;latestSeq 取 SeqRange 的磁盘 latest(hub 可能尚未经
//     StartRun 建立——只有同步 run 的会话即此形态);journal 存在但 miss →
//     未找到,不回落幂等表(持久化模式 journal 覆盖本会话全部 run,不发明第二
//     事实源);
//  3. 幂等表反查(仅纯内存,JournalSource 为 nil 或该会话无 journal):经
//     StartRun 记账的 RunHandle 是唯一观测面(按路由会话过滤)。
//
// replayIntegrity(I4)同源迁移:rt.Hub() 存在时取 hub.ReplayIntegrity()(三态:
// 本进程纪元内任何 key 事件落盘失败/合并 delta flush 失败 → Incomplete,粘滞
// 永不复位;journal 播种发现既有历史 → Unknown,跨进程纪元的完整性不可验证,
// 诚实优先;空历史/纯内存 → Complete);hub 为 nil → Complete——无事件流即无
// 「不完整回放」可言,该取值只携带失败证据、不发明完整性承诺。
//
// 未命中 → 包裹 runtimeagent.ErrRunNotFound 的错误(协议面映射 -32000)。
func (s *Service) RunStatus(sessionID, runID string) (string, uint64, string, error) {
	rt := s.route(sessionID)
	if rt == nil {
		return "", 0, runtimeagent.ReplayIntegrityComplete, sessionNotFoundErr(sessionID)
	}

	replayIntegrity := runtimeagent.ReplayIntegrityComplete
	if hub := rt.Hub(); hub != nil {
		replayIntegrity = hub.ReplayIntegrity()
	}

	// ① active 命中(在跑/排队)→ 实时快照。
	if run := rt.Active(); run != nil && run.ID == runID {
		latest := uint64(0)
		if hub := rt.Hub(); hub != nil {
			latest = hub.LatestSeq()
		}
		return string(run.State()), latest, replayIntegrity, nil
	}

	// ② journal 权威(被路由会话自己的事实源;存在即权威,miss 即未找到)。
	if s.journals != nil {
		if js := s.journals.RunJournal(rt); js != nil {
			rec, ok := js.LastRunState(runID)
			if !ok {
				return "", 0, replayIntegrity, fmt.Errorf("agent: run %q: %w", runID, runtimeagent.ErrRunNotFound)
			}
			_, latest, _, err := js.SeqRange()
			if err != nil {
				// I3:扫描失败显式上抛,不把读取失败静默降级为 latest=0(游标是
				// 「仅描述」,描述不可信时诚实报错优于编造一个看似合法的 0)。
				return "", 0, replayIntegrity, fmt.Errorf("agent: run %q: seq range: %w", runID, err)
			}
			return rec.State, latest, replayIntegrity, nil
		}
	}

	// ③ 幂等表反查(仅纯内存)。
	if h, ok := s.requests.byRun(rt.ID, runID); ok {
		latest := uint64(0)
		if hub := rt.Hub(); hub != nil {
			latest = hub.LatestSeq()
		}
		return string(h.State()), latest, replayIntegrity, nil
	}
	return "", 0, replayIntegrity, fmt.Errorf("agent: run %q: %w", runID, runtimeagent.ErrRunNotFound)
}
