package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// boundsession.go 实现 model.BoundSessionSource（Plan A2b1「会话面多会话化」）：
// ACP session/new 真新建、session/load 的 reload/重开/归档只读判定——多 session
// 并行的最后一公里（没有它，两条 ACP 连接仍绑同一个 current 会话）。
//
// 语义定调（plan A2「current 语义收缩」与 A2b1 任务书）：
//   - CreateBoundSession 真新建：新会话只是发起连接的路由目标，current 指针
//     不动（plan A2「current 收缩为 CLI/stdin 便利指针」）；旧 SetSession 链路
//     （cwd 一律落 current 的单活动语义）从 handler 的声明路径整体退役，本文件
//     的声明走 runtime 级（SessionRuntime.DeclareCWD + Session.SetCWD）——
//     cwd 是会话的属性，不再是 daemon 全局单值。
//   - LoadBoundSession 的「已注册 / 重开」二分支：判定源是 manager 注册表与
//     repo 持久化事实，与连接无关——多会话下「当前」是连接相对的（handler 的
//     SessionBinding），插件面只回答「这个 id 的会话在哪」。
//   - SessionArchived 是 session/load 只读分流的探测面：终态会话绝不重开成
//     活跃 runtime，只读重放。
//
// 并发与锁序：本文件全部方法都要求调用方持 ACP 管理门禁（SessionGate，
// A2b1 保持管理操作互斥）——CreateBoundSession/LoadBoundSession 的注册表突变、
// repo.Open/DeclareWorkspace 由此串行；SessionArchived/SessionHistory 是只读
// 快照（repo.Open 内部自锁、store 读面自锁），无门禁要求。lifecycle.go 的
// 收口评估（tryReleaseSession）与本文件的注册方向相反，二者经 manager/repo
// 的短临界区衔接，无嵌套锁。
//
// 编译期断言：AgentPlugin 满足可选能力接口，handler 工厂 type-assert 获取
// （RunControlSource/ConnectionBinder 先例，runcontrol.go）。
var _ model.BoundSessionSource = (*AgentPlugin)(nil)
var _ model.BoundSessionLoader = (*AgentPlugin)(nil)

// boundSessionUnknownErr 构造 BoundSessionSource 契约的「会话不存在」错误：
// 多 %w 同时包裹 model.ErrSessionUnknown（handler 据此映射 -32000 unknown
// session wire 契约——ACP 生产面不得 import ai-runtime/agent，路由面 sentinel
// 对 handler 不可见，model 层 sentinel 是唯一判定点，capability.go 注释）与
// runtimeagent.ErrSessionNotFound（插件内路由/控制面的既有 sentinel，两套
// 消费面各取所需，事实同源：这个 id 定位不到 runtime）。
func boundSessionUnknownErr(sessionID string) error {
	return fmt.Errorf("agent: session %q: %w (%w)", sessionID, model.ErrSessionUnknown, runtimeagent.ErrSessionNotFound)
}

// CreateBoundSession 实现 model.BoundSessionSource（Plan A2b1：session/new
// 真新建，定案需求 #2 的 ACP 面）。
//
// 流程：cwd 规范化与 sandbox 校验（拒绝式——会话尚不存在，创建前失败优于产出
// 带 cwdErr 标记的会话）→ repo.Create（A1 的「创建即声明」：CWD 非空落
// workspace 记录，空 CWD 不落记录）→ mountRuntime（独立 deps 装配 + 新 runtime
// 注册）→ cwd 声明落 runtime（store 面已由 Create 落记录）。
//
// **不动 current**：本方法不触碰 p.current——新会话与 CLI 的活跃会话互不相干；
// 发起方此前绑定的会话由 handler 的登记改绑（BindConnectionSession → prev
// 评估 → tryReleaseSession）走引用计数收口，不经本方法。
func (p *AgentPlugin) CreateBoundSession(cwd string) (string, error) {
	if p.repo == nil {
		return "", fmt.Errorf("agent: session persistence disabled")
	}
	declared, err := normalizeSessionCWD(cwd)
	if err != nil {
		return "", err
	}
	if err := p.validateSessionCWD(declared); err != nil {
		return "", err
	}
	st, err := p.repo.Create(context.Background(), session.CreateOptions{CWD: declared})
	if err != nil {
		return "", fmt.Errorf("agent: create session: %w", err)
	}
	rt, err := p.mountRuntime(st)
	if err != nil {
		// 注册失败（同 id 冲突仅理论可达——Create 的 id 是 128-bit 随机数）：
		// 回收刚建好的 store，不留孤儿缓存条目。失败只 warn：创建已失败，收口
		// 是尽力而为的卫生动作。
		if cerr := p.repo.CloseSession(st.SessionID()); cerr != nil {
			slog.Warn("agent: discard created session store failed", "session", st.SessionID(), "err", cerr)
		}
		return "", err
	}
	p.declareSessionCWD(rt, declared)
	return st.SessionID(), nil
}

// LoadBoundSession 实现 model.BoundSessionSource（Plan A2b1：session/load 的
// reload/重开统一入口）。
//
// 二分支（判定与连接无关，见文件头）：
//  1. 已注册 runtime（RouteSession：current 优先 + manager 注册表权威，CR4
//     同源解析）→ reload 内存视图——共享会话可能有别连接的 run 追加，重放前
//     把 conversation 对齐到 store 活跃视图；cwd 非空时重申声明（客户端重连
//     后的 load 携带它记住的工作目录）。cwd 为空 = 不动声明：多连接共享下
//     load 不再独占会话，空声明不该把别人的有效声明冲掉（旧 SetSession 的
//     「空 = 重置」语义随单活动模型退役，plan A2 实施注意点 3 的耦合面）。
//  2. 持久化但不在册 → 重开：repo.Open（A2.1①：A2a 收口 CloseSession 逐出
//     缓存后，重开走 flock 慢路径拿活实例）→ mountRuntime 注册 → cwd 恢复
//     （实参非空优先，否则 workspace 记录——A1 访问器，创建/上次声明的事实
//     源）→ MarkInterruptedRuns（与 startSession 的 P2-7 恢复扫描同款：重开
//     是跨进程的会话边界，上一进程遗留的非终态 run journal 必须标记，
//     恢复绝不重跑）。
//
// cwd 无效不拒绝 load：会话已存在，拒绝会让历史不可达——落 cwdErr 标记
// （prompt 诚实报错），与 SetSession 的既有无效声明语义一致（SetSession 保留
// 为 current 面的兼容方法，见 plugin.go 注释）。
func (p *AgentPlugin) LoadBoundSession(sessionID, cwd string) error {
	if rt := p.RouteSession(sessionID); rt != nil {
		// 分支 1：reload 已注册会话的视图。
		if s := rt.Session(); s != nil {
			if err := s.ReloadFromStore(); err != nil {
				return fmt.Errorf("agent: reload session %s: %w", sessionID, err)
			}
		}
		if normalized := normalizeSessionCWDSilent(cwd); normalized != "" {
			p.declareSessionCWD(rt, normalized)
		}
		return nil
	}
	// 分支 2：重开持久化但不在册的会话。纯内存降级（无 repo）下「不在册」即
	// 「不存在」——无持久化事实可恢复，诚实报未知。
	if p.repo == nil {
		return boundSessionUnknownErr(sessionID)
	}
	st, err := p.repo.Open(sessionID)
	if err != nil {
		if errors.Is(err, session.ErrSessionNotFound) {
			return boundSessionUnknownErr(sessionID)
		}
		return fmt.Errorf("agent: reopen session %s: %w", sessionID, err)
	}
	rt, err := p.mountRuntime(st)
	if err != nil {
		return err
	}
	// P2-7 恢复扫描（startSession 同款）：重开边界的 store 若挂着上一进程遗留
	// 的非终态 run journal，追加 interrupted 标记。失败仅 warn 不阻塞重开。
	if marked, err := p.repo.MarkInterruptedRuns(context.Background(), st.SessionID()); err != nil {
		slog.Warn("agent: mark interrupted runs failed on reopen", "err", err, "session", st.SessionID())
	} else if len(marked) > 0 {
		slog.Warn("agent: marked interrupted runs from previous process on reopen", "count", len(marked), "session", st.SessionID(), "runs", strings.Join(marked, ","))
	}
	// cwd 恢复：实参非空优先（客户端重申），否则 workspace 记录（A1 访问器；
	// 从未声明过则维持未声明——执行域回退既有语义，A4 的 tmp 注入接管空 cwd）。
	restore := normalizeSessionCWDSilent(cwd)
	if restore == "" {
		restore = st.Workspace()
	}
	p.declareSessionCWD(rt, restore)
	return nil
}

func (p *AgentPlugin) LoadBoundSessionForConnection(sessionID, cwd, connectionID string) ([]model.SessionUpdate, error) {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if err := p.LoadBoundSession(sessionID, cwd); err != nil {
		return nil, err
	}
	history, err := p.SessionHistory(sessionID)
	if err != nil {
		p.tryReleaseSessionLocked(sessionID)
		return nil, err
	}
	if p.control != nil {
		p.bindConnectionSessionLocked(connectionID, sessionID)
	}
	return history, nil
}

// SessionHistory 实现 model.BoundSessionSource：已注册会话的完整历史重放
// （session/load 响应前的重放面）。数据源 store.ActiveView()（store 锁内只读
// 快照），经 replayUpdates 管线（与 CurrentSessionHistory/归档重放共用，
// sessions.go）。未知会话报错；纯内存（storeFor 未命中——降级或仓库已收尾）
// 返回空切片，与 CurrentSessionHistory 的无持久化语义同型。
func (p *AgentPlugin) SessionHistory(sessionID string) ([]model.SessionUpdate, error) {
	rt := p.RouteSession(sessionID)
	if rt == nil {
		return nil, boundSessionUnknownErr(sessionID)
	}
	store := p.storeFor(rt)
	if store == nil {
		return []model.SessionUpdate{}, nil
	}
	return replayUpdates(store.ActiveView(), checkpointAnchors(store)), nil
}

// SessionArchived 实现 model.BoundSessionSource：repo 层只读探测会话是否已落
// archived 终态标记（Plan A1 记录模型）。探测是 session/load 只读分流的前提
// （handler 无门禁调用——终态重放是纯只读，与在飞 prompt 无共享可变状态），
// repo.Open 内部自锁（r.mu + flock），与门内操作无锁序交叉。
//
// 判定源是持久化事实而非内存标记：重启后依然为真（A7 设计点 4——h.readOnly
// 与 store.Archived() 并存会漂移，统一以 store 查询为准）。未知会话/持久化
// 禁用返回 false：探测不吞掉「不存在」，未知 id 落到 LoadBoundSession 诚实报
// ErrSessionUnknown；Open 的其他失败（flock 被别的进程持有等）同样按非终态
// 处理，让 load 面给出完整错误而非探测面的猜测。
//
// 副作用：Open 命中会进实例唯一缓存（决策 3）——对随后同 id 的重开恰好是缓存
// 命中（Probe→Load 的组合路径）；对探测后不 load 的 id，缓存条目（含 flock）
// 常驻到 Stop。取舍：Open 侧无法区分「探测性打开」与「使用性打开」（无引用
// 计数），为探测加第二套开关得不偿失；终态会话由 ArchivedSessionHistory 的
// 重放路径快照后显式释放（见其 repo 分支），非终态残留条目是惰性无害的活
// 实例（无写者、Stop 统一回收）。
func (p *AgentPlugin) SessionArchived(sessionID string) bool {
	if p.repo == nil {
		return false
	}
	st, err := p.repo.Open(sessionID)
	if err != nil {
		return false
	}
	return st.Archived()
}

// mountRuntime 为持久化 store st 装配一个独立会话 runtime 并注册进 manager
// （Plan A2b1：session/new 真新建与 session/load 重开共用的注册底座）。
//
// deps 以当前会话边界的装配（p.deps）为底座——skill/MCP 工具/memory/指令
// 快照/provider 工厂是宿主级资源，跨会话共享；Store/RunJournal/RunEvents 换绑
// 到 st（与 startSession 的 st != nil 分支同构，plugin.go）。**值拷贝换绑**：
// p.deps 本身是 current 会话边界的装配，重开/新建会话的换绑只能落在拷贝上，
// 否则 /new 的下一次边界重建会踩掉本会话的 store 指针。runtime id =
// st.SessionID()（store 稳定 id，sessionRuntimeIn/RouteSession 的寻址键，与
// startSession 的注册规则同源）。RunEvents 与 RunJournal 注入同一
// RunJournalStore 实例（P5-4 单例纪律，startSession 同款）。
//
// 注册失败（同 id 已在册——重开路径下意味着并发重开赢了，或 id 形态异常）：
// 显式报错。Create 路径的随机 id 冲突仅理论可达；重开路径的并发双开被 ACP
// 管理门禁串行化（A2b1），本分支是防御面。
func (p *AgentPlugin) mountRuntime(st *session.Store) (*runtimeagent.SessionRuntime, error) {
	deps := p.deps
	deps.Store = st
	journalStore := session.NewRunJournalStore(p.repo, st)
	deps.RunJournal = journalStore
	deps.RunEvents = journalStore
	rt := runtimeagent.NewSessionRuntime(st.SessionID(), runtimeagent.NewSession(deps))
	if st.Archived() {
		if err := rt.Archive(nil); err != nil {
			return nil, fmt.Errorf("agent: restore archived runtime %s: %w", rt.ID, err)
		}
	}
	if err := p.manager.Register(rt); err != nil {
		return nil, fmt.Errorf("agent: register session runtime %s: %w", rt.ID, err)
	}
	return rt, nil
}

// declareSessionCWD 把一次 cwd 声明落到 runtime（及会话本体与会话 store）：
// sandbox 校验 → 有效声明写三处（store workspace 记录 last-wins、runtime
// DeclareCWD、Session.SetCWD）；无效声明落 cwdErr 标记（cwd 清空 + err 记账，
// prompt 诚实报错——SetSession 同款，不让越界目录静默生效）。
//
// Plan A4（定案 #5，plan A2 实施注意点 3 的语义反转）：空串 = 未声明 →
// **注入缺省执行域**（tmpWorkspaceDir(rt.ID)，MkdirAll 0700），不再 no-op——
// 旧语义（load 不动声明）在「从未声明过的会话 load/重开」时留下空执行域，工具
// 回退 cfg.AllowedWorkDir 或进程 cwd；新语义给出确定答案：tmp 每会话目录
// （重启即失是有意语义）。注入的是**执行域**而非声明：applySessionCWD 的
// record=false——不写 workspace 记录，记录仍只落用户显式声明（清单 cwd 与
// load 恢复的持久事实源不变，A7 设计点 5）；load 的恢复次序（实参非空优先、
// 否则 workspace 记录、否则注入）见 LoadBoundSession。注入失败落 cwdErr 无效
// 标记（prompt 诚实报错），不静默回退。
func (p *AgentPlugin) declareSessionCWD(rt *runtimeagent.SessionRuntime, cwd string) {
	if cwd == "" {
		tmp, err := p.injectTmpWorkspace(rt.ID)
		if err != nil {
			rt.DeclareCWD("", err)
			slog.Warn("agent: inject default workspace failed", "session", rt.ID, "err", err)
			return
		}
		p.applySessionCWD(rt, tmp, false)
		return
	}
	p.applySessionCWD(rt, cwd, true)
}

// applySessionCWD 落地一次已定值的 cwd（declareSessionCWD 与 SetSession 的共用
// 执行面）：sandbox 校验 →（record=true 时）store workspace 记录 → runtime
// DeclareCWD + Session.SetCWD。record=false 是注入域路径（执行域不是声明，
// 不落记录）；校验失败统一落 cwdErr 标记（注入域受第二 allowed root 保护，
// 失败即环境异常，诚实暴露）。
//
// store 记录同值去重：workspace 是 last-wins 的追加记录，声明值与既有事实一致
// 时不再追加——CreateBoundSession 的「创建即声明」（repo.Create 已按 CWD 落记录）
// 与 load 对未变 cwd 的重申（实参 == 记录值，或空实参回落记录值）都经此收敛，
// 追加文件不累积同值冗余行；执行面（runtime/会话本体的 DeclareCWD/SetCWD）
// 照常刷新，幂等声明不因去重而缺失。跨进程另一写者同值追加的场景由 flock 排他
// + 本进程快照读取覆盖（store.mu 内读 Workspace）。
//
// store 记录写入失败只 warn 不回滚声明：执行面（工具 cwd）以 runtime/会话
// 本体为准，持久化缺口可观测、下次 load 重申即补——与 journalRunState 的
// warn 降级同款取舍（声明可用性高于记录完整性）。
func (p *AgentPlugin) applySessionCWD(rt *runtimeagent.SessionRuntime, cwd string, record bool) {
	if err := p.validateSessionCWD(cwd); err != nil {
		rt.DeclareCWD("", err)
		slog.Warn("agent: reject session cwd", "session", rt.ID, "cwd", cwd, "err", err)
		return
	}
	if record {
		if st := p.storeFor(rt); st != nil && st.Workspace() != cwd {
			if err := st.DeclareWorkspace(cwd); err != nil {
				slog.Warn("agent: declare workspace record failed", "session", rt.ID, "cwd", cwd, "err", err)
			}
		}
	}
	rt.DeclareCWD(cwd, nil)
	if s := rt.Session(); s != nil {
		s.SetCWD(cwd)
	}
}

// normalizeSessionCWD 规范化声明的 cwd（TrimSpace + Abs——cmd 与工具执行要求
// 绝对路径，回显也取规范化值，SetSession 同款）。空串合法（= 未声明）。
func normalizeSessionCWD(cwd string) (string, error) {
	cwd = strings.TrimSpace(cwd)
	if cwd == "" {
		return "", nil
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("agent: invalid session cwd %q: %w", cwd, err)
	}
	return abs, nil
}

// normalizeSessionCWDSilent 是 normalizeSessionCWD 的声明面变体：Abs 失败时
// 原样返回未规范化值而非报错——load 面的 cwd 无效走 declareSessionCWD 的
// 校验/标记路径（validateSessionCWD 对不可解析路径同样报错），不在这里二次
// 分叉错误形态。
func normalizeSessionCWDSilent(cwd string) string {
	normalized, err := normalizeSessionCWD(cwd)
	if err != nil {
		return strings.TrimSpace(cwd)
	}
	return normalized
}

// ArchiveSession 实现 model.BoundSessionSource 的归档执行面（Plan A3，
// x-goworker/session/archive；定案需求 #1：archive = 只读保留终态，不可逆、
// 无 unarchive）。fencing 双保险的分工：全局管理门禁由调用方（handler）持有
// （归档是管理操作，与 session/new|load 互斥——A2b2 收窄后的 gate），本方法
// 在门禁内被调；control lease fencing 在本方法内完成（route → WithControl，
// 与 run/cancel/审批同款——lease 校验与提交同临界区，接管后旧持有者一律
// 拒绝；MarkArchived 的一次 append+fsync 与 StartRun 同步段 journal fsync
// 同款先例，lease 临界区容纳毫秒级文件突变）。
//
// 动作语义：
//   - run 在飞拒绝（定案需求 #8：在飞即活跃）：终态标记与在飞 run 并存会让
//     只读执法（A7④）与事件面失真——瞬态 busy，等 run 终态（挂起收口的会话
//     同样在飞，同样拒）后重试；客户端契约与 new/load 的 busy 同款。
//   - 绑定中的会话**允许**归档（任务书定调）：绑定保留、prompt 转为只读拒绝
//     （A7④ store 执法，handleAgent 入口）、生命周期收口等引用归零（A2a
//     机制自然处理——归档不改变引用计数）。
//   - 已收口（不在册）的会话同样可归档：repo 层打开落标记后释放（探测性
//     打开的缓存副作用与 SessionArchived 同款取舍；归档后无后续使用面，
//     用后即释）。不在册会话的 lease 残留仍有效（LeaseRegistry 条目只增不
//     删，曾 attach 的连接 fencing 照常）。
//   - 幂等：MarkArchived 对已终态会话 no-op（A1 语义——归档时间定格首次），
//     重复归档安全。
func (p *AgentPlugin) ArchiveSession(sessionID, clientID string) error {
	if p.repo == nil {
		return fmt.Errorf("agent: session persistence disabled")
	}
	if p.control == nil {
		return fmt.Errorf("agent: archive session: control plane unavailable")
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	// ① 寻址（先于 fence：未知会话诚实拒绝，ErrSessionUnknown 契约）。
	//    在册 runtime 的 store 经实例唯一缓存（runtime 持有的活实例，不重复
	//    打开）；不在册走 repo.Open（flock 空闲即活实例，与 LoadBoundSession
	//    重开同链）。
	rt := p.RouteSession(sessionID)
	var st *session.Store
	if rt != nil {
		st = p.storeFor(rt)
		if st == nil {
			// 纯内存降级的 runtime（id 非 store id）：无持久化事实可标记，
			// 终态无处落——诚实报错而非静默 no-op。
			return fmt.Errorf("agent: session %q has no persisted store (纯内存降级，无法归档)", sessionID)
		}
	} else {
		opened, err := p.repo.Open(sessionID)
		if err != nil {
			if errors.Is(err, session.ErrSessionNotFound) {
				return boundSessionUnknownErr(sessionID)
			}
			return fmt.Errorf("agent: archive session %s: %w", sessionID, err)
		}
		st = opened
	}
	// ②-④ fencing 临界区：lease 校验、Runtime admission 与 store 归档在同一生命周期
	// 临界区内完成。Runtime 先进入只读状态，任何后续 Run 提交都会被拒绝；store
	// 写失败则回滚 admission，避免出现内存只读但持久化未归档的半状态。
	err := p.control.WithControl(sessionID, clientID, func() error {
		if rt != nil {
			if p.isSuspendedRelease(sessionID) {
				return fmt.Errorf("agent: session %q is busy: pending release after a run just finished (retry once the session settles)", sessionID)
			}
			if err := rt.Archive(st.MarkArchived); err != nil {
				if errors.Is(err, runtimeagent.ErrRunActive) {
					return fmt.Errorf("agent: session %q is busy: run in flight (archive after the run reaches terminal state): %w", sessionID, err)
				}
				return err
			}
			return nil
		}
		return st.MarkArchived()
	})
	// ⑤ 不在册探测 store 用后即释（fence 拒绝路径同样释放——探测句柄不残留）。
	if rt == nil {
		if cerr := p.repo.CloseSession(sessionID); cerr != nil {
			slog.Warn("agent: archived session store release failed", "session", sessionID, "err", cerr)
		}
	}
	if err != nil {
		return err
	}
	slog.Info("agent: session archived", "session", sessionID, "client", clientID)
	return nil
}
