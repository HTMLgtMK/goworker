// Package service 是 goworker 宿主侧的 /agent 插件适配器：把 ai-runtime 的 Session SDK
// 装配成 model.Plugin 接入 daemon Engine。Session 本体在 ai-runtime/agent（SDK），
// 这里只做资源装载（skill/MCP/memory/store/指令快照）、命令注册与生命周期管理。
package service

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	memory "github.com/tinguo/goworker/ai-memory"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/mcp"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-runtime/skills"
	"github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// defaultHTTPTimeout 是 HTTP 客户端总超时。检查点/压缩是非流式全量调用，
// 上下文一大（用户 config 里 compress_at 没开，会话无界累积）响应可能很慢，
// 30s 太紧，2min 起步。
const defaultHTTPTimeout = 2 * time.Minute

// AgentPlugin 是 /agent 插件入口，只负责三件事：持有会话状态、注册命令、管理生命周期。
// 配置与路径由 NewPlugin 构造函数注入（model.Hub.Config 已 any 化，插件不再从 hub 读配置）。
type AgentPlugin struct {
	cfg   *runtimeconfig.Config
	paths runtimeconfig.Paths
	hub   *model.Hub
	// manager 是多会话执行内核的注册表（§1.1 路由规则的解析点之一）：每个会话
	// 一个 SessionRuntime（独占执行权 gate + 运行记账），经 startSession 注册。
	// P0-4 阶段 runtime id 固定 "current" 单例，/new 重建时同 id 注册冲突、旧
	// runtime 保留在册（归档固化窗口内仍可 Get 触达）；P1-3 起 id 换 store 的
	// 真实 SessionID，新旧 runtime 以不同 id 共存。互斥不直接经过 manager——
	// 命令一律经 currentSession() 解析出 runtime 再在其 gate 上排队。
	manager *runtimeagent.SessionManager
	// current 是当前会话的 runtime。atomic.Pointer:/new 在 REPL goroutine 上换
	// 指针,ACP service 的 session/list|load 与各命令 handler 在别的 goroutine 上
	// 读(评审 I4),裸指针读写是没有 happens-before 的数据竞争。会话本体经
	// currentSession().Session() 取,不再单独持有裸 Session 指针(P0-4 起退役)。
	//
	// PR2(总方案 §六/§八)起它的定位收窄为「无 Binding 调用方的默认事实源」:
	// 连接级会话选择迁往 SessionBinding(agentcontrol,FrontendHandler 每连接一
	// 个,经 ctx 载体流入 withSession),标准 ACP 的会话执行路由 binding 优先、
	// 空则回落本字段(CLI 命令、stdio、测试、后台 Run 的嵌套命令)。§八迁移表
	// 「Plugin.CurrentSession → CLI/ACP SessionBinding,逐步淘汰」的第一步:本
	// 字段仍被 /new、startSession、SetSession 等生命周期与无绑定路径消费,不是
	// 立即删除,而是不再作为跨连接的会话事实源(§二不变量 6)。
	current atomic.Pointer[runtimeagent.SessionRuntime]
	// deps 是 Session 的资源依赖，startSession 每次会话边界全量重建（含指令快照
	// 与当前 store 注入），/new 经同一路径刷新后以新 deps 创建新会话。
	deps runtimeagent.SessionDeps
	// repo 是会话持久化仓库（P1-3）：每会话独立目录 <Session.Dir>/<id>/ + legacy
	// 根槽兼容挂载（<root>/current.jsonl 原地挂载，不移动用户数据）。进程内只开
	// 一次（首个 startSession 即 Init），此后 /new 复用同一仓库——Repository 的
	// 实例唯一缓存必须跨会话边界连续（重开缓存清空后对仍在使用的目录再次 Open
	// 会被文件锁拒绝）；且只写一次（Init 完成前无并发读者），与 current 的原子
	// 读并肩免去第二把锁。nil = 持久化禁用（cfg.Session.Enabled=false 或打开
	// 失败，纯内存路径与旧版 store=nil 语义一致）。
	repo       *session.Repository
	skills     []skills.Skill        // Init 时加载的 skill 清单，注册为 skill_* 工具
	mcpClients map[string]mcp.Client // server name → 连接，Stop 时统一关闭
	mcpTools   []core.Tool           // 从已连接 server 拉取的工具（静态，collectTools 复用）
	memory     *memory.Client        // 记忆组件（MTM+LTM），Init 打开 / Stop 关闭；nil = 禁用

	// mu 只保护 activeTransportSession:ACP 提交方经 session/new|load 声明的会话
	// cwd 已随 P1-4 状态绑定收口下沉到当前 runtime(SessionRuntime.cwd/cwdErr,
	// 每会话一份、经 currentSession() 解析),插件不再持有跨会话的 cwd 单值。
	mu          sync.Mutex
	allowRuleMu sync.Mutex
	// activeTransportSession 是最近一次有效声明的 transport 会话 id(连接方自造
	// 的 UUID,与 store 的稳定 id 不同源)。空 = 尚无 ACP 会话声明。单活动会话
	// 模式下它是「谁拥有 daemon 当前活动状态」的记号:FrontendHandler.Prompt 据此
	// 拒绝已被其他连接顶掉的旧会话,而不是让 prompt 静默串到新声明的工作区里跑。
	activeTransportSession string

	// control 是应用内唯一的 Application Control Plane(总方案 §4.2,PR1 建立):
	// RunControlSource 的 StartRun/CancelRun/RunStatus/ResolveApproval 与 control
	// lease 四操作全部委托至此——Lease fencing、(sessionID, requestID) 幂等
	// per-key single-flight、审批归属校验的实现迁入 agentcontrol 包,本插件不再
	// 直接维护 Lease 表和幂等表(原 reqMu/runRequests/requestOrder/guardsMu/
	// controlGuards 字段与 sessionControlGuard 实现随迁删除,见 runcontrol.go)。NewPlugin 装配;SessionRouter/JournalSource 窄接口由本
	// 插件自任(RouteSession/RunJournal)——Service 经此取 current 权威路由与被
	// 路由会话的 journal,不反向持有插件状态;锁序 lease.mu→registry.mu→manager
	// 单向,跨层 h.mu→session lease 单向(DE2-I1 论证随方法整体迁入)。
	control *agentcontrol.Service
}

// NewPlugin 构造 agent 插件。cfg 为 ai-runtime 运行配置，paths 为宿主注入的目录路径。
// 插件不再从 model.Hub.Config 读配置（Hub.Config 已 any 化）——配置与路径全部构造函数注入。
// 会话 runtime 不在此创建：首个（及后续每个）SessionRuntime 由 startSession 在
// 会话边界上构造并注册进 manager；此前 currentSession() 返回 nil，命令报
// 「no active session」（命令本就要等 Init 完成后才可派发）。
func NewPlugin(cfg *runtimeconfig.Config, paths runtimeconfig.Paths) *AgentPlugin {
	// P2-7：全局并发配额接 SessionConfig（openQuestion ③）。cfg 先于 NewPlugin
	// 注入（签名现状即成立）；WithRunLimits 对 <=0 回落默认 4/16——daemon app
	// config 默认块未设新字段（0 值）或旧配置文件缺键时由此兜底，不依赖 yaml
	// 载入路径（双保险，agent/manager.go WithRunLimits）。
	//
	// P5-1：runCtx 注入 sessionGateKey 重入标记（WithRunContext）——manager 起
	// 的每个后台 Run 的 runCtx 都携带 {sessionGateKey{} → p}，后台 Run 内 hub
	// 工具嵌套 Eval 同会话命令（tools.go collectTools 的 p.hub.Eval →
	// commands.go withSession）时 owner == p 放行，不再「持 gate 重复排队」
	// 死锁（P2-5 契约兑现，supervisor.go StartRun 注释②；§6 风险 2：P5-6/P5-7
	// 的 StartRun 接线以本步为顺序硬前置）。Key 保持包内非导出（commands.go
	// sessionGateKey），Value 携带本插件指针——与 withSession 的判等（owner == p）
	// 精确匹配，其他插件实例的同名 ctx 值不放行。
	p := &AgentPlugin{cfg: cfg, paths: paths}
	p.manager = runtimeagent.NewSessionManager(
		runtimeagent.WithRunLimits(cfg.Session.MaxConcurrentRuns, cfg.Session.MaxPendingRuns),
		runtimeagent.WithRunContext(runtimeagent.RunContextValue{Key: sessionGateKey{}, Value: p}),
	)
	// PR1(总方案 §4.2):装配应用内唯一的 Application Control Plane——plugin 与
	// ACP 共享同一实例(两套 Lease/Request Registry 是明确禁止的形态)。router/
	// journal 窄接口由本插件自任(RouteSession/RunJournal,runcontrol.go):Service
	// 经此取 current 权威路由与被路由会话的 journal,不反向持有插件状态。此时尚
	// 无会话(startSession 在 Init 创建),路由的 current 回退对 nil current 安全。
	p.control = agentcontrol.NewService(p.manager, p, p)
	return p
}

// currentSession 返回当前会话的 runtime——无 Binding 调用方(CLI 命令、stdio、
// 测试、后台 Run 的嵌套命令)的默认解析点:内部即 p.current.Load(),nil = 尚无
// 活动会话(Init 未完成或 Init 失败回滚),调用方按约定报「no active session」。
// PR2(总方案 §六)起标准 ACP 的会话执行路由不再直接取这里——连接级选择经
// SessionBinding 流入 withSession(binding 优先、空则回落本解析点),SessionSource
// 的元数据 RPC 面(SetSession/session/list/load 判定)也仍是 current 语义。
func (p *AgentPlugin) currentSession() *runtimeagent.SessionRuntime {
	return p.current.Load()
}

// boundSession 返回当前会话本体(currentSession().Session() 的 nil 防护组合)：
// 无活动 runtime 或 runtime 未绑定 Session 时返回 nil，取数点据此跳过会话操作。
func (p *AgentPlugin) boundSession() *runtimeagent.Session {
	if rt := p.currentSession(); rt != nil {
		return rt.Session()
	}
	return nil
}

// resolvedSessionKey 是 withSession 注入「已解析会话 runtime」的 ctx 键(包内
// 私有,与 sessionGateKey 同型):解析与互斥必须同源——withSession 按 ctx 的
// Binding 解析出 runtime 并在其 gate 上排队后,闭包内的全部取数点(sessionRuntimeIn/
// boundSessionIn/currentStoreIn)读同一注入值,不再各自重解析(重读 current 会把
// /new 换轨窗口劈进闭包;注入值在 gate 保护下整个命令回合稳定)。注入值经
// Session.Run 的 ctx 链(toolCtx)流入嵌套 hub.Eval(commands.go sessionGateKey
// 同路),嵌套命令解析到同一 runtime——嵌套 /new 作用于发起 run 的会话,与
// 单活动会话模型的既有语义一致。
type resolvedSessionKey struct{}

// injectResolvedSession 把解析出的 runtime 挂上命令回合的 ctx(仅 withSession
// 调用;rt 已判非 nil)。
func injectResolvedSession(ctx context.Context, rt *runtimeagent.SessionRuntime) context.Context {
	return context.WithValue(ctx, resolvedSessionKey{}, rt)
}

// sessionRuntimeIn 是 binding 优先的统一会话解析点(PR2,总方案 §六;currentSession
// 的 binding 形态)。解析序:
//  1. withSession 注入的已解析 runtime(命令回合内):直接返回——解析与互斥
//     同源,闭包内不重解析;
//  2. ctx 携带的连接级 SessionBinding(agentcontrol.BindingFromCtx):非空 →
//     RouteSession 同源解析(binding id 命中 current.ID → current runtime;否则
//     manager.Get 注册表权威,CR4 多会话路由);命中即返回。
//  3. 无 Binding 或 Binding 为空(未选择/已清除)→ currentSession()回落:无
//     Binding 调用方(CLI/stdio/测试)的原语义,单连接行为不变。
//
// Binding 非空但解析不到 runtime(/new 收口摘除了旧 runtime)→ 显式错误(包裹
// ErrSessionNotFound,与路由面同文案):诚实失败,绝不静默回落 current——那会把
// 请求串到别的会话的工作区,正是 superseded 守卫要防的形态。
func (p *AgentPlugin) sessionRuntimeIn(ctx context.Context) (*runtimeagent.SessionRuntime, error) {
	// nil ctx 防护:model.Context.Ctx 可为 nil(既有测试直接构造 Context),语义
	// 等价无绑定调用方,回落 current。
	if ctx == nil {
		return p.currentSession(), nil
	}
	if rt, ok := ctx.Value(resolvedSessionKey{}).(*runtimeagent.SessionRuntime); ok && rt != nil {
		return rt, nil
	}
	b := agentcontrol.BindingFromCtx(ctx)
	if b == nil {
		return p.currentSession(), nil
	}
	if id := b.Get(); id != "" {
		if cur := p.currentSession(); cur != nil && id == cur.ID {
			return cur, nil
		}
		if rt := p.manager.Get(id); rt != nil {
			return rt, nil
		}
		return nil, sessionNotFoundErr(id)
	}
	return p.currentSession(), nil
}

// boundSessionIn 是 boundSession 的 binding 形态(sessionRuntimeIn 的会话本体
// 防护组合):命令回合闭包内取「本回合解析的会话本体」。
func (p *AgentPlugin) boundSessionIn(ctx context.Context) *runtimeagent.Session {
	if rt, err := p.sessionRuntimeIn(ctx); err == nil && rt != nil {
		return rt.Session()
	}
	return nil
}

// currentStoreIn 是 currentStore 的 binding 形态:按命令回合解析的 runtime 取其
// 会话 store(经 storeFor → repo 实例唯一缓存,与 current 取数同链)。
func (p *AgentPlugin) currentStoreIn(ctx context.Context) *session.Store {
	rt, err := p.sessionRuntimeIn(ctx)
	if err != nil {
		return nil
	}
	return p.storeFor(rt)
}

// sessionCWDErrorIn 是 sessionCWDError 的 binding 形态(handleAgent 的 prompt
// 入口守卫):查命令回合解析的 runtime 最近一次 cwd 声明的校验错误。Binding
// 悬空(session 已被 /new 收口)按错误返回——会话不存在即无处执行,与路由面
// 同一诚实语义。
func (p *AgentPlugin) sessionCWDErrorIn(ctx context.Context) error {
	rt, err := p.sessionRuntimeIn(ctx)
	if err != nil {
		return err
	}
	if rt != nil {
		_, err := rt.DeclaredCWD()
		return err
	}
	return nil
}

// currentStore 返回当前会话的持久化 store——P1-3 的统一取数解析点(无 Binding
// 调用方的默认形态;binding 形态见 currentStoreIn)。Session 本体
// 不持 store（deps.Store 仅构造时注入），解析链是：runtime id 在会话边界上绑定
// store 的 SessionID（startSession ④），repo.Open 按 id 命中实例唯一缓存，返回的
// 正是 startSession 打开/创建的那个 *Store——取数与互斥同源于 current 解析，不再
// 单独持有 store 指针（旧 atomic.Pointer[session.Store] 字段随 P1-3 退役，/new
// 换轨只剩 current 一个切换点）。legacy 槽被 Archive 轮转后 store 的 metadata id
// 会更换，但缓存键仍是挂载时的 id，解析不受影响。nil = 持久化禁用/无活动会话，
// 调用方按旧文案降级（/rewind 提示未启用、清单返回空、/new 兜底全量快照）。
func (p *AgentPlugin) currentStore() *session.Store {
	return p.storeFor(p.currentSession())
}

// storeFor 是按 runtime 解析其会话 store 的统一底座(currentStore/currentStoreIn
// 共用,PR2 从 currentStore 提取):解析链与 currentStore 注释同源——rt.ID 即
// 会话边界绑定的 store id,repo.Open 命中实例唯一缓存;未命中即纯内存降级或仓库
// 已收尾,按无持久化处理。rt 为 nil 直接返回 nil。
func (p *AgentPlugin) storeFor(rt *runtimeagent.SessionRuntime) *session.Store {
	if rt == nil || p.repo == nil {
		return nil
	}
	st, err := p.repo.Open(rt.ID)
	if err != nil {
		// 正常路径是缓存命中（runtime id 即 store id）；未命中即纯内存降级
		// （runtime id 非 store id）或仓库已收尾，按无持久化处理，与旧版
		// store=nil 一致。
		return nil
	}
	return st
}

func (p *AgentPlugin) Name() string { return "agent" }

func (p *AgentPlugin) Init(h *model.Hub) error {
	p.hub = h
	p.mcpClients = make(map[string]mcp.Client)

	// Init 中途失败要回收已连接的 MCP 进程 —— Engine.Register 在 Init 报错时不会调用 Stop
	ok := false
	defer func() {
		if !ok {
			p.closeMCP()
			if h.UnregisterCapability != nil {
				h.UnregisterCapability(model.SessionSourceCapability)
			}
		}
	}()

	h.RegisterCommand(model.Command{
		Name:        "/agent",
		Aliases:     []string{"/llm", "/ai"},
		Description: "与 AI Agent 对话",
		Handler:     p.handleAgent,
	})

	h.RegisterCommand(model.Command{
		Name:        "/model",
		Description: "查看/设置 LLM 配置，用法见 /model help",
		Handler:     p.handleModel,
	})

	h.RegisterCommand(model.Command{
		Name:        "/usage",
		Description: "查看当前会话的 token 用量明细",
		Handler:     p.handleUsage,
	})

	h.RegisterCommand(model.Command{
		Name:        "/compact",
		Description: "用 LLM 压缩对话历史，释放上下文窗口",
		Handler:     p.handleCompact,
	})

	h.RegisterCommand(model.Command{
		Name:        "/history",
		Description: "查看当前会话的对话历史内容",
		Handler:     p.handleHistory,
	})

	h.RegisterCommand(model.Command{
		Name:        "/skills",
		Description: "查看已加载的 skill 列表",
		Handler:     p.handleSkills,
	})

	h.RegisterCommand(model.Command{
		Name:        "/mcp",
		Description: "查看 MCP server 连接状态与已加载工具",
		Handler:     p.handleMCP,
	})

	h.RegisterCommand(model.Command{
		Name:        "/memory",
		Description: "管理长期记忆（LTM）与任务档案（MTM），用法见 /memory help",
		Handler:     p.handleMemory,
	})

	h.RegisterCommand(model.Command{
		Name:        "/task",
		Description: "管理任务档案：固化/查看/开合 task，用法见 /task help",
		Handler:     p.handleTask,
	})

	h.RegisterCommand(model.Command{
		Name:        "/new",
		Description: "End current session: consolidate memory → clear STM → inject reminders next run",
		Handler:     p.handleNew,
	})

	h.RegisterCommand(model.Command{
		Name:        "/rules",
		Description: "Show the effective declarative instructions (AGENTS.md + USER.md injection snapshot)",
		Handler:     p.handleRules,
	})

	// /rewind 回溯到历史检查点（会话持久化 store 启用时可用）。定义在 rewind.go。
	p.registerRewindCommand(h)

	// 打开记忆存储。坏目录只降级为无记忆，不阻塞插件启动
	if p.cfg.Memory.Enabled {
		ms, err := memory.NewClient(p.cfg.Memory.Dir, p.cfg.Memory.TaskKeep)
		if err != nil {
			slog.Warn("memory store init failed, memory disabled", "err", err)
		} else {
			p.memory = ms
		}
	}

	// 开始session：加载 skill、连接 MCP server、拉取工具清单、构造声明式指令快照、创建首会话。
	p.startSession()

	// 未匹配的任何命令都转发给 agent 处理
	h.SetFallbackHandler(p.handleAgent)
	if h.RegisterCapability != nil {
		if err := h.RegisterCapability(model.SessionSourceCapability, p); err != nil {
			return fmt.Errorf("agent: register session capability: %w", err)
		}
	}

	ok = true
	return nil
}

func (p *AgentPlugin) Start() error { return nil }

func (p *AgentPlugin) Stop() error {
	if p.hub != nil && p.hub.UnregisterCapability != nil {
		p.hub.UnregisterCapability(model.SessionSourceCapability)
	}
	// MCP server 是外部进程，退出时统一回收，避免残留孤儿进程
	p.closeMCP()
	if p.memory != nil {
		// 进程退出 = 会话结束，把当前 conversation 固化进任务档案，供下次会话恢复。
		// 同步等一次 LLM（带超时），否则后台 goroutine 会被进程退出杀掉，固化直接丢。
		// LLM 网关慢时 20s 容易超时丢历史，放宽到 120s 给足时间。
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		// Client.Checkpoint 内部已打 applied 日志，这里不重复。nil 判空：Init 失败
		// 回滚路径下可能尚无会话（Engine.Register 在 Init 报错后不调用 Stop，但
		// 进程收尾等路径仍可能走到这里），直接解引用会 panic。
		if s := p.boundSession(); s != nil {
			if _, err := s.Consolidate(ctx); err != nil {
				slog.Warn("memory: stop checkpoint failed", "err", err)
			}
		}
		cancel()
		if err := p.memory.Close(); err != nil {
			slog.Warn("memory close error", "err", err)
		}
	}
	// store 生命周期收尾（P1-3）：consolidate 后关闭当前会话 store 与仓库缓存的
	// 全部实例——/new 换轨后旧 store 实例滞留在 repo.open 缓存（实例唯一不变量
	// 要求其存活），统一在此回收。两级都幂等：currentStore 与 repo 缓存常为同一
	// 实例，Store.Close 对已关闭/未持锁 store 为 no-op（store.go Close 对 nil file
	// 安全）；repo/current 为 nil（纯内存、Init 未完成或失败回滚）时整体跳过。
	if st := p.currentStore(); st != nil {
		if err := st.Close(); err != nil {
			slog.Warn("session: store close failed", "err", err)
		}
	}
	if p.repo != nil {
		if err := p.repo.Close(); err != nil {
			slog.Warn("session: repository close failed", "err", err)
		}
	}
	return nil
}

// closeMCP 关闭所有已连接的 MCP client。Stop 和 Init 失败回滚共用。
func (p *AgentPlugin) closeMCP() {
	for name, c := range p.mcpClients {
		if err := c.Close(); err != nil {
			slog.Warn("mcp close error", "server", name, "err", err)
		}
	}
	p.mcpClients = make(map[string]mcp.Client)
}

func (p *AgentPlugin) persistAllowRule(rule sandbox.AllowRule) error {
	p.allowRuleMu.Lock()
	defer p.allowRuleMu.Unlock()

	match := strings.Join(rule.MatchTokens, " ")
	if match == "" {
		return fmt.Errorf("allow_always: empty command rule")
	}
	candidate := sandbox.AllowRuleConfig{
		Match:   match,
		MaxRisk: rule.MaxRisk.String(),
		Effects: rule.Effects.Names(),
	}
	for _, existing := range p.cfg.Sandbox.AllowRules {
		if existing.Match == candidate.Match && existing.MaxRisk == candidate.MaxRisk && strings.Join(existing.Effects, ",") == strings.Join(candidate.Effects, ",") {
			return nil
		}
	}

	previous := append([]sandbox.AllowRuleConfig(nil), p.cfg.Sandbox.AllowRules...)
	p.cfg.Sandbox.AllowRules = append(p.cfg.Sandbox.AllowRules, candidate)
	if p.hub == nil || p.hub.SaveConfig == nil {
		p.cfg.Sandbox.AllowRules = previous
		return fmt.Errorf("allow_always: config persistence is unavailable")
	}
	if err := p.hub.SaveConfig(p.cfg); err != nil {
		p.cfg.Sandbox.AllowRules = previous
		return fmt.Errorf("allow_always: save config: %w", err)
	}
	return nil
}

func (p *AgentPlugin) persistToolAllowRule(rule sandbox.ToolAllowRule) error {
	p.allowRuleMu.Lock()
	defer p.allowRuleMu.Unlock()

	if strings.TrimSpace(rule.Key) == "" {
		return fmt.Errorf("allow_always: empty tool key")
	}
	candidate := sandbox.ToolAllowRuleConfig{
		Key:     rule.Key,
		MaxRisk: rule.MaxRisk.String(),
		Desc:    rule.Desc,
	}
	for _, existing := range p.cfg.Sandbox.ToolAllowRules {
		if existing.Key == candidate.Key && existing.MaxRisk == candidate.MaxRisk && existing.Desc == candidate.Desc {
			return nil
		}
	}

	previous := append([]sandbox.ToolAllowRuleConfig(nil), p.cfg.Sandbox.ToolAllowRules...)
	p.cfg.Sandbox.ToolAllowRules = append(p.cfg.Sandbox.ToolAllowRules, candidate)
	if p.hub == nil || p.hub.SaveConfig == nil {
		p.cfg.Sandbox.ToolAllowRules = previous
		return fmt.Errorf("allow_always: config persistence is unavailable")
	}
	if err := p.hub.SaveConfig(p.cfg); err != nil {
		p.cfg.Sandbox.ToolAllowRules = previous
		return fmt.Errorf("allow_always: save config: %w", err)
	}
	return nil
}

func (p *AgentPlugin) startSession() {
	// ① 打开会话持久化仓库（P1-3：每会话独立目录 + legacy 兼容挂载）。失败降级
	// 不阻塞插件启动。进程内只开一次（首个会话边界即 Init），/new 复用同一仓库：
	// 实例唯一缓存跨边界连续，重开会被缓存清空 + 跨进程文件锁拒绝。
	if p.repo == nil && p.cfg.Session.Enabled {
		repo, err := session.OpenRepository(p.cfg.Session.Dir)
		if err != nil {
			slog.Warn("session: repository open failed, persistence disabled", "err", err)
		} else {
			p.repo = repo
		}
	}

	// ② 解析本会话边界的 store：首个边界（Init，current 尚为 nil）对已有 legacy
	// 槽原地挂载，维持旧版「打开 <dir>/current.jsonl」的启动行为（用户数据不动）；
	// /new 驱动的边界一律 Create 独立目录新会话（新会话一律进 <root>/<id>/，
	// repository 决策 2）——若仍挂载刚被 Archive 轮转出的空槽，repo.Open 会经同
	// 目录别名规则复用旧实例，「新」会话拿不到独立目录。打开/创建失败降级纯内存。
	var st *session.Store
	if p.repo != nil {
		st = p.openSessionStore(p.currentSession() == nil)
	}

	// P2-7 启动恢复扫描（总方案第九节「Daemon 重启后」）：本会话边界的 store 若
	// 挂着上一进程遗留的非终态 run journal（runs/*.jsonl 末记录 queued/running），
	// 追加 interrupted 标记——恢复绝不重跑（纯标记 + 尾截断，幂等，P2-2 保证）。
	// 失败仅 warn 不阻塞启动；有标记时 warn 提示（用户可见行为不变：只进日志，
	// 不进任何前端通道）。每个会话边界（Init / /new）执行一次。
	if p.repo != nil && st != nil {
		if marked, err := p.repo.MarkInterruptedRuns(context.Background(), st.SessionID()); err != nil {
			slog.Warn("session: mark interrupted runs failed", "err", err)
		} else if len(marked) > 0 {
			slog.Warn("session: marked interrupted runs from previous process", "count", len(marked), "session", st.SessionID(), "runs", strings.Join(marked, ","))
		}
	}

	// 加载 skill：用户级 + 项目级（后者覆盖前者）。坏 skill 跳过并记录，不拖垮其他的
	discovered, skillErrs := skills.Discover(
		p.paths.SkillsUser,
		p.paths.SkillsProject,
	)
	for _, err := range skillErrs {
		slog.Warn("skill load error, skipping", "err", err)
	}
	p.skills = discovered

	// 连接 MCP server 并拉取工具清单。坏 server 只降级不阻塞插件启动
	p.loadMCP()

	// 创建 httpclient
	client := p.createhHttpClient()

	// 会话层组装：资源就绪后构造 deps 与首会话。

	modelProvider := acpworker.ProviderFactory(client)

	p.deps = runtimeagent.SessionDeps{
		Config:               p.cfg,
		AuditDir:             p.paths.AuditDir,
		Memory:               p.memory,
		CollectTools:         p.collectTools,
		NewProvider:          modelProvider,
		PersistAllowRule:     p.persistAllowRule,
		PersistToolAllowRule: p.persistToolAllowRule,
	}
	// P0-3 起 deps.Store 是 ConversationStore 接口：仅持久化启用且解析成功时注入。
	// 不能写 Store: st —— st 为 nil 时会塞进 typed-nil（接口持 nil 指针），Session
	// 的 `deps.Store == nil` 纯内存判断随之失效。P2-7 起 RunJournal 同纪律：仅
	// st 非 nil 时注入 *session.RunJournalStore（按 runID 委派 repo.OpenRunJournal，
	// 句柄随 run 终态关闭）；纯内存（st == nil）不设，与 deps.Store 的 typed-nil
	// 纪律一致。编译期断言在 runjournal_wiring_test.go。
	//
	// P5-4 起 RunEvents 与 RunJournal 注入同一个 *session.RunJournalStore 实例
	//（P3-1 起该类型同时实现 AppendState/AppendEvent/ReadEvents/SeqRange，编译断言
	// event_hub.go:62）——事件持久化与 run 状态 journal 同文件同源。typed-nil 纪律
	// 同 spec.go:96：st == nil 不设，RunEvents 的「nil = 不启用事件流」判断保持真
	// nil。零行为：bindEvents 仅 StartRun 触发（supervisor.go:147），daemon 现无
	// StartRun 调用方；同步 RecordRun 路径不建 hub、不写事件（P5-6/7 起
	// x-goworker 路径改变）。
	if st != nil {
		p.deps.Store = st
		journalStore := session.NewRunJournalStore(p.repo, st)
		p.deps.RunJournal = journalStore
		p.deps.RunEvents = journalStore
	}
	// 指令快照与记忆组件同开关：memory disabled 时留 nil，buildSystemPrompt 跳过指令段。
	// 会话边界（Init / /new）经 startSession 重载 —— system prompt 在会话创建前就绪。
	if p.cfg.Memory.Enabled {
		p.deps.Instruction = p.loadInstructions()
	}
	// 多会话内核接线（P0-4/P1-3）：每会话一个 SessionRuntime，构造后注册进 manager
	// 并置为 current。P1-3 起 id 取 store 的真实 SessionID——它是 currentStore()
	// 经 repo 实例唯一缓存解析取数的键，新旧 runtime 以不同 id 共存（旧 runtime
	// 保留在册，归档固化窗口内仍可经 manager.Get 触达）；纯内存降级（无 store）
	// 维持 "current" 稳定单例，/new 重建时同 id 注册冲突、旧条目保留。
	s := runtimeagent.NewSession(p.deps)
	id := "current"
	if st != nil {
		id = st.SessionID()
	}
	rt := runtimeagent.NewSessionRuntime(id, s)

	// 会话重建 = 新的会话边界。cwd 声明走 runtime 级迁移（P1-4）：旧 current
	// runtime 的有效声明迁移到新 runtime 并应用到新会话 —— 工作区没变，/new 不该
	// 让工具悄悄回退到别的目录；无效声明作废（新 runtime 零值起步，cwdErr 不随
	// 边界存活），否则会毒化新会话的每个 prompt。Register/NewSessionRuntime 不
	// 继承声明，迁移只能在这里显式做。取舍不变：不追踪「声明了 cwd 的 transport
	// 会话与 store 会话的对应关系」，单活动会话模型下一律落当前会话。
	// activeTransportSession 有意不清：/new 只是 REPL 内换掉运行时会话对象，
	// transport 会话的声明（连同其活动身份）依旧成立，prompt 它不算 superseded。
	var carriedCwd string
	if old := p.currentSession(); old != nil { // Init 首边界无旧 runtime,即无声明可迁
		if cwd, err := old.DeclaredCWD(); err == nil {
			carriedCwd = cwd
			rt.DeclareCWD(cwd, nil)
		}
	}
	if err := p.manager.Register(rt); err != nil {
		slog.Debug("session: keep previous runtime registered", "id", rt.ID, "err", err)
	}
	p.current.Store(rt)
	// s 即刚注册进 current runtime 的会话本体（等价 p.currentSession().Session()）。
	s.SetCWD(carriedCwd)
}

// openSessionStore 在仓库里解析本会话边界的 store（P1-3 startSession ②）。
// firstCycle 标记进程首个会话边界（Init，此刻 currentSession() 仍为 nil）：
//   - firstCycle 且 root 下已有 legacy 槽（current.jsonl）→ 原地挂载：id 经只读
//     OpenArchiveView 读出（解析链与 repo.Open 的 legacy 分支同源——metadata 记录
//     优先、缺失时按首节点/内容摘要合成 legacy id），不触碰写句柄；repo.Open
//     命中后进实例唯一缓存。旧版启动行为（打开 <dir>/current.jsonl 并恢复对话）
//     由此保持，现有用户数据不移动（总方案 P1 硬性要求）。
//   - 其余（无 legacy 槽的首次启动、全部 /new 边界）→ Create 独立目录新会话
//     （<root>/<id>/，目录名 == 文件内 SessionID）。
//
// 失败返回 nil（调用方降级纯内存，不阻塞会话创建）。
func (p *AgentPlugin) openSessionStore(firstCycle bool) *session.Store {
	if firstCycle {
		legacyPath := filepath.Join(p.cfg.Session.Dir, "current.jsonl")
		if _, err := os.Stat(legacyPath); err == nil {
			view, err := session.OpenArchiveView(legacyPath)
			if err != nil {
				slog.Warn("session: legacy slot inspect failed, persistence disabled", "err", err)
				return nil
			}
			st, err := p.repo.Open(view.SessionID())
			if err != nil {
				slog.Warn("session: legacy store open failed, persistence disabled", "err", err)
				return nil
			}
			return st
		}
	}
	st, err := p.repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		slog.Warn("session: store create failed, persistence disabled", "err", err)
		return nil
	}
	return st
}

// SetSession 记录 ACP 提交方声明的会话工作目录并应用到当前会话（单活动会话模型：
// transport sessionID 与 store 的 head id 不同源，cwd 一律落当前活动会话）。
//
// sandbox 边界在插件侧校验：cfg.Sandbox.AllowedWorkDir 非空时，声明的 cwd 必须
// 落在它之下（Abs + EvalSymlinks 解析后判断前缀，符号链接与相对路径钉到真实
// 位置）；非法声明存为无效标记（P1-4 起落 current runtime 的 cwdErr，cwd 随之
// 清空），session/prompt 时返回明确错误，不让越界目录静默生效。AllowedWorkDir
// 为空 = 不限制，接受任意 cwd；cwd 为空 = 未声明，清空会话目录回退现行为。
func (p *AgentPlugin) SetSession(sessionID, cwd string) {
	cwd = strings.TrimSpace(cwd)
	if cwd != "" {
		if abs, err := filepath.Abs(cwd); err == nil {
			cwd = abs // cmd.Dir 要求绝对路径；SessionInfo.Cwd 也回显规范化值
		}
	}
	// 声明与应用取同一 current 快照：P1-4 起声明落 runtime（SessionRuntime.cwd/
	// cwdErr），会话本体绑定其上——分开解析可能在 /new 换轨瞬间把声明与应用
	// 劈到两个 runtime。
	rt := p.currentSession()
	if err := p.validateSessionCWD(cwd); err != nil {
		// 无效标记落 current runtime（cwd 清空 + err 记账）。无 runtime（Init 未
		// 跑完/失败回滚）时无处落声明，直接丢弃——该窗口内 prompt 本就被
		// withSession 的「no active session」挡住，标记没有可守之门。
		if rt != nil {
			rt.DeclareCWD("", err)
		}
		slog.Warn("agent: reject session cwd", "session", sessionID, "cwd", cwd, "err", err)
		return
	}
	p.mu.Lock()
	// 有效声明（含空 cwd 的「未声明」重置）才转移活动会话标记。取舍：无效声明
	// 不能顶掉别连接的好会话——B 连接的坏 session/new 若同时转移 active，A 的
	// 好会话会被顶成 superseded 而无法 prompt；保持不动则与 prompt 侧的 cwdErr
	// 防线一致（B 的 prompt 本来就会被拒）。
	p.activeTransportSession = sessionID
	p.mu.Unlock()
	// 无活动 runtime（Init 未跑完/失败回滚）时声明无处落、会话不存在，无事可做：
	// 正常路径上首个 runtime 在 Init 创建，任何 ACP 声明都晚于它。
	if rt == nil {
		return
	}
	rt.DeclareCWD(cwd, nil)
	if s := rt.Session(); s != nil {
		s.SetCWD(cwd)
	}
}

// ActiveSessionID 实现 model.SessionSource 的 transport 活动会话语义：返回最近
// 一次经 session/new|load 有效声明的 transport 会话 id（锁内快照，空 = 未声明）。
// 注意与 CurrentSessionID 的区别：那边是 store 的稳定逻辑 id，这边是连接方
// 自造的会话句柄；FrontendHandler.Prompt 用它做 superseded 守卫。
func (p *AgentPlugin) ActiveSessionID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.activeTransportSession
}

// validateSessionCWD 校验声明的 cwd 是否落在 sandbox 边界内。
// allowed 为空 = 不限制；cwd 为空 = 未声明，均直接放行。
func (p *AgentPlugin) validateSessionCWD(cwd string) error {
	allowed := ""
	if p.cfg != nil {
		allowed = p.cfg.Sandbox.AllowedWorkDir
	}
	if allowed == "" || cwd == "" {
		return nil
	}
	resolvedCwd, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return fmt.Errorf("invalid session cwd %q: %w", cwd, err)
	}
	absAllowed, err := filepath.Abs(allowed)
	if err != nil {
		return fmt.Errorf("invalid session cwd %q: sandbox boundary %q: %w", cwd, allowed, err)
	}
	resolvedAllowed, err := filepath.EvalSymlinks(absAllowed)
	if err != nil {
		return fmt.Errorf("invalid session cwd %q: sandbox boundary %q: %w", cwd, allowed, err)
	}
	if resolvedCwd != resolvedAllowed && !strings.HasPrefix(resolvedCwd, resolvedAllowed+string(os.PathSeparator)) {
		return fmt.Errorf("invalid session cwd %q: outside sandbox boundary %q", cwd, allowed)
	}
	return nil
}

// sessionCWDError 返回当前会话 runtime 最近一次 cwd 声明的校验错误（无活动
// runtime、无声明或已通过 = nil）。session/prompt 入口（handleAgent）先查它：
// 无效声明下不执行任何工具。P1-4 起声明随 current runtime 走——/new 后无效
// 标记随旧 runtime 作废，不会毒化新会话。
func (p *AgentPlugin) sessionCWDError() error {
	if rt := p.currentSession(); rt != nil {
		_, err := rt.DeclaredCWD()
		return err
	}
	return nil
}

/**
 * 创建 http.Client：GOWORKER_PROXY 显式指定抓包代理时走它，否则回退 http.ProxyFromEnvironment。
 */
func (p *AgentPlugin) createhHttpClient() *http.Client {
	// GOWORKER_PROXY 显式指定抓包代理（如 whistle）时走它，否则回退
	// http.ProxyFromEnvironment：尊重系统 HTTP(S)_PROXY，没配就直连，
	// 行为跟不设置 Transport 的默认 http.Client 一致。
	// 抓包代理是 MITM，证书链必然校验不过，所以代理一旦显式配置就跳过
	// TLS 校验——只在这条调试路径生效，生产不设 GOWORKER_PROXY 保持严格校验。
	proxyFn := http.ProxyFromEnvironment
	tlsConfig := &tls.Config{}
	if proxyURL := strings.TrimSpace(os.Getenv("GOWORKER_PROXY")); proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			slog.Warn("GOWORKER_PROXY 解析失败，回退系统代理", "proxy", proxyURL, "err", err)
		} else {
			proxyFn = http.ProxyURL(u)
			tlsConfig = &tls.Config{InsecureSkipVerify: true}
		}
	}

	return &http.Client{
		Timeout: defaultHTTPTimeout,
		Transport: &http.Transport{
			Proxy:           proxyFn,
			TLSClientConfig: tlsConfig,
		},
	}
}
