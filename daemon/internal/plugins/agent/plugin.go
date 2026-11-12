// Package service adapts ai-runtime sessions to the daemon's model.Plugin interface.
// It assembles resources and session runtimes, exposes protocol capabilities, and
// coordinates shutdown. Run-control policy lives in agentcontrol; execution lives in
// ai-runtime/agent.
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
	"strconv"
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
	// manager 注册每个会话的 runtime，并提供执行 gate 与 run 生命周期管理。Stop 时
	// 通过 manager.Close 收口全部 run。
	manager *runtimeagent.SessionManager
	// current 是无 binding 调用方（CLI、stdio、测试）的默认会话，也供生命周期逻辑
	// 使用；标准 ACP 请求按连接级 binding 路由。atomic.Pointer 保证跨 goroutine
	// 换轨与读取安全。
	current atomic.Pointer[runtimeagent.SessionRuntime]
	// deps 是 Session 的资源依赖，startSession 每次会话边界全量重建（含指令快照
	// 与当前 store 注入），/new 经同一路径刷新后以新 deps 创建新会话。
	deps runtimeagent.SessionDeps
	// repo caches one store instance per session across boundaries. It is initialized during
	// Init before concurrent readers exist; nil means persistence is disabled.
	repo       *session.Repository
	skills     []skills.Skill        // Init 时加载的 skill 清单，注册为 skill_* 工具
	mcpClients map[string]mcp.Client // server name → 连接，Stop 时统一关闭
	mcpTools   []core.Tool           // 从已连接 server 拉取的工具（静态，collectTools 复用）
	memory     *memory.Client        // 记忆组件（MTM+LTM），Init 打开 / Stop 关闭；nil = 禁用

	// mu 只保护 activeTransportSession:ACP 提交方经 session/new|load 声明的会话
	// cwd 已随状态绑定收口下沉到当前 runtime(SessionRuntime.cwd/cwdErr,
	// 每会话一份、经 currentSession() 解析),插件不再持有跨会话的 cwd 单值。
	mu          sync.Mutex
	allowRuleMu sync.Mutex
	// activeTransportSession 是最近一次有效声明的 transport 会话 id(连接方自造
	// 的 UUID,与 store 的稳定 id 不同源)。空 = 尚无 ACP 会话声明。它已是
	// 零生产调用方的兼容残留(ActiveSessionID 注释):原消费方 Prompt 的全局
	// 回落守卫已删除,写入点只剩 SetSession(接口兑现面),字段随接口保留。
	activeTransportSession string

	// control 是应用内唯一的 agentcontrol.Service。run 控制能力委托给它；本插件
	// 提供当前会话路由和 journal。锁序为 lease.mu → registry.mu → manager，
	// 跨层路径的 h.mu → session lease 也保持单向。
	control *agentcontrol.Service

	// inFlight tracks external maintenance requests, which may be queued at a session gate.
	// Stop closes admission, closes the manager, waits for these requests, and then releases
	// resources. Nested run evaluation is covered by manager.Close and is not tracked.
	inFlight inFlightTracker

	// suspendMu/suspendedReaps 记录引用归零但仍有活动 run 的会话。run 终态回调
	// 仅对台账中的会话重试收口，避免普通 run 结束时误释放 current 会话。
	// lifecycleMu serializes binding changes, delayed reaping, and run completion handling.
	lifecycleMu    sync.Mutex
	suspendMu      sync.Mutex
	suspendedReaps map[string]struct{}

	// bgFinalizers 计数插件派生的后台收尾任务。Stop 在关闭 memory 前等待它们完成。
	bgFinalizers sync.WaitGroup
}

// NewPlugin builds the agent from injected runtime configuration and host paths. Session
// runtimes are not created here: startSession registers them at Init and /new boundaries;
// before that, commands report "no active session".
func NewPlugin(cfg *runtimeconfig.Config, paths runtimeconfig.Paths) *AgentPlugin {
	// Manager options apply configured run quotas, publish the gate reentry marker into
	// run contexts, and register the terminal-run hook. The reentry marker lets a run invoke
	// same-session commands without requeuing on its own gate.
	p := &AgentPlugin{cfg: cfg, paths: paths}
	p.manager = runtimeagent.NewSessionManager(
		runtimeagent.WithRunLimits(cfg.Session.MaxConcurrentRuns, cfg.Session.MaxPendingRuns),
		runtimeagent.WithRunContext(runtimeagent.RunContextValue{Key: sessionGateKey{}, Value: p}),
		// run 终态通知面——「有未完 run 时延迟到 run 终态再关」的挂起收口由它
		// 唤醒(lifecycle.go onSessionRunDone 只对挂起台账中的会话重判,普通 run
		// 收尾零开销)。闭包捕获 p:此刻 p 已构造完成,回调只在 run 终态后才执行,
		// 无初始化次序问题。
		runtimeagent.WithRunDoneHook(p.onSessionRunDone),
	)
	// Assemble the shared application control plane used by both plugin and ACP; separate
	// lease or request registries are forbidden. This plugin supplies routing and journal
	// lookup, and nil current is safe because the first session is created later in Init.
	p.control = agentcontrol.NewService(p.manager, p, p)
	return p
}

// currentSession 返回当前会话的 runtime——无 Binding 调用方(CLI 命令、stdio、
// 测试、后台 Run 的嵌套命令)的默认解析点:内部即 p.current.Load(),nil = 尚无
// 活动会话(Init 未完成或 Init 失败回滚),调用方按约定报「no active session」。
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

// resolvedSessionKey carries the runtime selected for a command round so nested
// operations use the same session as the gate.
type resolvedSessionKey struct{}

// injectResolvedSession 把解析出的 runtime 挂上命令回合的 ctx(仅 withSession
// 调用;rt 已判非 nil)。
func injectResolvedSession(ctx context.Context, rt *runtimeagent.SessionRuntime) context.Context {
	return context.WithValue(ctx, resolvedSessionKey{}, rt)
}

// sessionRuntimeIn resolves the round's runtime, preferring a binding and falling back
// to current only when no binding exists; a dangling binding is an explicit error.
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

// currentStore returns the current runtime's persistent store. Disabled persistence, a
// missing runtime, and stale post-shutdown references all resolve to nil.
func (p *AgentPlugin) currentStore() *session.Store {
	return p.storeFor(p.currentSession())
}

// storeFor returns the store cached for the runtime's ID. Peek is intentionally cache-only:
// a miss means in-memory operation, an unknown ID, or repository shutdown, so callers must
// not reopen and revive a store. Nil runtimes or repositories also return nil.
func (p *AgentPlugin) storeFor(rt *runtimeagent.SessionRuntime) *session.Store {
	if rt == nil || p.repo == nil {
		return nil
	}
	st, ok := p.repo.Peek(rt.ID)
	if !ok {
		// 缓存未命中：纯内存降级（runtime id 非 store id 的历史旧形态）、
		// 仓库已收尾（Stop 后的 stale 引用）。按无持久化处理，与旧版 store=nil
		// 一致——绝不走 Open 慢路径复活实例。
		return nil
	}
	return st
}

func (p *AgentPlugin) Name() string { return "agent" }

func (p *AgentPlugin) Init(h *model.Hub) error {
	p.hub = h
	p.mcpClients = make(map[string]mcp.Client)

	// Init 中途失败要回收已连接的 MCP 进程 —— Engine.Register 在 Init 报错时不会调用 Stop。
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

	// 打开记忆存储。坏目录只降级为无记忆，不阻塞插件启动。
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

	// 未匹配的任何命令都转发给 agent 处理。
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

// Stop 先关闭维护请求入口，取消并等待所有 run，再等待已登记的维护请求和后台
// 固化，随后关闭 MCP、memory、store 与 repository。等待均为协作式；不能在 run
// 仍可能写 store 时超时后强行关闭资源。外部工具需自行配置超时以保证收敛。
func (p *AgentPlugin) Stop() error {
	// 先关维护请求入口，再关闭执行平面。closeAdmission 只持 tracker 锁修改
	// 状态，随后 manager.Close 与 wait 均在锁外等待，避免互相卡住。
	p.inFlight.closeAdmission()
	if p.hub != nil && p.hub.UnregisterCapability != nil {
		p.hub.UnregisterCapability(model.SessionSourceCapability)
	}
	// manager.Close 取消宿主 runCtx，等待全部 Run 终态落盘，再收口事件枢纽并清
	// 注册表。返回后无 Run 继续写 journal/conversation。
	p.manager.Close()
	// 维护请求可能仍在 Session Gate 前排队；等它们完整返回后才可关闭其访问的
	// MCP、memory、store 与 repo 句柄。
	p.inFlight.wait()
	// MCP server 是外部进程，退出时统一回收，避免残留孤儿进程。
	p.closeMCP()
	// 等待 /new 派生的后台固化完成——固化 goroutine 是
	// memory.Client 的最后使用方之一，先于下方 memory.Close 收敛；固化不触
	// store/MCP，放在 closeMCP 之后仅是次序习惯（MCP 先收、固化后等，互不依赖）。
	p.bgFinalizers.Wait()
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
	// Close persistent stores after all run and maintenance work has stopped. The current
	// store is closed before the repository closes its remaining cached instances; both
	// operations are idempotent and skip setups without persistence.
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
	// ① 打开会话持久化仓库（每会话独立目录 + legacy 兼容挂载）。失败降级
	// 不阻塞插件启动。进程内只开一次（首个会话边界即 Init），/new 复用同一仓库：
	// 实例唯一缓存跨边界连续，重开会被缓存清空 + 跨进程文件锁拒绝。
	if p.repo == nil && p.cfg.Session.Enabled {
		repo, err := session.OpenRepository(p.cfg.Session.Dir)
		if err != nil {
			slog.Warn("session: repository open failed, persistence disabled", "err", err)
		} else {
			p.repo = repo
			// Run the one-time legacy layout migration after opening the repository but
			// before opening this boundary's store. Failures are logged without blocking startup.
			if stats, merr := session.MigrateLegacyLayout(p.cfg.Session.Dir); merr != nil {
				slog.Warn("session: legacy layout migration failed", "dir", p.cfg.Session.Dir, "err", merr)
			} else if stats != (session.LayoutMigration{}) {
				slog.Info("session: legacy layout migration done",
					"legacy_slots", stats.Legacy,
					"archived_files", stats.Archived,
					"aligned", stats.Aligned,
					"shells_removed", stats.Shells,
					"dropped", stats.Dropped)
			}
		}
	}

	// ② 解析本会话边界的 store：首个边界（Init，current 尚为 nil）对已有 legacy
	// 槽原地挂载，维持旧版「打开 <dir>/current.jsonl」的启动行为（用户数据不动）；
	// /new 驱动的边界一律 Create 独立目录新会话（新会话一律进 <root>/<id>/，
	// repository 的创建规则）。打开/创建失败降级纯内存。
	var st *session.Store
	if p.repo != nil {
		st = p.openSessionStore(p.currentSession() == nil)
	}

	// 启动恢复扫描（「Daemon 重启后」）：本会话边界的 store 若挂着上一进程遗留
	// 的非终态 run journal（runs/*.jsonl 末记录 queued/running），追加 interrupted
	// 标记——恢复绝不重跑（纯标记 + 尾截断，幂等）。失败仅 warn 不阻塞启动；
	// 有标记时 warn 提示（用户可见行为不变：只进日志，不进任何前端通道）。
	// 每个会话边界（Init / /new）执行一次。
	if p.repo != nil && st != nil {
		if marked, err := p.repo.MarkInterruptedRuns(context.Background(), st.SessionID()); err != nil {
			slog.Warn("session: mark interrupted runs failed", "err", err)
		} else if len(marked) > 0 {
			slog.Warn("session: marked interrupted runs from previous process", "count", len(marked), "session", st.SessionID(), "runs", strings.Join(marked, ","))
		}
	}

	// 加载 skill：用户级 + 项目级（后者覆盖前者）。坏 skill 跳过并记录，不拖垮其他加载流程。
	discovered, skillErrs := skills.Discover(
		p.paths.SkillsUser,
		p.paths.SkillsProject,
	)
	for _, err := range skillErrs {
		slog.Warn("skill load error, skipping", "err", err)
	}
	p.skills = discovered

	// 连接 MCP server 并拉取工具清单。坏 server 只降级不阻塞插件启动。
	p.loadMCP()

	// 创建 HTTP client。
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
	// Inject persistent conversation and journal dependencies only when st is non-nil.
	// A nil pointer assigned to these interfaces would create a typed-nil and break the
	// SDK's in-memory checks; the same journal store backs state, events, and reads.
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
	// 多会话内核接线：每会话一个 SessionRuntime，构造后注册进 manager 并置为
	// current。id 取 store 的真实 SessionID——它是 currentStore() 经 repo 实例
	// 唯一缓存解析取数的键，新旧 runtime 以不同 id 共存（旧 runtime 保留在册，
	// 归档固化窗口内仍可经 manager.Get 触达）；纯内存降级（无 store）维持
	// "current" 稳定单例，/new 重建时同 id 注册冲突、旧条目保留。
	s := runtimeagent.NewSession(p.deps)
	id := "current"
	if st != nil {
		id = st.SessionID()
	}
	rt := runtimeagent.NewSessionRuntime(id, s)

	// Preserve a valid explicit cwd across the new session boundary, but do not carry a
	// previous tmp workspace because its path is scoped to the old session ID. With no
	// valid declaration, inject a new per-session tmp workspace; failed injection leaves
	// cwd undeclared and is logged.
	var carriedCwd string
	if old := p.currentSession(); old != nil { // Init 首边界无旧 runtime,即无声明可迁
		if cwd, err := old.DeclaredCWD(); err == nil {
			carriedCwd = cwd
		}
	}
	switch {
	case carriedCwd == "" || isTmpWorkspacePath(carriedCwd):
		if tmp, err := p.injectTmpWorkspace(id); err == nil {
			carriedCwd = tmp
			rt.DeclareCWD(tmp, nil)
		} else {
			slog.Warn("agent: inject default workspace failed", "session", id, "err", err)
		}
	default:
		rt.DeclareCWD(carriedCwd, nil)
	}
	if err := p.manager.Register(rt); err != nil {
		slog.Debug("session: keep previous runtime registered", "id", rt.ID, "err", err)
	}
	p.current.Store(rt)
	// s 即刚注册进 current runtime 的会话本体（等价 p.currentSession().Session()）。
	s.SetCWD(carriedCwd)
}

// openSessionStore resolves this boundary's persistent store. On the first boundary it
// mounts an existing legacy current.jsonl, preserving startup after interrupted or failed
// layout migration; later boundaries create a per-ID store. Failure returns nil, and the
// caller degrades to in-memory operation.
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

// SetSession 记录 ACP 会话声明的工作目录并应用到当前 runtime。transport sessionID
// 与 store 的逻辑 id 不同源，因此声明绑定当前活动会话。cwd 经 sandbox 边界校验；
// 无效值保存为错误状态，后续 prompt 会显式拒绝。空 cwd 表示未声明，为该会话
// 创建独立的临时执行目录；该默认目录不作为用户声明持久化。
func (p *AgentPlugin) SetSession(sessionID, cwd string) {
	cwd = strings.TrimSpace(cwd)
	if cwd != "" {
		if abs, err := filepath.Abs(cwd); err == nil {
			cwd = abs // cmd.Dir 要求绝对路径；SessionInfo.Cwd 也回显规范化值
		}
	}
	// 声明与应用取同一 current 快照：声明落 runtime（SessionRuntime.cwd/
	// cwdErr），会话本体绑定其上——分开解析可能在 /new 换轨瞬间把声明与应用
	// 劈到两个 runtime。
	rt := p.currentSession()
	if cwd == "" {
		// 空 cwd 分支：注入缺省执行域（与 declareSessionCWD 同源，见其注释）。
		p.mu.Lock()
		p.activeTransportSession = sessionID
		p.mu.Unlock()
		if rt == nil {
			return
		}
		tmp, err := p.injectTmpWorkspace(rt.ID)
		if err != nil {
			rt.DeclareCWD("", err)
			slog.Warn("agent: inject default workspace failed", "session", rt.ID, "err", err)
			return
		}
		p.applySessionCWD(rt, tmp, false)
		return
	}
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
	// 有效声明才转移活动会话标记。取舍：无效声明不能顶掉别连接的好会话——B
	// 连接的坏 session/new 若同时转移 active，A 的好会话会被顶成 superseded 而
	// 无法 prompt；保持不动则与 prompt 侧的 cwdErr 防线一致（B 的 prompt 本来
	// 就会被拒）。
	p.activeTransportSession = sessionID
	p.mu.Unlock()
	// 无活动 runtime（Init 未跑完/失败回滚）时声明无处落、会话不存在，无事可做：
	// 正常路径上首个 runtime 在 Init 创建，任何 ACP 声明都晚于它。
	if rt == nil {
		return
	}
	p.applySessionCWD(rt, cwd, true)
}

// ActiveSessionID 返回最近一次有效 ACP 会话声明的 transport session id；它与
// CurrentSessionID 返回的 store 逻辑 id 不同。保留此接口方法以实现 model.SessionSource。
func (p *AgentPlugin) ActiveSessionID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.activeTransportSession
}

// validateSessionCWD accepts an unset policy or cwd, then checks the resolved path against
// the configured sandbox root and the default tmp workspace root. Both checks compare
// absolute paths after symlink resolution.
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
	// 主边界：解析失败按既有契约报错（boundary 不可读是配置面故障，诚实上抛）。
	ok, err := cwdUnderRoot(resolvedCwd, allowed)
	if err != nil {
		return fmt.Errorf("invalid session cwd %q: sandbox boundary %q: %w", cwd, allowed, err)
	}
	if ok {
		return nil
	}
	// 第二边界（tmp 执行域根）：best-effort——根目录尚未创建（进程内
	// 还没有任何注入）视为不命中、继续走主边界的拒绝路径，绝不让它改变既有
	// 的「outside sandbox boundary」错误形态（它不是用户配置的边界，缺席是
	// 正常态而非故障）。
	if under, err := cwdUnderRoot(resolvedCwd, tmpWorkspaceRoot()); err == nil && under {
		return nil
	}
	return fmt.Errorf("invalid session cwd %q: outside sandbox boundary %q", cwd, allowed)
}

// cwdUnderRoot 报告已解析的 cwd 是否落在 root（解析后）之下——EvalSymlinks 前缀
// 比较的公共实现（validateSessionCWD 的两个边界共用，防两份实现漂移）。
func cwdUnderRoot(resolvedCwd, root string) (bool, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return false, err
	}
	resolvedRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return false, err
	}
	return resolvedCwd == resolvedRoot || strings.HasPrefix(resolvedCwd, resolvedRoot+string(os.PathSeparator)), nil
}

// tmpWorkspaceRoot returns the per-user default workspace root under the system temp dir.
// Session workspaces occupy one <sessionID> directory beneath it.
func tmpWorkspaceRoot() string {
	root := filepath.Join(os.TempDir(), "goworker-workspaces")
	if uid := os.Getuid(); uid >= 0 {
		root = filepath.Join(root, strconv.Itoa(uid))
	}
	return root
}

// isTmpWorkspacePath 报告 cwd 是否为缺省执行域命名空间内的路径（同源前缀比较，
// 不做 symlink 解析——两侧都出自 tmpWorkspaceDir 的同一原始派生，比较口径
// 一致；沙箱校验那侧的 symlink 解析见 validateSessionCWD）。
func isTmpWorkspacePath(cwd string) bool {
	root := tmpWorkspaceRoot()
	return cwd == root || strings.HasPrefix(cwd, root+string(os.PathSeparator))
}

// tmpWorkspaceDir 返回会话的缺省执行域目录：<根>/<sessionID>/。
func tmpWorkspaceDir(sessionID string) string {
	return filepath.Join(tmpWorkspaceRoot(), sessionID)
}

// injectTmpWorkspace 为会话创建缺省执行域（MkdirAll 0700）并返回其绝对路径。
// 注入的是**执行域**而非声明：不写 workspace 记录（记录仍只落用户显式声明，
// 「清单 cwd / load 恢复以用户声明为唯一持久事实」的语义不变），
// 重启即失是有意语义（系统 tmp，避免遗留垃圾）。失败上抛，
// 调用方落 cwdErr 无效标记（prompt 诚实报错，不让静默回退到进程 cwd）。
func (p *AgentPlugin) injectTmpWorkspace(sessionID string) (string, error) {
	dir := tmpWorkspaceDir(sessionID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("agent: create default workspace %s: %w", dir, err)
	}
	return dir, nil
}

// sessionCWDError 返回当前会话 runtime 最近一次 cwd 声明的校验错误（无活动
// runtime、无声明或已通过 = nil）。session/prompt 入口（handleAgent）先查它：
// 无效声明下不执行任何工具。声明随 current runtime 走——/new 后无效
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
