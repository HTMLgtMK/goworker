// Package agent 是 goworker 宿主侧的 /agent 插件适配器：把 ai-runtime 的 Session SDK
// 装配成 plugin.Plugin 接入 daemon Engine。Session 本体在 ai-runtime/agent（SDK），
// 这里只做资源装载（skill/MCP/memory/store/指令快照）、命令注册与生命周期管理。
package agent

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	memory "github.com/tinguo/goworker/ai-memory"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/mcp"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-runtime/skills"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// defaultHTTPTimeout 是 HTTP 客户端总超时。检查点/压缩是非流式全量调用，
// 上下文一大（用户 config 里 compress_at 没开，会话无界累积）响应可能很慢，
// 30s 太紧，2min 起步。
const defaultHTTPTimeout = 2 * time.Minute

// AgentPlugin 是 /agent 插件入口，只负责三件事：持有会话状态、注册命令、管理生命周期。
// 配置与路径由 NewPlugin 构造函数注入（plugin.Hub.Config 已 any 化，插件不再从 hub 读配置）。
type AgentPlugin struct {
	cfg   *runtimeconfig.Config
	paths runtimeconfig.Paths
	hub   *plugin.Hub
	// runGate 是 agent 执行串行门禁（容量 1 的信号量）：/agent 与自然语言 fallback
	// 共用 handleAgent 入口，gate 保证同一时刻只有一个命令回合在跑（session.Run 全程互斥）。
	// 只 gate handleAgent —— /new /compact /task /memory 等 handler 绝不能获取它：
	// collectTools 会在 agent 运行期间嵌套 hub.Eval 执行这些命令，重复取锁即死锁。
	runGate chan struct{}
	// deps 是 Session 的资源依赖，startSession 每次会话边界全量重建（含指令快照），
	// /new 经同一路径刷新后以新 deps 创建新会话。
	deps runtimeagent.SessionDeps
	// session 是当前会话。NewSession 创建（Init 一次 + /new 一次）；/new 用新实例替换，
	// 旧会话状态（conversation/usage）随对象回收。
	session    *runtimeagent.Session
	store      *session.Store        // 会话持久化 store，nil = 禁用
	skills     []skills.Skill        // Init 时加载的 skill 清单，注册为 skill_* 工具
	mcpClients map[string]mcp.Client // server name → 连接，Stop 时统一关闭
	mcpTools   []core.Tool           // 从已连接 server 拉取的工具（静态，collectTools 复用）
	memory     *memory.Client        // 记忆组件（MTM+LTM），Init 打开 / Stop 关闭；nil = 禁用
}

// NewPlugin 构造 agent 插件。cfg 为 ai-runtime 运行配置，paths 为宿主注入的目录路径。
// 插件不再从 plugin.Hub.Config 读配置（Hub.Config 已 any 化）——配置与路径全部构造函数注入。
func NewPlugin(cfg *runtimeconfig.Config, paths runtimeconfig.Paths) *AgentPlugin {
	return &AgentPlugin{cfg: cfg, paths: paths, runGate: make(chan struct{}, 1)}
}

func (p *AgentPlugin) Name() string { return "agent" }

func (p *AgentPlugin) Init(h *plugin.Hub) error {
	p.hub = h
	p.mcpClients = make(map[string]mcp.Client)

	// Init 中途失败要回收已连接的 MCP 进程 —— Engine.Register 在 Init 报错时不会调用 Stop
	ok := false
	defer func() {
		if !ok {
			p.closeMCP()
		}
	}()

	h.RegisterCommand(plugin.Command{
		Name:        "/agent",
		Aliases:     []string{"/llm", "/ai"},
		Description: "与 AI Agent 对话",
		Handler:     p.handleAgent,
	})

	h.RegisterCommand(plugin.Command{
		Name:        "/model",
		Description: "查看/设置 LLM 配置，用法见 /model help",
		Handler:     p.handleModel,
	})

	h.RegisterCommand(plugin.Command{
		Name:        "/usage",
		Description: "查看当前会话的 token 用量明细",
		Handler:     p.handleUsage,
	})

	h.RegisterCommand(plugin.Command{
		Name:        "/compact",
		Description: "用 LLM 压缩对话历史，释放上下文窗口",
		Handler:     p.handleCompact,
	})

	h.RegisterCommand(plugin.Command{
		Name:        "/history",
		Description: "查看当前会话的对话历史内容",
		Handler:     p.handleHistory,
	})

	h.RegisterCommand(plugin.Command{
		Name:        "/skills",
		Description: "查看已加载的 skill 列表",
		Handler:     p.handleSkills,
	})

	h.RegisterCommand(plugin.Command{
		Name:        "/mcp",
		Description: "查看 MCP server 连接状态与已加载工具",
		Handler:     p.handleMCP,
	})

	h.RegisterCommand(plugin.Command{
		Name:        "/memory",
		Description: "管理长期记忆（LTM）与任务档案（MTM），用法见 /memory help",
		Handler:     p.handleMemory,
	})

	h.RegisterCommand(plugin.Command{
		Name:        "/task",
		Description: "管理任务档案：固化/查看/开合 task，用法见 /task help",
		Handler:     p.handleTask,
	})

	h.RegisterCommand(plugin.Command{
		Name:        "/new",
		Description: "End current session: consolidate memory → clear STM → inject reminders next run",
		Handler:     p.handleNew,
	})

	h.RegisterCommand(plugin.Command{
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

	ok = true
	return nil
}

func (p *AgentPlugin) Start() error { return nil }

func (p *AgentPlugin) Stop() error {
	// MCP server 是外部进程，退出时统一回收，避免残留孤儿进程
	p.closeMCP()
	if p.memory != nil {
		// 进程退出 = 会话结束，把当前 conversation 固化进任务档案，供下次会话恢复。
		// 同步等一次 LLM（带超时），否则后台 goroutine 会被进程退出杀掉，固化直接丢。
		// LLM 网关慢时 20s 容易超时丢历史，放宽到 120s 给足时间。
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		// Client.Checkpoint 内部已打 applied 日志，这里不重复
		if _, err := p.session.Consolidate(ctx); err != nil {
			slog.Warn("memory: stop checkpoint failed", "err", err)
		}
		cancel()
		if err := p.memory.Close(); err != nil {
			slog.Warn("memory close error", "err", err)
		}
	}
	// store 生命周期收尾：consolidate 后关闭
	if p.store != nil {
		if err := p.store.Close(); err != nil {
			slog.Warn("session: store close failed", "err", err)
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

func (p *AgentPlugin) startSession() {
	// 打开会话持久化 store。失败降级不阻塞插件启动。
	p.store = nil
	if p.cfg.Session.Enabled {
		st, err := session.Open(p.cfg.Session.Dir)
		if err != nil {
			slog.Warn("session: store open failed, persistence disabled", "err", err)
		} else {
			p.store = st
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

	modelProvider := ProviderFactory(client)

	p.deps = runtimeagent.SessionDeps{
		Config:       p.cfg,
		AuditDir:     p.paths.AuditDir,
		Memory:       p.memory,
		CollectTools: p.collectTools,
		NewProvider:  modelProvider,
		Store:        p.store,
	}
	// 指令快照与记忆组件同开关：memory disabled 时留 nil，buildSystemPrompt 跳过指令段。
	// 会话边界（Init / /new）经 startSession 重载 —— system prompt 在会话创建前就绪。
	if p.cfg.Memory.Enabled {
		p.deps.Instruction = p.loadInstructions()
	}
	p.session = runtimeagent.NewSession(p.deps)
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
