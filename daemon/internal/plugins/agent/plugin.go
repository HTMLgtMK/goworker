package agent

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/mcp"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/skills"
	"github.com/tinguo/goworker/daemon/internal/spec"
	"github.com/tinguo/goworker/memory"
)

// AgentPlugin 是 /agent 插件入口，只负责三件事：持有会话状态、注册命令、管理生命周期。
// 命令 handler 与工具收集按职责拆到本包其他文件：
//
//	commands.go   — /model /usage /compact /skills /history /rules（配置与诊断）
//	session.go    — /agent /new（会话运行与控制）
//	memory_cmd.go — /memory /task（记忆与任务档案）
//	checkpoint.go — 检查点固化（会话 → Task + LTM）
//	mcp.go        — MCP server 连接与工具桥接
//	tools.go      — 工具收集与 profile 工具
//	util.go       — 展示层小工具函数
type AgentPlugin struct {
	hub *spec.Hub
	mu  sync.Mutex
	// conversation 跨 /agent 调用累积，也被工具重入触发的 /compact 改写 ——
	// 与 usage tracker 同理由，加锁保护（每次只是短暂快照/写回，不跨 Run 持锁）。
	conversation []core.Message        // 跨 /agent 调用的对话历史
	usage        *core.UsageTracker    // 当前 /agent 会话的 token 统计（纯数据，供 /usage 读取）
	skills       []skills.Skill        // Init 时加载的 skill 清单，注册为 skill_* 工具
	mcpClients   map[string]mcp.Client // server name → 连接，Stop 时统一关闭
	mcpTools     []core.Tool           // 从已连接 server 拉取的工具（静态，collectTools 复用）
	memory       *memory.Client        // 记忆组件（MTM+LTM），Init 打开 / Stop 关闭；nil = 禁用
	// instructions 是声明式指令快照（USER.md + AGENTS.md），冻结于会话启动。
	instructions *memory.InstructionSet
	checkpointMu sync.Mutex // 串行化检查点固化：读快照→LLM→应用的整个周期
}

func (p *AgentPlugin) Name() string { return "agent" }

func (p *AgentPlugin) Init(h *spec.Hub) error {
	p.hub = h
	p.usage = core.NewUsageTracker()
	p.mcpClients = make(map[string]mcp.Client)

	// Init 中途失败要回收已连接的 MCP 进程 —— Engine.Register 在 Init 报错时不会调用 Stop
	ok := false
	defer func() {
		if !ok {
			p.closeMCP()
		}
	}()

	h.RegisterCommand(spec.Command{
		Name:        "/agent",
		Aliases:     []string{"/llm", "/ai"},
		Description: "与 AI Agent 对话",
		Handler:     p.handleAgent,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/model",
		Description: "查看/设置 LLM 配置，用法见 /model help",
		Handler:     p.handleModel,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/usage",
		Description: "查看当前会话的 token 用量明细",
		Handler:     p.handleUsage,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/compact",
		Description: "用 LLM 压缩对话历史，释放上下文窗口",
		Handler:     p.handleCompact,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/history",
		Description: "查看当前会话的对话历史内容",
		Handler:     p.handleHistory,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/skills",
		Description: "查看已加载的 skill 列表",
		Handler:     p.handleSkills,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/mcp",
		Description: "查看 MCP server 连接状态与已加载工具",
		Handler:     p.handleMCP,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/memory",
		Description: "管理长期记忆（LTM）与任务档案（MTM），用法见 /memory help",
		Handler:     p.handleMemory,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/task",
		Description: "管理任务档案：固化/查看/开合 task，用法见 /task help",
		Handler:     p.handleTask,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/new",
		Description: "End current session: consolidate memory → clear STM → inject reminders next run",
		Handler:     p.handleNew,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/rules",
		Description: "Show the effective declarative instructions (AGENTS.md + USER.md injection snapshot)",
		Handler:     p.handleRules,
	})

	// 打开记忆存储。坏目录只降级为无记忆，不阻塞插件启动
	if p.hub.Config.Memory.Enabled {
		ms, err := memory.NewClient(p.hub.Config.Memory.Dir, p.hub.Config.Memory.TaskKeep)
		if err != nil {
			slog.Warn("memory store init failed, memory disabled", "err", err)
		} else {
			p.memory = ms
		}
	}

	// 声明式指令层（USER.md + AGENTS.md），与记忆组件同开关。独立于 JSONL 存储：
	// 记忆 store 打不开时指令仍可加载（纯文件读取）。
	if p.hub.Config.Memory.Enabled {
		p.instructions = p.loadInstructions()
	}

	// 加载 skill：用户级 + 项目级（后者覆盖前者）。坏 skill 跳过并记录，不拖垮其他的
	discovered, skillErrs := skills.Discover(
		filepath.Join(config.DefaultDir(), "skills"),
		filepath.Join(".goworker", "skills"),
	)
	for _, err := range skillErrs {
		slog.Warn("skill load error, skipping", "err", err)
	}
	p.skills = discovered

	// 连接 MCP server 并拉取工具清单。坏 server 只降级不阻塞插件启动
	p.loadMCP()

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
		// LLM 网关慢时 20s 容易超时丢历史，放宽到 45s 给足时间。
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		// checkpointSnapshot 内部已打 applied 日志，这里不重复
		if _, err := p.checkpointMemory(ctx); err != nil {
			slog.Warn("memory: stop checkpoint failed", "err", err)
		}
		cancel()
		if err := p.memory.Close(); err != nil {
			slog.Warn("memory close error", "err", err)
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
