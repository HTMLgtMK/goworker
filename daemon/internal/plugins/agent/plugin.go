package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
	"github.com/tinguo/goworker/daemon/internal/mcp"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/middlewares"
	"github.com/tinguo/goworker/daemon/internal/sandbox"
	"github.com/tinguo/goworker/daemon/internal/skills"
	"github.com/tinguo/goworker/daemon/internal/spec"
	"github.com/tinguo/goworker/memory"
)

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
	checkpointMu sync.Mutex            // 串行化检查点固化：读快照→LLM→应用的整个周期
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

	// 打开记忆存储。坏目录只降级为无记忆，不阻塞插件启动
	if p.hub.Config.Memory.Enabled {
		ms, err := memory.NewClient(p.hub.Config.Memory.Dir, p.hub.Config.Memory.TaskKeep)
		if err != nil {
			slog.Warn("memory store init failed, memory disabled", "err", err)
		} else {
			p.memory = ms
		}
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
		if err := p.checkpointMemory(ctx); err != nil {
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

// ---- /model 命令 ----

func (p *AgentPlugin) handleModel(ctx *spec.Context) error {
	args := ctx.Args

	if len(args) == 0 {
		p.showConfig(ctx)
		return nil
	}

	switch args[0] {
	case "help":
		ctx.Writer("用法:\n")
		ctx.Writer("  /model            — 查看当前配置\n")
		ctx.Writer("  /model set <k>=<v> — 设置配置\n")
		ctx.Writer("  可用 key: endpoint, model, api_key, context_window, compress_at, compact_keep, max_iterations, sandbox_mode\n")
		ctx.Writer("  context_window: 模型上下文窗口，如 32768 / 32k / 128k\n")
		ctx.Writer("  compress_at: 历史压缩触发阈值（0-1），用量达窗口该比例自动压缩，0 关闭\n")
		ctx.Writer("  compact_keep: 滚动压缩保留的最近消息条数\n")
		ctx.Writer("  max_iterations: ReAct 循环最大迭代数（模型往返次数），0 = 默认 15\n")
		ctx.Writer("  sandbox_mode: off, normal, strict, readonly\n")

	case "set":
		if len(args) < 2 {
			ctx.Writer("用法: /model set <key>=<value>\n")
			return nil
		}
		kv := strings.SplitN(args[1], "=", 2)
		if len(kv) != 2 {
			ctx.Writer("格式错误，示例: /model set endpoint=http://localhost:8000/v1\n")
			return nil
		}
		key, val := kv[0], kv[1]

		cfg := p.hub.Config
		switch key {
		case "endpoint":
			cfg.LLM.Endpoint = val
		case "model":
			cfg.LLM.Model = val
		case "api_key":
			cfg.LLM.APIKey = val
		case "context_window":
			n, err := config.ParseContextWindow(val)
			if err != nil {
				ctx.Writer(fmt.Sprintf("✘ %v\n", err))
				return nil
			}
			cfg.LLM.ContextWindow = n
		// compress_at/compact_keep 委托 SetField，校验与 /config 单一来源
		case "compress_at":
			if err := cfg.SetField("llm.compress_at", val); err != nil {
				ctx.Writer(fmt.Sprintf("✘ %v\n", err))
				return nil
			}
		case "compact_keep":
			if err := cfg.SetField("llm.compact_keep", val); err != nil {
				ctx.Writer(fmt.Sprintf("✘ %v\n", err))
				return nil
			}
		case "max_iterations":
			if err := cfg.SetField("llm.max_iterations", val); err != nil {
				ctx.Writer(fmt.Sprintf("✘ %v\n", err))
				return nil
			}
		case "sandbox_mode":
			cfg.Sandbox.Mode = val
		default:
			ctx.Writer(fmt.Sprintf("未知配置项: %s（可用: endpoint, model, api_key, context_window, compress_at, compact_keep, max_iterations, sandbox_mode）\n", key))
			return nil
		}

		if err := p.hub.SaveConfig(cfg); err != nil {
			ctx.Writer(fmt.Sprintf("✘ 保存失败: %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ %s 已更新\n", key))

	default:
		ctx.Writer("未知子命令，使用 /model help 查看用法\n")
	}

	return nil
}

func (p *AgentPlugin) showConfig(ctx *spec.Context) {
	cfg := p.hub.Config
	keyDisplay := cfg.LLM.APIKey
	if keyDisplay != "" {
		keyDisplay = "***"
	} else {
		keyDisplay = "(未设置)"
	}

	ctx.Writer(fmt.Sprintf("Endpoint:      %s\n", cfg.LLM.Endpoint))
	ctx.Writer(fmt.Sprintf("Model:         %s\n", cfg.LLM.Model))
	ctx.Writer(fmt.Sprintf("API Key:       %s\n", keyDisplay))
	ctx.Writer(fmt.Sprintf("Context Window: %d\n", cfg.LLM.ContextWindow))
	ctx.Writer(fmt.Sprintf("Compress At:    %v\n", cfg.LLM.CompressAt))
	ctx.Writer(fmt.Sprintf("Compact Keep:   %d\n", cfg.LLM.CompactKeep))
	ctx.Writer(fmt.Sprintf("Max Iterations: %d\n", cfg.LLM.MaxIterations))
	ctx.Writer(fmt.Sprintf("Sandbox Mode:  %s\n", cfg.Sandbox.Mode))
}

// ---- /agent 命令 ----

func (p *AgentPlugin) handleAgent(ctx *spec.Context) error {
	input := strings.Join(ctx.Args, " ")
	if input == "" {
		ctx.Writer("用法: /agent <你的问题>\n")
		return nil
	}

	// 构建沙箱配置
	sandboxCfg := p.sandboxConfig()

	// 收集工具
	tools := p.collectTools(&sandboxCfg)

	// 创建 Provider、Middleware 和 Agent
	cfg := p.hub.Config
	provider := NewOpenAIProvider(cfg.LLM.Endpoint, cfg.LLM.APIKey, cfg.LLM.Model)
	decisions := make(chan spec.HITLDecision, 1)
	hitlMw := middlewares.NewHITLMiddleware(sandboxCfg, middlewares.NewChannelDecisionProvider(decisions))
	// 新一轮会话清零统计；usage middleware 动态注册，从注册时刻开始记账
	p.usage.Reset()
	usageMw := middlewares.NewUsageMiddleware(p.usage, func(u core.Usage) {
		if ctx.Publish != nil {
			ctx.Publish(statusbar.EventUsage, statusbar.Usage{
				EstimateTokens:   u.EstimateTokens,
				PromptTokens:     u.PromptTokens,
				CompletionTokens: u.CompletionTokens,
				TotalTokens:      u.TotalTokens,
				LastPromptTokens: u.LastPromptTokens,
				ContextWindow:    cfg.LLM.ContextWindow,
			})
		}
	})
	// 自动压缩：BeforeModel 预检，估算用量超阈值就滚动压缩历史。
	// protectSystem=true：history[0] 是本轮注入的系统提示，不能被压进摘要。
	compressMw := middlewares.NewCompressionMiddleware(
		core.NewCompressor(provider, cfg.LLM.CompactKeep, true, func(r core.CompressReport) {
			p.usage.RecordCompaction(core.Compaction{BeforeMsgs: r.BeforeMsgs, AfterMsgs: r.AfterMsgs, Tokens: r.Tokens})
		}),
		cfg.LLM.ContextWindow,
		cfg.LLM.CompressAt,
	)
	// memory middleware：每次 query 都从记忆组件检索相关条目注入提醒。
	// 必须在 compressMw 之前，让压缩器测量的是注入后的完整 history。
	mws := []core.Middleware{hitlMw, usageMw, compressMw}
	if p.memory != nil {
		memMw := middlewares.NewMemoryMiddleware(p.memory, cfg.Memory, cfg.LLM.ContextWindow)
		mws = []core.Middleware{hitlMw, usageMw, memMw, compressMw}
	}
	agent := NewAgent(provider, tools, mws,
		WithMaxIterations(cfg.LLM.MaxIterations))
	agent.OnIteration = func() {
		if ctx.Publish != nil {
			ctx.Publish(statusbar.EventIteration, nil)
		}
	}

	// agentCtx 不设硬超时：多轮 tool call 总耗时不可控，5 分钟硬超时只会在工具循环
	// 中途切断会话，且超时瞬间 sendToken 会把唯一的错误提示吞掉，前端表现成"莫名停止"。
	// 兜底交给单次请求自身：provider http 30s 超时；bash 类子进程工具受 60s 限制
	// （CommandContext + 进程组 kill），read_file/write_file 的同步 syscall 不认 ctx，
	// 用 goroutine + select 包装让取消能提前返回，真正挂死的 OS 层仍依赖系统恢复。
	agentCtx, cancel := context.WithCancel(ctx.Ctx)
	defer cancel()

	// 快照历史给本轮的 Run（工具重入触发的 /compact 不会污染本次运行的输入）
	p.mu.Lock()
	conv := p.conversation
	p.mu.Unlock()

	tokenCh, msgCh, err := agent.Run(agentCtx, conv, input)
	if err != nil {
		ctx.Writer(fmt.Sprintf("✘ %v\n", err))
		return nil
	}

	for tok := range tokenCh {
		// 先输出内容再检查 Done — Done token 也可能带内容（如错误信息）
		if tok.Content != "" {
			kind, c := p.renderKind(tok)
			ctx.WriteToken(kind, c)
		}
		if tok.Done {
			break
		}
		if tok.Type == core.TokenTypeInterrupt && tok.Interrupt != nil {
			// HITL 决策由前端按 spec 契约完成，plugin 只负责转交
			decision := spec.HITLDecision{InterruptID: tok.Interrupt.ID, Type: spec.DecisionReject}
			if ctx.Decide != nil {
				decision = ctx.Decide(tok.Interrupt)
			}
			select {
			case decisions <- decision:
			case <-agentCtx.Done():
			}
			continue
		}
	}
	ctx.Writer("\n")
	// 保存对话历史（剔除首条 system prompt + memory 注入的记忆提醒块）。
	// 记忆块只服务本次 run 的注入，若漏进 STM：下次 run 会重发它、检查点会把
	// 它当对话内容固化（自指污染）—— 必须过滤掉。
	messages := <-msgCh
	if len(messages) > 1 {
		conv := stripMemoryBlocks(messages[1:])
		p.mu.Lock()
		p.conversation = conv
		p.mu.Unlock()
	}

	return nil
}

// stripMemoryBlocks 剔除 memory middleware 注入的记忆提醒块（system 消息）。
// 普通 system 消息（如压缩摘要）不受影响 —— 只匹配 MemoryBlockPrefix 前缀。
func stripMemoryBlocks(msgs []core.Message) []core.Message {
	var out []core.Message
	for _, m := range msgs {
		if m.Role == "system" && strings.HasPrefix(m.Content, middlewares.MemoryBlockPrefix) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// ---- /usage 命令 ----

func (p *AgentPlugin) handleUsage(ctx *spec.Context) error {
	calls := p.usage.Calls()
	comps := p.usage.Compactions()
	if len(calls) == 0 && len(comps) == 0 {
		ctx.Writer("(no agent calls yet — run /agent first)\n")
		return nil
	}
	total := p.usage.Snapshot()

	if len(calls) > 0 {
		ctx.Writer(fmt.Sprintf("usage: %d model calls this session\n\n", len(calls)))
		for i, c := range calls {
			ctx.Writer(fmt.Sprintf("  #%-2d  in %-7s  out %-7s  (est %s)\n",
				i+1, humanize(c.PromptTokens), humanize(c.CompletionTokens), humanize(c.EstimateTokens)))
		}
		ctx.Writer("\n")
		ctx.Writer(fmt.Sprintf("  total: in %s  out %s  total %s\n",
			humanize(total.PromptTokens), humanize(total.CompletionTokens), humanize(total.TotalTokens)))

		// context usage uses "last prompt / window" — the final ReAct request already holds all history
		if w := p.hub.Config.LLM.ContextWindow; w > 0 && total.LastPromptTokens > 0 {
			pct := float64(total.LastPromptTokens) / float64(w) * 100
			ctx.Writer(fmt.Sprintf("  context: %.2f%% (last in %s / window %s)\n",
				pct, humanize(total.LastPromptTokens), humanize(w)))
		}
	}

	// 压缩记录：压缩是循环外的模型调用，消耗和效果都该看得见
	if len(comps) > 0 {
		var msgsIn, msgsOut, tokens int
		for _, c := range comps {
			msgsIn += c.BeforeMsgs
			msgsOut += c.AfterMsgs
			tokens += c.Tokens
		}
		ctx.Writer(fmt.Sprintf("  compact: %d time(s), %d msgs → %d msgs (summarize %s)\n",
			len(comps), msgsIn, msgsOut, humanize(tokens)))
	}

	// 当前历史占用：压缩后的直观体现，不必等下一次模型调用。
	// 锁内只拷引用，估算（JSON 序列化）放到锁外，别把大对象拖进临界区。
	p.mu.Lock()
	conv := p.conversation
	p.mu.Unlock()
	if convLen := len(conv); convLen > 0 {
		convEst := core.EstimateTokens(conv)
		line := fmt.Sprintf("  history: %s est (%d msgs)", humanize(convEst), convLen)
		if w := p.hub.Config.LLM.ContextWindow; w > 0 {
			line += fmt.Sprintf(", %.2f%% of window", float64(convEst)/float64(w)*100)
		}
		ctx.Writer(line + "\n")
	}
	return nil
}

// ---- /compact 命令 ----

func (p *AgentPlugin) handleCompact(ctx *spec.Context) error {
	// 压缩会丢原文，先固化到任务档案 —— 这是原文丢失前的最后一次机会。
	if p.memory != nil {
		if err := p.checkpointMemory(ctx.Ctx); err != nil {
			ctx.Writer(fmt.Sprintf("⚠ Memory consolidation failed (original text lost after compression): %v\n", err))
		}
	}

	// 一次性锁内快照，空检查与后续压缩共用同一份数据
	p.mu.Lock()
	history := p.conversation
	before := len(history)
	p.mu.Unlock()
	if before == 0 {
		ctx.Writer("(no conversation history yet — run /agent first)\n")
		return nil
	}

	cfg := p.hub.Config
	provider := NewOpenAIProvider(cfg.LLM.Endpoint, cfg.LLM.APIKey, cfg.LLM.Model)
	// protectSystem=false：p.conversation 不含系统提示，首位可能是上次的摘要，允许被再次滚动
	compressor := core.NewCompressor(provider, cfg.LLM.CompactKeep, false, func(r core.CompressReport) {
		p.usage.RecordCompaction(core.Compaction{BeforeMsgs: r.BeforeMsgs, AfterMsgs: r.AfterMsgs, Tokens: r.Tokens})
	})

	cctx, cancel := context.WithTimeout(ctx.Ctx, 2*time.Minute)
	defer cancel()
	compacted, err := compressor.Compress(cctx, history)
	if err != nil {
		ctx.Writer(fmt.Sprintf("✘ 压缩失败: %v\n", err))
		return nil
	}
	if len(compacted) >= before {
		ctx.Writer("(history too short or no safe split point; not compacted)\n")
		return nil
	}

	p.mu.Lock()
	p.conversation = compacted
	p.mu.Unlock()
	ctx.Writer(fmt.Sprintf("✔ 已压缩: %d 条 → %d 条\n", before, len(compacted)))
	return nil
}

// ---- /skills 命令 ----

func (p *AgentPlugin) handleSkills(ctx *spec.Context) error {
	if len(p.skills) == 0 {
		ctx.Writer("(no skills loaded — drop SKILL.md files in ~/.config/goworker/skills/ or .goworker/skills/)\n")
		return nil
	}
	ctx.Writer(fmt.Sprintf("skills: %d loaded\n\n", len(p.skills)))
	for _, s := range p.skills {
		ctx.Writer(fmt.Sprintf("  - %s: %s\n", s.Name, s.Description))
	}
	return nil
}

// ---- /mcp 命令 ----

func (p *AgentPlugin) handleMCP(ctx *spec.Context) error {
	servers := p.hub.Config.MCP.Servers
	if len(servers) == 0 {
		ctx.Writer("(no MCP servers configured — add mcp.servers to config.yaml)\n")
		return nil
	}
	ctx.Writer(fmt.Sprintf("mcp: %d configured\n\n", len(servers)))
	for _, srv := range servers {
		status := "❌"
		if _, ok := p.mcpClients[srv.Name]; ok {
			status = "✔"
		}
		ctx.Writer(fmt.Sprintf("  %-14s %s  %s %s\n", srv.Name, status, srv.Command, strings.Join(srv.Args, " ")))
	}
	if n := len(p.mcpTools); n > 0 {
		ctx.Writer(fmt.Sprintf("\n  tools: %d loaded\n", n))
	}
	return nil
}

// loadMCP 连接配置的 MCP server 并拉取工具清单。
// MCP server 是外部进程，可能没起/连不上——单个失败只降级跳过，不阻塞插件启动。
// 每个 server 握手+列工具共限时 10s，避免坏配置卡住启动。
func (p *AgentPlugin) loadMCP() {
	for _, srv := range p.hub.Config.MCP.Servers {
		// 重复 name：先连接的那个会被 map 覆盖成孤儿进程，直接跳过
		if _, exists := p.mcpClients[srv.Name]; exists {
			slog.Warn("duplicate mcp server name, skipping", "server", srv.Name)
			continue
		}
		c, err := mcp.NewStdioClient(srv.Command, srv.Args)
		if err != nil {
			slog.Warn("mcp connect failed, skipping", "server", srv.Name, "err", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := c.Initialize(ctx, &mcp.InitializeRequest{
			ProtocolVersion: mcp.LatestProtocolVersion,
			ClientInfo:      mcp.Implementation{Name: "goworker", Version: "0.1.0"},
		}); err != nil {
			cancel()
			c.Close()
			slog.Warn("mcp initialize failed, skipping", "server", srv.Name, "err", err)
			continue
		}
		tools, err := c.ListTools(ctx, &mcp.ListToolsRequest{})
		cancel()
		if err != nil {
			c.Close()
			slog.Warn("mcp tools/list failed, skipping", "server", srv.Name, "err", err)
			continue
		}

		p.mcpClients[srv.Name] = c
		for _, t := range tools.Tools {
			if tool, ok := p.mcpToolToCore(c, srv.Name, t); ok {
				p.mcpTools = append(p.mcpTools, tool)
			}
		}
		slog.Info("mcp connected", "server", srv.Name, "tools", len(tools.Tools))
	}
}

// mcpToolNameRe 限定合成工具名只含 OpenAI 允许的字符（^[a-zA-Z0-9_-]{1,64}$）。
// server/tool 名来自配置或外部 server，可能带空格/点/冒号，必须在源头拦截。
var mcpToolNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// normalizeSchema 兜底空 schema 为 OpenAI 工具要求的 object 结构。
// spec/skill/MCP 三种工具构造统一走这里，避免 schema 处理分叉。
func normalizeSchema(p map[string]any) map[string]any {
	if len(p) == 0 {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return p
}

// mcpToolToCore 把 MCP 工具转成 agent 工具。工具名带 server 前缀防跨 server 冲突。
// 名字非法字符或超长时跳过 —— 一个坏工具名会让整个 tools 数组被 OpenAI 400 拒绝。
func (p *AgentPlugin) mcpToolToCore(c mcp.Client, server string, t mcp.Tool) (core.Tool, bool) {
	name := "mcp_" + server + "_" + t.Name
	if !mcpToolNameRe.MatchString(name) {
		slog.Warn("mcp tool name has invalid characters, skipping", "server", server, "tool", t.Name, "name", name)
		return core.Tool{}, false
	}
	if len(name) > 64 {
		slog.Warn("mcp tool name too long, skipping", "server", server, "tool", t.Name, "name", name)
		return core.Tool{}, false
	}
	return core.Tool{
		Name:        name,
		Description: t.Description,
		Parameters:  normalizeSchema(t.InputSchema),
		Execute: func(ctx context.Context, args map[string]any) (string, error) {
			ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			res, err := c.CallTool(ctx, &mcp.CallToolRequest{Name: t.Name, Arguments: args})
			if err != nil {
				return "", fmt.Errorf("mcp %s: %w", t.Name, err)
			}
			if res.IsError {
				return "", fmt.Errorf("mcp %s: %s", t.Name, mcpContentText(res))
			}
			return mcpContentText(res), nil
		},
	}, true
}

// mcpContentText 把 MCP 返回的 content 块拼成单块文本。
func mcpContentText(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, blk := range res.Content {
		b.WriteString(blk.Text)
	}
	return b.String()
}

// ---- /history 命令 ----

func (p *AgentPlugin) handleHistory(ctx *spec.Context) error {
	p.mu.Lock()
	conv := p.conversation
	p.mu.Unlock()
	if len(conv) == 0 {
		ctx.Writer("(no conversation history yet — run /agent first)\n")
		return nil
	}

	ctx.Writer(fmt.Sprintf("history: %d messages\n\n", len(conv)))
	for i, m := range conv {
		ctx.Writer(fmt.Sprintf("  #%-2d [%-9s] %s\n", i+1, m.Role, describeMessage(m)))
	}
	return nil
}

// ---- /memory 命令 ----

func (p *AgentPlugin) handleMemory(ctx *spec.Context) error {
	if p.memory == nil {
		ctx.Writer("memory 未启用（检查 config.yaml 的 memory.enabled 与存储目录权限）\n")
		return nil
	}
	args := ctx.Args
	if len(args) == 0 {
		p.memoryHelp(ctx)
		return nil
	}

	switch args[0] {
	case "help":
		p.memoryHelp(ctx)

	case "add":
		if len(args) < 2 {
			ctx.Writer("用法: /memory add <要记住的事实>\n")
			return nil
		}
		content := strings.TrimSpace(strings.Join(args[1:], " "))
		f := &memory.Fact{Content: content, Topic: "manual", Source: "user"}
		if err := p.memory.AddFact(f); err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ added %s\n", f.ID))

	case "search":
		if len(args) < 2 {
			ctx.Writer("用法: /memory search <关键词> [条数]\n")
			return nil
		}
		n := 8
		q := strings.Join(args[1:], " ")
		if len(args) >= 3 {
			if v, err := strconv.Atoi(args[len(args)-1]); err == nil && v > 0 {
				n = v
				q = strings.Join(args[1:len(args)-1], " ")
			}
		}
		facts, err := p.memory.SearchFacts(q, n)
		if err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if len(facts) == 0 {
			ctx.Writer("(no matching facts)\n")
			return nil
		}
		ctx.Writer(fmt.Sprintf("facts: %d matched\n\n", len(facts)))
		for i, f := range facts {
			ctx.Writer(fmt.Sprintf("  #%d [%s|%s] %s (%s)\n",
				i+1, f.ID, f.Topic, truncate(oneLine(f.Content), 160), f.UpdatedAt.Format("01-02 15:04")))
		}

	case "list":
		n := 20
		if len(args) >= 2 {
			if v, err := strconv.Atoi(args[1]); err == nil && v > 0 {
				n = v
			}
		}
		facts, err := p.memory.ListFacts(n)
		if err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if len(facts) == 0 {
			ctx.Writer("(no facts yet — run /agent to auto-extract, or /memory add <text>)\n")
			return nil
		}
		ctx.Writer(fmt.Sprintf("facts: %d\n\n", len(facts)))
		for i, f := range facts {
			ctx.Writer(fmt.Sprintf("  #%d [%s|%s] %s (%s)\n",
				i+1, f.ID, f.Topic, truncate(oneLine(f.Content), 160), f.UpdatedAt.Format("01-02 15:04")))
		}

	case "tasks":
		n := 10
		if len(args) >= 2 {
			if v, err := strconv.Atoi(args[1]); err == nil && v > 0 {
				n = v
			}
		}
		tasks, err := p.memory.ListTasks(n)
		if err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if len(tasks) == 0 {
			ctx.Writer("(no tasks yet — run /agent then /compact or /new to checkpoint)\n")
			return nil
		}
		ctx.Writer(fmt.Sprintf("tasks: %d\n\n", len(tasks)))
		for i, t := range tasks {
			ctx.Writer(fmt.Sprintf("  #%d [%s|%s] %s\n     %s\n",
				i+1, t.ID, t.Status, truncate(oneLine(t.Title), 80), truncate(oneLine(t.Summary), 160)))
			if len(t.NextSteps) > 0 {
				ctx.Writer("     下一步: " + strings.Join(t.NextSteps, "；") + "\n")
			}
		}

	case "forget":
		if len(args) < 2 {
			ctx.Writer("用法: /memory forget <id> [id...]\n")
			return nil
		}
		for _, id := range args[1:] {
			if err := p.memory.DeleteFact(id); err != nil {
				ctx.Writer(fmt.Sprintf("✘ %v\n", err))
				return nil
			}
		}
		ctx.Writer(fmt.Sprintf("✔ forgot %d\n", len(args)-1))

	default:
		ctx.Writer(fmt.Sprintf("未知子命令: %s（使用 /memory help 查看用法）\n", args[0]))
	}
	return nil
}

func (p *AgentPlugin) memoryHelp(ctx *spec.Context) {
	ctx.Writer("用法:\n")
	ctx.Writer("  /memory                 — 显示本帮助\n")
	ctx.Writer("  /memory add <文本>      — 手动记住一条事实\n")
	ctx.Writer("  /memory search <关键词> [n] — 检索相关事实\n")
	ctx.Writer("  /memory list [n]        — 列出最近事实（默认 20）\n")
	ctx.Writer("  /memory tasks [n]       — 查看任务档案（含已关闭）\n")
	ctx.Writer("  /memory forget <id>...  — 删除指定事实\n")
	ctx.Writer("  /task list|start|end    — 管理未完成任务\n")
	ctx.Writer("  /new                    — 结束会话：固化+清 STM+注入提醒\n")
}

// ---- 固化检查点 ----

// checkpointMemory 把当前 conversation 固化进任务档案（Task + LTM）。
// 同步入口：锁内快照 conversation 后调 checkpointSnapshot。
// 触发点都是低频事件：/compact、进程退出、/task checkpoint —— 这些调用方
// 语义上必须等固化完成（压缩前、退出前），故保持同步。
func (p *AgentPlugin) checkpointMemory(ctx context.Context) error {
	p.mu.Lock()
	conv := slices.Clone(p.conversation)
	p.mu.Unlock()
	_, err := p.checkpointSnapshot(ctx, conv)
	return err
}

// checkpointSnapshot 把给定 conversation 快照固化进任务档案。
// 一次 LLM 调用（Checkpointer）输出 task 归属/新建/关闭 + 事实决策。
// 读快照→LLM 推理→写回是整个周期，store 的 mutex 只串行化单次写。
// 并发检查点（如 /compact 与退出 Stop、后台固化重叠）会基于过期快照互相覆盖，
// 必须整体互斥。返回本次固化应用的总条目数（task 更新 + fact 决策）。
// 快照必须由调用方在锁内克隆 —— /new 的后台固化依赖此保证清 STM 后仍能固化旧历史。
func (p *AgentPlugin) checkpointSnapshot(ctx context.Context, conv []core.Message) (int, error) {
	// checkpointMu 可能被后台固化（/new 异步，最长 120s）持有 —— 拿锁不能无界阻塞，
	// 否则 Stop 的 45s 超时形同虚设：等拿到锁时 ctx 已取消，固化静默失败。
	acquired := make(chan struct{})
	go func() {
		p.checkpointMu.Lock()
		close(acquired)
	}()
	select {
	case <-acquired:
		defer p.checkpointMu.Unlock()
	case <-ctx.Done():
		// 放弃固化，但拿锁的 goroutine 仍会拿到锁 —— 等它完成后自行释放，别让锁永久持有
		go func() {
			<-acquired
			p.checkpointMu.Unlock()
		}()
		return 0, ctx.Err()
	}

	if p.memory == nil || len(conv) == 0 {
		return 0, nil
	}

	cfg := p.hub.Config
	provider := NewOpenAIProvider(cfg.LLM.Endpoint, cfg.LLM.APIKey, cfg.LLM.Model)
	cwd, _ := os.Getwd()
	cp := memory.NewCheckpointer(&llmAdapter{inner: provider}, cwd)

	openTasks, err := p.memory.OpenTasks(50)
	if err != nil {
		return 0, fmt.Errorf("open tasks: %w", err)
	}
	facts, err := p.memory.ListFacts(50)
	if err != nil {
		return 0, fmt.Errorf("list facts: %w", err)
	}
	// 检查点 id：也是 Task.Runs 与 Fact.Source 的标记
	cpID := fmt.Sprintf("cp-%d", time.Now().UnixNano())
	res, err := cp.Run(ctx, toMemoryMessages(conv), openTasks, facts)
	if err != nil {
		return 0, err
	}
	// ltm_extract=false：tasks/decisions 是同一 LLM 调用输出的，无法只跳过调用，
	// 但应用层不落库事实 —— 配置开关生效。
	if !cfg.Memory.LtmExtract {
		res.Decisions = nil
	}
	tokens := 0
	if p.usage != nil {
		tokens = p.usage.Snapshot().TotalTokens
	}
	n, err := memory.ApplyCheckpoint(p.memory, res, openTasks, facts, cpID, tokens, cwd)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		slog.Info("memory: checkpoint applied", "cp", cpID, "applied", n)
	}
	return n, nil
}

// checkpointAsync 后台固化旧会话：/new 不阻塞用户输入，固化结果完成后回显。
// writer 是捕获的命令 Writer（stdin 实现线程安全），固化期间用户可立即输入下一行。
// 注意：若 /new 后立刻退出进程，后台固化可能未跑完，这段历史只留 STM 会丢 ——
// 这是异步固化的代价，Stop 的同步固化兜底下一段对话。
func (p *AgentPlugin) checkpointAsync(conv []core.Message, writer func(string)) {
	// 独立 ctx：命令 ctx 已随请求返回被释放，后台固化不能继承它。
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	n, err := p.checkpointSnapshot(ctx, conv)
	if err != nil {
		writer(fmt.Sprintf("⚠ Consolidation failed (history kept only in STM, lost on exit): %v\n", err))
		return
	}
	writer(fmt.Sprintf("✔ Consolidated previous session: %d entries\n", n))
}

// ---- /new 命令 ----

func (p *AgentPlugin) handleNew(ctx *spec.Context) error {
	// 结束当前会话：固化异步后台执行（不阻塞输入），立即清 STM，置会话边界标记。
	if p.memory != nil {
		p.mu.Lock()
		conv := slices.Clone(p.conversation)
		p.mu.Unlock()
		if len(conv) > 0 {
			// writer 在后台 goroutine 里回显，stdin 的 Write 线程安全
			go p.checkpointAsync(conv, ctx.Writer)
			ctx.Writer("✔ New session started, consolidating previous session in background…\n")
		} else {
			ctx.Writer("✔ New session started\n")
		}
	} else {
		ctx.Writer("✔ New session started (memory disabled)\n")
	}
	p.mu.Lock()
	p.conversation = nil
	p.mu.Unlock()
	ctx.Writer("✔ STM cleared, memory will be retrieved on every query\n")
	return nil
}

// ---- /task 命令 ----

func (p *AgentPlugin) handleTask(ctx *spec.Context) error {
	if p.memory == nil {
		ctx.Writer("memory 未启用（检查 config.yaml 的 memory.enabled 与存储目录权限）\n")
		return nil
	}
	args := ctx.Args
	if len(args) == 0 {
		p.taskHelp(ctx)
		return nil
	}

	switch args[0] {
	case "help":
		p.taskHelp(ctx)

	case "list":
		tasks, err := p.memory.OpenTasks(20)
		if err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if len(tasks) == 0 {
			ctx.Writer("(no open tasks — run /agent then checkpoint via /compact or /new)\n")
			return nil
		}
		ctx.Writer(fmt.Sprintf("tasks: %d open\n\n", len(tasks)))
		for i, t := range tasks {
			ctx.Writer(fmt.Sprintf("  #%d [%s] %s\n     %s\n",
				i+1, t.ID, truncate(oneLine(t.Title), 80), truncate(oneLine(t.Summary), 160)))
			if len(t.NextSteps) > 0 {
				ctx.Writer("     下一步: " + strings.Join(t.NextSteps, "；") + "\n")
			}
		}

	case "start":
		if len(args) < 2 {
			ctx.Writer("用法: /task start <任务标题>\n")
			return nil
		}
		title := strings.TrimSpace(strings.Join(args[1:], " "))
		t := &memory.Task{Title: title, Status: "open"}
		if err := p.memory.UpsertTask(t); err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ 已创建 task %s，后续对话将在检查点时归入\n", t.ID))

	case "end":
		id := ""
		if len(args) >= 2 {
			id = args[1]
		} else if tasks, err := p.memory.OpenTasks(1); err == nil && len(tasks) > 0 {
			id = tasks[0].ID // 默认关最近活跃的 open task
		}
		if id == "" {
			ctx.Writer("(no open task to close)\n")
			return nil
		}
		if err := p.memory.CloseTask(id); err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ 已关闭 task %s\n", id))

	case "checkpoint":
		if err := p.checkpointMemory(ctx.Ctx); err != nil {
			ctx.Writer(fmt.Sprintf("✘ Consolidation failed: %v\n", err))
			return nil
		}
		ctx.Writer("✔ Session consolidated\n")

	default:
		ctx.Writer(fmt.Sprintf("未知子命令: %s（使用 /task help 查看用法）\n", args[0]))
	}
	return nil
}

func (p *AgentPlugin) taskHelp(ctx *spec.Context) {
	ctx.Writer("用法:\n")
	ctx.Writer("  /task               — 显示本帮助\n")
	ctx.Writer("  /task list          — 列出未完成任务\n")
	ctx.Writer("  /task start <标题>   — 手动开始一个新任务（LLM 判定不准时干预）\n")
	ctx.Writer("  /task end [id]      — 关闭任务（默认最近活跃的）\n")
	ctx.Writer("  /task checkpoint    — 立即把当前会话固化进任务档案\n")
}

// describeMessage 把一条消息压成单行描述，供 /history 逐条展示。
// 工具调用展开成 name(args)，多行内容折叠成一行，长内容截断。
func describeMessage(m core.Message) string {
	switch {
	case len(m.ToolCalls) > 0:
		var parts []string
		for _, tc := range m.ToolCalls {
			parts = append(parts, fmt.Sprintf("%s(%s)", tc.Function.Name, tc.Function.Arguments))
		}
		return truncate(strings.Join(parts, " | "), 160)
	case m.Content == "":
		return "(empty)"
	default:
		return truncate(oneLine(m.Content), 160)
	}
}

// oneLine 把多行文本折叠成单行，换行替换为 ⏎，避免长工具输出铺满屏幕。
func oneLine(s string) string {
	return strings.ReplaceAll(s, "\n", " ⏎ ")
}

// truncate 按 rune 截断长文本，避免字节切半中文。n <= 0 时不截断。
func truncate(s string, n int) string {
	r := []rune(s)
	if n <= 0 || len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// humanize 把 token 数格式化为千分位缩写，12345 -> "12.3k"。
// 与 frontend/stdin 的 humanize 保持同一套规则，避免跨包依赖。
func humanize(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// renderKind 将 core.Token 类型映射为 spec.RenderKind，剥离渲染逻辑。
func (p *AgentPlugin) renderKind(t core.Token) (spec.RenderKind, string) {
	switch t.Type {
	case core.TokenTypeToolCall:
		return spec.KindToolCall, t.Content
	case core.TokenTypeToolResult:
		return spec.KindToolResult, t.Content
	default:
		// TokenTypeText 及未知类型一律按 markdown 渲染
		return spec.KindText, t.Content
	}
}

func (p *AgentPlugin) sandboxConfig() sandbox.Config {
	return *sandbox.NewFromConfig(&p.hub.Config.Sandbox)
}

func (p *AgentPlugin) collectTools(cfg *sandbox.Config) []core.Tool {
	var tools []core.Tool
	tools = append(tools, DefaultTools(cfg)...)

	for _, t := range p.hub.Tools() {
		tool := t
		tools = append(tools, core.Tool{
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  parseSchema(tool.Schema),
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				var buf strings.Builder
				err := p.hub.Eval(spec.NewContext(context.Background(), func(s string) { buf.WriteString(s) }, nil, nil), "/"+tool.Name)
				if err != nil {
					return buf.String(), err
				}
				return strings.TrimSpace(buf.String()), nil
			},
		})
	}

	// skill 注册成工具：LLM 自己判断何时加载，调用即把指令正文取回上下文。
	// 内容不常驻 system prompt，按需进上下文，省 token。
	for _, s := range p.skills {
		skill := s
		tools = append(tools, core.Tool{
			Name:        "skill_" + skill.Name,
			Description: fmt.Sprintf("Load the %s skill. Call it when you need to: %s", skill.Name, skill.Description),
			Parameters:  normalizeSchema(nil),
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				return skill.Content, nil
			},
		})
	}

	// memory_search：agent 主动回顾历史的结构化入口。不依赖 agent 猜档案路径 ——
	// 检索含 closed 的历史档案 + LTM 事实（SearchAll 全量，区别于注入只取 open，
	// agent 主动查档时过期快照不构成误导）。
	if p.memory != nil {
		tools = append(tools, core.Tool{
			Name:        "memory_search",
			Description: "Search historical memory: task archive (including completed tasks) and long-term facts. Use when the user asks about past sessions, previous work, or things you did before. Returns ranked task summaries and facts.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{"type": "string", "description": "search keywords"},
					"top_k": map[string]any{"type": "number", "description": "max results to return"},
				},
				"required": []string{"query"},
			},
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				query, _ := args["query"].(string)
				if strings.TrimSpace(query) == "" {
					return "", fmt.Errorf("memory_search: empty query")
				}
				topK := 5
				if n, ok := args["top_k"].(float64); ok && int(n) > 0 {
					topK = int(n)
				}
				if topK > 20 {
					topK = 20 // 防模型传超大 top_k 把全部档案渲染进上下文撑爆窗口
				}
				results, err := p.memory.SearchAll(ctx, query, topK, topK)
				if err != nil {
					return "", fmt.Errorf("memory_search: %w", err)
				}
				if len(results) == 0 {
					return "(no matching memory)", nil
				}
				var b strings.Builder
				for _, r := range results {
					switch {
					case r.Task != nil:
						t := r.Task
						fmt.Fprintf(&b, "- task [%s] %s\n", t.Status, oneLine(t.Title))
						if t.Summary != "" {
							b.WriteString("  " + oneLine(t.Summary) + "\n")
						}
					case r.Fact != nil:
						f := r.Fact
						if f.Topic != "" {
							fmt.Fprintf(&b, "- fact [%s] %s\n", f.Topic, oneLine(f.Content))
						} else {
							fmt.Fprintf(&b, "- fact %s\n", oneLine(f.Content))
						}
					}
				}
				return b.String(), nil
			},
		})
	}

	// MCP 工具：Init 时已从 server 拉取定义，这里是静态复用。
	// 工具是外部进程执行，不受本地 sandbox 管控，信任由"用户显式配置了哪个 server"建立。
	tools = append(tools, p.mcpTools...)
	return tools
}

// ---- Util ----

func parseSchema(schema []byte) map[string]any {
	if len(schema) == 0 {
		return map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}
	}
	var m map[string]any
	json.Unmarshal(schema, &m) // best effort
	return m
}
