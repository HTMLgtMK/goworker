package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/middlewares"
	"github.com/tinguo/goworker/daemon/internal/sandbox"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

type AgentPlugin struct {
	hub *spec.Hub
	mu  sync.Mutex
	// conversation 跨 /agent 调用累积，也被工具重入触发的 /compact 改写 ——
	// 与 usage tracker 同理由，加锁保护（每次只是短暂快照/写回，不跨 Run 持锁）。
	conversation []core.Message     // 跨 /agent 调用的对话历史
	usage        *core.UsageTracker // 当前 /agent 会话的 token 统计（纯数据，供 /usage 读取）
}

func (p *AgentPlugin) Name() string { return "agent" }

func (p *AgentPlugin) Init(h *spec.Hub) error {
	p.hub = h
	p.usage = core.NewUsageTracker()

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

	// 未匹配的任何命令都转发给 agent 处理
	h.SetFallbackHandler(p.handleAgent)

	return nil
}

func (p *AgentPlugin) Start() error { return nil }
func (p *AgentPlugin) Stop() error  { return nil }

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
		ctx.Writer("  可用 key: endpoint, model, api_key, context_window, compress_at, compact_keep, sandbox_mode\n")
		ctx.Writer("  context_window: 模型上下文窗口，如 32768 / 32k / 128k\n")
		ctx.Writer("  compress_at: 历史压缩触发阈值（0-1），用量达窗口该比例自动压缩，0 关闭\n")
		ctx.Writer("  compact_keep: 滚动压缩保留的最近消息条数\n")
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
		case "sandbox_mode":
			cfg.Sandbox.Mode = val
		default:
			ctx.Writer(fmt.Sprintf("未知配置项: %s（可用: endpoint, model, api_key, context_window, compress_at, compact_keep, sandbox_mode）\n", key))
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
	agent := NewAgent(provider, tools, []core.Middleware{hitlMw, usageMw, compressMw})
	agent.OnIteration = func() {
		if ctx.Publish != nil {
			ctx.Publish(statusbar.EventIteration, nil)
		}
	}

	agentCtx, cancel := context.WithTimeout(ctx.Ctx, 5*time.Minute)
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
	// 保存对话历史（剔除首条 system prompt）
	messages := <-msgCh
	if len(messages) > 1 {
		p.mu.Lock()
		p.conversation = messages[1:]
		p.mu.Unlock()
	}

	return nil
}

// ---- /usage 命令 ----

func (p *AgentPlugin) handleUsage(ctx *spec.Context) error {
	calls := p.usage.Calls()
	comps := p.usage.Compactions()
	if len(calls) == 0 && len(comps) == 0 {
		ctx.Writer("(当前会话还没有 agent 调用，先跑一次 /agent)\n")
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
	// 一次性锁内快照，空检查与后续压缩共用同一份数据
	p.mu.Lock()
	history := p.conversation
	before := len(history)
	p.mu.Unlock()
	if before == 0 {
		ctx.Writer("(还没有对话历史，先跑一次 /agent)\n")
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
		ctx.Writer("(历史太短或找不到安全切点，没压缩)\n")
		return nil
	}

	p.mu.Lock()
	p.conversation = compacted
	p.mu.Unlock()
	ctx.Writer(fmt.Sprintf("✔ 已压缩: %d 条 → %d 条\n", before, len(compacted)))
	return nil
}

// ---- /history 命令 ----

func (p *AgentPlugin) handleHistory(ctx *spec.Context) error {
	p.mu.Lock()
	conv := p.conversation
	p.mu.Unlock()
	if len(conv) == 0 {
		ctx.Writer("(还没有对话历史，先跑一次 /agent)\n")
		return nil
	}

	ctx.Writer(fmt.Sprintf("history: %d messages\n\n", len(conv)))
	for i, m := range conv {
		ctx.Writer(fmt.Sprintf("  #%-2d [%-9s] %s\n", i+1, m.Role, describeMessage(m)))
	}
	return nil
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
