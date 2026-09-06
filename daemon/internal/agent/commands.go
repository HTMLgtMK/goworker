package agent

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-memory"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// 本文件是配置与诊断类命令：/agent /new /model /usage /compact /skills /history /rules。
// 命令逻辑全部委托给 Session SDK，这里只做 plugin.Context → RunCallbacks 的薄适配。

// ---- /agent 命令 ----

func (p *AgentPlugin) handleAgent(ctx *plugin.Context) error {
	return p.session.Run(ctx.Ctx, runtimeagent.RunRequest{Input: strings.Join(ctx.Args, " ")}, callbacksFromPlugin(ctx))
}

// callbacksFromPlugin 把 plugin.Context 的 I/O 回调适配成 runtimeagent.RunCallbacks。
// WriteToken 只在 ctx 提供时注入，避免把 nil 包装成非 nil 绕过 SDK 的 nil 保护。
func callbacksFromPlugin(ctx *plugin.Context) runtimeagent.RunCallbacks {
	cb := runtimeagent.RunCallbacks{
		Write:   ctx.Writer,
		Decide:  ctx.Decide,
		Publish: ctx.Publish,
	}
	if ctx.WriteToken != nil {
		cb.WriteToken = func(kind runtimeagent.RenderKind, content string, done bool) {
			ctx.WriteToken(plugin.RenderKind(kind), content, done)
		}
	}
	return cb
}

// ---- /new 命令 ----

func (p *AgentPlugin) handleNew(ctx *plugin.Context) error {
	// 结束当前会话：用新实例替换旧实例，旧会话状态（conversation/usage）随对象回收。
	// 后台固化旧会话，不阻塞输入；快照走只读，新会话创建不影响这份引用。
	old := p.session

	// pending 快照（store 生效时用游标后的增量，否则全量）
	var pending []core.Message
	if p.store != nil {
		pending = p.store.PendingAfterCursor()
		if len(pending) == 0 {
			pending = old.Conversation() // 兜底全量
		}
	} else {
		pending = old.Conversation()
	}

	if p.memory != nil && len(pending) > 0 {
		// 后台固化旧会话，不阻塞输入。登记 bgWg：Stop 关 memory 前等它结束。
		p.bgWg.Add(1)
		go func() {
			defer p.bgWg.Done()
			old.CheckpointAsync(pending, ctx.Writer)
		}()
		ctx.Writer("✔ New session started, consolidating previous session in background…\n")
	} else if p.memory == nil {
		ctx.Writer("✔ New session started (memory disabled)\n")
	} else {
		ctx.Writer("✔ New session started\n")
	}

	// 先归档旧 store，再开新会话（新 store 写新文件）
	// pending 快照已在 Archive 前取出，不受归档影响
	if p.store != nil {
		if err := p.store.Archive(); err != nil {
			slog.Warn("session: archive failed", "err", err)
		}
	}

	// 创建新会话（新对象 = 新会话，旧状态随对象回收）
	p.startSession()

	ctx.Writer("✔ STM cleared, memory will be retrieved on every query\n")
	return nil
}

// ---- /model 命令 ----

func (p *AgentPlugin) handleModel(ctx *plugin.Context) error {
	if len(ctx.Args) == 0 {
		p.showConfig(ctx)
		return nil
	}
	switch ctx.Args[0] {
	case "list":
		names := make([]string, 0, len(p.cfg.LLM.Providers))
		for name := range p.cfg.LLM.Providers {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			marker := " "
			if name == p.cfg.LLM.DefaultProvider {
				marker = "*"
			}
			provider := p.cfg.LLM.Providers[name]
			ctx.Writer(fmt.Sprintf("%s %s (%s, %s)\n", marker, name, provider.Type, provider.Model))
		}
	case "use":
		if len(ctx.Args) != 2 {
			ctx.Writer("用法: /model use <provider>\n")
			return nil
		}
		candidate := *p.cfg
		candidate.LLM = p.cfg.LLM.Clone()
		candidate.LLM.DefaultProvider = ctx.Args[1]
		if err := candidate.LLM.Validate(); err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if err := p.hub.SaveConfig(&candidate); err != nil {
			ctx.Writer(fmt.Sprintf("✘ 保存失败: %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ 已切换到 provider %s\n", ctx.Args[1]))
	case "set":
		if len(ctx.Args) != 2 {
			ctx.Writer("用法: /model set <key>=<value>\n")
			return nil
		}
		kv := strings.SplitN(ctx.Args[1], "=", 2)
		if len(kv) != 2 {
			ctx.Writer("格式错误，示例: /model set model=gpt-4o\n")
			return nil
		}
		candidate := *p.cfg
		candidate.LLM = p.cfg.LLM.Clone()
		if err := setModelField(&candidate, kv[0], kv[1]); err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if err := p.hub.SaveConfig(&candidate); err != nil {
			ctx.Writer(fmt.Sprintf("✘ 保存失败: %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ %s 已更新\n", kv[0]))
	case "help":
		ctx.Writer("用法: /model | /model list | /model use <provider> | /model set <key>=<value>\n")
		ctx.Writer("Provider 字段: endpoint, model, api_key, context_window, thinking_request_mode, thinking_effort, auth_type, max_tokens\n")
		ctx.Writer("全局字段: global.compress_at, global.compact_keep, global.max_iterations, global.thinking_show\n")
	default:
		ctx.Writer("未知子命令，使用 /model help 查看用法\n")
	}
	return nil
}

func setModelField(cfg *runtimeconfig.Config, key, value string) error {
	if strings.HasPrefix(key, "global.") {
		switch strings.TrimPrefix(key, "global.") {
		case "compress_at":
			parsed, err := runtimeconfig.ParseCompressAt(value)
			if err != nil {
				return err
			}
			cfg.LLM.CompressAt = parsed
		case "compact_keep":
			parsed, err := runtimeconfig.ParseCompactKeep(value)
			if err != nil {
				return err
			}
			cfg.LLM.CompactKeep = parsed
		case "max_iterations":
			parsed, err := runtimeconfig.ParseMaxIterations(value)
			if err != nil {
				return err
			}
			cfg.LLM.MaxIterations = parsed
		case "thinking_show":
			parsed, err := runtimeconfig.ParseThinkingShow(value)
			if err != nil {
				return err
			}
			cfg.LLM.Thinking.Show = parsed
		default:
			return fmt.Errorf("未知全局配置项: %s", key)
		}
		return cfg.LLM.Validate()
	}
	providerName := cfg.LLM.DefaultProvider
	provider := cfg.LLM.Providers[providerName]
	switch key {
	case "endpoint":
		provider.Endpoint = value
	case "model":
		provider.Model = value
	case "api_key":
		provider.APIKey = value
	case "context_window":
		parsed, err := runtimeconfig.ParseContextWindow(value)
		if err != nil {
			return err
		}
		provider.ContextWindow = parsed
	case "thinking_request_mode":
		parsed, err := runtimeconfig.ParseThinkingRequestMode(value)
		if err != nil {
			return err
		}
		provider.Thinking.RequestMode = parsed
	case "thinking_effort":
		parsed, err := runtimeconfig.ParseThinkingEffort(value)
		if err != nil {
			return err
		}
		provider.Thinking.Effort = parsed
	case "auth_type":
		provider.AuthType = runtimeconfig.AnthropicAuthType(value)
	case "max_tokens":
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return fmt.Errorf("无效 max_tokens: %s", value)
		}
		provider.MaxTokens = parsed
	default:
		return fmt.Errorf("未知 provider 配置项: %s", key)
	}
	cfg.LLM.Providers[providerName] = provider
	return cfg.LLM.Validate()
}

func (p *AgentPlugin) showConfig(ctx *plugin.Context) {
	name, provider, err := p.cfg.LLM.ResolveDefault()
	if err != nil {
		ctx.Writer(fmt.Sprintf("✘ Provider 配置错误: %v\n", err))
		return
	}
	keyDisplay := "(未设置)"
	if provider.APIKey != "" {
		keyDisplay = "***"
	}
	ctx.Writer(fmt.Sprintf("Provider:       %s (%s)\n", name, provider.Type))
	ctx.Writer(fmt.Sprintf("Endpoint:       %s\n", provider.Endpoint))
	ctx.Writer(fmt.Sprintf("Model:          %s\n", provider.Model))
	ctx.Writer(fmt.Sprintf("API Key:        %s\n", keyDisplay))
	ctx.Writer(fmt.Sprintf("Context Window: %d\n", provider.ContextWindow))
	ctx.Writer(fmt.Sprintf("Compress At:    %v\n", p.cfg.LLM.CompressAt))
	ctx.Writer(fmt.Sprintf("Compact Keep:   %d\n", p.cfg.LLM.CompactKeep))
	ctx.Writer(fmt.Sprintf("Max Iterations: %d\n", p.cfg.LLM.MaxIterations))
	ctx.Writer(fmt.Sprintf("Thinking Show:  %t\n", p.cfg.LLM.Thinking.Show))
	if provider.Type == runtimeconfig.ProviderTypeOpenAI {
		ctx.Writer(fmt.Sprintf("Thinking Mode:  %s\n", provider.Thinking.RequestMode))
		ctx.Writer(fmt.Sprintf("Thinking Effort: %s\n", provider.Thinking.Effort))
	}
	ctx.Writer(fmt.Sprintf("Sandbox Mode:   %s\n", p.cfg.Sandbox.Mode))
}

func selectedContextWindow(cfg *runtimeconfig.Config) int {
	_, provider, err := cfg.LLM.ResolveDefault()
	if err != nil {
		return 0
	}
	return provider.ContextWindow
}

// ---- /usage 命令 ----

func (p *AgentPlugin) handleUsage(ctx *plugin.Context) error {
	calls, comps, total := p.session.UsageSnapshot()
	if len(calls) == 0 && len(comps) == 0 {
		ctx.Writer("(no agent calls yet — run /agent first)\n")
		return nil
	}

	if len(calls) > 0 {
		ctx.Writer(fmt.Sprintf("usage: %d model calls this session\n\n", len(calls)))
		for i, c := range calls {
			line := fmt.Sprintf("  #%-2d  in %-7s  out %-7s  (est %s)",
				i+1, runtimeagent.Humanize(c.PromptTokens), runtimeagent.Humanize(c.CompletionTokens), runtimeagent.Humanize(c.EstimateTokens))
			// 模型没返回 cache 字段时明细保持简洁，不挂一串 0
			if c.PromptCacheHitTokens+c.PromptCacheMissTokens > 0 {
				line += fmt.Sprintf("  hit: %-7s miss: %-7s / %s",
					runtimeagent.Humanize(c.PromptCacheHitTokens), runtimeagent.Humanize(c.PromptCacheMissTokens), runtimeagent.Humanize(c.PromptCacheHitTokens+c.PromptCacheMissTokens))
			}
			ctx.Writer(line + "\n")
		}
		ctx.Writer("\n")
		totalLine := fmt.Sprintf("  total: in %s  out %s  total %s",
			runtimeagent.Humanize(total.PromptTokens), runtimeagent.Humanize(total.CompletionTokens), runtimeagent.Humanize(total.TotalTokens))
		if rate, ok := total.CacheHitRate(); ok {
			totalLine += fmt.Sprintf("  cache %.1f%% (%s hit / %s miss)",
				rate, runtimeagent.Humanize(total.PromptCacheHitTokens), runtimeagent.Humanize(total.PromptCacheMissTokens))
		}
		ctx.Writer(totalLine + "\n")

		// context usage uses "last prompt / window" — the final ReAct request already holds all history
		if w := selectedContextWindow(p.cfg); w > 0 && total.LastPromptTokens > 0 {
			pct := float64(total.LastPromptTokens) / float64(w) * 100
			ctx.Writer(fmt.Sprintf("  context: %.2f%% (last in %s / window %s)\n",
				pct, runtimeagent.Humanize(total.LastPromptTokens), runtimeagent.Humanize(w)))
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
			len(comps), msgsIn, msgsOut, runtimeagent.Humanize(tokens)))
	}

	// 当前历史占用：压缩后的直观体现，不必等下一次模型调用。
	conv := p.session.Conversation()
	if convLen := len(conv); convLen > 0 {
		convEst := core.EstimateTokens(conv)
		line := fmt.Sprintf("  history: %s est (%d msgs)", runtimeagent.Humanize(convEst), convLen)
		if w := selectedContextWindow(p.cfg); w > 0 {
			line += fmt.Sprintf(", %.2f%% of window", float64(convEst)/float64(w)*100)
		}
		ctx.Writer(line + "\n")
	}
	return nil
}

// ---- /compact 命令 ----

func (p *AgentPlugin) handleCompact(ctx *plugin.Context) error {
	return p.session.Compact(ctx.Ctx, callbacksFromPlugin(ctx))
}

// ---- /skills 命令 ----

func (p *AgentPlugin) handleSkills(ctx *plugin.Context) error {
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

// ---- /history 命令 ----

func (p *AgentPlugin) handleHistory(ctx *plugin.Context) error {
	conv := p.session.Conversation()
	if len(conv) == 0 {
		ctx.Writer("(no conversation history yet — run /agent first)\n")
		return nil
	}

	ctx.Writer(fmt.Sprintf("history: %d messages\n\n", len(conv)))
	for i, m := range conv {
		ctx.Writer(fmt.Sprintf("  #%-2d [%-9s] %s\n", i+1, m.Role, runtimeagent.DescribeMessage(m)))
	}
	return nil
}

// ---- /rules 命令 ----

// handleRules 只读展示当前生效的指令快照 —— 与注入 system prompt 的内容一致，
// 方便核对"agent 到底被灌了什么规矩"。
func (p *AgentPlugin) handleRules(ctx *plugin.Context) error {
	if p.deps.Instruction == nil {
		ctx.Writer("declarative instructions disabled (memory.enabled=false or load failed)\n")
		return nil
	}
	snap := p.deps.Instruction.Snapshot()
	if snap == "" {
		ctx.Writer("(no active instructions — global AGENTS.md/USER.md go in ~/.config/goworker/, project AGENTS.md in the project root)\n")
		return nil
	}
	ctx.Writer(snap + "\n")
	return nil
}

// loadInstructions 构造声明式指令快照。失败降级为 nil（指令禁用，不阻塞）。
func (p *AgentPlugin) loadInstructions() *memory.InstructionSet {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}
	inst, err := memory.LoadInstructions(
		p.paths.ConfigDir,
		cwd,
		memory.Cap{
			UserMaxChars:   p.cfg.Memory.UserMaxChars,
			AgentsMaxChars: p.cfg.Memory.AgentsMaxChars,
		},
	)
	if err != nil {
		slog.Warn("instructions load failed, instructions disabled", "err", err)
		return nil
	}
	return inst
}
