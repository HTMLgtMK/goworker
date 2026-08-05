package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/spec"
	"github.com/tinguo/goworker/memory"
)

// 本文件是配置与诊断类命令：/model /usage /compact /skills /history /rules。

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
		sum, err := p.checkpointMemory(ctx.Ctx)
		if err != nil {
			ctx.Writer(fmt.Sprintf("⚠ Memory consolidation failed (original text lost after compression): %v\n", err))
		} else if notice := renderCheckpointNotice(sum); notice != "" {
			ctx.Writer(notice)
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

// ---- /rules 命令 ----

// handleRules 只读展示当前生效的指令快照 —— 与注入 system prompt 的内容一致，
// 方便核对"agent 到底被灌了什么规矩"。
func (p *AgentPlugin) handleRules(ctx *spec.Context) error {
	if p.instructions == nil {
		ctx.Writer("declarative instructions disabled (memory.enabled=false or load failed)\n")
		return nil
	}
	snap := p.instructions.Snapshot()
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
		config.DefaultDir(),
		cwd,
		memory.Cap{
			UserMaxChars:   p.hub.Config.Memory.UserMaxChars,
			AgentsMaxChars: p.hub.Config.Memory.AgentsMaxChars,
		},
	)
	if err != nil {
		slog.Warn("instructions load failed, instructions disabled", "err", err)
		return nil
	}
	return inst
}
