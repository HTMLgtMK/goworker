package agent

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/middlewares"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// 本文件是会话运行与控制命令：/agent（对话）与 /new（结束会话）。

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
				EstimateTokens:        u.EstimateTokens,
				PromptTokens:          u.PromptTokens,
				PromptCacheHitTokens:  u.PromptCacheHitTokens,
				PromptCacheMissTokens: u.PromptCacheMissTokens,
				CompletionTokens:      u.CompletionTokens,
				TotalTokens:           u.TotalTokens,
				LastPromptTokens:      u.LastPromptTokens,
				ContextWindow:         cfg.LLM.ContextWindow,
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
	opts := []Option{WithMaxIterations(cfg.LLM.MaxIterations)}
	if p.instructions != nil {
		opts = append(opts, WithSystemExtra(p.instructions.Snapshot()))
	}
	agent := NewAgent(provider, tools, mws, opts...)
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
	// 会话边界：重新构造声明式指令快照，会话内手改的 AGENTS.md/USER.md 从此生效
	if p.instructions != nil {
		if inst := p.loadInstructions(); inst != nil {
			p.instructions = inst
		} else {
			slog.Warn("instructions reload failed, keeping stale snapshot")
		}
	}
	ctx.Writer("✔ STM cleared, memory will be retrieved on every query\n")
	return nil
}
