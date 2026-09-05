package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	coreagent "github.com/tinguo/goworker/ai-core/agent"
	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/middlewares"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
)

// 本文件是会话层：Session 收敛会话状态（conversation/usage）与核心操作，
// 命令 /agent 与 /new 经薄壳调用。AgentPlugin 只保留资源装载与命令注册。

// ---- Session：会话状态与核心操作 ----
//
// Session/SessionDeps 类型定义见 spec.go。本文件是实现：Run/Compact/流式输出/历史写入。

// NewSession 创建一个会话。usage 初始清零，conversation 从空开始。
// 有 store 时从持久化恢复会话（重启恢复），无 store（nil）时纯内存。
func NewSession(deps SessionDeps) *Session {
	s := &Session{usage: core.NewUsageTracker(), deps: deps}
	_ = s.refreshConversation() // 构造时无并发，锁无害；与运行时共用统一刷新路径
	return s
}

// Run 执行一轮 /agent 对话：组装 agent → 跑 ReAct 循环 → 流式输出 → 写回 conversation。
func (s *Session) Run(ctx context.Context, req RunRequest, cb RunCallbacks) error {
	input := strings.TrimSpace(req.Input)
	if input == "" {
		if cb.Write != nil {
			cb.Write("用法: /agent <你的问题>\n")
		}
		return nil
	}

	// usage 是会话累计账本：跨多次 /agent 累计，直到 /new 用新对象替换当前会话才清零。
	sandboxCfg := *sandbox.NewFromConfig(&s.deps.Config.Sandbox)
	tools := s.deps.CollectTools(&sandboxCfg)
	cfg := s.deps.Config

	// 命令决策审计：配置开启时首次 Run 惰性打开，复用文件句柄（不每轮重开）。
	if s.audit == nil && cfg.Sandbox.AuditLog {
		lg, err := sandbox.OpenAudit(s.deps.AuditDir)
		if err != nil {
			slog.Warn("sandbox: audit open failed, audit disabled", "err", err)
		} else {
			s.audit = lg
		}
	}

	provider, err := s.deps.NewProvider(cfg)
	if err != nil {
		if cb.Write != nil {
			cb.Write(fmt.Sprintf("✘ Provider 配置错误: %v\n", err))
		}
		return nil
	}
	decisions := make(chan hitl.Decision, 1)
	mws := s.buildMiddlewareChain(cb, provider, &sandboxCfg, cfg, decisions)
	systemPrompt := s.buildSystemPrompt(cfg, tools)

	opts := []coreagent.Option{coreagent.WithMaxIterations(cfg.LLM.MaxIterations)}
	agent := coreagent.NewAgent(provider, systemPrompt, tools, mws, opts...)

	// agentCtx 不设硬超时：多轮 tool call 总耗时不可控，5 分钟硬超时只会在工具循环
	// 中途切断会话，且超时瞬间 sendToken 会把唯一的错误提示吞掉，前端表现成"莫名停止"。
	// 兜底交给单次请求自身：provider http 30s 超时；bash 类子进程工具受 60s 限制。
	agentCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 快照历史给本轮的 Run（工具重入触发的 /compact 不会污染本次运行的输入）
	s.mu.Lock()
	conv := s.conversation
	s.mu.Unlock()

	tokenCh, msgCh, err := agent.Run(agentCtx, conv, input)
	if err != nil {
		if cb.Write != nil {
			cb.Write(fmt.Sprintf("✘ %v\n", err))
		}
		return nil
	}
	s.streamTokens(cb, agentCtx, tokenCh, decisions)

	// 保存对话历史（剔除首条 system prompt + memory 注入的记忆提醒块）。
	// 记忆块只服务本次 run 的注入，若漏进 STM：下次 run 会重发它、检查点会把
	// 它当对话内容固化（自指污染）—— 必须过滤掉。
	messages := <-msgCh
	if len(messages) > 1 {
		conv2 := stripMemoryBlocks(messages[1:]) // messages[0] 是 system prompt
		if s.deps.Store == nil {
			// 纯内存模式：保持旧行为
			s.mu.Lock()
			s.conversation = conv2
			s.mu.Unlock()
		} else {
			// 持久化模式：检测压缩 → compact 或直接 commit
			if from, to, summary, ok := session.DetectCompact(conv, conv2); ok {
				// conv2[0] 是摘要（system），由 compact 节点承载，不落 msg
				if err := s.deps.Store.Compact(from, to, summary); err != nil {
					slog.Warn("session: compact failed", "err", err)
				}
				if len(conv2) > 1 {
					if _, err := s.deps.Store.Commit(conv2[1:]); err != nil {
						slog.Warn("session: commit after compact failed", "err", err)
					}
				}
			} else {
				if _, err := s.deps.Store.Commit(conv2); err != nil {
					slog.Warn("session: commit failed", "err", err)
				}
			}
			// 提交后自动打 checkpoint，作为 /rewind 回溯锚点（plan「核心模型」）
			if err := s.deps.Store.Checkpoint(Truncate(input, 80)); err != nil {
				slog.Warn("session: checkpoint after commit failed", "err", err)
			}
			if err := s.refreshConversation(); err != nil {
				slog.Warn("session: refresh conversation after run failed", "err", err)
			}
		}
	}
	return nil
}

// streamTokens 消费 agent 的流式输出：渲染 token、转发 HITL 决策。
// HITL 决策由前端按 spec 契约完成，plugin 只负责转交；agentCtx 取消时决策投递放弃。
func (s *Session) streamTokens(cb RunCallbacks, agentCtx context.Context, tokenCh <-chan core.Token, decisions chan hitl.Decision) {
	for tok := range tokenCh {
		// 先输出内容再检查 Done — Done token 也可能带内容（如错误信息）
		showToken := tok.Type != core.TokenTypeThinking || s.deps.Config.LLM.Thinking.Show
		if showToken && tok.Content != "" && cb.WriteToken != nil {
			kind, c := renderKind(tok)
			cb.WriteToken(kind, c)
		}
		if tok.Done {
			break
		}
		if tok.Type == core.TokenTypeEvent && tok.Event != nil && tok.Event.Type == hitl.EventInterrupt {
			var req hitl.InterruptRequest
			if err := json.Unmarshal(tok.Event.Data, &req); err != nil {
				slog.Warn("hitl: decode interrupt event failed", "err", err)
				continue
			}
			decision := hitl.Decision{InterruptID: req.ID, Type: hitl.DecisionReject}
			if cb.Decide != nil {
				decision = cb.Decide(&req)
			}
			select {
			case decisions <- decision:
			case <-agentCtx.Done():
			}
			continue
		}
	}
	if cb.Write != nil {
		cb.Write("\n")
	}
}

// buildMiddlewareChain 组装本轮 Run 的中间件链。
// 顺序：hitl → usage → iteration →（memory 启用时插入）→ compress。
// memory 必须在 compress 前：压缩器测量的是注入记忆后的完整 history。
func (s *Session) buildMiddlewareChain(cb RunCallbacks, provider core.Provider, sandboxCfg *sandbox.Config, cfg *runtimeconfig.Config, decisions chan hitl.Decision) []core.Middleware {
	publish := func(event string, data any) {
		if cb.Publish != nil {
			cb.Publish(event, data)
		}
	}

	hitlMw := middlewares.NewHITLMiddleware(*sandboxCfg, hitl.NewChannelDecisionProvider(decisions), middlewares.WithAudit(s.audit))
	// usage：观察 AfterModel 记账（累计到会话边界才清零），publish 抛 core.Usage 快照。
	// 事件契约在 ai-runtime/config（UsageEvent），前端 addon 订阅后自行渲染 —— statusbar 不进 SDK。
	usageMw := middlewares.NewUsageMiddleware(s.usage, func(u core.Usage) {
		publish(runtimeconfig.EventUsage, runtimeconfig.UsageEvent{
			Usage:         u,
			ContextWindow: contextWindow(cfg),
		})
	})
	// iteration：每轮迭代发一个计数事件
	iterationMw := middlewares.NewIterationMiddleware(func() {
		publish(runtimeconfig.EventIteration, nil)
	})
	// compress：自动压缩，protectSystem=true —— 保护本轮注入的系统提示
	compressMw := middlewares.NewCompressionMiddleware(
		s.newCompressor(provider, true),
		contextWindow(cfg),
		cfg.LLM.CompressAt,
	)

	mws := []core.Middleware{hitlMw, usageMw, iterationMw}
	if s.deps.Memory != nil {
		memMw := middlewares.NewMemoryMiddleware(&memoryClientAdapter{s.deps.Memory}, cfg.Memory, contextWindow(cfg))
		mws = append(mws, memMw)
	}
	return append(mws, compressMw)
}

func contextWindow(cfg *runtimeconfig.Config) int {
	_, provider, err := cfg.LLM.ResolveDefault()
	if err != nil {
		return 0
	}
	return provider.ContextWindow
}

// newCompressor 构造压缩器，onCompress 统一记进 token 账本（/usage 能看到压缩）。
// protectSystem=true：history[0] 是本轮注入的系统提示，压掉模型就忘了怎么用工具；
// /compact 场景 conversation 不含系统提示，首位可能是上次的摘要，允许被再次滚动，传 false。
func (s *Session) newCompressor(provider core.Provider, protectSystem bool) *middlewares.Compressor {
	cfg := s.deps.Config
	return middlewares.NewCompressor(provider, cfg.LLM.CompactKeep, protectSystem, func(r middlewares.CompressReport) {
		s.usage.RecordCompaction(core.Compaction{BeforeMsgs: r.BeforeMsgs, AfterMsgs: r.AfterMsgs, Tokens: r.Tokens})
	})
}

// Compact 执行 /compact：压缩会丢原文，先固化到任务档案，再压缩当前 conversation。
func (s *Session) Compact(ctx context.Context, cb RunCallbacks) error {
	if s.deps.Store == nil {
		// 纯内存模式：保持旧逻辑
		return s.compactMemory(ctx, cb)
	}
	return s.compactStore(ctx, cb)
}

// compactMemory 纯内存压缩（store 禁用时的回退路径）。
func (s *Session) compactMemory(ctx context.Context, cb RunCallbacks) error {
	s.mu.Lock()
	history := s.conversation
	before := len(history)
	s.mu.Unlock()

	// 压缩会丢原文，先固化到任务档案 —— 这是原文丢失前的最后一次机会。
	if s.deps.Memory != nil {
		sum, err := s.checkpoint(ctx, history, s.deps.Config)
		if err != nil {
			if cb.Write != nil {
				cb.Write(fmt.Sprintf("⚠ Memory consolidation failed (original text lost after compression): %v\n", err))
			}
		} else if notice := RenderCheckpointNotice(sum); notice != "" && cb.Write != nil {
			cb.Write(notice)
		}
	}

	if before == 0 {
		if cb.Write != nil {
			cb.Write("(no conversation history yet — run /agent first)\n")
		}
		return nil
	}

	cfg := s.deps.Config
	provider, err := s.deps.NewProvider(cfg)
	if err != nil {
		if cb.Write != nil {
			cb.Write(fmt.Sprintf("✘ Provider 配置错误: %v\n", err))
		}
		return nil
	}
	compressor := s.newCompressor(provider, false)

	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	compacted, err := compressor.Compress(cctx, history)
	if err != nil {
		if cb.Write != nil {
			cb.Write(fmt.Sprintf("✘ 压缩失败: %v\n", err))
		}
		return nil
	}
	if len(compacted) >= before {
		if cb.Write != nil {
			cb.Write("(history too short or no safe split point; not compacted)\n")
		}
		return nil
	}

	s.mu.Lock()
	s.conversation = compacted
	s.mu.Unlock()
	if cb.Write != nil {
		cb.Write(fmt.Sprintf("✔ 已压缩: %d 条 → %d 条\n", before, len(compacted)))
	}
	return nil
}

// compactStore store 持久化模式压缩：ActiveView 先固化 → 压缩 → detectCompact → 落 compact。
func (s *Session) compactStore(ctx context.Context, cb RunCallbacks) error {
	// 先固化再折叠（原文丢失前最后一次机会）
	_, _ = s.Consolidate(ctx)

	view := s.deps.Store.ActiveView()
	if len(view) == 0 {
		if cb.Write != nil {
			cb.Write("(no conversation history yet — run /agent first)\n")
		}
		return nil
	}
	before := len(view)

	cfg := s.deps.Config
	provider, err := s.deps.NewProvider(cfg)
	if err != nil {
		if cb.Write != nil {
			cb.Write(fmt.Sprintf("✘ Provider 配置错误: %v\n", err))
		}
		return nil
	}
	// protectSystem=false：conversation 不含系统提示，首位可能是上次的摘要，允许被再次滚动
	compressor := s.newCompressor(provider, false)

	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	compacted, err := compressor.Compress(cctx, view)
	if err != nil {
		if cb.Write != nil {
			cb.Write(fmt.Sprintf("✘ 压缩失败: %v\n", err))
		}
		return nil
	}
	if len(compacted) >= before {
		if cb.Write != nil {
			cb.Write("(history too short or no safe split point; not compacted)\n")
		}
		return nil
	}

	// detectCompact 检测压缩前后变化，提取摘要 + 覆盖段
	if from, to, summary, ok := session.DetectCompact(view, compacted); ok {
		if err := s.deps.Store.Compact(from, to, summary); err != nil {
			if cb.Write != nil {
				cb.Write(fmt.Sprintf("✘ 压缩失败: %v\n", err))
			}
			return nil
		}
		if err := s.refreshConversation(); err != nil {
			if cb.Write != nil {
				cb.Write(fmt.Sprintf("✘ 刷新会话失败: %v\n", err))
			}
			return nil
		}
		if cb.Write != nil {
			cb.Write(fmt.Sprintf("✔ 已压缩: %d 条 → %d 条\n", before, len(s.conversation)))
		}
	} else {
		// 不可压缩（detectCompact 判断不满足条件），保持现状
		if cb.Write != nil {
			cb.Write("(history too short or no safe split point; not compacted)\n")
		}
	}
	return nil
}

// Conversation 返回当前 conversation 的锁内快照。只读用途统一走这里：
// /history 展示、/usage 估算、checkpoint 固化、/new 归档共用。
func (s *Session) Conversation() []core.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conversation
}

// ReloadFromStore 从持久化 store 刷新 conversation 为当前活跃视图。
// /rewind 等操作改变 store head 后调用，使内存 session 与磁盘状态同步。
func (s *Session) ReloadFromStore() error {
	return s.refreshConversation()
}

// refreshConversation 从 store 刷新 conversation 为当前活跃视图。
// store 为 nil 时 no-op（纯内存模式）。NewSession/Run/compactStore/rewind 共用，
// 锁内一致刷新，避免各写各的同步逻辑漂移。
func (s *Session) refreshConversation() error {
	if s.deps.Store == nil {
		return nil
	}
	s.mu.Lock()
	s.conversation = s.deps.Store.ActiveView()
	s.mu.Unlock()
	return nil
}

// UsageSnapshot 返回当前会话的用量数据（/usage 展示用，渲染留 Plugin 层）。
func (s *Session) UsageSnapshot() ([]core.Usage, []core.Compaction, core.Usage) {
	return s.usage.Calls(), s.usage.Compactions(), s.usage.Snapshot()
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

// 生成系统提示词
func (s *Session) buildSystemPrompt(cfg *runtimeconfig.Config, tools []core.Tool) string {
	var b strings.Builder
	b.WriteString("You are a coding assistant with tool access.\n")
	b.WriteString("Use tools when you need to explore, run commands, or modify files.\n")
	b.WriteString("Think step by step. After getting tool results, continue reasoning.\n")
	b.WriteString("When you have enough info, provide a complete answer.\n\n")

	// 注入 AGENT.md + USER.md（memory disabled 或装载失败时 Instruction 为 nil，跳过指令段）
	if s.deps.Instruction != nil {
		b.WriteString(s.deps.Instruction.Snapshot())
	}

	// 注入 tools 清单
	b.WriteString("Available tools:\n")
	for _, t := range tools {
		b.WriteString(fmt.Sprintf("- %s: %s\n", t.Name, t.Description))
	}
	b.WriteString("\nRespond naturally. Use tools when needed.")
	return b.String()
}
