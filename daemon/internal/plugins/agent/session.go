package agent

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/middlewares"
	"github.com/tinguo/goworker/daemon/internal/sandbox"
	"github.com/tinguo/goworker/daemon/internal/session"
	"github.com/tinguo/goworker/daemon/internal/spec"
	"github.com/tinguo/goworker/memory"
)

// 本文件是会话层：Session 收敛会话状态（conversation/usage）与核心操作，
// 命令 /agent 与 /new 经薄壳调用。AgentPlugin 只保留资源装载与命令注册。

// ---- Session：会话状态与核心操作 ----

// Session 是一段会话：收敛会话状态（conversation/usage）与核心操作（Run/Compact/…）。
// NewSession 创建会话（Init / /new 各一次）；/new 用新实例替换旧实例，旧会话状态随对象回收。
// AgentPlugin 只保留资源装载（memory/mcp/skills/instructions）与命令注册。
// store 留待 A（会话持久化）落地后接入 —— 骨架阶段不碰。
type Session struct {
	mu           sync.Mutex
	conversation []core.Message
	usage        *core.UsageTracker

	deps SessionDeps
}

// SessionDeps 是会话构造输入包：插件级资源 + 本会话参数，Init 组装后每次 NewSession 复用。
// 其余字段跨会话不变。可变资源（instructions）的装载在 plugin 层，这里只收最终值。
type SessionDeps struct {
	Hub          *spec.Hub
	Memory       *memory.Client                         // nil = 禁用
	CollectTools func(cfg *sandbox.Config) []core.Tool  // 方法值捕获 p，按需收集工具
	NewProvider  func(cfg *config.Config) core.Provider // 默认 NewOpenAIProvider，测试注入 fake
	Instruction  *memory.InstructionSet                 // 会话边界刷新（Init / /new 经 startSession 重载）
	Store        *session.Store                         // nil = 持久化禁用
}

// NewSession 创建一个会话。usage 初始清零，conversation 从空开始。
// 有 store 时从持久化恢复会话（重启恢复），无 store（nil）时纯内存。
func NewSession(deps SessionDeps) *Session {
	s := &Session{usage: core.NewUsageTracker(), deps: deps}
	_ = s.refreshConversation() // 构造时无并发，锁无害；与运行时共用统一刷新路径
	return s
}

// Run 执行一轮 /agent 对话：组装 agent → 跑 ReAct 循环 → 流式输出 → 写回 conversation。
func (s *Session) Run(ctx *spec.Context) error {
	input := strings.Join(ctx.Args, " ")
	if input == "" {
		ctx.Writer("用法: /agent <你的问题>\n")
		return nil
	}

	// usage 是会话累计账本：跨多次 /agent 累计，直到 /new 用新对象替换当前会话才清零。
	sandboxCfg := *sandbox.NewFromConfig(&s.deps.Hub.Config.Sandbox)
	tools := s.deps.CollectTools(&sandboxCfg)
	cfg := s.deps.Hub.Config
	provider := s.deps.NewProvider(cfg)
	decisions := make(chan spec.HITLDecision, 1)
	mws := s.buildMiddlewareChain(ctx, provider, &sandboxCfg, cfg, decisions)
	systemPrompt := s.buildSystemPrompt(cfg, tools)

	opts := []Option{WithMaxIterations(cfg.LLM.MaxIterations)}
	agent := NewAgent(provider, systemPrompt, tools, mws, opts...)

	// agentCtx 不设硬超时：多轮 tool call 总耗时不可控，5 分钟硬超时只会在工具循环
	// 中途切断会话，且超时瞬间 sendToken 会把唯一的错误提示吞掉，前端表现成"莫名停止"。
	// 兜底交给单次请求自身：provider http 30s 超时；bash 类子进程工具受 60s 限制。
	agentCtx, cancel := context.WithCancel(ctx.Ctx)
	defer cancel()

	// 快照历史给本轮的 Run（工具重入触发的 /compact 不会污染本次运行的输入）
	s.mu.Lock()
	conv := s.conversation
	s.mu.Unlock()

	tokenCh, msgCh, err := agent.Run(agentCtx, conv, input)
	if err != nil {
		ctx.Writer(fmt.Sprintf("✘ %v\n", err))
		return nil
	}
	s.streamTokens(ctx, agentCtx, tokenCh, decisions)

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
			if err := s.deps.Store.Checkpoint(truncate(input, 80)); err != nil {
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
func (s *Session) streamTokens(ctx *spec.Context, agentCtx context.Context, tokenCh <-chan core.Token, decisions chan spec.HITLDecision) {
	for tok := range tokenCh {
		// 先输出内容再检查 Done — Done token 也可能带内容（如错误信息）
		if tok.Content != "" {
			kind, c := renderKind(tok)
			ctx.WriteToken(kind, c)
		}
		if tok.Done {
			break
		}
		if tok.Type == core.TokenTypeInterrupt && tok.Interrupt != nil {
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
}

// buildMiddlewareChain 组装本轮 Run 的中间件链。
// 顺序：hitl → usage → iteration →（memory 启用时插入）→ compress。
// memory 必须在 compress 前：压缩器测量的是注入记忆后的完整 history。
func (s *Session) buildMiddlewareChain(ctx *spec.Context, provider core.Provider, sandboxCfg *sandbox.Config, cfg *config.Config, decisions chan spec.HITLDecision) []core.Middleware {
	// 发布能力收拢：spec.Context 的 Publish 是通用广播（event string, data any），
	// middleware 只认专用签名，在这里做适配。nil 保护集中在收拢点。
	publish := func(event string, data any) {
		if ctx.Publish != nil {
			ctx.Publish(event, data)
		}
	}

	hitlMw := middlewares.NewHITLMiddleware(*sandboxCfg, middlewares.NewChannelDecisionProvider(decisions))
	// usage：观察 AfterModel 记账（累计到会话边界才清零），publish 把 core.Usage 转成 statusbar.Usage 事件
	usageMw := middlewares.NewUsageMiddleware(s.usage, func(u core.Usage) {
		publish(statusbar.EventUsage, statusbar.Usage{
			EstimateTokens:        u.EstimateTokens,
			PromptTokens:          u.PromptTokens,
			PromptCacheHitTokens:  u.PromptCacheHitTokens,
			PromptCacheMissTokens: u.PromptCacheMissTokens,
			CompletionTokens:      u.CompletionTokens,
			TotalTokens:           u.TotalTokens,
			LastPromptTokens:      u.LastPromptTokens,
			ContextWindow:         cfg.LLM.ContextWindow,
		})
	})
	// iteration：每轮迭代发一个计数事件
	iterationMw := middlewares.NewIterationMiddleware(func() {
		publish(statusbar.EventIteration, nil)
	})
	// compress：自动压缩，protectSystem=true —— 保护本轮注入的系统提示
	compressMw := middlewares.NewCompressionMiddleware(
		s.newCompressor(provider, true),
		cfg.LLM.ContextWindow,
		cfg.LLM.CompressAt,
	)

	mws := []core.Middleware{hitlMw, usageMw, iterationMw}
	if s.deps.Memory != nil {
		memMw := middlewares.NewMemoryMiddleware(s.deps.Memory, cfg.Memory, cfg.LLM.ContextWindow)
		mws = append(mws, memMw)
	}
	return append(mws, compressMw)
}

// newCompressor 构造压缩器，onCompress 统一记进 token 账本（/usage 能看到压缩）。
// protectSystem=true：history[0] 是本轮注入的系统提示，压掉模型就忘了怎么用工具；
// /compact 场景 conversation 不含系统提示，首位可能是上次的摘要，允许被再次滚动，传 false。
func (s *Session) newCompressor(provider core.Provider, protectSystem bool) *core.Compressor {
	cfg := s.deps.Hub.Config
	return core.NewCompressor(provider, cfg.LLM.CompactKeep, protectSystem, func(r core.CompressReport) {
		s.usage.RecordCompaction(core.Compaction{BeforeMsgs: r.BeforeMsgs, AfterMsgs: r.AfterMsgs, Tokens: r.Tokens})
	})
}

// Compact 执行 /compact：压缩会丢原文，先固化到任务档案，再压缩当前 conversation。
func (s *Session) Compact(ctx *spec.Context) error {
	if s.deps.Store == nil {
		// 纯内存模式：保持旧逻辑
		return s.compactMemory(ctx)
	}
	return s.compactStore(ctx)
}

// compactMemory 纯内存压缩（store 禁用时的回退路径）。
func (s *Session) compactMemory(ctx *spec.Context) error {
	s.mu.Lock()
	history := s.conversation
	before := len(history)
	s.mu.Unlock()

	// 压缩会丢原文，先固化到任务档案 —— 这是原文丢失前的最后一次机会。
	if s.deps.Memory != nil {
		sum, err := s.checkpoint(ctx.Ctx, history)
		if err != nil {
			ctx.Writer(fmt.Sprintf("⚠ Memory consolidation failed (original text lost after compression): %v\n", err))
		} else if notice := renderCheckpointNotice(sum); notice != "" {
			ctx.Writer(notice)
		}
	}

	if before == 0 {
		ctx.Writer("(no conversation history yet — run /agent first)\n")
		return nil
	}

	cfg := s.deps.Hub.Config
	provider := s.deps.NewProvider(cfg)
	compressor := s.newCompressor(provider, false)

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

	s.mu.Lock()
	s.conversation = compacted
	s.mu.Unlock()
	ctx.Writer(fmt.Sprintf("✔ 已压缩: %d 条 → %d 条\n", before, len(compacted)))
	return nil
}

// compactStore store 持久化模式压缩：ActiveView 先固化 → 压缩 → detectCompact → 落 compact。
func (s *Session) compactStore(ctx *spec.Context) error {
	// 先固化再折叠（原文丢失前最后一次机会）
	_, _ = s.consolidate(ctx.Ctx)

	view := s.deps.Store.ActiveView()
	if len(view) == 0 {
		ctx.Writer("(no conversation history yet — run /agent first)\n")
		return nil
	}
	before := len(view)

	cfg := s.deps.Hub.Config
	provider := s.deps.NewProvider(cfg)
	// protectSystem=false：conversation 不含系统提示，首位可能是上次的摘要，允许被再次滚动
	compressor := s.newCompressor(provider, false)

	cctx, cancel := context.WithTimeout(ctx.Ctx, 2*time.Minute)
	defer cancel()
	compacted, err := compressor.Compress(cctx, view)
	if err != nil {
		ctx.Writer(fmt.Sprintf("✘ 压缩失败: %v\n", err))
		return nil
	}
	if len(compacted) >= before {
		ctx.Writer("(history too short or no safe split point; not compacted)\n")
		return nil
	}

	// detectCompact 检测压缩前后变化，提取摘要 + 覆盖段
	if from, to, summary, ok := session.DetectCompact(view, compacted); ok {
		if err := s.deps.Store.Compact(from, to, summary); err != nil {
			ctx.Writer(fmt.Sprintf("✘ 压缩失败: %v\n", err))
			return nil
		}
		if err := s.refreshConversation(); err != nil {
			ctx.Writer(fmt.Sprintf("✘ 刷新会话失败: %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ 已压缩: %d 条 → %d 条\n", before, len(s.conversation)))
	} else {
		// 不可压缩（detectCompact 判断不满足条件），保持现状
		ctx.Writer("(history too short or no safe split point; not compacted)\n")
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

// ---- /agent 命令（薄壳，全部逻辑在 Session） ----

func (p *AgentPlugin) handleAgent(ctx *spec.Context) error {
	return p.session.Run(ctx)
}

// ---- /new 命令 ----

func (p *AgentPlugin) handleNew(ctx *spec.Context) error {
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
		// 后台固化旧会话，不阻塞输入
		go old.checkpointAsync(pending, ctx.Writer)
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

// 生成系统提示词
func (s *Session) buildSystemPrompt(cfg *config.Config, tools []core.Tool) string {
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
