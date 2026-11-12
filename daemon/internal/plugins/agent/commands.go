package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-memory"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// 本文件是配置与诊断类命令：/agent /new /model /usage /compact /skills /history /rules。
// 命令逻辑全部委托给 Session SDK，这里只做 model.Context → RunCallbacks 的薄适配。

// ---- /agent 命令 ----

// sessionGateKey 是 withSession 的 ctx 重入标记：agent 运行期间 collectTools
// 嵌套 hub.Eval 执行会话命令时，嵌套调用已经处于 gate 临界区内，直接放行，
// 避免重复排队死锁。标记值经 ai-core 的工具执行链路（agent.go 的 toolCtx）
// 流入嵌套 Eval。
type sessionGateKey struct{}

// errAgentStopping 表示 Stop 已关闭维护请求入口。避免命令继续访问正在收尾的
// store、memory 或 MCP 资源；停机后新请求与 manager 的 closed 错误语义一致。
var errAgentStopping = errors.New("agent: plugin is stopping")

// inFlightTracker 是插件级维护请求屏障。外部维护命令不属于 AgentRun，
// manager.Close 不会等待它们；Stop 关闭入口后等待已登记请求，再释放其依赖资源。
// 计数和 closed 状态共用一把锁，确保 WaitGroup.Add 不会与零计数 Wait 竞争。
// 零值可用；Run 内嵌套 Eval 由 manager.Close 的 run.done 等待面覆盖。
type inFlightTracker struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	closed bool
}

// enter 登记一个外部在途维护请求；Stop 已关闭入口（closed）时拒绝并返回 false。
func (f *inFlightTracker) enter() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return false
	}
	f.wg.Add(1)
	return true
}

// exit 摘除登记；必须与成功的 enter 一一配对（调用方以 defer 持有）。
func (f *inFlightTracker) exit() { f.wg.Done() }

// closeAdmission 关闭新的维护请求入口但不等待既有请求。幂等；与 enter 使用
// 同一把锁，保证请求要么在关闭前完成登记、要么被拒绝。
func (f *inFlightTracker) closeAdmission() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
}

// wait 只等待已经登记的维护请求；入口必须先由 closeAdmission 关闭。这里不持有
// tracker 锁，避免 Stop 等待请求或 manager 时形成锁环。幂等。
func (f *inFlightTracker) wait() { f.wg.Wait() }

// withSession resolves by binding, falls back to current, and executes under the runtime gate.
// A dangling binding is an explicit error. The resolved runtime is injected for nested
// operations; runs reenter via sessionGateKey to avoid deadlocking on their own gate.
func (p *AgentPlugin) withSession(ctx context.Context, run func(context.Context) error) error {
	// nil ctx 归一(既有测试直接构造 Context 不带 Ctx):按无绑定处理。
	if ctx == nil {
		ctx = context.Background()
	}
	// Run 内部嵌套 Eval 已经持有 Session Gate，必须绕过 tracker 和再次排队；这类
	// 闭包属于 manager 管理的 Run 生命周期，manager.Close 会等待其 run.done。
	if owner, _ := ctx.Value(sessionGateKey{}).(*AgentPlugin); owner == p {
		return run(ctx)
	}
	// 外部维护请求在插件入口即被接受/拒绝，不能等拿到 Session Gate 后才登记。
	if !p.inFlight.enter() {
		return errAgentStopping
	}
	defer p.inFlight.exit()

	rt, err := p.sessionRuntimeIn(ctx)
	if err != nil {
		return err
	}
	if rt == nil {
		return fmt.Errorf("agent: no active session")
	}
	release, err := rt.AcquireRun(ctx)
	if err != nil {
		return err
	}
	defer release()
	return run(injectResolvedSession(context.WithValue(ctx, sessionGateKey{}, p), rt))
}

func withPluginContext(ctx *model.Context, runCtx context.Context) *model.Context {
	copy := *ctx
	copy.Ctx = runCtx
	return &copy
}

// cancelCleanupTimeout bounds the wait for a canceled run to reach a persisted terminal state.
const cancelCleanupTimeout = 10 * time.Second

// handleAgent runs a prompt against the resolved session and waits for its run to finish.
// If the caller cancels, it submits cancellation and waits briefly for terminal persistence;
// a cleanup timeout returns cancellation_pending while the run continues in the background.
func (p *AgentPlugin) handleAgent(ctx *model.Context) error {
	// 最近一次会话 cwd 声明未通过 sandbox 边界校验 → 明确报错（无效声明语义，
	// 见 SetSession）。/new /compact 等维护命令不受影响，只有真正的 prompt 被挡。
	// binding 形态解析——prompt 链携带连接级 Binding 时查被路由会话的声明,
	// 悬空 Binding(session 已被 /new 收口)在此诚实报错。
	if err := p.sessionCWDErrorIn(ctx.Ctx); err != nil {
		return err
	}
	// nil ctx 归一(既有测试直接构造 Context 不带 Ctx):按不可取消的请求处理。
	reqCtx := ctx.Ctx
	if reqCtx == nil {
		reqCtx = context.Background()
	}
	// 解析与提交同源(binding 优先形态解析点):解析出的 runtime 即提交
	// 目标(rt.ID 经 Service 路由回同一 runtime),闭包内不再重读 current。
	rt, err := p.sessionRuntimeIn(reqCtx)
	if err != nil {
		return err
	}
	if rt == nil {
		return fmt.Errorf("agent: no active session")
	}
	// Archived sessions remain read-only even when a connection has not loaded them.
	// Reject prompts here; history and usage remain read-only, and /new creates a new session.
	if st := p.storeFor(rt); st != nil && st.Archived() {
		return fmt.Errorf("agent: session %q is archived and read-only", rt.ID)
	}
	handle, err := p.StartRunSync(rt.ID, runtimeagent.RunRequest{
		Input:      strings.Join(ctx.Args, " "),
		ToolSource: ctx.ToolSource,
	}, callbacksFromPlugin(ctx))
	if err != nil {
		return err
	}
	res, waitErr := handle.Wait(reqCtx)
	if waitErr != nil {
		// Wait 仅以 ctx.Err() 失败(RunHandle.Wait 契约)⇒ 请求 ctx 已取消:
		// Legacy 取消语义显式落地(Adapter 形态)。取消结果分类:只有
		// 「取消已发出(成功)」与「可证实的幂等空操作」才继续收尾等待,其余
		// 失败不假设取消已发出。
		if cerr := p.CancelRunSync(rt.ID, handle.RunID); cerr != nil {
			switch {
			case errors.Is(cerr, runtimeagent.ErrRunNotFound):
				// 取消与完成竞争落败:对已终态 Run 的取消是幂等空操作。
				// finalizeRun 先 run.finish 关 done 再清 active(runtime.go),
				// Active() 已清 ⇒ 终态已落盘,收尾 Wait 立即返回真实终态。
			case errors.Is(cerr, runtimeagent.ErrManagerClosed):
				// 宿主已关闭:manager.Close 取消全部在途 Run 并等待终态落盘,
				// 收尾等待消费的正是宿主关闭驱动的收敛结果。
			default:
				// 非预期失败(路由丢失等):取消是否落地不可知,继续等待可能在
				// Run 未收到取消信号时无限阻塞。slog.Error 留痕(sessionID/
				// runID/取消原因)并显式上抛,绝不静默吞掉、不伪装已取消。
				slog.Error("agent: prompt cancel failed", "session", rt.ID, "run", handle.RunID, "err", cerr)
				return fmt.Errorf("agent: cancel run %s in session %s after caller cancel: %w", handle.RunID, rt.ID, cerr)
			}
		}
		// Wait with a fresh, bounded context for terminal persistence.
		// The journal is written before done closes; timeout returns cancellation_pending while the run
		// continues and remains queryable by RunID.
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), cancelCleanupTimeout)
		res, waitErr = handle.Wait(cleanupCtx)
		cancelCleanup()
		if waitErr != nil {
			return fmt.Errorf("run %s cancellation pending: %w", handle.RunID, waitErr)
		}
	}
	slog.Debug("agent: run finished", "state", res.State, "persisted", res.Persisted, "err", res.Err)
	return nil
}

// callbacksFromPlugin 把 model.Context 的 I/O 回调适配成 runtimeagent.RunCallbacks。
// WriteToken 只在 ctx 提供时注入，避免把 nil 包装成非 nil 绕过 SDK 的 nil 保护。
func callbacksFromPlugin(ctx *model.Context) runtimeagent.RunCallbacks {
	cb := runtimeagent.RunCallbacks{
		Write:   ctx.Writer,
		Decide:  ctx.Decide,
		Publish: ctx.Publish,
	}
	if ctx.EmitToken != nil {
		cb.EmitToken = ctx.EmitToken
	}
	if ctx.WriteToken != nil {
		cb.WriteToken = func(kind runtimeagent.RenderKind, content string, done bool) {
			ctx.WriteToken(model.RenderKind(kind), content, done)
		}
	}
	return cb
}

// ---- /new 命令 ----

func (p *AgentPlugin) handleNew(ctx *model.Context) error {
	return p.withSession(ctx.Ctx, func(runCtx context.Context) error {
		runContext := withPluginContext(ctx, runCtx)
		// 结束当前会话：用新实例替换旧实例，旧会话状态（conversation/usage）随对象回收。
		// 同步固化旧会话（阻塞，状态栏显示进度）；快照走只读，先取好再重建。
		oldRT, rtErr := p.sessionRuntimeIn(runCtx)
		if rtErr != nil {
			// Binding 悬空(指向的会话已被收口)诚实失败:无可换轨的会话,不静默
			// 回落 current——那会把别人(或新会话)的工作区卷进本连接的 /new。
			return rtErr
		}
		var old *runtimeagent.Session
		if oldRT != nil {
			old = oldRT.Session()
		}
		// 会话 store 经同一 runtime 解析(storeFor):快照落在 startSession
		// 换轨前的同一实例上。
		oldStore := p.storeFor(oldRT)

		// pending 快照（store 生效时用游标后的增量，否则全量）。/new 不再轮转
		// 旧 store，快照的「归档前取出」次序约束消失，
		// 仍在此处先取——CheckpointSync 消费的是换轨前捕获的同一份增量。
		var pending []core.Message
		if oldStore != nil {
			pending = oldStore.PendingAfterCursor()
			if len(pending) == 0 && old != nil {
				pending = old.Conversation() // 兜底全量
			}
		} else if old != nil {
			pending = old.Conversation()
		}
		consolidate := p.memory != nil && len(pending) > 0

		if p.memory == nil {
			runContext.Writer("✔ New session started (memory disabled)\n")
		} else {
			runContext.Writer("✔ New session started\n")
		}

		// /new creates a fresh session and leaves the old persisted session unchanged.
		// Archiving is explicit; /new does not archive or rotate the old session.

		// 创建新会话（新对象 = 新会话，旧状态随对象回收）。startSession
		// 经 Repository.Create 产出独立目录新会话（<Session.Dir>/<id>/）。
		p.startSession()

		runContext.Writer("✔ STM cleared, memory will be retrieved on every query\n")

		// Background consolidation survives disconnects and cancellation; failures are reported through callbacks.
		if consolidate {
			p.bgFinalizers.Add(1)
			go func() {
				defer p.bgFinalizers.Done()
				old.CheckpointSync(context.Background(), pending, callbacksFromPlugin(runContext))
			}()
		}

		// Remove this connection's binding, then retire the old runtime only when no references or runs remain.
		if clientID := agentcontrol.ClientIDFromCtx(runCtx); clientID != "" && p.control != nil {
			p.control.UnbindSession(clientID)
		}
		if oldRT != nil {
			p.tryReleaseSession(oldRT.ID)
		}

		// /new clears only the initiating connection's selection. It does not cancel runs or release the control lease.
		if b := agentcontrol.BindingFromCtx(runCtx); b != nil {
			b.Set("")
		}
		return nil
	})
}

// ---- /model 命令 ----

func (p *AgentPlugin) handleModel(ctx *model.Context) error {
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

func (p *AgentPlugin) showConfig(ctx *model.Context) {
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

func (p *AgentPlugin) handleUsage(ctx *model.Context) error {
	return p.withSession(ctx.Ctx, func(runCtx context.Context) error {
		runContext := withPluginContext(ctx, runCtx)
		// /usage 不执行工具（无嵌套 Eval 可换会话），闭包内一次性解析本回合会话
		// (binding 优先)。
		s := p.boundSessionIn(runCtx)
		if s == nil {
			runContext.Writer("(no active session)\n")
			return nil
		}
		calls, comps, total := s.UsageSnapshot()
		if len(calls) == 0 && len(comps) == 0 {
			runContext.Writer("(no agent calls yet — run /agent first)\n")
			return nil
		}

		if len(calls) > 0 {
			runContext.Writer(fmt.Sprintf("usage: %d model calls this session\n\n", len(calls)))
			for i, c := range calls {
				line := fmt.Sprintf("  #%-2d  in %-7s  out %-7s  (est %s)",
					i+1, runtimeagent.Humanize(c.PromptTokens), runtimeagent.Humanize(c.CompletionTokens), runtimeagent.Humanize(c.EstimateTokens))
				// 模型没返回 cache 字段时明细保持简洁，不挂一串 0。
				if c.PromptCacheHitTokens+c.PromptCacheMissTokens > 0 {
					line += fmt.Sprintf("  hit: %-7s miss: %-7s / %s",
						runtimeagent.Humanize(c.PromptCacheHitTokens), runtimeagent.Humanize(c.PromptCacheMissTokens), runtimeagent.Humanize(c.PromptCacheHitTokens+c.PromptCacheMissTokens))
				}
				runContext.Writer(line + "\n")
			}
			runContext.Writer("\n")
			totalLine := fmt.Sprintf("  total: in %s  out %s  total %s",
				runtimeagent.Humanize(total.PromptTokens), runtimeagent.Humanize(total.CompletionTokens), runtimeagent.Humanize(total.TotalTokens))
			if rate, ok := total.CacheHitRate(); ok {
				totalLine += fmt.Sprintf("  cache %.1f%% (%s hit / %s miss)",
					rate, runtimeagent.Humanize(total.PromptCacheHitTokens), runtimeagent.Humanize(total.PromptCacheMissTokens))
			}
			runContext.Writer(totalLine + "\n")

			// context usage uses "last prompt / window" — the final ReAct request already holds all history.
			if w := selectedContextWindow(p.cfg); w > 0 && total.LastPromptTokens > 0 {
				pct := float64(total.LastPromptTokens) / float64(w) * 100
				runContext.Writer(fmt.Sprintf("  context: %.2f%% (last in %s / window %s)\n",
					pct, runtimeagent.Humanize(total.LastPromptTokens), runtimeagent.Humanize(w)))
			}
		}

		// 压缩记录：压缩是循环外的模型调用，消耗和效果都该看得见。
		if len(comps) > 0 {
			var msgsIn, msgsOut, tokens int
			for _, c := range comps {
				msgsIn += c.BeforeMsgs
				msgsOut += c.AfterMsgs
				tokens += c.Tokens
			}
			runContext.Writer(fmt.Sprintf("  compact: %d time(s), %d msgs → %d msgs (summarize %s)\n",
				len(comps), msgsIn, msgsOut, runtimeagent.Humanize(tokens)))
		}

		// 当前历史占用：压缩后的直观体现，不必等下一次模型调用。
		conv := s.Conversation()
		if convLen := len(conv); convLen > 0 {
			convEst := core.EstimateTokens(conv)
			line := fmt.Sprintf("  history: %s est (%d msgs)", runtimeagent.Humanize(convEst), convLen)
			if w := selectedContextWindow(p.cfg); w > 0 {
				line += fmt.Sprintf(", %.2f%% of window", float64(convEst)/float64(w)*100)
			}
			runContext.Writer(line + "\n")
		}
		return nil
	})
}

// ---- /compact 命令 ----

func (p *AgentPlugin) handleCompact(ctx *model.Context) error {
	return p.withSession(ctx.Ctx, func(runCtx context.Context) error {
		runContext := withPluginContext(ctx, runCtx)
		s := p.boundSessionIn(runCtx)
		if s == nil {
			runContext.Writer("(no active session)\n")
			return nil
		}
		// 只读执法收尾（「只读保留终态」）：写型维护命令与
		// prompt/run 提交同源拒 archived（store 持久事实，重启后依然拒）——
		// ACP 客户端归档一个 CLI 正在用的会话后，/compact 不再能写穿终态。
		// 取数用本命令既有的 currentStoreIn 解析链（同一 gate 临界区、同一会话）。
		if st := p.currentStoreIn(runCtx); st != nil && st.Archived() {
			return fmt.Errorf("agent: session %q is archived and read-only", st.SessionID())
		}
		return s.Compact(runCtx, callbacksFromPlugin(runContext))
	})
}

// ---- /skills 命令 ----

func (p *AgentPlugin) handleSkills(ctx *model.Context) error {
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

func (p *AgentPlugin) handleHistory(ctx *model.Context) error {
	return p.withSession(ctx.Ctx, func(runCtx context.Context) error {
		runContext := withPluginContext(ctx, runCtx)
		var conv []core.Message
		if s := p.boundSessionIn(runCtx); s != nil {
			conv = s.Conversation()
		}
		if len(conv) == 0 {
			runContext.Writer("(no conversation history yet — run /agent first)\n")
			return nil
		}

		runContext.Writer(fmt.Sprintf("history: %d messages\n\n", len(conv)))
		for i, m := range conv {
			runContext.Writer(fmt.Sprintf("  #%-2d [%-9s] %s\n", i+1, m.Role, runtimeagent.DescribeMessage(m)))
		}
		return nil
	})
}

// ---- /rules 命令 ----

// handleRules 只读展示当前生效的指令快照 —— 与注入 system prompt 的内容一致，
// 方便核对"agent 到底被灌了什么规矩"。
func (p *AgentPlugin) handleRules(ctx *model.Context) error {
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
