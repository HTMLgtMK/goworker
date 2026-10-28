package service

import (
	"context"
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

// withSession 在本命令回合解析出的会话 runtime 的独占执行权下运行 run(PR2 起
// 解析 binding 优先,总方案 §六):解析点 sessionRuntimeIn——ctx 携带连接级
// SessionBinding(标准 ACP 的 prompt 链,FrontendHandler 经 agentcontrol.WithBinding
// 注入)时按 binding 路由(binding 命中 current → current;否则 manager.Get 注册表
// 权威;悬空 → 诚实错误),无 Binding/空 Binding(CLI 命令、stdio、测试、后台 Run
// 的嵌套命令)回落 p.currentSession()。互斥点在解析出的同一 runtime 的 gate 上
// (容量 1,AcquireRun 监听 ctx 取消)——解析与互斥同源,杜绝「锁在 A 会话、写在
// B 会话」;解析出的 runtime 随注入进 run ctx(resolvedSessionKey),闭包内取数点
// 与嵌套命令全部读到同一 runtime。6 个维护类会话命令 handler(/new /compact
// /usage /history /task /rewind)经此排队,同一时刻命令回合与在途 Run(execute
// 持 gate)互斥;PR4 起 /agent(handleAgent)不经此排队——prompt 的提交走
// manager.StartRun,互斥由 execute 的 AcquireRun 承担、占位拒绝由 active 检查
// 承担(总方案 §九统一门禁),运行中的嵌套命令靠 manager runCtx 携带的
// sessionGateKey 重入标记(WithRunContext,P5-1)放行。agent 运行期间
// collectTools 嵌套 hub.Eval 执行这些命令时,靠 sessionGateKey ctx 重入标记
// 放行,绝不在持有 gate 时重复排队(重复取锁即死锁)——嵌套回合的 ctx 已携带
// 外层注入的 runtime,闭包解析与外层同源。解析结果为 nil(无 Binding 回落后
// 仍无活动 runtime:Init 未完成/失败回滚)时报错;Binding 悬空(会话已被 /new
// 收口)诚实报 session not found,不回落 current。
func (p *AgentPlugin) withSession(ctx context.Context, run func(context.Context) error) error {
	// nil ctx 归一(既有测试直接构造 Context 不带 Ctx):按无绑定处理。
	if ctx == nil {
		ctx = context.Background()
	}
	if owner, _ := ctx.Value(sessionGateKey{}).(*AgentPlugin); owner == p {
		return run(ctx)
	}
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

// handleAgent 是 /agent、未匹配输入(fallback)与标准 ACP session/prompt 的统一
// 执行终点(Engine.Eval → fallback)。PR4(总方案 §九「同步与后台 Run 最终统一」)
// 起 Run 提交与结果结算统一到 manager.StartRun:同步 prompt 与 xgw 后台 run 共用
// 同一套 per-session 门禁(每会话至多一个活动 Run,第二提交 ErrRunActive)、
// 全局 runSlots/pending 配额、journal 记账(queued→running→终态)、事件面(hub
// tee:token/审批/usage 照常进事件流,CLI/ACP 前端回调经 tee 原样转发)与终态化
// (finalizeRun)——RecordRun 记账形态退役;既有能力(Checkpoint、Error Capture、
// Usage Commit、HITL 链)全部在 Session.Run 内沿用,无复制执行流程。
//
// 流序:cwd 守卫(不变)→ binding 优先解析本回合会话(不再经 withSession 排队
// ——会话互斥由 execute 的 AcquireRun 承担,提交即占位)→ StartRunSync 提交
// (立即返回句柄;拒绝显式上抛)→ Legacy 等待面:
//   - Wait 阻塞至终态:「prompt 返回 = run 完成」语义保留(CLI/stdio);
//   - 请求 ctx 取消 → 显式 CancelRunSync(总方案 §九:StartRun 的 runCtx 来自
//     宿主,请求 ctx 不传播,Legacy CLI 的「取消即终止 Agent」由 Adapter 显式
//     实现,不依赖请求 ctx 传播)→ 换无取消上下文收尾等待终态落盘;
//   - 取消与完成竞争:CancelRun 对已终态 Run 返回 ErrRunNotFound(幂等空操作),
//     收尾 Wait 返回真实终态,两种交错同收敛。
//
// 错误面:提交拒绝(ErrRunActive/ErrSessionNotFound 等)上抛;Run 执行结果与
// 迁移前同型——res 进 debug 日志,handleAgent 恒返回 nil(CLI/ACP 错误面不变,
// 取消路径亦然)。
func (p *AgentPlugin) handleAgent(ctx *model.Context) error {
	// 最近一次会话 cwd 声明未通过 sandbox 边界校验 → 明确报错（无效声明语义，
	// 见 SetSession）。/new /compact 等维护命令不受影响，只有真正的 prompt 被挡。
	// PR2:binding 形态解析——prompt 链携带连接级 Binding 时查被路由会话的声明,
	// 悬空 Binding(session 已被 /new 收口)在此诚实报错。
	if err := p.sessionCWDErrorIn(ctx.Ctx); err != nil {
		return err
	}
	// nil ctx 归一(既有测试直接构造 Context 不带 Ctx):按不可取消的请求处理。
	reqCtx := ctx.Ctx
	if reqCtx == nil {
		reqCtx = context.Background()
	}
	// 解析与提交同源(binding 优先,PR2 形态解析点):解析出的 runtime 即提交
	// 目标(rt.ID 经 Service 路由回同一 runtime),闭包内不再重读 current。
	rt, err := p.sessionRuntimeIn(reqCtx)
	if err != nil {
		return err
	}
	if rt == nil {
		return fmt.Errorf("agent: no active session")
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
		// Legacy 取消语义显式落地(§九 Adapter 形态)。
		if cerr := p.CancelRunSync(rt.ID, handle.RunID); cerr != nil {
			slog.Debug("agent: prompt cancel", "err", cerr, "run", handle.RunID)
		}
		// 换无取消上下文收尾:阻塞至 run 终态(终态 journal 落盘先于 done 关闭,
		// finalizeRun 次序不变量)——「prompt 返回 = run 完成」在取消路径同样成立。
		res, _ = handle.Wait(context.Background())
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
		// PR2:/new 作用于「发起方的 binding 会话」——binding 优先解析(闭包 ctx 携带
		// 连接级 Binding 时即该连接选择的会话,含悬空判定),无 Binding(CLI)回落
		// current,原语义不变。oldRT/old/oldStore 同点捕获:闭包持 gate(withSession),
		// 本回合解析出的 runtime 在临界区内稳定,startSession 换轨前取到的都是同一
		// runtime(P5-5 收口要用它的 id)。
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
		// 会话 store 经同一 runtime 解析(P1-3 链,storeFor):快照与归档都落在
		// startSession 换轨前的同一实例上。
		oldStore := p.storeFor(oldRT)

		// pending 快照（store 生效时用游标后的增量，否则全量）
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

		// 先归档旧 store，再开新会话（新 store 写新文件）
		// pending 快照已在 Archive 前取出，不受归档影响。legacy 槽内轮转
		// （current.jsonl → root/archive/）；独立目录会话同样在其目录内轮转
		// （Archive 以 store 自身 dir 为基准）。
		if oldStore != nil {
			if err := oldStore.Archive(); err != nil {
				slog.Warn("session: archive failed", "err", err)
			}
		}

		// 创建新会话（新对象 = 新会话，旧状态随对象回收）。P1-3 起 startSession
		// 经 Repository.Create 产出独立目录新会话（<Session.Dir>/<id>/）。
		p.startSession()

		runContext.Writer("✔ STM cleared, memory will be retrieved on every query\n")

		// 同步固化旧会话：阻塞至完成（内部 120s 超时），进度经 ctx.Publish 进状态栏，
		// 结果/警告经 ctx.Writer 回显。主 goroutine 执行，可直接读共享配置。
		if consolidate {
			old.CheckpointSync(runCtx, pending, callbacksFromPlugin(runContext))
		}

		// P5-5 收口（遗留①）：旧 runtime 注销出 manager、旧 store 关闭。
		//
		// Remove（首用 manager.Remove，勘察 K——此前 daemon 对 Remove 零调用，
		// manager 随 /new 无界累积）：拍板⑤「V1 仅 current 可路由」成立后，旧
		// runtime 留册只剩误路由面（经 manager.Get(旧id) 触达的是已换轨的死会话），
		// 注销后对旧 id 的 run/start|attach（P5-6/8）得 ErrSessionNotFound → 诚实
		// 报错，客户端语义随 P5-10 写入 PROTOCOL.md。纯内存降级（无 store）下新旧
		// runtime 同 id "current"、Register 冲突时旧条目保留（plugin.go startSession）：
		// Remove 移除的正是旧条目（新 runtime 本就不在册），注册表随之归零。
		//
		// Close：旧 store 归档/快照后已无写者——在途 run 与本闭包被 gate 串行
		// （handleNew 持 gate 期间旧 runtime 不可能有活动 run），Remove 后经 manager
		// 也再触达不了旧 runtime。Close 幂等（store.go:177-199，对已关闭/未持锁
		// store 为 no-op）。只 Close、不从 repo 缓存逐出：P1-3 的实例唯一不变量要求
		// 缓存条目存活到 Stop 的 repo.Close 统一回收；repo.Open 命中缓存直接返回
		// 实例、不触碰文件锁（repository.go:161-163），已关闭实例留缓存不会引发
		// 二次 Open 的 ErrStoreLocked。
		if oldRT != nil {
			p.manager.Remove(oldRT.ID)
		}
		if oldStore != nil {
			if err := oldStore.Close(); err != nil {
				slog.Warn("session: old store close failed", "err", err)
			}
		}

		// PR2 /new 收口的 Binding 面(总方案 §六「只摘发起方的 binding」):仅清除
		// 发起本命令的连接 Binding(agentcontrol.BindingFromCtx,FrontendHandler 经
		// prompt 链注入;CLI 无 Binding 时为 nil,零动作)——此后该连接回落 current
		// 语义,自然落到 startSession 刚换轨的新会话,单连接行为与旧版一致。TEARDOWN
		// 语义不变:重选/清除 Binding 不删除会话(删除只发生在上面的既有收口)、不
		// 取消后台 Run、不触碰 control lease(§6.2 四条纪律);其他连接的 Binding
		// 连接私有、这里不可见也不可触及——指向被收口会话的它们在下次解析时诚实
		// 失败(sessionRuntimeIn 的悬空分支),不会静默串到新会话。
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
		// (binding 优先,PR2)。
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
				// 模型没返回 cache 字段时明细保持简洁，不挂一串 0
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

			// context usage uses "last prompt / window" — the final ReAct request already holds all history
			if w := selectedContextWindow(p.cfg); w > 0 && total.LastPromptTokens > 0 {
				pct := float64(total.LastPromptTokens) / float64(w) * 100
				runContext.Writer(fmt.Sprintf("  context: %.2f%% (last in %s / window %s)\n",
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
