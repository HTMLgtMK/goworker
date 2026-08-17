package middlewares

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/memory"
)

// MemoryBlockPrefix 是注入记忆块的 system 消息前缀。plugin 把 messages 写回
// STM（conversation）时用它过滤 —— 记忆块只服务本次 run 的注入，若漏进 STM
// 会被下次 run 重发、甚至被检查点当对话内容固化（自指污染）。
const MemoryBlockPrefix = "[记忆]"

// MemoryClient 是 middleware 对记忆组件的最小依赖面：每次 query 统一检索。
// 保持接口小 —— memory module 的 *Client 天然满足。
type MemoryClient interface {
	Search(ctx context.Context, query string, taskTopK, factTopK int) ([]memory.Result, error)
}

// MemoryMiddleware 每次用户 query 都从记忆组件检索相关条目（open task + LTM 事实）
// 作为一条 system 消息注入 —— 给 agent "这次查询相关的前情提要"。
//
// 每次 /agent 构造一个新实例，injected 保证本次 run 内只注入一次（ReAct 每迭代
// fire OnBeforeModel，重复检索注入既费 token 又污染历史）。
//
// 固化（写入）不在 middleware —— 由 plugin 层的检查点触发（/compact、进程退出、
// /new、/task checkpoint），见 Checkpointer。
type MemoryMiddleware struct {
	client   MemoryClient
	cfg      config.MemoryConfig
	window   int  // 模型上下文窗口，0 = 未知，注入预算用兜底值
	injected bool // 本次 /agent run 只注入一次
}

func NewMemoryMiddleware(client MemoryClient, cfg config.MemoryConfig, window int) *MemoryMiddleware {
	return &MemoryMiddleware{client: client, cfg: cfg, window: window}
}

func (m *MemoryMiddleware) Name() string { return "memory" }

// ---- BeforeModel：每次 query 检索注入 ----

func (m *MemoryMiddleware) OnBeforeModel(ev *core.BeforeModelEvent) *core.MiddlewareResponse {
	if !m.cfg.Enabled || m.client == nil || m.injected {
		return nil
	}
	results, err := m.client.Search(ev.Ctx, ev.Input, m.cfg.TaskInjectN, m.cfg.LtmInjectTopK)
	if err != nil {
		slog.Warn("memory: search failed, skipping injection", "err", err)
		return nil
	}
	if len(results) == 0 {
		return nil
	}
	block := m.buildMemoryBlock(results)
	if block == "" {
		m.injected = true // 检索结果不会随 ReAct 迭代变化，别再每轮重搜重渲染
		return nil
	}
	ev.History = insertSystemBlock(ev.History, block)
	m.injected = true
	return nil
}

// buildMemoryBlock 渲染注入块并按预算裁剪。返回空串 = 本次放弃注入。
// results 已按分数降序，超预算就从末尾丢最低分的；单条 fact 再做一次截断。
func (m *MemoryMiddleware) buildMemoryBlock(results []memory.Result) string {
	budget := 1200 // window 未知时的兜底预算
	if m.window > 0 {
		budget = int(float64(m.window) * m.cfg.InjectBudgetRatio)
	}
	if budget <= 0 {
		return ""
	}
	trimmed := false // 单条 fact 的截断只做一次，否则超预算时永远重复截到同一长度 → 死循环
	for {
		block := renderMemoryBlock(results)
		if core.EstimateTokens([]core.Message{{Role: "system", Content: block}}) <= budget {
			return block
		}
		switch {
		case len(results) > 1:
			results = results[:len(results)-1] // 丢最低分
		case len(results) == 1 && !trimmed && results[0].Fact != nil:
			results[0].Fact.Content = memory.TruncateRunes(results[0].Fact.Content, 300)
			trimmed = true
		default:
			slog.Warn("memory: injection over budget and nothing left to trim, skipping", "budget", budget)
			return ""
		}
	}
}

// insertSystemBlock 把记忆块作为 system 消息插在最后一个 user（本轮输入）之前。
// 原来插在连续前导 system 之后 —— 记忆块每次检索结果都不同，它一变就把后面
// 整段 history 的 LLM 前缀缓存击穿（history 每次请求重发却永远 miss）。
// 挪到 history 之后、user 前：agent prompt + 全部历史保持前缀连续，只有记忆块
// 自己 miss。从尾部找 user 保证不拆散 history 内的 assistant/tool 配对。
func insertSystemBlock(h []core.Message, block string) []core.Message {
	at := len(h) // 找不到 user（防御）时插末尾
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].Role == "user" {
			at = i
			break
		}
	}
	out := make([]core.Message, 0, len(h)+1)
	out = append(out, h[:at]...)
	out = append(out, core.Message{Role: "system", Content: block})
	out = append(out, h[at:]...)
	return out
}

// ---- 渲染 ----

// renderMemoryBlock 把检索结果按 task（相关任务）→ fact（长期事实）分组渲染。
// 注入面是结果里的内存对象，只读。
func renderMemoryBlock(results []memory.Result) string {
	var b strings.Builder
	b.WriteString(MemoryBlockPrefix + " 来自记忆，仅供参考，可能与当前任务无关：\n")
	var tasks []memory.Task
	var facts []memory.Fact
	for _, r := range results {
		if r.Task != nil {
			tasks = append(tasks, *r.Task)
		} else if r.Fact != nil {
			facts = append(facts, *r.Fact)
		}
	}
	if len(tasks) > 0 {
		b.WriteString("### 相关任务\n")
		for _, t := range tasks {
			fmt.Fprintf(&b, "- [%s] %s\n", t.ID, oneLine(t.Title))
			if t.Summary != "" {
				b.WriteString("  " + oneLine(t.Summary) + "\n")
			}
			if len(t.NextSteps) > 0 {
				b.WriteString("  下一步: " + strings.Join(t.NextSteps, "；") + "\n")
			}
		}
	}
	if len(facts) > 0 {
		b.WriteString("### 长期事实\n")
		for _, f := range facts {
			line := oneLine(f.Content)
			if f.Topic != "" {
				fmt.Fprintf(&b, "- [%s] %s\n", f.Topic, line)
			} else {
				fmt.Fprintf(&b, "- %s\n", line)
			}
		}
	}
	// 引导 agent 主动深挖历史：注入只是被动提醒（open tasks + 相关 facts），
	// 更早的会话或已完成任务要靠 memory_search 工具主动查档。
	b.WriteString("\n提示：如需回忆更早的会话或已完成的任务，可用 memory_search 工具查询。\n")
	return b.String()
}

// ---- 小工具 ----

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
