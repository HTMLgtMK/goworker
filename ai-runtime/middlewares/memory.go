package middlewares

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/config"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

const MemoryBlockPrefix = "[记忆]"

type MemoryTask struct {
	ID        string
	Title     string
	Summary   string
	NextSteps []string
}

type MemoryFact struct {
	Topic   string
	Content string
}

type MemoryResult struct {
	Task  *MemoryTask
	Fact  *MemoryFact
	Score float64
}

type MemoryClient interface {
	Search(ctx context.Context, query string, taskTopK, factTopK int) ([]MemoryResult, error)
}

type MemoryMiddleware struct {
	client   MemoryClient
	cfg      runtimeconfig.MemoryConfig
	window   int
	injected bool
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
func (m *MemoryMiddleware) buildMemoryBlock(results []MemoryResult) string {
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
			results[0].Fact.Content = truncateRunes(results[0].Fact.Content, 300)
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
func renderMemoryBlock(results []MemoryResult) string {
	var b strings.Builder
	b.WriteString(MemoryBlockPrefix + " 来自记忆，仅供参考，可能与当前任务无关：\n")
	var tasks []MemoryTask
	var facts []MemoryFact
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

// truncateRunes 按 rune 截断字符串，超长加省略号。
func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max]) + "…"
}
