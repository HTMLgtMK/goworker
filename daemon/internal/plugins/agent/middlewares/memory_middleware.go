package middlewares

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/memory"
)

// MemoryBlockPrefix 是注入记忆块的 system 消息前缀。plugin 把 messages 写回
// STM（conversation）时用它过滤 —— 记忆块只服务本次 run 的注入，若漏进 STM
// 会被下次 run 重发、甚至被检查点当对话内容固化（自指污染）。
const MemoryBlockPrefix = "[记忆]"

// MemoryMiddleware 只在"会话边界"（/new 后的首个 run）的 BeforeModel 注入记忆：
// open tasks + 相关 LTM 事实，作为一条 system 消息 —— 给 agent 一个
// "上次干过什么、有哪些未完成任务"的提醒。平时不注入：会话内连续性由 STM
// 的 conversation 提供，重复提醒是冗余且浪费 token。
//
// 固化（写入）不在 middleware —— 由 plugin 层的检查点触发（/compact、
// 进程退出、/new、/task checkpoint），见 Checkpointer。
//
// 每次 /agent 构造一个实例（plugin.handleAgent），inject 是会话边界标记。
// 装配顺序必须在 CompressionMiddleware 之前：先注入再让压缩器测量整个 history。
type MemoryMiddleware struct {
	store    memory.Store
	cfg      config.MemoryConfig
	window   int  // 模型上下文窗口，0 = 未知，注入预算用兜底值
	inject   bool // /new 后的首个 run 才注入
	injected bool // 本 run 只注入一次，不随 ReAct 迭代重放
}

func NewMemoryMiddleware(store memory.Store, cfg config.MemoryConfig, window int, inject bool) *MemoryMiddleware {
	return &MemoryMiddleware{store: store, cfg: cfg, window: window, inject: inject}
}

func (m *MemoryMiddleware) Name() string { return "memory" }

// ---- BeforeModel：会话边界注入 ----

func (m *MemoryMiddleware) OnBeforeModel(ev *core.BeforeModelEvent) *core.MiddlewareResponse {
	if !m.cfg.Enabled || m.store == nil || !m.inject || m.injected {
		return nil
	}
	tasks, _ := m.store.OpenTasks(m.cfg.TaskInjectN)
	facts, _ := m.store.SearchFacts(ev.Input, m.cfg.LtmInjectTopK)
	if len(tasks) == 0 && len(facts) == 0 {
		return nil
	}
	block := m.buildMemoryBlock(tasks, facts)
	if block == "" {
		return nil
	}
	ev.History = insertSystemBlock(ev.History, block)
	m.injected = true
	return nil
}

// buildMemoryBlock 渲染注入块并按预算裁剪。返回空串 = 本次放弃注入。
// 裁剪顺序：先丢大块（最旧 task），再丢低分 fact，最后截断 content ——
// 保证留下的是"少而精"的最相关内容。
func (m *MemoryMiddleware) buildMemoryBlock(tasks []memory.Task, facts []memory.Fact) string {
	budget := 1200 // window 未知时的兜底预算
	if m.window > 0 {
		budget = int(float64(m.window) * m.cfg.InjectBudgetRatio)
	}
	if budget <= 0 {
		return ""
	}
	trimmed := false // 单条 fact 的截断只做一次，否则超预算时永远重复截到同一长度 → 死循环
	for {
		block := renderMemoryBlock(tasks, facts)
		if core.EstimateTokens([]core.Message{{Role: "system", Content: block}}) <= budget {
			return block
		}
		switch {
		case len(tasks) > 1:
			tasks = tasks[:len(tasks)-1] // OpenTasks 最近在前，丢最旧的
		case len(facts) > 1:
			facts = facts[:len(facts)-1] // 分数降序，丢最低分的
		case len(facts) == 1 && !trimmed:
			facts[0].Content = truncateRunes(facts[0].Content, 300)
			trimmed = true
		default:
			slog.Warn("memory: injection over budget and nothing left to trim, skipping", "budget", budget)
			return ""
		}
	}
}

// insertSystemBlock 把记忆块作为 system 消息插在连续前导 system 之后。
// 保证 history[0] 仍是 agent 提示词（CompressionMiddleware.protectSystem 语义不变），
// 且不拆散任何 assistant/tool 配对。
func insertSystemBlock(h []core.Message, block string) []core.Message {
	i := 0
	for i < len(h) && h[i].Role == "system" {
		i++
	}
	out := make([]core.Message, 0, len(h)+1)
	out = append(out, h[:i]...)
	out = append(out, core.Message{Role: "system", Content: block})
	out = append(out, h[i:]...)
	return out
}

// ---- 渲染 ----

// renderMemoryBlock 把未完成任务 + 相关事实渲染成一条 system 消息。task 在前。
func renderMemoryBlock(tasks []memory.Task, facts []memory.Fact) string {
	var b strings.Builder
	b.WriteString(MemoryBlockPrefix + " 来自之前的会话，仅供参考，可能与当前任务无关：\n")
	if len(tasks) > 0 {
		b.WriteString("### 未完成任务\n")
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
	return b.String()
}

// ---- 小工具 ----

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
