package core

import "sync"

// Usage 单次 Chat 调用的 token 账本。
type Usage struct {
	Iteration        int // ReAct 第几轮（从 0 开始）
	PromptTokens     int // 模型返回的输入 token
	CompletionTokens int // 模型返回的输出 token
	TotalTokens      int // 模型返回的总计
	EstimateTokens   int // 发送前粗估（JSON 字节数 / 4），对照模型返回值

	LastPromptTokens int // 仅快照有效：最近一次 Chat 的输入 token（上下文占用计算用）
}

// UsageTracker 累加一次 /agent 会话内所有 Chat 调用的用量。
//
// 写入方是 agent 的 ReAct 循环（goroutine），读取方是 status bar addon
// 与 /usage 命令（可能在不同 goroutine），故内部用 mutex 保护。
// 只记当前会话，跨会话累计交给 slog 日志 + 未来的持久化。
type UsageTracker struct {
	mu    sync.Mutex
	calls []Usage // 明细，/usage 命令展示用
	total Usage   // 累计快照，status bar 用
}

// NewUsageTracker 创建一个空 tracker。
func NewUsageTracker() *UsageTracker {
	return &UsageTracker{}
}

// Record 记录一次 Chat 调用的用量并累加。u 可为 nil（模型不返回 usage）。
func (t *UsageTracker) Record(iter, estimate int, u *UsageInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()

	call := Usage{Iteration: iter, EstimateTokens: estimate}
	if u != nil {
		call.PromptTokens = u.PromptTokens
		call.CompletionTokens = u.CompletionTokens
		call.TotalTokens = u.TotalTokens
	}
	t.calls = append(t.calls, call)

	t.total.PromptTokens += call.PromptTokens
	t.total.CompletionTokens += call.CompletionTokens
	t.total.TotalTokens += call.TotalTokens
	t.total.EstimateTokens += estimate
}

// Snapshot 返回当前会话的累计用量（线程安全）。
// LastPromptTokens 从最近一次调用推导，模型不返回 usage 时退回估算值。
func (t *UsageTracker) Snapshot() Usage {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.total
	if n := len(t.calls); n > 0 {
		last := t.calls[n-1]
		s.LastPromptTokens = last.PromptTokens
		if s.LastPromptTokens == 0 {
			s.LastPromptTokens = last.EstimateTokens
		}
	}
	return s
}

// Calls 返回明细的深拷贝，避免调用方在锁外持引用（线程安全）。
func (t *UsageTracker) Calls() []Usage {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Usage, len(t.calls))
	copy(out, t.calls)
	return out
}

// Reset 清空状态，准备新一轮 agent 运行。
func (t *UsageTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = t.calls[:0]
	t.total = Usage{}
}
