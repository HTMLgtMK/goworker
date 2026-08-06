package stdin

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/tinguo/goworker/daemon/internal/frontend/statusbar"
)

// compile-time checks
var (
	_ statusbar.Addon      = (*UsageAddon)(nil)
	_ statusbar.Registerer = (*UsageAddon)(nil)
)

// UsageAddon 显示当前 /agent 会话的累计 token 用量。
//
// 订阅 statusbar.EventUsage 事件，事件携带最新累计快照（statusbar.Usage）。
// 与 IterationAddon 不同，这里只关心"最新值"、不需要跨事件累加，
// 所以不走 channel，mutex 保护字段直接存快照即可。
// frontend 不持有 plugin 引用，完全通过事件总线解耦。
type UsageAddon struct {
	mu    sync.Mutex
	usage statusbar.Usage
}

// NewUsageAddon 创建一个 token 用量 addon。
func NewUsageAddon() *UsageAddon {
	return &UsageAddon{}
}

// OnRegister 实现 statusbar.Registerer，注册时订阅 usage 事件。
func (a *UsageAddon) OnRegister(b *statusbar.Bar) {
	b.Subscribe(statusbar.EventUsage, func(data any) {
		u, ok := data.(statusbar.Usage)
		if !ok {
			return
		}
		a.mu.Lock()
		a.usage = u
		a.mu.Unlock()
	})
}

func (a *UsageAddon) Name() string { return "usage" }

// Tick 无轮询逻辑——Render 直接读最新快照即可。
func (a *UsageAddon) Tick(ctx context.Context) {}

func (a *UsageAddon) Render() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.usage.TotalTokens == 0 && a.usage.EstimateTokens == 0 {
		return ""
	}
	// 配置了窗口就用上下文占用百分比（最近一次请求的输入 / 窗口）。
	// 不 clamp 到 100%——超了正好当"快爆窗口"的告警信号。
	// 保留两位小数：低占用（如 3.47%）时整数直接抹成 0%，看不出量级。
	var s string
	if w := a.usage.ContextWindow; w > 0 && a.usage.LastPromptTokens > 0 {
		pct := float64(a.usage.LastPromptTokens) / float64(w) * 100
		s = fmt.Sprintf("ctx %.2f%%", pct)
	} else if a.usage.TotalTokens > 0 {
		// 模型不返回 usage（TotalTokens 为 0）时退回估算值，~ 前缀标记"非精确"
		s = fmt.Sprintf("tok %s", humanize(a.usage.TotalTokens))
	} else {
		s = fmt.Sprintf("tok ~%s", humanize(a.usage.EstimateTokens))
	}
	// 上下文缓存命中率，模型没返回 cache 字段时不显示。
	// 保留一位小数：命中率 0.4% 时整数会抹成 0%，看不出"几乎没命中"。
	if rate, ok := a.usage.CacheHitRate(); ok {
		s += fmt.Sprintf(" cache %.1f%%", rate)
	}
	return s
}

// humanize 把大数格式化为千分位缩写，12345 -> "12.3k"。
func humanize(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return strconv.Itoa(n)
	}
}

// Reset 清空快照，准备新一轮 agent 运行。
func (a *UsageAddon) Reset() {
	a.mu.Lock()
	a.usage = statusbar.Usage{}
	a.mu.Unlock()
}
