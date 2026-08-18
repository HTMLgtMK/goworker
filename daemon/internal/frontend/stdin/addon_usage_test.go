package stdin

import (
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

func usageEvent(win int, u core.Usage) runtimeconfig.UsageEvent {
	return runtimeconfig.UsageEvent{Usage: u, ContextWindow: win}
}

func TestUsageAddon_RenderCacheRate(t *testing.T) {
	tests := []struct {
		name  string
		usage runtimeconfig.UsageEvent
		want  string // 空串 = 不显示
	}{
		{
			name:  "empty",
			usage: runtimeconfig.UsageEvent{},
			want:  "",
		},
		{
			name:  "cache rate appended to ctx",
			usage: usageEvent(100_000, core.Usage{TotalTokens: 1700, EstimateTokens: 1700, LastPromptTokens: 1000, PromptCacheHitTokens: 600, PromptCacheMissTokens: 400}),
			want:  "ctx 1.00% cache 60.0%",
		},
		{
			name:  "cache rate appended to tok",
			usage: usageEvent(0, core.Usage{TotalTokens: 1700, EstimateTokens: 1700, PromptCacheHitTokens: 1500, PromptCacheMissTokens: 500}),
			want:  "tok 1.7k cache 75.0%",
		},
		{
			name:  "no cache segment when model omits cache fields",
			usage: usageEvent(0, core.Usage{TotalTokens: 1700, EstimateTokens: 1700, PromptCacheHitTokens: 0, PromptCacheMissTokens: 0}),
			want:  "tok 1.7k",
		},
		{
			name:  "low hit rate not rounded to zero",
			usage: usageEvent(0, core.Usage{TotalTokens: 1000, EstimateTokens: 1000, PromptCacheHitTokens: 1, PromptCacheMissTokens: 999}),
			want:  "tok 1.0k cache 0.1%",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewUsageAddon()
			a.usage = tt.usage
			if got := a.Render(); got != tt.want {
				t.Errorf("Render() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUsageAddon_RenderIgnore(t *testing.T) {
	// 空快照 / 全零不该污染状态栏（UsageAddon 是可选段）。
	a := NewUsageAddon()
	if s := a.Render(); s != "" {
		t.Fatalf("empty Render() = %q, want \"\"", s)
	}
	a.usage = usageEvent(0, core.Usage{TotalTokens: 0, EstimateTokens: 0, PromptCacheHitTokens: 10, PromptCacheMissTokens: 0})
	if s := a.Render(); s != "" {
		t.Fatalf("zero tokens but cache set Render() = %q, want \"\"", s)
	}
}
