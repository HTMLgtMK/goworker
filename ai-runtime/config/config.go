package config

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"github.com/tinguo/goworker/ai-sandbox"
)

// DefaultLLM 返回默认 LLM 注册表（不含 APIKey）。
func DefaultLLM() LLMConfig {
	return LLMConfig{
		DefaultProvider: "openai",
		Providers: map[string]ProviderConfig{
			"openai": {
				Type:          ProviderTypeOpenAI,
				Endpoint:      "http://localhost:8000/v1",
				Model:         "gpt-4o",
				ContextWindow: 128000,
				Thinking: ProviderThinkingConfig{
					RequestMode: ThinkingRequestAuto,
					Effort:      ThinkingEffortMedium,
				},
			},
		},
		CompressAt:    0.8,
		CompactKeep:   10,
		MaxIterations: 15,
		Thinking:      ThinkingConfig{Show: true},
	}
}

// Clone 返回不共享 Provider map 的 LLM 配置副本。
func (c LLMConfig) Clone() LLMConfig {
	clone := c
	clone.Providers = make(map[string]ProviderConfig, len(c.Providers))
	for name, provider := range c.Providers {
		clone.Providers[name] = provider
	}
	return clone
}

// ResolveDefault 返回当前默认 Provider 的稳定名称和配置。
func (c LLMConfig) ResolveDefault() (string, ProviderConfig, error) {
	if c.DefaultProvider == "" {
		return "", ProviderConfig{}, fmt.Errorf("default provider is not configured")
	}
	provider, ok := c.Providers[c.DefaultProvider]
	if !ok {
		return "", ProviderConfig{}, fmt.Errorf("default provider %q does not exist", c.DefaultProvider)
	}
	if err := validateProvider(c.DefaultProvider, provider); err != nil {
		return "", ProviderConfig{}, err
	}
	return c.DefaultProvider, provider, nil
}

// Validate 确保全局策略和每个命名 Provider 在请求前可用。
func (c LLMConfig) Validate() error {
	if _, _, err := c.ResolveDefault(); err != nil {
		return err
	}
	if c.CompressAt < 0 || c.CompressAt > 1 || math.IsNaN(c.CompressAt) {
		return fmt.Errorf("invalid compress_at %v", c.CompressAt)
	}
	if c.CompactKeep <= 0 {
		return fmt.Errorf("compact_keep must be positive")
	}
	if c.MaxIterations <= 0 {
		return fmt.Errorf("max_iterations must be positive")
	}
	for name, provider := range c.Providers {
		if err := validateProvider(name, provider); err != nil {
			return err
		}
	}
	return nil
}

func validateProvider(name string, provider ProviderConfig) error {
	if !validProviderName(name) {
		return fmt.Errorf("invalid provider name %q", name)
	}
	if provider.Endpoint == "" {
		return fmt.Errorf("provider %q endpoint is required", name)
	}
	endpoint, err := url.ParseRequestURI(provider.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return fmt.Errorf("provider %q endpoint is invalid", name)
	}
	if provider.Model == "" {
		return fmt.Errorf("provider %q model is required", name)
	}
	if provider.ContextWindow <= 0 {
		return fmt.Errorf("provider %q context_window must be positive", name)
	}
	switch provider.Type {
	case ProviderTypeOpenAI:
		if provider.AuthType != "" || provider.MaxTokens != 0 {
			return fmt.Errorf("openai provider %q has Anthropic-only fields", name)
		}
		if provider.Thinking.RequestMode == "" {
			return fmt.Errorf("openai provider %q thinking request_mode is required", name)
		}
		if _, err := ParseThinkingRequestMode(string(provider.Thinking.RequestMode)); err != nil {
			return fmt.Errorf("openai provider %q: %w", name, err)
		}
		if _, err := ParseThinkingEffort(string(provider.Thinking.Effort)); err != nil {
			return fmt.Errorf("openai provider %q: %w", name, err)
		}
	case ProviderTypeAnthropic:
		if provider.APIKey == "" {
			return fmt.Errorf("anthropic provider %q api_key is required", name)
		}
		if provider.MaxTokens <= 0 {
			return fmt.Errorf("anthropic provider %q max_tokens must be positive", name)
		}
		switch provider.AuthType {
		case AnthropicAuthBearer, AnthropicAuthAPIKey:
		default:
			return fmt.Errorf("anthropic provider %q auth_type is invalid", name)
		}
		if provider.Thinking.RequestMode != "" || provider.Thinking.Effort != "" {
			return fmt.Errorf("anthropic provider %q has OpenAI-only thinking fields", name)
		}
	default:
		return fmt.Errorf("provider %q has unsupported type %q", name, provider.Type)
	}
	return nil
}

func validProviderName(name string) bool {
	if name == "" || strings.Contains(name, ".") {
		return false
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

// DefaultMemory 返回默认记忆配置。Dir 留空，由宿主按配置目录派生。
func DefaultMemory() MemoryConfig {
	return MemoryConfig{
		Enabled:           true,
		TaskKeep:          50,
		TaskInjectN:       3,
		LtmInjectTopK:     8,
		LtmExtract:        true,
		InjectBudgetRatio: 0.15,
		UserMaxChars:      1500,
		AgentsMaxChars:    4096,
	}
}

// Default 返回 ai-runtime 的默认配置。Memory.Dir/Session.Dir 留空，由宿主按配置目录派生。
func Default() *Config {
	return &Config{
		LLM:    DefaultLLM(),
		Memory: DefaultMemory(),
		Sandbox: sandbox.SandboxConfig{
			Mode: "normal",
		},
		Session: SessionConfig{Enabled: true},
	}
}

func ParseThinkingShow(value string) (bool, error) {
	show, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("无效 thinking show: %s（应为 true/false）", value)
	}
	return show, nil
}

func ParseThinkingRequestMode(value string) (ThinkingRequestMode, error) {
	mode := ThinkingRequestMode(strings.TrimSpace(value))
	switch mode {
	case ThinkingRequestAuto, ThinkingRequestEnable, ThinkingRequestEffort:
		return mode, nil
	default:
		return "", fmt.Errorf("无效 thinking request_mode: %s（可用: auto/enable_thinking/reasoning_effort）", value)
	}
}

func ParseThinkingEffort(value string) (ThinkingEffort, error) {
	effort := ThinkingEffort(strings.TrimSpace(value))
	switch effort {
	case ThinkingEffortLow, ThinkingEffortMedium, ThinkingEffortHigh:
		return effort, nil
	default:
		return "", fmt.Errorf("无效 thinking effort: %s（可用: low/medium/high）", value)
	}
}

// ParseContextWindow 解析上下文窗口大小，支持纯数字或 k/m 后缀：
//
//	"32768" → 32768
//	"32k"   → 32768
//	"1.5m"  → 1572864
//
// k/m 按 1024 进制换算，贴合主流模型 2 的幂窗口（32768/65536/131072）。
// 返回 token 数，非法输入返回错误。
func ParseContextWindow(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("空值")
	}
	mult := 1
	switch last := s[len(s)-1]; last {
	case 'k', 'K':
		mult = 1024
		s = s[:len(s)-1]
	case 'm', 'M':
		mult = 1024 * 1024
		s = s[:len(s)-1]
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f <= 0 {
		return 0, fmt.Errorf("无效上下文窗口 %q（应为正整数或带 k/m 后缀，如 32768 / 32k / 128k）", s)
	}
	n := int(f * float64(mult))
	if n <= 0 {
		return 0, fmt.Errorf("上下文窗口过小: %q", s)
	}
	return n, nil
}

// ParseCompressAt 解析压缩触发阈值（0-1 比例）。
func ParseCompressAt(v string) (float64, error) {
	f, err := strconv.ParseFloat(v, 64)
	// ParseFloat("NaN") 不报错且 NaN 比较恒 false，需显式排除
	if err != nil || math.IsNaN(f) || f < 0 || f > 1 {
		return 0, fmt.Errorf("无效 compress_at: %s（应为 0-1 的比例，如 0.8）", v)
	}
	return f, nil
}

// ParseCompactKeep 解析压缩保留条数（正整数）。
func ParseCompactKeep(v string) (int, error) {
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("无效 compact_keep: %s（应为正整数，如 10）", v)
	}
	return n, nil
}

// ParseMaxIterations 解析 ReAct 最大迭代数（正整数）。
func ParseMaxIterations(v string) (int, error) {
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("无效 max_iterations: %s（应为正整数，如 15）", v)
	}
	return n, nil
}
