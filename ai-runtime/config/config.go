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

const defaultContextWindow = 256000

// DefaultLLM 返回默认 LLM 配置（不含 APIKey）。
func DefaultLLM() LLMConfig {
	return LLMConfig{
		Current: CurrentConfig{Provider: "openai", Model: "gpt-4o"},
		Providers: map[string]ProviderConfig{
			"openai": {
				Type:     ProviderTypeOpenAI,
				Endpoint: "http://localhost:8000/v1",
				Models: map[string]ModelConfig{
					"gpt-4o": {
						ContextWindow: defaultContextWindow,
						Thinking: ProviderThinkingConfig{
							RequestMode: ThinkingRequestAuto,
							Effort:      ThinkingEffortMedium,
						},
					},
				},
			},
		},
		CompressAt:    0.8,
		CompactKeep:   10,
		MaxIterations: 15,
		Thinking:      ThinkingConfig{Show: true},
	}
}

// WithDefaults 返回补齐模型缺省值的配置副本。
func (c LLMConfig) WithDefaults() LLMConfig {
	clone := c.Clone()
	for name, provider := range clone.Providers {
		for modelID, model := range provider.Models {
			if provider.Type == ProviderTypeOpenAI {
				if model.ContextWindow == 0 {
					model.ContextWindow = defaultContextWindow
				}
				if model.Thinking.RequestMode == "" {
					model.Thinking.RequestMode = ThinkingRequestAuto
				}
				if model.Thinking.Effort == "" {
					model.Thinking.Effort = ThinkingEffortMedium
				}
			}
			provider.Models[modelID] = model
		}
		clone.Providers[name] = provider
	}
	return clone
}

// Clone 返回不共享 Provider 或 Model map 的 LLM 配置副本。
func (c LLMConfig) Clone() LLMConfig {
	clone := c
	clone.Providers = make(map[string]ProviderConfig, len(c.Providers))
	for name, provider := range c.Providers {
		provider.Models = make(map[string]ModelConfig, len(provider.Models))
		for modelID, model := range c.Providers[name].Models {
			provider.Models[modelID] = model
		}
		clone.Providers[name] = provider
	}
	return clone
}

// ResolveCurrent 返回当前选择对应的 Provider 连接配置和模型属性。
func (c LLMConfig) ResolveCurrent() (ProviderConfig, ModelConfig, error) {
	c = c.WithDefaults()
	name := c.Current.Provider
	if name == "" {
		return ProviderConfig{}, ModelConfig{}, fmt.Errorf("current provider is not configured")
	}
	provider, ok := c.Providers[name]
	if !ok {
		return ProviderConfig{}, ModelConfig{}, fmt.Errorf("current provider %q does not exist", name)
	}
	model, ok := provider.Models[c.Current.Model]
	if !ok {
		return ProviderConfig{}, ModelConfig{}, fmt.Errorf("provider %q model %q does not exist", name, c.Current.Model)
	}
	if err := validateProvider(name, provider, c.Current.Model, model); err != nil {
		return ProviderConfig{}, ModelConfig{}, err
	}
	return provider, model, nil
}

// Validate 确保全局策略、Provider 连接及注册的所有模型在请求前有效。
func (c LLMConfig) Validate() error {
	c = c.WithDefaults()
	if _, _, err := c.ResolveCurrent(); err != nil {
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
		if !validProviderName(name) {
			return fmt.Errorf("invalid provider name %q", name)
		}
		if err := validateProviderConnection(name, provider); err != nil {
			return err
		}
		if len(provider.Models) == 0 {
			return fmt.Errorf("provider %q must define at least one model", name)
		}
		for modelID, model := range provider.Models {
			if modelID == "" {
				return fmt.Errorf("provider %q has an empty model id", name)
			}
			if err := validateProvider(name, provider, modelID, model); err != nil {
				return fmt.Errorf("provider %q model %q: %w", name, modelID, err)
			}
		}
	}
	return nil
}

func validateProvider(name string, provider ProviderConfig, modelID string, model ModelConfig) error {
	if err := validateProviderConnection(name, provider); err != nil {
		return err
	}
	if modelID == "" {
		return fmt.Errorf("provider %q model is required", name)
	}
	if model.ContextWindow <= 0 {
		return fmt.Errorf("provider %q model %q context_window must be positive", name, modelID)
	}
	switch provider.Type {
	case ProviderTypeOpenAI:
		if model.MaxTokens != 0 || provider.AuthType != "" {
			return fmt.Errorf("openai provider %q has Anthropic-only fields", name)
		}
		if model.Thinking.RequestMode == "" {
			return fmt.Errorf("openai provider %q thinking request_mode is required", name)
		}
		if _, err := ParseThinkingRequestMode(string(model.Thinking.RequestMode)); err != nil {
			return fmt.Errorf("openai provider %q: %w", name, err)
		}
		if _, err := ParseThinkingEffort(string(model.Thinking.Effort)); err != nil {
			return fmt.Errorf("openai provider %q: %w", name, err)
		}
	case ProviderTypeAnthropic:
		if model.MaxTokens <= 0 {
			return fmt.Errorf("anthropic provider %q max_tokens must be positive", name)
		}
		if model.Thinking.RequestMode != "" || model.Thinking.Effort != "" {
			return fmt.Errorf("anthropic provider %q has OpenAI-only thinking fields", name)
		}
	}
	return nil
}

func validateProviderConnection(name string, provider ProviderConfig) error {
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
	switch provider.Type {
	case ProviderTypeOpenAI:
		if provider.AuthType != "" {
			return fmt.Errorf("openai provider %q has Anthropic-only fields", name)
		}
	case ProviderTypeAnthropic:
		if provider.APIKey == "" {
			return fmt.Errorf("anthropic provider %q api_key is required", name)
		}
		switch provider.AuthType {
		case AnthropicAuthBearer, AnthropicAuthAPIKey:
		default:
			return fmt.Errorf("anthropic provider %q auth_type is invalid", name)
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
		// MaxConcurrentRuns/MaxPendingRuns 取总方案第四节建议值 4/16;
		// CapabilityWaitTimeout 取 openQuestions ③ 拍板值 45(秒,硬约束 <
		// ai-core toolCtx 60s,ai-core/agent/agent.go:147-153)。daemon app
		// config 默认块未设的字段为 0,分别由 WithRunLimits /
		// resolveCapabilityWait 的 <=0 回落兜底(双保险)。
		Session: SessionConfig{Enabled: true, MaxConcurrentRuns: 4, MaxPendingRuns: 16, CapabilityWaitTimeout: 45},
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

// ParseContextWindow 解析正数 token 数，支持无后缀及 k/m 后缀（1024 进制），如
// 32768、32k、1.5m；非法输入返回错误。
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
