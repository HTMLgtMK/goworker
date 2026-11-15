package config

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDefaultLLMRegistry(t *testing.T) {
	llm := DefaultLLM()
	provider, model, err := llm.ResolveCurrent()
	if err != nil {
		t.Fatal(err)
	}
	if llm.Current != (CurrentConfig{Provider: "openai", Model: "gpt-4o"}) || provider.Type != ProviderTypeOpenAI || provider.Endpoint != "http://localhost:8000/v1" || model.ContextWindow != 256000 || model.Thinking.RequestMode != ThinkingRequestAuto || model.Thinking.Effort != ThinkingEffortMedium || !llm.Thinking.Show {
		t.Fatalf("unexpected defaults: %#v %#v", provider, model)
	}
}

func TestLLMConfigDefaultsOmittedOpenAIFields(t *testing.T) {
	llm := DefaultLLM()
	p := llm.Providers["openai"]
	p.Models["gpt-4o"] = ModelConfig{}
	llm.Providers["openai"] = p
	if err := llm.Validate(); err != nil {
		t.Fatal(err)
	}
	_, model, err := llm.ResolveCurrent()
	if err != nil || model.ContextWindow != 256000 || model.Thinking.RequestMode != ThinkingRequestAuto || model.Thinking.Effort != ThinkingEffortMedium {
		t.Fatalf("resolved defaults = %#v, %v", model, err)
	}
}

func TestLLMConfigResolveValidateAndClone(t *testing.T) {
	llm := DefaultLLM()
	if err := llm.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*LLMConfig){
		"missing provider": func(c *LLMConfig) { c.Current.Provider = "missing" },
		"missing model":    func(c *LLMConfig) { c.Current.Model = "missing" },
		"unknown type":     func(c *LLMConfig) { p := c.Providers["openai"]; p.Type = "wat"; c.Providers["openai"] = p },
		"invalid name":     func(c *LLMConfig) { c.Providers["bad.name"] = c.Providers["openai"] },
		"anthropic auth": func(c *LLMConfig) {
			p := c.Providers["openai"]
			p.Type = ProviderTypeAnthropic
			p.APIKey = "key"
			c.Providers["openai"] = p
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := llm.Clone()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	clone := llm.Clone()
	clone.Providers["openai"] = ProviderConfig{}
	if reflect.DeepEqual(clone.Providers, llm.Providers) {
		t.Fatal("Clone shares provider map")
	}
}

func TestLLMConfigResolveCurrentErrorsAreActionable(t *testing.T) {
	_, _, err := (LLMConfig{}).ResolveCurrent()
	if err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("ResolveCurrent error = %v", err)
	}
}

func TestLLMConfigUnmarshalNewModelDomain(t *testing.T) {
	data := []byte(`
current:
  provider: openai
  model: gpt-4.1
providers:
  openai:
    type: openai
    endpoint: https://api.example.com/v1
    api_key: secret
    models:
      gpt-4.1:
        context_window: 1048576
        thinking:
          request_mode: reasoning_effort
          effort: high
      o3:
        context_window: 200000
        thinking:
          request_mode: enable_thinking
          effort: medium
  anthropic:
    type: anthropic
    endpoint: https://api.anthropic.com
    api_key: secret
    auth_type: x-api-key
    models:
      claude-sonnet:
        context_window: 200000
        max_tokens: 8192
`)

	llm := DefaultLLM()
	if err := yaml.Unmarshal(data, &llm); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got, want := llm.Current, (CurrentConfig{Provider: "openai", Model: "gpt-4.1"}); got != want {
		t.Fatalf("current = %#v, want %#v", got, want)
	}
	openai := llm.Providers["openai"]
	if openai.Endpoint != "https://api.example.com/v1" || openai.APIKey != "secret" {
		t.Fatalf("openai connection config = %#v", openai)
	}
	if got := openai.Models["gpt-4.1"]; got.ContextWindow != 1048576 || got.Thinking.Effort != ThinkingEffortHigh {
		t.Fatalf("selected model config = %#v", got)
	}
	if got := llm.Providers["anthropic"].Models["claude-sonnet"]; got.ContextWindow != 200000 || got.MaxTokens != 8192 {
		t.Fatalf("anthropic model config = %#v", got)
	}
	if err := llm.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	_, selected, err := llm.ResolveCurrent()
	if err != nil || selected.ContextWindow != 1048576 {
		t.Fatalf("resolved model = %#v, %v", selected, err)
	}
}

func TestRejectLegacyLLMFields(t *testing.T) {
	for _, field := range []string{"endpoint: https://example.com", "model: old", "api_key: secret", "context_window: 1000", "default_provider: openai"} {
		t.Run(field, func(t *testing.T) {
			cfg := DefaultLLM()
			if err := yaml.Unmarshal([]byte(field), &cfg); err == nil {
				t.Fatalf("legacy field %q must be rejected", field)
			}
		})
	}
	for _, field := range []string{"model: old", "context_window: 1000", "thinking: {effort: high}", "max_tokens: 100"} {
		t.Run("provider/"+field, func(t *testing.T) {
			cfg := DefaultLLM()
			data := "providers:\n  openai:\n    " + field + "\n"
			if err := yaml.Unmarshal([]byte(data), &cfg); err == nil {
				t.Fatalf("legacy provider field %q must be rejected", field)
			}
		})
	}
}

func TestCanonicalModelChangesSurviveResolution(t *testing.T) {
	llm := DefaultLLM()
	provider := llm.Providers[llm.Current.Provider]
	model := provider.Models[llm.Current.Model]
	model.ContextWindow = 12345
	model.Thinking.Effort = ThinkingEffortHigh
	provider.Models[llm.Current.Model] = model
	llm.Providers[llm.Current.Provider] = provider
	_, resolved, err := llm.ResolveCurrent()
	if err != nil || resolved.ContextWindow != 12345 || resolved.Thinking.Effort != ThinkingEffortHigh {
		t.Fatalf("resolved model = %#v, %v", resolved, err)
	}
}

func TestLLMConfigUnmarshalRejectsMixedModelFormats(t *testing.T) {
	for _, data := range []string{
		"current: {provider: openai, model: new-model}\nproviders:\n  openai:\n    type: openai\n    endpoint: https://example.com\n    model: old-model",
		"current: {provider: openai, model: new-model}\nproviders:\n  openai:\n    type: openai\n    endpoint: https://example.com\n    models: {new-model: {context_window: 1000}}\ndefault_provider: openai",
		"default_provider: openai\nproviders:\n  openai:\n    type: openai\n    endpoint: https://example.com\n    models: {new-model: {context_window: 1000}}",
	} {
		var llm LLMConfig
		if err := yaml.Unmarshal([]byte(data), &llm); err == nil {
			t.Errorf("Unmarshal mixed formats should fail for:\n%s", data)
		}
	}
}

func TestLLMConfigCloneDeepCopiesModelRegistry(t *testing.T) {
	llm := LLMConfig{
		Current: CurrentConfig{Provider: "openai", Model: "gpt-4.1"},
		Providers: map[string]ProviderConfig{
			"openai": {Models: map[string]ModelConfig{"gpt-4.1": {ContextWindow: 1000}}},
		},
	}
	clone := llm.Clone()
	provider := clone.Providers["openai"]
	provider.Models["gpt-4.1"] = ModelConfig{ContextWindow: 2000}
	clone.Providers["openai"] = provider
	if llm.Providers["openai"].Models["gpt-4.1"].ContextWindow != 1000 {
		t.Fatal("Clone shares nested model map with original")
	}
}

func TestLLMConfigResolveCurrentReturnsModelAttributes(t *testing.T) {
	llm := DefaultLLM()
	provider, model, err := llm.ResolveCurrent()
	if err != nil {
		t.Fatalf("ResolveCurrent: %v", err)
	}
	if provider.Endpoint != "http://localhost:8000/v1" || model.ContextWindow != defaultContextWindow {
		t.Fatalf("resolved provider/model = %#v / %#v", provider, model)
	}
}

func TestParseThinkingShow(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  bool
	}{
		{value: "true", want: true},
		{value: "false", want: false},
	} {
		got, err := ParseThinkingShow(tt.value)
		if err != nil || got != tt.want {
			t.Errorf("ParseThinkingShow(%q) = %v, %v; want %v, nil", tt.value, got, err, tt.want)
		}
	}
	if _, err := ParseThinkingShow("sometimes"); err == nil {
		t.Error("unknown thinking show value should fail")
	}
}

func TestParseThinkingRequestMode(t *testing.T) {
	for _, value := range []string{"auto", "enable_thinking", "reasoning_effort"} {
		if _, err := ParseThinkingRequestMode(value); err != nil {
			t.Errorf("ParseThinkingRequestMode(%q): %v", value, err)
		}
	}
	if _, err := ParseThinkingRequestMode("guess"); err == nil {
		t.Error("unknown thinking request mode should fail")
	}
}

func TestParseThinkingEffort(t *testing.T) {
	for _, value := range []string{"low", "medium", "high"} {
		if _, err := ParseThinkingEffort(value); err != nil {
			t.Errorf("ParseThinkingEffort(%q): %v", value, err)
		}
	}
	if _, err := ParseThinkingEffort("extreme"); err == nil {
		t.Error("unknown thinking effort should fail")
	}
}

func TestParseContextWindow(t *testing.T) {
	cases := []struct {
		in   string
		want int
		ok   bool
	}{
		{"32768", 32768, true},
		{"32k", 32768, true}, // 1024 进制，命中主流模型窗口
		{"32K", 32768, true},
		{"128k", 131072, true},
		{"1.5m", 1572864, true},
		{"4M", 4194304, true},
		{" 64k ", 65536, true}, // 容忍首尾空格
		{"", 0, false},
		{"abc", 0, false},
		{"k", 0, false},
		{"0", 0, false},
		{"-1k", 0, false},
		{"12x", 0, false},
		{"1.5.5k", 0, false},
	}
	for _, c := range cases {
		got, err := ParseContextWindow(c.in)
		if c.ok && (err != nil || got != c.want) {
			t.Errorf("ParseContextWindow(%q) = %d, %v; want %d, nil", c.in, got, err, c.want)
		}
		if !c.ok && err == nil {
			t.Errorf("ParseContextWindow(%q) = %d, nil; want error", c.in, got)
		}
	}
}

func TestParseCompressAt(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"0.8", 0.8, true},
		{"0", 0, true},
		{"1", 1, true},
		{"1.5", 0, false},
		{"-0.1", 0, false},
		{"NaN", 0, false},
		{"abc", 0, false},
	}
	for _, c := range cases {
		got, err := ParseCompressAt(c.in)
		if c.ok && (err != nil || got != c.want) {
			t.Errorf("ParseCompressAt(%q) = %v, %v; want %v, nil", c.in, got, err, c.want)
		}
		if !c.ok && err == nil {
			t.Errorf("ParseCompressAt(%q) = %v, nil; want error", c.in, got)
		}
	}
}

func TestParseCompactKeepAndMaxIterations(t *testing.T) {
	for name, fn := range map[string]func(string) (int, error){
		"compact_keep":   ParseCompactKeep,
		"max_iterations": ParseMaxIterations,
	} {
		if _, err := fn("10"); err != nil {
			t.Errorf("%s: 10 -> %v", name, err)
		}
		if _, err := fn("0"); err == nil {
			t.Errorf("%s: 0 should error", name)
		}
		if _, err := fn("-3"); err == nil {
			t.Errorf("%s: -3 should error", name)
		}
		if _, err := fn("abc"); err == nil {
			t.Errorf("%s: abc should error", name)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := Default()
	if cfg == nil {
		t.Fatal("Default() returned nil")
	}
	wantMemory := MemoryConfig{
		Enabled: true, TaskKeep: 50, TaskInjectN: 3, LtmInjectTopK: 8, LtmExtract: true,
		InjectBudgetRatio: 0.15, UserMaxChars: 1500, AgentsMaxChars: 4096,
	}
	if cfg.Memory != wantMemory {
		t.Errorf("Memory = %#v, want %#v", cfg.Memory, wantMemory)
	}
	if cfg.Session != (SessionConfig{Enabled: true, MaxConcurrentRuns: 4, MaxPendingRuns: 16, CapabilityWaitTimeout: 45}) {
		t.Errorf("Session = %#v", cfg.Session)
	}
	if cfg.Sandbox.Mode != "normal" {
		t.Errorf("Sandbox.Mode = %q, want normal", cfg.Sandbox.Mode)
	}
}

func TestDispatchConfigValidate(t *testing.T) {
	worker := WorkerConfig{Name: "worker-a", Command: "worker"}
	validWorkers := []WorkerConfig{worker, {Name: "worker-b", Command: "other"}}
	tests := []struct {
		name    string
		config  DispatchConfig
		wantErr string
	}{
		{name: "disabled empty is valid"},
		{name: "valid disabled configuration", config: DispatchConfig{Workers: []WorkerConfig{worker}}},
		{name: "valid enabled configuration", config: DispatchConfig{
			Enabled: true, Workers: validWorkers, DefaultWorker: "worker-a", MaxParallel: 2,
			Routes: []RouteConfig{{Keywords: []string{"bug"}, Worker: "worker-b"}},
		}},
		{name: "empty worker name", config: DispatchConfig{Workers: []WorkerConfig{{Command: "worker"}}}, wantErr: "name is required"},
		{name: "duplicate worker name", config: DispatchConfig{Workers: []WorkerConfig{worker, worker}}, wantErr: "duplicate name"},
		{name: "empty worker command", config: DispatchConfig{Workers: []WorkerConfig{{Name: "worker-a"}}}, wantErr: "command is required"},
		{name: "unknown default worker", config: DispatchConfig{Workers: []WorkerConfig{worker}, DefaultWorker: "missing"}, wantErr: "is not in workers"},
		{name: "negative max parallel", config: DispatchConfig{MaxParallel: -1}, wantErr: "must be >= 0"},
		{name: "invalid permission policy", config: DispatchConfig{Workers: []WorkerConfig{{Name: "worker-a", Command: "worker", OnPermission: "prompt"}}}, wantErr: "invalid on_permission"},
		{name: "route without keywords", config: DispatchConfig{Workers: []WorkerConfig{worker}, Routes: []RouteConfig{{Worker: "worker-a"}}}, wantErr: "keywords is required"},
		{name: "route references unknown worker", config: DispatchConfig{Workers: []WorkerConfig{worker}, Routes: []RouteConfig{{Keywords: []string{"bug"}, Worker: "missing"}}}, wantErr: "worker \"missing\" is not in workers"},
		{name: "enabled without workers", config: DispatchConfig{Enabled: true}, wantErr: "requires at least one worker"},
		{name: "enabled without default worker", config: DispatchConfig{Enabled: true, Workers: []WorkerConfig{worker}}, wantErr: "default_worker is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestDispatchConfigResolveDefaultWorker(t *testing.T) {
	for _, tt := range []struct {
		name    string
		config  DispatchConfig
		want    string
		wantErr bool
	}{
		{name: "explicit default", config: DispatchConfig{DefaultWorker: "chosen", Workers: []WorkerConfig{{Name: "first"}}}, want: "chosen"},
		{name: "first worker fallback", config: DispatchConfig{Workers: []WorkerConfig{{Name: "first"}, {Name: "second"}}}, want: "first"},
		{name: "no workers", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.config.ResolveDefaultWorker()
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "no workers") {
					t.Fatalf("ResolveDefaultWorker() = %q, %v; want no-workers error", got, err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("ResolveDefaultWorker() = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestDispatchConfigMatchWorker(t *testing.T) {
	cfg := DispatchConfig{Routes: []RouteConfig{
		{Keywords: []string{"", "panic"}, Worker: "first"},
		{Keywords: []string{"PANIC", "crash"}, Worker: "second"},
	}}
	for _, tt := range []struct {
		prompt string
		want   string
		match  bool
	}{
		{prompt: "PANIC in production", want: "first", match: true},
		{prompt: "please CRASH", want: "second", match: true},
		{prompt: "ordinary request", match: false},
	} {
		got, ok := cfg.MatchWorker(tt.prompt)
		if got != tt.want || ok != tt.match {
			t.Errorf("MatchWorker(%q) = %q, %v; want %q, %v", tt.prompt, got, ok, tt.want, tt.match)
		}
	}
}

func TestDispatchConfigWorker(t *testing.T) {
	cfg := DispatchConfig{Workers: []WorkerConfig{{Name: "one", Command: "cmd"}}}
	if got, ok := cfg.Worker("one"); !ok || got.Command != "cmd" {
		t.Fatalf("Worker(one) = %#v, %v", got, ok)
	}
	if got, ok := cfg.Worker("missing"); ok || got.Name != "" || got.Command != "" {
		t.Fatalf("Worker(missing) = %#v, %v; want zero value, false", got, ok)
	}
}

func TestLLMConfigValidateFailures(t *testing.T) {
	base := DefaultLLM()
	anthropic := LLMConfig{
		Current: CurrentConfig{Provider: "anthropic", Model: "claude"},
		Providers: map[string]ProviderConfig{"anthropic": {
			Type: ProviderTypeAnthropic, Endpoint: "https://api.example.com", APIKey: "key",
			AuthType: AnthropicAuthAPIKey, Models: map[string]ModelConfig{"claude": {ContextWindow: 1000, MaxTokens: 100}},
		}},
		CompressAt: 0.8, CompactKeep: 10, MaxIterations: 5,
	}
	tests := []struct {
		name    string
		mutate  func(*LLMConfig)
		wantErr string
	}{
		{name: "compress below zero", mutate: func(c *LLMConfig) { c.CompressAt = -0.1 }, wantErr: "invalid compress_at"},
		{name: "compress above one", mutate: func(c *LLMConfig) { c.CompressAt = 1.1 }, wantErr: "invalid compress_at"},
		{name: "compress NaN", mutate: func(c *LLMConfig) { c.CompressAt = math.NaN() }, wantErr: "invalid compress_at"},
		{name: "compact keep must be positive", mutate: func(c *LLMConfig) { c.CompactKeep = 0 }, wantErr: "compact_keep must be positive"},
		{name: "iterations must be positive", mutate: func(c *LLMConfig) { c.MaxIterations = 0 }, wantErr: "max_iterations must be positive"},
		{name: "invalid provider name", mutate: func(c *LLMConfig) { c.Providers["bad.name"] = c.Providers["openai"] }, wantErr: "invalid provider name"},
		{name: "endpoint required", mutate: func(c *LLMConfig) { p := c.Providers["openai"]; p.Endpoint = ""; c.Providers["openai"] = p }, wantErr: "endpoint is required"},
		{name: "endpoint must be absolute uri", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			p.Endpoint = "localhost:8000/v1"
			c.Providers["openai"] = p
		}, wantErr: "endpoint is invalid"},
		{name: "unsupported provider type", mutate: func(c *LLMConfig) { p := c.Providers["openai"]; p.Type = "other"; c.Providers["openai"] = p }, wantErr: "unsupported type"},
		{name: "openai rejects auth type", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			p.AuthType = AnthropicAuthAPIKey
			c.Providers["openai"] = p
		}, wantErr: "Anthropic-only fields"},
		{name: "anthropic needs api key", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			p.Type = ProviderTypeAnthropic
			p.APIKey = ""
			p.AuthType = AnthropicAuthAPIKey
			p.Models["gpt-4o"] = ModelConfig{ContextWindow: 1000, MaxTokens: 100}
			c.Providers["openai"] = p
		}, wantErr: "api_key is required"},
		{name: "anthropic auth type required", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			p.Type = ProviderTypeAnthropic
			p.APIKey = "key"
			p.AuthType = "invalid"
			p.Models["gpt-4o"] = ModelConfig{ContextWindow: 1000, MaxTokens: 100}
			c.Providers["openai"] = p
		}, wantErr: "auth_type is invalid"},
		{name: "provider must have model", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			c.Providers["empty"] = ProviderConfig{Type: ProviderTypeOpenAI, Endpoint: "https://api.example.com", Models: map[string]ModelConfig{}}
			c.Providers["openai"] = p
		}, wantErr: "must define at least one model"},
		{name: "model id required", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			p.Models[""] = ModelConfig{ContextWindow: 1, Thinking: ProviderThinkingConfig{RequestMode: ThinkingRequestAuto, Effort: ThinkingEffortLow}}
			c.Providers["openai"] = p
		}, wantErr: "empty model id"},
		{name: "context window positive", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			m := p.Models["gpt-4o"]
			m.ContextWindow = -1
			p.Models["gpt-4o"] = m
			c.Providers["openai"] = p
		}, wantErr: "context_window must be positive"},
		{name: "openai rejects max tokens", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			m := p.Models["gpt-4o"]
			m.MaxTokens = 1
			p.Models["gpt-4o"] = m
			c.Providers["openai"] = p
		}, wantErr: "Anthropic-only fields"},
		{name: "openai request mode enum", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			m := p.Models["gpt-4o"]
			m.Thinking.RequestMode = "invalid"
			p.Models["gpt-4o"] = m
			c.Providers["openai"] = p
		}, wantErr: "thinking request_mode"},
		{name: "openai effort enum", mutate: func(c *LLMConfig) {
			p := c.Providers["openai"]
			m := p.Models["gpt-4o"]
			m.Thinking.Effort = "invalid"
			p.Models["gpt-4o"] = m
			c.Providers["openai"] = p
		}, wantErr: "thinking effort"},
		{name: "anthropic max tokens positive", mutate: func(c *LLMConfig) {
			*c = anthropic.Clone()
			p := c.Providers["anthropic"]
			m := p.Models["claude"]
			m.MaxTokens = 0
			p.Models["claude"] = m
			c.Providers["anthropic"] = p
		}, wantErr: "max_tokens must be positive"},
		{name: "anthropic rejects thinking fields", mutate: func(c *LLMConfig) {
			*c = anthropic.Clone()
			p := c.Providers["anthropic"]
			m := p.Models["claude"]
			m.Thinking.Effort = ThinkingEffortHigh
			p.Models["claude"] = m
			c.Providers["anthropic"] = p
		}, wantErr: "OpenAI-only thinking fields"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base.Clone()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestLLMConfigValidateProviderModelIndependently(t *testing.T) {
	cfg := DefaultLLM()
	cfg.Current.Model = "missing"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), `model "missing" does not exist`) {
		t.Fatalf("Validate() = %v, want missing-current-model error", err)
	}

	cfg = DefaultLLM()
	cfg.Providers["extra"] = ProviderConfig{
		Type: ProviderTypeAnthropic, Endpoint: "https://api.example.com", APIKey: "key", AuthType: AnthropicAuthBearer,
		Models: map[string]ModelConfig{"model": {ContextWindow: 10, MaxTokens: 0}},
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "max_tokens must be positive") {
		t.Fatalf("Validate() = %v, want invalid non-current provider model", err)
	}
}

func TestValidateProviderRequiresOpenAIRequestMode(t *testing.T) {
	provider := ProviderConfig{Type: ProviderTypeOpenAI, Endpoint: "https://api.example.com"}
	model := ModelConfig{ContextWindow: 10, Thinking: ProviderThinkingConfig{Effort: ThinkingEffortLow}}
	if err := validateProvider("openai", provider, "model", model); err == nil || !strings.Contains(err.Error(), "thinking request_mode is required") {
		t.Fatalf("validateProvider() = %v, want missing request mode error", err)
	}
}

func TestLLMConfigYAMLProviderReplacementAndDecodeErrors(t *testing.T) {
	cfg := DefaultLLM()
	if err := yaml.Unmarshal([]byte("compress_at: 0.5"), &cfg); err != nil {
		t.Fatalf("Unmarshal partial config: %v", err)
	}
	if _, ok := cfg.Providers["openai"]; !ok {
		t.Fatal("omitted providers should preserve existing provider registry")
	}
	if err := yaml.Unmarshal([]byte("providers: {}"), &cfg); err != nil {
		t.Fatalf("Unmarshal empty provider registry: %v", err)
	}
	if len(cfg.Providers) != 0 {
		t.Fatalf("explicit empty provider registry = %#v, want empty", cfg.Providers)
	}
	if err := yaml.Unmarshal([]byte("current: [not, a, mapping]"), &cfg); err == nil {
		t.Fatal("malformed current value should return decode error")
	}
}

func TestParseContextWindowRejectsValuesThatRoundToZero(t *testing.T) {
	if got, err := ParseContextWindow("0.0000001k"); err == nil || got != 0 {
		t.Fatalf("ParseContextWindow(tiny) = %d, %v; want zero and error", got, err)
	}
}
