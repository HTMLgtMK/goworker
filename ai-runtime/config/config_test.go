package config

import (
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
