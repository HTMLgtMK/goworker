package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestDefaultLLMRegistry(t *testing.T) {
	llm := DefaultLLM()
	name, provider, err := llm.ResolveDefault()
	if err != nil {
		t.Fatalf("ResolveDefault: %v", err)
	}
	if name != "openai" {
		t.Errorf("default provider = %q, want openai", name)
	}
	if provider.Type != ProviderTypeOpenAI || provider.Endpoint != "http://localhost:8000/v1" || provider.Model != "gpt-4o" || provider.ContextWindow != 128000 {
		t.Errorf("default provider = %#v", provider)
	}
	if !llm.Thinking.Show {
		t.Error("thinking should be shown by default")
	}
	if provider.Thinking.RequestMode != ThinkingRequestAuto || provider.Thinking.Effort != ThinkingEffortMedium {
		t.Errorf("provider thinking = %#v", provider.Thinking)
	}
}

func TestLLMConfigResolveValidateAndClone(t *testing.T) {
	llm := DefaultLLM()
	if err := llm.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	for name, cfg := range map[string]LLMConfig{
		"missing default": {DefaultProvider: "missing", Providers: llm.Providers, CompressAt: llm.CompressAt, CompactKeep: llm.CompactKeep, MaxIterations: llm.MaxIterations, Thinking: llm.Thinking},
		"unknown type":    {DefaultProvider: "bad", Providers: map[string]ProviderConfig{"bad": {Type: "wat", Endpoint: "http://localhost", Model: "x", ContextWindow: 1}}, CompressAt: 0.8, CompactKeep: 1, MaxIterations: 1},
		"invalid name":    {DefaultProvider: "bad.name", Providers: map[string]ProviderConfig{"bad.name": llm.Providers["openai"]}, CompressAt: 0.8, CompactKeep: 1, MaxIterations: 1},
		"anthropic auth":  {DefaultProvider: "anthropic", Providers: map[string]ProviderConfig{"anthropic": {Type: ProviderTypeAnthropic, Endpoint: "http://localhost", Model: "claude", APIKey: "key", ContextWindow: 1, MaxTokens: 1}}, CompressAt: 0.8, CompactKeep: 1, MaxIterations: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate should reject invalid registry")
			}
		})
	}

	clone := llm.Clone()
	clone.Providers["openai"] = ProviderConfig{Model: "changed"}
	if reflect.DeepEqual(clone.Providers, llm.Providers) {
		t.Fatal("Clone shares provider map with original")
	}
}

func TestLLMConfigResolveDefaultErrorsAreActionable(t *testing.T) {
	_, _, err := (LLMConfig{}).ResolveDefault()
	if err == nil || !strings.Contains(err.Error(), "default provider") {
		t.Fatalf("ResolveDefault error = %v", err)
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
