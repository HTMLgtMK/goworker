package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	sandbox "github.com/tinguo/goworker/ai-sandbox"
	"gopkg.in/yaml.v3"
)

func TestRiskPatternConfigUnmarshalYAML(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    RiskPatternConfig
		wantErr bool
	}{
		{name: "legacy scalar", input: `"rm\\s+"`, want: RiskPatternConfig{Pattern: `rm\s+`}},
		{name: "structured mapping", input: `{pattern: "sudo .*", desc: "privileged command"}`, want: RiskPatternConfig{Pattern: "sudo .*", Desc: "privileged command"}},
		{name: "invalid sequence", input: `[invalid]`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got RiskPatternConfig
			err := yaml.Unmarshal([]byte(tt.input), &got)
			if tt.wantErr {
				if err == nil {
					t.Fatal("yaml.Unmarshal succeeded, want invalid input error")
				}
				return
			}
			if err != nil {
				t.Fatalf("yaml.Unmarshal: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSandboxRuntimeConversions(t *testing.T) {
	parsed := SandboxConfig{
		Mode:           "auto",
		AutoMaxRisk:    "high",
		AllowedWorkDir: "/workspace",
		DeniedPatterns: []string{"rm -rf /"},
		RiskyPatterns:  []RiskPatternConfig{{Pattern: "git push --force", Desc: "rewrites remote history"}},
		SafeCommands:   []string{"pwd"},
		AllowRules:     []AllowRuleConfig{{Match: "git push", MaxRisk: "high", Effects: []string{"network"}, Desc: "trusted remote"}},
		ToolAllowRules: []ToolAllowRuleConfig{{Key: "mcp_writer", MaxRisk: "medium", Desc: "trusted tool"}},
		AuditLog:       true,
	}
	want := sandbox.SandboxConfig{
		Mode:           "auto",
		AutoMaxRisk:    "high",
		AllowedWorkDir: "/workspace",
		DeniedPatterns: []string{"rm -rf /"},
		RiskyPatterns:  []sandbox.RiskPatternConfig{{Pattern: "git push --force", Desc: "rewrites remote history"}},
		SafeCommands:   []string{"pwd"},
		AllowRules:     []sandbox.AllowRuleConfig{{Match: "git push", MaxRisk: "high", Effects: []string{"network"}, Desc: "trusted remote"}},
		ToolAllowRules: []sandbox.ToolAllowRuleConfig{{Key: "mcp_writer", MaxRisk: "medium", Desc: "trusted tool"}},
		AuditLog:       true,
	}
	if got := parsed.ToSandbox(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ToSandbox() = %+v, want %+v", got, want)
	}

	cfg := Default()
	runtimeCfg := cfg.ToRuntime()
	runtimeCfg.Sandbox = want
	runtimeCfg.LLM.Providers["openai"] = runtimeconfig.ProviderConfig{
		Type: runtimeconfig.ProviderTypeOpenAI, Endpoint: "https://api.example.test/v1",
		Models: map[string]runtimeconfig.ModelConfig{"model": {ContextWindow: 8192}},
	}
	runtimeCfg.Memory.Dir = "/runtime/memory"
	runtimeCfg.Session.Dir = "/runtime/sessions"
	runtimeCfg.MCP.Servers = []runtimeconfig.MCPServer{{Name: "fs", Command: "fs-server"}}
	runtimeCfg.Dispatch = runtimeconfig.DispatchConfig{Enabled: true, DefaultWorker: "worker"}
	cfg.ApplyRuntime(runtimeCfg)

	if !reflect.DeepEqual(cfg.Sandbox, parsed) {
		t.Errorf("ApplyRuntime sandbox = %+v, want %+v", cfg.Sandbox, parsed)
	}
	if cfg.LLM.Providers["openai"].Endpoint != "https://api.example.test/v1" || cfg.Memory.Dir != "/runtime/memory" ||
		cfg.Session.Dir != "/runtime/sessions" || len(cfg.MCP.Servers) != 1 || !cfg.Dispatch.Enabled {
		t.Errorf("ApplyRuntime did not copy runtime fields: %+v", cfg)
	}
}

func TestSetFieldLLMProviderAndModelPaths(t *testing.T) {
	cfg := Default()
	provider := cfg.LLM.Providers["openai"]
	provider.Models["z-model"] = runtimeconfig.ModelConfig{ContextWindow: 8192}
	provider.Models["a-model"] = runtimeconfig.ModelConfig{ContextWindow: 4096}
	cfg.LLM.Providers["openai"] = provider
	cfg.LLM.Providers["anthropic"] = runtimeconfig.ProviderConfig{
		Type: runtimeconfig.ProviderTypeAnthropic, Endpoint: "https://api.anthropic.com/v1",
		APIKey: "test-key", AuthType: runtimeconfig.AnthropicAuthBearer,
		Models: map[string]runtimeconfig.ModelConfig{"claude": {ContextWindow: 200000, MaxTokens: 4096}},
	}

	valid := []struct {
		key, value string
		check      func() bool
	}{
		{"frontend.stdin.theme", "dark", func() bool { return cfg.Frontend.Stdin.Theme == "dark" }},
		{"sandbox.mode", "auto", func() bool { return cfg.Sandbox.Mode == "auto" }},
		{"sandbox.allowed_work_dir", "/workspace", func() bool { return cfg.Sandbox.AllowedWorkDir == "/workspace" }},
		{"llm.current.provider", "openai", func() bool { return cfg.LLM.Current.Provider == "openai" && cfg.LLM.Current.Model == "gpt-4o" }},
		{"llm.current.provider", "anthropic", func() bool { return cfg.LLM.Current.Provider == "anthropic" && cfg.LLM.Current.Model == "claude" }},
		{"llm.providers.openai.type", "openai", func() bool { return cfg.LLM.Providers["openai"].Type == runtimeconfig.ProviderTypeOpenAI }},
		{"llm.providers.openai.endpoint", "https://api.example.test/v1", func() bool { return cfg.LLM.Providers["openai"].Endpoint == "https://api.example.test/v1" }},
		{"llm.providers.openai.api_key", "secret", func() bool { return cfg.LLM.Providers["openai"].APIKey == "secret" }},
		{"llm.providers.anthropic.auth_type", "x-api-key", func() bool { return cfg.LLM.Providers["anthropic"].AuthType == runtimeconfig.AnthropicAuthAPIKey }},
		{"llm.providers.anthropic.models.claude.display_name", "Claude", func() bool { return cfg.LLM.Providers["anthropic"].Models["claude"].DisplayName == "Claude" }},
		{"llm.providers.anthropic.models.claude.context_window", "256k", func() bool { return cfg.LLM.Providers["anthropic"].Models["claude"].ContextWindow == 262144 }},
		{"llm.providers.anthropic.models.claude.max_tokens", "8192", func() bool { return cfg.LLM.Providers["anthropic"].Models["claude"].MaxTokens == 8192 }},
		{"llm.current.model", "claude", func() bool { return cfg.LLM.Current.Model == "claude" }},
	}
	for _, tt := range valid {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			if err := cfg.SetField(tt.key, tt.value); err != nil {
				t.Fatalf("SetField: %v", err)
			}
			if !tt.check() {
				t.Errorf("SetField(%q, %q) was not applied", tt.key, tt.value)
			}
		})
	}

	// Selecting a provider without the current model chooses its first model deterministically.
	cfg.LLM.Current = runtimeconfig.CurrentConfig{Provider: "anthropic", Model: "missing"}
	if err := cfg.SetField("llm.current.provider", "openai"); err != nil {
		t.Fatalf("select openai provider: %v", err)
	}
	if got := cfg.LLM.Current.Model; got != "a-model" {
		t.Errorf("selected fallback model = %q, want sorted first model a-model", got)
	}
}

func TestSetFieldLLMRejectsInvalidPathsAndValues(t *testing.T) {
	cfg := Default()
	before := cfg.LLM.Clone()
	tests := []struct{ key, value string }{
		{"unknown", "x"},
		{"llm.unknown", "x"},
		{"llm.providers", "x"},
		{"llm.providers.missing.endpoint", "https://example.test"},
		{"llm.providers.openai.unknown", "x"},
		{"llm.providers.openai.auth_type", "bearer"},
		{"llm.providers.openai.endpoint", "not a URL"},
		{"llm.providers.openai.models.missing.display_name", "name"},
		{"llm.providers.openai.models.gpt-4o.unknown", "x"},
		{"llm.providers.openai.models.gpt-4o.context_window", "0"},
		{"llm.providers.openai.models.gpt-4o.max_tokens", "abc"},
		{"llm.providers.openai.models.gpt-4o.max_tokens", "0"},
		{"llm.providers.openai.models.gpt-4o.thinking.request_mode", "unknown"},
		{"llm.providers.openai.models.gpt-4o.thinking.effort", "extreme"},
		{"llm.current.provider", "missing"},
		{"llm.current.model", "missing"},
		{"llm.current.provider", "empty"},
	}
	cfg.LLM.Providers["empty"] = runtimeconfig.ProviderConfig{Models: map[string]runtimeconfig.ModelConfig{}}
	for _, tt := range tests {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			if err := cfg.SetField(tt.key, tt.value); err == nil {
				t.Fatalf("SetField(%q, %q) succeeded, want error", tt.key, tt.value)
			}
		})
	}
	if !reflect.DeepEqual(cfg.LLM.Current, before.Current) || !reflect.DeepEqual(cfg.LLM.Providers["openai"], before.Providers["openai"]) {
		t.Errorf("failed SetField mutated current/provider config: %+v", cfg.LLM)
	}
}

func TestSetFieldValidationBoundaries(t *testing.T) {
	cfg := Default()
	for _, tt := range []struct{ key, value string }{
		{"log.level", ""},
		{"log.max_size_mb", "0"},
		{"log.max_age_days", "-1"},
		{"memory.enabled", ""},
		{"memory.task_keep", "-1"},
		{"memory.task_inject_n", "abc"},
		{"memory.ltm_inject_top_k", "-1"},
		{"memory.ltm_extract", "nope"},
		{"memory.inject_budget_ratio", "NaN"},
		{"session.enabled", "perhaps"},
	} {
		if err := cfg.SetField(tt.key, tt.value); err == nil {
			t.Errorf("SetField(%q, %q) succeeded, want validation error", tt.key, tt.value)
		}
	}
	if err := cfg.SetField("memory.inject_budget_ratio", "1"); err != nil {
		t.Errorf("SetField(memory.inject_budget_ratio, 1) rejected inclusive upper bound: %v", err)
	}
	if cfg.Memory.InjectBudgetRatio != 1 {
		t.Errorf("InjectBudgetRatio = %v, want 1 at inclusive upper bound", cfg.Memory.InjectBudgetRatio)
	}
}

func TestLoadFallbackAndErrorPaths(t *testing.T) {
	t.Run("missing file returns defaults", func(t *testing.T) {
		cfg := Load(filepath.Join(t.TempDir(), "missing.yaml"))
		if cfg == nil || cfg.LLM.Current.Provider != "openai" || cfg.Frontend.ACP.Enabled != true {
			t.Fatalf("Load missing file = %+v, want defaults", cfg)
		}
	})
	t.Run("read error", func(t *testing.T) {
		if cfg := Load(t.TempDir()); cfg != nil {
			t.Fatalf("Load directory = %+v, want nil on read error", cfg)
		}
	})
	for _, tt := range []struct {
		name string
		data string
	}{
		{name: "invalid yaml", data: "llm: [unterminated"},
		{name: "invalid llm", data: "llm:\n  current: {provider: absent, model: none}\n"},
		{name: "invalid dispatch", data: "dispatch:\n  enabled: true\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tt.data), 0o600); err != nil {
				t.Fatal(err)
			}
			if cfg := Load(path); cfg != nil {
				t.Fatalf("Load(%s) = %+v, want nil", tt.name, cfg)
			}
		})
	}
}

func TestSaveErrorPaths(t *testing.T) {
	cfg := Default()

	t.Run("parent path is a file", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(parent, []byte("file"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := Save(cfg, filepath.Join(parent, "config.yaml")); err == nil {
			t.Fatal("Save with file parent succeeded, want mkdir error")
		}
	})

	t.Run("destination is a directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := Save(cfg, path); err == nil {
			t.Fatal("Save over directory succeeded, want rename error")
		}
		entries, err := os.ReadDir(filepath.Dir(path))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].Name() != "config.yaml" {
			t.Errorf("temporary files were not cleaned up after rename failure: %v", entries)
		}
	})
}
