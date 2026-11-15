package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

func TestDefault_IncludesLog(t *testing.T) {
	cfg := Default()
	if cfg.Log.Level != "info" {
		t.Errorf("Log.Level = %q, want info", cfg.Log.Level)
	}
	if cfg.Log.MaxSizeMB != 10 {
		t.Errorf("Log.MaxSizeMB = %d, want 10", cfg.Log.MaxSizeMB)
	}
	if cfg.Log.MaxAgeDays != 7 {
		t.Errorf("Log.MaxAgeDays = %d, want 7", cfg.Log.MaxAgeDays)
	}
	if cfg.Log.File == "" {
		t.Error("Log.File should have a default path, got empty")
	}
	if !strings.HasPrefix(cfg.Log.File, os.TempDir()) {
		t.Errorf("Log.File = %q, want under temp dir %q", cfg.Log.File, os.TempDir())
	}
}

func TestDisplayRedactsProviderEndpointPath(t *testing.T) {
	cfg := Default()
	provider := cfg.LLM.Providers["openai"]
	provider.Endpoint = "https://user:pass@example.test/private-token/v1?key=query-secret#fragment-secret"
	provider.APIKey = "api-secret"
	cfg.LLM.Providers["openai"] = provider

	display := cfg.Display()
	for _, secret := range []string{"private-token", "query-secret", "fragment-secret", "user:pass", "api-secret"} {
		if strings.Contains(display, secret) {
			t.Errorf("Display leaked %q: %s", secret, display)
		}
	}
	if !strings.Contains(display, "https://example.test/%5Bredacted%5D") {
		t.Fatalf("Display omitted redacted endpoint host: %s", display)
	}
}

func TestLoad_NamedProvidersAndRejectLegacy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := `llm:
  current: {provider: cc-switch, model: claude-sonnet-4-6}
  providers:
    cc-switch:
      type: anthropic
      endpoint: http://127.0.0.1:15721
      api_key: PROXY_MANAGED
      auth_type: bearer
      models:
        claude-sonnet-4-6: {max_tokens: 8192, context_window: 1048576}
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := Load(path)
	if cfg == nil {
		t.Fatal("Load failed")
	}
	provider, model, err := cfg.LLM.ResolveCurrent()
	if err != nil || provider.Type != runtimeconfig.ProviderTypeAnthropic || model.MaxTokens != 8192 || len(cfg.LLM.Providers) != 1 {
		t.Fatalf("provider = %#v model = %#v, %v", provider, model, err)
	}
	if strings.Contains(cfg.Display(), "PROXY_MANAGED") {
		t.Fatal("Display leaked api key")
	}
	if err := os.WriteFile(path, []byte("llm:\n  model: legacy\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if Load(path) != nil {
		t.Fatal("legacy config must be rejected")
	}
}

func TestLoad_OpenAIProviderUsesDefaultsForOmittedFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := "llm:\n  current: {provider: openai, model: gpt-4o}\n  providers:\n    openai:\n      type: openai\n      endpoint: http://localhost:8000/v1\n      models: {gpt-4o: {}}\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := Load(path)
	if cfg == nil {
		t.Fatal("Load failed")
	}
	model := cfg.LLM.Providers["openai"].Models["gpt-4o"]
	if model.ContextWindow != 256000 || model.Thinking.RequestMode != runtimeconfig.ThinkingRequestAuto || model.Thinking.Effort != runtimeconfig.ThinkingEffortMedium {
		t.Fatalf("model defaults = %#v", model)
	}
}

func TestSetField_ProviderPaths(t *testing.T) {
	cfg := Default()
	provider := cfg.LLM.Providers["openai"]
	provider.Models["gpt-4.1"] = runtimeconfig.ModelConfig{}
	cfg.LLM.Providers["openai"] = provider
	for _, setting := range [][2]string{{"llm.current.provider", "openai"}, {"llm.current.model", "gpt-4.1"}, {"llm.providers.openai.models.gpt-4.1.context_window", "64k"}} {
		if err := cfg.SetField(setting[0], setting[1]); err != nil {
			t.Fatalf("SetField %s: %v", setting[0], err)
		}
	}
	if cfg.LLM.Current.Model != "gpt-4.1" || cfg.LLM.Providers["openai"].Models["gpt-4.1"].ContextWindow != 65536 {
		t.Fatalf("config = %#v", cfg.LLM)
	}
}

func TestLoad_MixedLegacyAndProvidersRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mixed.yaml")
	if err := os.WriteFile(path, []byte(`
llm:
  endpoint: http://localhost:8000/v1
  default_provider: openai
  providers:
    openai:
      type: openai
      endpoint: http://localhost:8000/v1
      model: gpt-4o
      context_window: 128000
      thinking:
        request_mode: auto
        effort: medium
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg := Load(path); cfg != nil {
		t.Fatal("mixed legacy and registry configuration should fail")
	}
}

func TestLoad_ParsesLogSection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(`
log:
  level: debug
  file: /tmp/goworker.log
  max_size_mb: 64
  max_age_days: 30
`), 0644)

	cfg := Load(path)
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q, want debug", cfg.Log.Level)
	}
	if cfg.Log.File != "/tmp/goworker.log" {
		t.Errorf("Log.File = %q, want /tmp/goworker.log", cfg.Log.File)
	}
	if cfg.Log.MaxSizeMB != 64 {
		t.Errorf("Log.MaxSizeMB = %d, want 64", cfg.Log.MaxSizeMB)
	}
	if cfg.Log.MaxAgeDays != 30 {
		t.Errorf("Log.MaxAgeDays = %d, want 30", cfg.Log.MaxAgeDays)
	}
}

func TestLoad_MissingLogKeepsDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("llm:\n  current: {provider: openai, model: gpt-4o}\n"), 0644)

	cfg := Load(path)
	if cfg.Log.Level != "info" {
		t.Errorf("missing log section should keep default, got %q", cfg.Log.Level)
	}
	if cfg.Log.File == "" {
		t.Error("missing log section should keep default file path, got empty")
	}
}

func TestSetField_Log(t *testing.T) {
	cfg := Default()
	tests := []struct {
		key, val string
		check    func(*Config) bool
	}{
		{"log.level", "debug", func(c *Config) bool { return c.Log.Level == "debug" }},
		{"log.file", "/tmp/x.log", func(c *Config) bool { return c.Log.File == "/tmp/x.log" }},
		{"log.max_size_mb", "64", func(c *Config) bool { return c.Log.MaxSizeMB == 64 }},
		{"log.max_age_days", "30", func(c *Config) bool { return c.Log.MaxAgeDays == 30 }},
	}
	for _, tt := range tests {
		if err := cfg.SetField(tt.key, tt.val); err != nil {
			t.Fatalf("SetField(%q): %v", tt.key, err)
		}
		if !tt.check(cfg) {
			t.Errorf("SetField(%q, %q) not applied", tt.key, tt.val)
		}
	}
}

func TestSetField_LogRejectsBadValues(t *testing.T) {
	cfg := Default()
	if err := cfg.SetField("log.level", "not-a-level"); err == nil {
		t.Error("SetField(log.level, garbage) should error")
	}
	if err := cfg.SetField("log.max_size_mb", "abc"); err == nil {
		t.Error("SetField(log.max_size_mb, abc) should error")
	}
}

func TestSetField_Thinking(t *testing.T) {
	cfg := Default()
	tests := []struct {
		key, value string
		check      func(*Config) bool
	}{
		{"llm.thinking.show", "false", func(c *Config) bool { return !c.LLM.Thinking.Show }},
		{"llm.providers.openai.models.gpt-4o.thinking.request_mode", "enable_thinking", func(c *Config) bool {
			return c.LLM.Providers["openai"].Models["gpt-4o"].Thinking.RequestMode == runtimeconfig.ThinkingRequestEnable
		}},
		{"llm.providers.openai.models.gpt-4o.thinking.effort", "high", func(c *Config) bool {
			return c.LLM.Providers["openai"].Models["gpt-4o"].Thinking.Effort == runtimeconfig.ThinkingEffortHigh
		}},
	}
	for _, tt := range tests {
		if err := cfg.SetField(tt.key, tt.value); err != nil {
			t.Fatalf("SetField(%q, %q): %v", tt.key, tt.value, err)
		}
		if !tt.check(cfg) {
			t.Errorf("SetField(%q, %q) not applied", tt.key, tt.value)
		}
	}
}

func TestThinkingYAMLRoundtripAndMissingDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := Default()
	cfg.LLM.Thinking.Show = false
	provider := cfg.LLM.Providers["openai"]
	model := provider.Models["gpt-4o"]
	model.ContextWindow = 12345
	model.Thinking.RequestMode = runtimeconfig.ThinkingRequestEffort
	model.Thinking.Effort = runtimeconfig.ThinkingEffortHigh
	provider.Models["gpt-4o"] = model
	cfg.LLM.Providers["openai"] = provider
	if err := Save(cfg, path); err != nil {
		t.Fatal(err)
	}
	loaded := Load(path)
	if loaded == nil {
		t.Fatal("Load failed")
	}
	_, got, err := loaded.LLM.ResolveCurrent()
	if err != nil || got != model || loaded.LLM.Thinking != cfg.LLM.Thinking {
		t.Fatalf("roundtrip model = %#v, %v", got, err)
	}
}

func TestSetField_ThinkingRejectsBadValuesWithoutMutation(t *testing.T) {
	for _, tt := range []struct {
		key, value string
	}{
		{"llm.thinking.show", "sometimes"},
		{"llm.providers.openai.models.gpt-4o.thinking.request_mode", "guess"},
		{"llm.providers.openai.models.gpt-4o.thinking.effort", "extreme"},
	} {
		cfg := Default()
		before := cfg.LLM.Thinking
		if err := cfg.SetField(tt.key, tt.value); err == nil {
			t.Errorf("SetField(%q, %q) should error", tt.key, tt.value)
		}
		if cfg.LLM.Thinking != before {
			t.Errorf("SetField(%q, %q) mutated config after error", tt.key, tt.value)
		}
	}
}

func TestSetField_Compression(t *testing.T) {
	cfg := Default()
	if cfg.LLM.CompressAt != 0.8 || cfg.LLM.CompactKeep != 10 || cfg.LLM.MaxIterations != 15 {
		t.Fatalf("defaults = %v/%d/%d, want 0.8/10/15", cfg.LLM.CompressAt, cfg.LLM.CompactKeep, cfg.LLM.MaxIterations)
	}

	if err := cfg.SetField("llm.compress_at", "0.5"); err != nil {
		t.Fatalf("SetField compress_at: %v", err)
	}
	if cfg.LLM.CompressAt != 0.5 {
		t.Errorf("CompressAt = %v, want 0.5", cfg.LLM.CompressAt)
	}
	if err := cfg.SetField("llm.compact_keep", "20"); err != nil {
		t.Fatalf("SetField compact_keep: %v", err)
	}
	if cfg.LLM.CompactKeep != 20 {
		t.Errorf("CompactKeep = %d, want 20", cfg.LLM.CompactKeep)
	}
	if err := cfg.SetField("llm.max_iterations", "30"); err != nil {
		t.Fatalf("SetField max_iterations: %v", err)
	}
	if cfg.LLM.MaxIterations != 30 {
		t.Errorf("MaxIterations = %d, want 30", cfg.LLM.MaxIterations)
	}
}

func TestSetField_CompressionRejectsBadValues(t *testing.T) {
	cfg := Default()
	// NaN 是特例：ParseFloat 不报错且比较恒 false，必须显式拦截
	for _, v := range []string{"abc", "1.5", "-0.1", "NaN", "nan"} {
		if err := cfg.SetField("llm.compress_at", v); err == nil {
			t.Errorf("SetField(llm.compress_at, %q) should error", v)
		}
	}
	for _, v := range []string{"abc", "0", "-3"} {
		if err := cfg.SetField("llm.compact_keep", v); err == nil {
			t.Errorf("SetField(llm.compact_keep, %q) should error", v)
		}
	}
	for _, v := range []string{"abc", "0", "-3"} {
		if err := cfg.SetField("llm.max_iterations", v); err == nil {
			t.Errorf("SetField(llm.max_iterations, %q) should error", v)
		}
	}
}

func TestLoad_ParsesMCPSection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(`
mcp:
  servers:
    - name: filesystem
      command: npx
      args: ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
`), 0644)

	cfg := Load(path)
	if len(cfg.MCP.Servers) != 1 {
		t.Fatalf("MCP.Servers = %+v", cfg.MCP.Servers)
	}
	s := cfg.MCP.Servers[0]
	if s.Name != "filesystem" || s.Command != "npx" || len(s.Args) != 3 {
		t.Errorf("server = %+v", s)
	}
}

func TestDefaultDir_EnvOverride(t *testing.T) {
	t.Setenv("GOWORKER_CONFIG_DIR", "/tmp/gw-test")
	if got := DefaultDir(); got != "/tmp/gw-test" {
		t.Errorf("DefaultDir = %q, want /tmp/gw-test", got)
	}
	if got := DefaultPath(); got != filepath.Join("/tmp/gw-test", "config.yaml") {
		t.Errorf("DefaultPath = %q", got)
	}
}

func TestDefault_IncludesMemory(t *testing.T) {
	cfg := Default()
	if !cfg.Memory.Enabled {
		t.Error("Memory.Enabled should default true")
	}
	if cfg.Memory.TaskKeep != 50 || cfg.Memory.TaskInjectN != 3 ||
		cfg.Memory.LtmInjectTopK != 8 || !cfg.Memory.LtmExtract {
		t.Errorf("memory defaults wrong: %+v", cfg.Memory)
	}
	if cfg.Memory.InjectBudgetRatio != 0.15 {
		t.Errorf("InjectBudgetRatio = %v, want 0.15", cfg.Memory.InjectBudgetRatio)
	}
	if cfg.Memory.Dir != filepath.Join(DefaultDir(), "memory") {
		t.Errorf("Memory.Dir = %q, want under DefaultDir/memory", cfg.Memory.Dir)
	}
}

func TestSetField_Memory(t *testing.T) {
	cfg := Default()
	tests := []struct {
		key, val string
		check    func(*Config) bool
	}{
		{"memory.dir", "/tmp/gwmem", func(c *Config) bool { return c.Memory.Dir == "/tmp/gwmem" }},
		{"memory.enabled", "false", func(c *Config) bool { return !c.Memory.Enabled }},
		{"memory.task_keep", "0", func(c *Config) bool { return c.Memory.TaskKeep == 0 }},
		{"memory.task_inject_n", "5", func(c *Config) bool { return c.Memory.TaskInjectN == 5 }},
		{"memory.ltm_inject_top_k", "12", func(c *Config) bool { return c.Memory.LtmInjectTopK == 12 }},
		{"memory.ltm_extract", "false", func(c *Config) bool { return !c.Memory.LtmExtract }},
		{"memory.inject_budget_ratio", "0.3", func(c *Config) bool { return c.Memory.InjectBudgetRatio == 0.3 }},
	}
	for _, tt := range tests {
		if err := cfg.SetField(tt.key, tt.val); err != nil {
			t.Fatalf("SetField(%q): %v", tt.key, err)
		}
		if !tt.check(cfg) {
			t.Errorf("SetField(%q, %q) not applied", tt.key, tt.val)
		}
	}
}

func TestSetField_MemoryRejectsBadValues(t *testing.T) {
	cfg := Default()
	for _, tt := range []struct{ key, val string }{
		{"memory.enabled", "notabool"},
		{"memory.task_keep", "abc"},
		{"memory.task_keep", "-1"},
		{"memory.task_inject_n", "-2"},
		{"memory.ltm_inject_top_k", "abc"},
		{"memory.ltm_extract", "yes"},
		{"memory.inject_budget_ratio", "0"},
		{"memory.inject_budget_ratio", "1.5"},
		{"memory.inject_budget_ratio", "abc"},
	} {
		if err := cfg.SetField(tt.key, tt.val); err == nil {
			t.Errorf("SetField(%q, %q) should error", tt.key, tt.val)
		}
	}
}

func TestLoad_ParsesMemorySection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(`
memory:
  enabled: true
  task_keep: 100
  task_inject_n: 2
  ltm_inject_top_k: 5
  ltm_extract: false
  inject_budget_ratio: 0.2
  dir: /custom/mem
`), 0644)

	cfg := Load(path)
	if !cfg.Memory.Enabled || cfg.Memory.TaskKeep != 100 || cfg.Memory.TaskInjectN != 2 ||
		cfg.Memory.LtmInjectTopK != 5 || cfg.Memory.LtmExtract || cfg.Memory.InjectBudgetRatio != 0.2 ||
		cfg.Memory.Dir != "/custom/mem" {
		t.Errorf("memory section not parsed: %+v", cfg.Memory)
	}
}

func TestLoad_MissingMemoryKeepsDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("llm:\n  current: {provider: openai, model: gpt-4o}\n"), 0644)

	cfg := Load(path)
	if !cfg.Memory.Enabled {
		t.Error("missing memory section should keep default enabled=true")
	}
}

func TestDefault_IncludesSession(t *testing.T) {
	cfg := Default()
	if cfg.Session.Dir == "" {
		t.Error("Session.Dir should have a default path, got empty")
	}
	if !cfg.Session.Enabled {
		t.Error("Session.Enabled should default true")
	}
}

func TestSetField_Session(t *testing.T) {
	cfg := Default()
	tests := []struct {
		key, val string
		check    func(*Config) bool
	}{
		{"session.dir", "/tmp/foo", func(c *Config) bool { return c.Session.Dir == "/tmp/foo" }},
		{"session.enabled", "false", func(c *Config) bool { return !c.Session.Enabled }},
	}
	for _, tt := range tests {
		if err := cfg.SetField(tt.key, tt.val); err != nil {
			t.Fatalf("SetField(%q): %v", tt.key, err)
		}
		if !tt.check(cfg) {
			t.Errorf("SetField(%q, %q) not applied", tt.key, tt.val)
		}
	}
}

func TestSetField_SessionRejectsBadValues(t *testing.T) {
	cfg := Default()
	if err := cfg.SetField("session.enabled", "yes"); err == nil {
		t.Error("SetField(session.enabled, yes) should error")
	}
}

func TestLoad_ParsesSandboxAllowRules(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(`
sandbox:
  mode: normal
  allow_rules:
    - match: "git push"
      max_risk: "high"
      effects: ["network", "file_write"]
      desc: "git push 免确认"
    - match: "pip install"
      max_risk: "medium"
  audit_log: true
`), 0644)

	cfg := Load(path)
	if !cfg.Sandbox.AuditLog {
		t.Error("AuditLog should be true after load")
	}
	if len(cfg.Sandbox.AllowRules) != 2 {
		t.Fatalf("AllowRules len = %d, want 2", len(cfg.Sandbox.AllowRules))
	}
	r := cfg.Sandbox.AllowRules[0]
	if r.Match != "git push" || r.MaxRisk != "high" || r.Desc != "git push 免确认" {
		t.Errorf("rule0 = %+v", r)
	}
	if len(r.Effects) != 2 || r.Effects[0] != "network" || r.Effects[1] != "file_write" {
		t.Errorf("rule0 effects = %v, want [network file_write]", r.Effects)
	}
}

func TestLoad_ParsesUnifiedSandboxPolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`
sandbox:
  mode: auto
  auto_max_risk: elevated
  tool_allow_rules:
    - key: "mcp_filesystem_write"
      max_risk: medium
      desc: "trusted filesystem writer"
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := Load(path)
	if cfg.Sandbox.AutoMaxRisk != "elevated" {
		t.Fatalf("AutoMaxRisk = %q, want elevated", cfg.Sandbox.AutoMaxRisk)
	}
	if len(cfg.Sandbox.ToolAllowRules) != 1 {
		t.Fatalf("ToolAllowRules len = %d, want 1", len(cfg.Sandbox.ToolAllowRules))
	}
	if rule := cfg.Sandbox.ToolAllowRules[0]; rule.Key != "mcp_filesystem_write" || rule.MaxRisk != "medium" {
		t.Errorf("tool rule = %+v", rule)
	}

	runtimeCfg := cfg.ToRuntime()
	if runtimeCfg.Sandbox.AutoMaxRisk != "elevated" || len(runtimeCfg.Sandbox.ToolAllowRules) != 1 {
		t.Fatalf("runtime sandbox policy = %+v", runtimeCfg.Sandbox)
	}
	if err := Save(cfg, filepath.Join(dir, "roundtrip.yaml")); err != nil {
		t.Fatalf("Save roundtrip: %v", err)
	}
	loaded := Load(filepath.Join(dir, "roundtrip.yaml"))
	if loaded.Sandbox.AutoMaxRisk != "elevated" || len(loaded.Sandbox.ToolAllowRules) != 1 {
		t.Fatalf("roundtrip sandbox policy = %+v", loaded.Sandbox)
	}
}

func TestLoad_RejectsInvalidAutoMaxRisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("sandbox:\n  auto_max_risk: unknown-level\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg := Load(path); cfg != nil {
		t.Fatalf("Load(invalid auto_max_risk) = %+v, want nil", cfg)
	}
}

func TestDefault_SandboxAuditLogOff(t *testing.T) {
	cfg := Default()
	if cfg.Sandbox.AuditLog {
		t.Error("AuditLog should default false")
	}
	if len(cfg.Sandbox.AllowRules) != 0 {
		t.Errorf("AllowRules should default empty, got %v", cfg.Sandbox.AllowRules)
	}
}

// vscode ACP 前端已下线（统一走 app-service 的 frontend/acp.sock）。
// 存量 config.yaml 里的 frontend.vscode 段必须被静默忽略：Load 用非严格
// yaml.Unmarshal，未知字段不报错，且不能影响其余段的解析。
func TestLoad_IgnoresLegacyVscodeFrontendSection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`
frontend:
  vscode:
    enabled: true
    socket: /tmp/x.sock
  acp:
    enabled: false
    socket: /tmp/legacy-acp.sock
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := Load(path)
	if cfg == nil {
		t.Fatal("Load returned nil for config containing legacy frontend.vscode section")
	}
	if cfg.Frontend.ACP.Enabled {
		t.Error("Frontend.ACP.Enabled = true, want false from yaml")
	}
	if cfg.Frontend.ACP.Socket != "/tmp/legacy-acp.sock" {
		t.Errorf("Frontend.ACP.Socket = %q, want /tmp/legacy-acp.sock", cfg.Frontend.ACP.Socket)
	}
}
