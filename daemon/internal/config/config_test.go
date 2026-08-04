package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	os.WriteFile(path, []byte("llm:\n  model: gpt-4o\n"), 0644)

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
	os.WriteFile(path, []byte("llm:\n  model: gpt-4o\n"), 0644)

	cfg := Load(path)
	if !cfg.Memory.Enabled {
		t.Error("missing memory section should keep default enabled=true")
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
