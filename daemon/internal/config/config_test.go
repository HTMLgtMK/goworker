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

func TestParseContextWindow(t *testing.T) {
	cases := []struct {
		in   string
		want int
		ok   bool
	}{
		{"32768", 32768, true},
		{"32k", 32768, true},  // 1024 进制，命中主流模型窗口
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
