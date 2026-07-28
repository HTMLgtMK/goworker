// Package config 定义 goworker 的全局配置结构。
//
// 配置从 YAML 文件加载，缺失字段由默认值兜底。
// 文件路径：~/.config/goworker/config.yaml（可用 GOWORKER_CONFIG_DIR 覆盖目录）。
package config

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config 是 goworker 的整体配置。
type Config struct {
	LLM     LLMConfig     `yaml:"llm"`
	Sandbox SandboxConfig `yaml:"sandbox"`
}

type LLMConfig struct {
	Endpoint string `yaml:"endpoint"`
	Model    string `yaml:"model"`
	APIKey   string `yaml:"api_key"`
}

type SandboxConfig struct {
	Mode           string   `yaml:"mode"`
	AllowedWorkDir string   `yaml:"allowed_work_dir"` // 空 = 使用当前目录
	DeniedPatterns []string `yaml:"denied_patterns"`   // 空 = 使用 sandbox 默认
	RiskyPatterns  []string `yaml:"risky_patterns"`    // 空 = 使用 sandbox 默认
}

// Default 返回带默认值的 Config。
func Default() *Config {
	return &Config{
		LLM: LLMConfig{
			Endpoint: "http://localhost:8000/v1",
			Model:    "gpt-4o",
			APIKey:   "",
		},
		Sandbox: SandboxConfig{
			Mode: "normal",
		},
	}
}

// DefaultPath 返回默认的配置文件路径。
func DefaultPath() string {
	if d := os.Getenv("GOWORKER_CONFIG_DIR"); d != "" {
		return filepath.Join(d, "config.yaml")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "config.yaml"
	}
	return filepath.Join(home, ".config", "goworker", "config.yaml")
}

// Load 读取 YAML 配置文件，返回合并默认值后的 Config。
// 文件不存在或解析失败时返回默认配置（不报错）。
func Load(path string) *Config {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg
	}
	yaml.Unmarshal(data, cfg) // 缺失字段保留默认值
	return cfg
}

// Save 原子写入配置文件：先写临时文件再 rename，避免 crash 丢数据。
func Save(cfg *Config, path string) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	// 写入临时文件
	tmpPath := filepath.Join(dir, ".config.yaml.tmp")
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	// rename 在 POSIX 上是原子的
	return os.Rename(tmpPath, path)
}
