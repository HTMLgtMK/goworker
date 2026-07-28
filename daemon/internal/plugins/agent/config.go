package agent

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config 是 agent 插件的配置结构。
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
	Mode string `yaml:"mode"`
}

func defaultConfig() *Config {
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

func configPath() string {
	return filepath.Join(configDir(), "config.yaml")
}

// loadConfig 读取 YAML 配置文件，缺失字段用 defaultConfig 兜底。
func loadConfig() *Config {
	cfg := defaultConfig()
	data, err := os.ReadFile(configPath())
	if err != nil {
		return cfg
	}
	// 只覆盖 YAML 中存在的字段，缺失的保持默认值
	yaml.Unmarshal(data, cfg)
	return cfg
}

// saveConfig 原子写入 YAML 配置文件：
// 先写临时文件再 rename，避免 crash 丢数据或留下残缺文件。
func saveConfig(cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	dir := configDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	// 写入临时文件
	tmpPath := filepath.Join(dir, ".config.yaml.tmp")
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	// rename 在 POSIX 上是原子的
	return os.Rename(tmpPath, configPath())
}
