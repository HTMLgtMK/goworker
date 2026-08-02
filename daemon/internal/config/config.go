// Package config 定义 goworker 的全局配置结构。
//
// 配置从 YAML 文件加载，缺失字段由默认值兜底。
// 文件路径：~/.config/goworker/config.yaml（可用 GOWORKER_CONFIG_DIR 覆盖目录）。
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/logger"
	"gopkg.in/yaml.v3"
)

// Config 是 goworker 的整体配置。
type Config struct {
	Frontend FrontendConfig `yaml:"frontend"`
	LLM      LLMConfig      `yaml:"llm"`
	Sandbox  SandboxConfig  `yaml:"sandbox"`
	Log      logger.Config  `yaml:"log"`
}

type FrontendConfig struct {
	Stdin StdinConfig `yaml:"stdin"`
}

type StdinConfig struct {
	Theme string `yaml:"theme"` // "default" 或主题 JSON 文件路径
}

type LLMConfig struct {
	Endpoint      string `yaml:"endpoint"`
	Model         string `yaml:"model"`
	APIKey        string `yaml:"api_key"`
	ContextWindow int    `yaml:"context_window"` // 模型上下文窗口（token），0 = 未知
}

// RiskPatternConfig 表示一个风险命令模式及其人类可读描述。
type RiskPatternConfig struct {
	Pattern string `yaml:"pattern"`
	Desc    string `yaml:"desc"`
}

// UnmarshalYAML 兼容两种格式：
//   - "rm\s+"           ← 纯字符串（旧格式）
//   - {pattern, desc}   ← struct 格式
func (r *RiskPatternConfig) UnmarshalYAML(value *yaml.Node) error {
	// 先试纯字符串
	var s string
	if err := value.Decode(&s); err == nil {
		r.Pattern = s
		return nil
	}
	// 再试 struct
	type raw RiskPatternConfig
	return value.Decode((*raw)(r))
}

type SandboxConfig struct {
	Mode           string              `yaml:"mode"`
	AllowedWorkDir string              `yaml:"allowed_work_dir"` // 空 = 使用当前目录
	DeniedPatterns []string            `yaml:"denied_patterns"`  // 空 = 使用 sandbox 默认
	RiskyPatterns  []RiskPatternConfig `yaml:"risky_patterns"`   // 空 = 使用 sandbox 默认
}

// Default 返回带默认值的 Config。
func Default() *Config {
	logCfg := logger.Default()
	logCfg.File = defaultLogPath()
	return &Config{
		Frontend: FrontendConfig{
			Stdin: StdinConfig{
				Theme: "default",
			},
		},
		LLM: LLMConfig{
			Endpoint: "http://localhost:8000/v1",
			Model:    "gpt-4o",
			APIKey:   "",
		},
		Sandbox: SandboxConfig{
			Mode: "normal",
		},
		Log: logCfg,
	}
}

// defaultLogPath 返回默认日志文件路径。
// 放 <tmp>/goworker/<uid>/ 下，按 uid 隔离避免多用户互相污染日志。
func defaultLogPath() string {
	base := filepath.Join(os.TempDir(), "goworker")
	if uid := os.Getuid(); uid >= 0 {
		base = filepath.Join(base, strconv.Itoa(uid))
	}
	return filepath.Join(base, "goworker.log")
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

// Display 返回 YAML 格式的配置文本（API Key 自动脱敏）。
func (c *Config) Display() string {
	cfg := *c // 浅拷贝，不修改原对象
	if cfg.LLM.APIKey != "" {
		cfg.LLM.APIKey = "***"
	}
	// 忽略序列化错误，Marshal 基本不会失败
	data, _ := yaml.Marshal(cfg)
	return string(data)
}

// ParseContextWindow 解析上下文窗口值，支持 k/m 简写：
//
//	"32768"  → 32768
//	"32k"    → 32768
//	"128k"   → 131072
//	"1.5m"   → 1572864
//
// k/m 按 1024 进制换算，贴合主流模型 2 的幂窗口（32768/65536/131072）。
// 返回 token 数，非法输入返回错误。
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

// SetField 按点分 key 设置配置项（如 "llm.endpoint"、"sandbox.mode"）。
func (c *Config) SetField(key, value string) error {
	switch key {
	case "frontend.stdin.theme":
		c.Frontend.Stdin.Theme = value
	case "llm.endpoint":
		c.LLM.Endpoint = value
	case "llm.model":
		c.LLM.Model = value
	case "llm.api_key":
		c.LLM.APIKey = value
	case "llm.context_window":
		n, err := ParseContextWindow(value)
		if err != nil {
			return err
		}
		c.LLM.ContextWindow = n
	case "sandbox.mode":
		c.Sandbox.Mode = value
	case "sandbox.allowed_work_dir":
		c.Sandbox.AllowedWorkDir = value
	case "log.level":
		if !logger.ValidLevel(value) {
			return fmt.Errorf("无效日志等级: %s（可用: debug/info/warn/error）", value)
		}
		c.Log.Level = value
	case "log.file":
		c.Log.File = value
	case "log.max_size_mb":
		n, err := strconv.Atoi(value)
		if err != nil || n <= 0 {
			return fmt.Errorf("无效 max_size_mb: %s", value)
		}
		c.Log.MaxSizeMB = n
	case "log.max_age_days":
		n, err := strconv.Atoi(value)
		if err != nil || n <= 0 {
			return fmt.Errorf("无效 max_age_days: %s", value)
		}
		c.Log.MaxAgeDays = n
	default:
		valid := "frontend.stdin.theme, llm.endpoint, llm.model, llm.api_key, llm.context_window, sandbox.mode, sandbox.allowed_work_dir, log.level, log.file, log.max_size_mb, log.max_age_days"
		return fmt.Errorf("未知配置项: %s（可用: %s）", key, valid)
	}
	return nil
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
