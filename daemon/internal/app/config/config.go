// Package config 定义 goworker 的全局配置结构。
//
// 配置从 YAML 文件加载，缺失字段由默认值兜底。
// 文件路径：~/.config/goworker/config.yaml（可用 GOWORKER_CONFIG_DIR 覆盖目录）。
//
// 分层：本包是 YAML 解析层 + 路径中枢；runtime 段(LLM/Memory/Session/MCP)用 ai-runtime/config，
// Sandbox 保留本地解析层（risky_patterns 字符串/结构体双格式兼容），经 ToRuntime() 转成 ai-runtime 的聚合配置注入插件。
package config

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/ai-sandbox"
	"gopkg.in/yaml.v3"
)

// Config 是 goworker 的整体配置。所有段顶层平铺，旧 config.yaml 不缩进 → 兼容。
type Config struct {
	Frontend FrontendConfig              `yaml:"frontend"`
	LLM      runtimeconfig.LLMConfig     `yaml:"llm"`
	Memory   runtimeconfig.MemoryConfig  `yaml:"memory"`
	Sandbox  SandboxConfig               `yaml:"sandbox"`
	Session  runtimeconfig.SessionConfig `yaml:"session"`
	MCP      runtimeconfig.MCPConfig     `yaml:"mcp"`
	Log      logger.Config               `yaml:"log"`
}

type FrontendConfig struct {
	Stdin StdinConfig `yaml:"stdin"`
}

type StdinConfig struct {
	Theme string `yaml:"theme"` // "default" 或主题 JSON 文件路径
}

// ---- sandbox 解析层（保留双格式兼容，运行时转 ai-sandbox）----

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
	AllowedWorkDir string              `yaml:"allowed_work_dir"`      // 空 = 使用当前目录
	DeniedPatterns []string            `yaml:"denied_patterns"`       // 空 = 使用 sandbox 默认
	RiskyPatterns  []RiskPatternConfig `yaml:"risky_patterns"`        // 空 = 使用 sandbox 默认
	SafeCommands   []string            `yaml:"safe_commands"`         // 追加的只读安全命令名（直接放行）
	AllowRules     []AllowRuleConfig   `yaml:"allow_rules,omitempty"` // 用户预批准规则（normal 模式免确认）
	AuditLog       bool                `yaml:"audit_log,omitempty"`   // 决策审计 jsonl 落盘（默认关）
}

// AllowRuleConfig 是一条用户预批准规则：命令 token 前缀 + 风险上限 + 允许副作用子集。
// Match 用 token 前缀匹配（非正则，避免配置注入），如 "git push" 命中 `git push --force origin`。
type AllowRuleConfig struct {
	Match   string   `yaml:"match"`             // 归一化主命令 token 前缀："git push"
	MaxRisk string   `yaml:"max_risk"`          // "R0".."R7"，超过该等级的命令不放行
	Effects []string `yaml:"effects,omitempty"` // 允许的副作用子集，空 = 全部允许
	Desc    string   `yaml:"desc,omitempty"`    // 人类可读描述
}

// ToSandbox 把解析层（含双格式兼容）转成 ai-sandbox 的运行时 DTO。
func (s SandboxConfig) ToSandbox() sandbox.SandboxConfig {
	out := sandbox.SandboxConfig{
		Mode:           s.Mode,
		AllowedWorkDir: s.AllowedWorkDir,
		DeniedPatterns: s.DeniedPatterns,
		SafeCommands:   s.SafeCommands,
		AuditLog:       s.AuditLog,
	}
	for _, r := range s.RiskyPatterns {
		out.RiskyPatterns = append(out.RiskyPatterns, sandbox.RiskPatternConfig{Pattern: r.Pattern, Desc: r.Desc})
	}
	for _, a := range s.AllowRules {
		out.AllowRules = append(out.AllowRules, sandbox.AllowRuleConfig{Match: a.Match, MaxRisk: a.MaxRisk, Effects: a.Effects, Desc: a.Desc})
	}
	return out
}

// fromSandbox 把 ai-sandbox 的 DTO 转回解析层。
func fromSandbox(s sandbox.SandboxConfig) SandboxConfig {
	out := SandboxConfig{
		Mode:           s.Mode,
		AllowedWorkDir: s.AllowedWorkDir,
		DeniedPatterns: s.DeniedPatterns,
		SafeCommands:   s.SafeCommands,
		AuditLog:       s.AuditLog,
	}
	for _, r := range s.RiskyPatterns {
		out.RiskyPatterns = append(out.RiskyPatterns, RiskPatternConfig{Pattern: r.Pattern, Desc: r.Desc})
	}
	for _, a := range s.AllowRules {
		out.AllowRules = append(out.AllowRules, AllowRuleConfig{Match: a.Match, MaxRisk: a.MaxRisk, Effects: a.Effects, Desc: a.Desc})
	}
	return out
}

// ---- 双视图同步 ----

// ToRuntime 复制出 ai-runtime 的聚合配置，供插件消费。
func (c *Config) ToRuntime() *runtimeconfig.Config {
	return &runtimeconfig.Config{
		LLM:     c.LLM.Clone(),
		Memory:  c.Memory,
		Sandbox: c.Sandbox.ToSandbox(),
		Session: c.Session,
		MCP:     c.MCP,
	}
}

// ApplyRuntime 把插件持有的 ai-runtime 配置回写到解析层（SaveConfig 双向同步）。
func (c *Config) ApplyRuntime(r *runtimeconfig.Config) {
	c.LLM = r.LLM.Clone()
	c.Memory = r.Memory
	c.Session = r.Session
	c.MCP = r.MCP
	c.Sandbox = fromSandbox(r.Sandbox)
}

// ---- 默认值 ----

// Default 返回带默认值的 Config。
func Default() *Config {
	logCfg := logger.Default()
	logCfg.File = defaultLogPath()
	mem := runtimeconfig.DefaultMemory()
	mem.Dir = filepath.Join(DefaultDir(), "memory")
	return &Config{
		Frontend: FrontendConfig{
			Stdin: StdinConfig{Theme: "default"},
		},
		LLM:    runtimeconfig.DefaultLLM(),
		Memory: mem,
		Sandbox: SandboxConfig{
			Mode: "normal",
		},
		Session: runtimeconfig.SessionConfig{
			Dir:     filepath.Join(DefaultDir(), "sessions"),
			Enabled: true,
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

// DefaultDir 返回配置目录。
// 优先 GOWORKER_CONFIG_DIR，否则 ~/.config/goworker。
// skill、日志等衍生路径都基于它，避免各包各自算路径。
func DefaultDir() string {
	if d := os.Getenv("GOWORKER_CONFIG_DIR"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return filepath.Join(home, ".config", "goworker")
}

// DefaultPath 返回默认的配置文件路径。
func DefaultPath() string {
	return filepath.Join(DefaultDir(), "config.yaml")
}

// Display 返回 YAML 格式的配置文本（API Key 自动脱敏）。
func (c *Config) Display() string {
	cfg := *c
	cfg.LLM = c.LLM.Clone()
	for name, provider := range cfg.LLM.Providers {
		if provider.APIKey != "" {
			provider.APIKey = "***"
			cfg.LLM.Providers[name] = provider
		}
	}
	data, _ := yaml.Marshal(cfg)
	return string(data)
}

// SetField 按点分 key 设置配置项（如 "llm.endpoint"、"sandbox.mode"）。
func (c *Config) SetField(key, value string) error {
	if strings.HasPrefix(key, "llm.") {
		return c.setLLMField(strings.TrimPrefix(key, "llm."), value)
	}

	switch key {
	case "frontend.stdin.theme":
		c.Frontend.Stdin.Theme = value
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
	case "memory.dir":
		c.Memory.Dir = value
	case "memory.enabled":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("无效 memory.enabled: %s（应为 true/false）", value)
		}
		c.Memory.Enabled = b
	case "memory.task_keep":
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return fmt.Errorf("无效 task_keep: %s（应为非负整数，0=不裁剪）", value)
		}
		c.Memory.TaskKeep = n
	case "memory.task_inject_n":
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return fmt.Errorf("无效 task_inject_n: %s（应为非负整数）", value)
		}
		c.Memory.TaskInjectN = n
	case "memory.ltm_inject_top_k":
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return fmt.Errorf("无效 ltm_inject_top_k: %s（应为非负整数）", value)
		}
		c.Memory.LtmInjectTopK = n
	case "memory.ltm_extract":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("无效 memory.ltm_extract: %s（应为 true/false）", value)
		}
		c.Memory.LtmExtract = b
	case "memory.inject_budget_ratio":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(f) || f <= 0 || f > 1 {
			return fmt.Errorf("无效 inject_budget_ratio: %s（应为 0-1 的比例，如 0.15）", value)
		}
		c.Memory.InjectBudgetRatio = f
	case "session.dir":
		c.Session.Dir = value
	case "session.enabled":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("无效 session.enabled: %s（应为 true/false）", value)
		}
		c.Session.Enabled = b
	default:
		valid := "frontend.stdin.theme, llm.default_provider, llm.providers.<name>.{type,endpoint,model,api_key,context_window,max_tokens,auth_type,thinking.request_mode,thinking.effort}, llm.compress_at, llm.compact_keep, llm.max_iterations, llm.thinking.show, sandbox.mode, sandbox.allowed_work_dir, log.level, log.file, log.max_size_mb, log.max_age_days, memory.dir, memory.enabled, memory.task_keep, memory.task_inject_n, memory.ltm_inject_top_k, memory.ltm_extract, session.dir, session.enabled, memory.inject_budget_ratio"
		return fmt.Errorf("未知配置项: %s（可用: %s）", key, valid)
	}
	return nil
}

func (c *Config) setLLMField(key, value string) error {
	if key == "default_provider" {
		candidate := c.LLM.Clone()
		candidate.DefaultProvider = value
		if _, _, err := candidate.ResolveDefault(); err != nil {
			return err
		}
		c.LLM = candidate
		return nil
	}
	if key == "compress_at" {
		parsed, err := runtimeconfig.ParseCompressAt(value)
		if err != nil {
			return err
		}
		c.LLM.CompressAt = parsed
		return nil
	}
	if key == "compact_keep" {
		parsed, err := runtimeconfig.ParseCompactKeep(value)
		if err != nil {
			return err
		}
		c.LLM.CompactKeep = parsed
		return nil
	}
	if key == "max_iterations" {
		parsed, err := runtimeconfig.ParseMaxIterations(value)
		if err != nil {
			return err
		}
		c.LLM.MaxIterations = parsed
		return nil
	}
	if key == "thinking.show" {
		parsed, err := runtimeconfig.ParseThinkingShow(value)
		if err != nil {
			return err
		}
		c.LLM.Thinking.Show = parsed
		return nil
	}

	parts := strings.Split(key, ".")
	if len(parts) < 3 || parts[0] != "providers" {
		return fmt.Errorf("未知 LLM 配置项: llm.%s", key)
	}
	provider, ok := c.LLM.Providers[parts[1]]
	if !ok {
		return fmt.Errorf("provider %q does not exist", parts[1])
	}
	field := strings.Join(parts[2:], ".")
	switch field {
	case "type":
		provider.Type = runtimeconfig.ProviderType(value)
	case "endpoint":
		provider.Endpoint = value
	case "model":
		provider.Model = value
	case "api_key":
		provider.APIKey = value
	case "context_window":
		parsed, err := runtimeconfig.ParseContextWindow(value)
		if err != nil {
			return err
		}
		provider.ContextWindow = parsed
	case "auth_type":
		provider.AuthType = runtimeconfig.AnthropicAuthType(value)
	case "max_tokens":
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return fmt.Errorf("无效 max_tokens: %s（应为正整数）", value)
		}
		provider.MaxTokens = parsed
	case "thinking.request_mode":
		parsed, err := runtimeconfig.ParseThinkingRequestMode(value)
		if err != nil {
			return err
		}
		provider.Thinking.RequestMode = parsed
	case "thinking.effort":
		parsed, err := runtimeconfig.ParseThinkingEffort(value)
		if err != nil {
			return err
		}
		provider.Thinking.Effort = parsed
	default:
		return fmt.Errorf("未知 Provider 配置项: %s", field)
	}
	candidate := c.LLM.Clone()
	candidate.Providers[parts[1]] = provider
	if err := candidate.Validate(); err != nil {
		return err
	}
	c.LLM = candidate
	return nil
}

// Load 读取 YAML 配置文件，返回合并默认值后的 Config。
// 文件不存在时返回默认配置；无效配置返回 nil，调用方必须停止启动而不是带着默认值瞎跑。
func Load(path string) *Config {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return Default()
	}
	if err != nil {
		slog.Error("config: read failed", "err", err)
		return nil
	}
	if hasMixedLLMFormats(data) {
		slog.Error("config: legacy LLM fields and providers cannot be mixed")
		return nil
	}
	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		slog.Error("config: parse failed", "err", err)
		return nil
	}
	if !hasProviders(data) {
		var legacy struct {
			LLM struct {
				Endpoint      string `yaml:"endpoint"`
				Model         string `yaml:"model"`
				APIKey        string `yaml:"api_key"`
				ContextWindow int    `yaml:"context_window"`
				Thinking      struct {
					Show        *bool                             `yaml:"show"`
					RequestMode runtimeconfig.ThinkingRequestMode `yaml:"request_mode"`
					Effort      runtimeconfig.ThinkingEffort      `yaml:"effort"`
				} `yaml:"thinking"`
			} `yaml:"llm"`
		}
		if err := yaml.Unmarshal(data, &legacy); err != nil {
			slog.Error("config: parse legacy LLM failed", "err", err)
			return nil
		}
		provider := cfg.LLM.Providers["openai"]
		if legacy.LLM.Endpoint != "" {
			provider.Endpoint = legacy.LLM.Endpoint
		}
		if legacy.LLM.Model != "" {
			provider.Model = legacy.LLM.Model
		}
		if legacy.LLM.APIKey != "" {
			provider.APIKey = legacy.LLM.APIKey
		}
		if legacy.LLM.ContextWindow > 0 {
			provider.ContextWindow = legacy.LLM.ContextWindow
		}
		if legacy.LLM.Thinking.RequestMode != "" {
			provider.Thinking.RequestMode = legacy.LLM.Thinking.RequestMode
		}
		if legacy.LLM.Thinking.Effort != "" {
			provider.Thinking.Effort = legacy.LLM.Thinking.Effort
		}
		if legacy.LLM.Thinking.Show != nil {
			cfg.LLM.Thinking.Show = *legacy.LLM.Thinking.Show
		}
		cfg.LLM.Providers = map[string]runtimeconfig.ProviderConfig{"openai": provider}
		cfg.LLM.DefaultProvider = "openai"
	}
	if err := cfg.LLM.Validate(); err != nil {
		slog.Error("config: invalid LLM configuration", "err", err)
		return nil
	}
	return cfg
}

func hasProviders(data []byte) bool {
	return llmYAMLKeys(data)["providers"]
}

func hasMixedLLMFormats(data []byte) bool {
	keys := llmYAMLKeys(data)
	if !keys["providers"] {
		return false
	}
	for _, key := range []string{"endpoint", "model", "api_key", "context_window"} {
		if keys[key] {
			return true
		}
	}
	return false
}

func llmYAMLKeys(data []byte) map[string]bool {
	var root yaml.Node
	if yaml.Unmarshal(data, &root) != nil || len(root.Content) == 0 {
		return nil
	}
	keys := make(map[string]bool)
	mapping := root.Content[0]
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value != "llm" || mapping.Content[i+1].Kind != yaml.MappingNode {
			continue
		}
		llm := mapping.Content[i+1]
		for j := 0; j+1 < len(llm.Content); j += 2 {
			keys[llm.Content[j].Value] = true
		}
	}
	return keys
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
	// 写入临时文件：CreateTemp 避免并发写互踩；0600 收窄权限（文件含 API key）
	tmp, err := os.CreateTemp(dir, ".config-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // rename 失败时不留垃圾文件
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// rename 在 POSIX 上是原子的
	return os.Rename(tmpPath, path)
}
