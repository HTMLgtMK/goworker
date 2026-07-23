package agent

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tinguo/goworker/daemon/internal/sandbox"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// config keys
const (
	cfgEndpoint    = "LLM_ENDPOINT"
	cfgModel       = "LLM_MODEL"
	cfgAPIKey      = "LLM_API_KEY"
	cfgSandboxMode = "SANDBOX_MODE"
)

var defaults = map[string]string{
	cfgEndpoint:    "http://localhost:8000/v1",
	cfgModel:       "gpt-4o",
	cfgAPIKey:      "",
	cfgSandboxMode: string(sandbox.ModeNormal),
}

type AgentPlugin struct {
	hub    *spec.Hub
	config map[string]string // in-memory config, overrides env

	mu        sync.Mutex
	approved  map[string]bool // command hash -> 用户已批准的缓存
}

func (p *AgentPlugin) Name() string { return "agent" }

func (p *AgentPlugin) Init(h *spec.Hub) error {
	p.hub = h
	p.config = loadEnvFile()
	p.approved = make(map[string]bool)

	h.RegisterCommand(spec.Command{
		Name:        "/agent",
		Aliases:     []string{"/llm", "/ai"},
		Description: "与 AI Agent 对话",
		Handler:     p.handleAgent,
	})

	h.RegisterCommand(spec.Command{
		Name:        "/model",
		Description: "查看/设置 LLM 配置，用法见 /model help",
		Handler:     p.handleModel,
	})

	return nil
}

func (p *AgentPlugin) Start() error { return nil }
func (p *AgentPlugin) Stop() error  { return nil }

// ---- /model 命令 ----

func (p *AgentPlugin) handleModel(ctx *spec.Context) error {
	args := ctx.Args

	if len(args) == 0 {
		p.showConfig(ctx)
		return nil
	}

	switch args[0] {
	case "help":
		ctx.Writer("用法:\n")
		ctx.Writer("  /model            — 查看当前配置\n")
		ctx.Writer("  /model set <k>=<v> — 设置配置\n")
		ctx.Writer("  可用 key: endpoint, model, api_key, sandbox_mode\n")
		ctx.Writer("  sandbox_mode: off, normal, strict, readonly\n")

	case "set":
		if len(args) < 2 {
			ctx.Writer("用法: /model set <key>=<value>\n")
			return nil
		}
		kv := strings.SplitN(args[1], "=", 2)
		if len(kv) != 2 {
			ctx.Writer("格式错误，示例: /model set endpoint=http://localhost:8000/v1\n")
			return nil
		}
		key, val := kv[0], kv[1]

		envKey := ""
		switch key {
		case "endpoint":
			envKey = cfgEndpoint
		case "model":
			envKey = cfgModel
		case "api_key":
			envKey = cfgAPIKey
		case "sandbox_mode":
			envKey = cfgSandboxMode
		default:
			ctx.Writer(fmt.Sprintf("未知配置项: %s（可用: endpoint, model, api_key, sandbox_mode）\n", key))
			return nil
		}

		p.config[envKey] = val
		if err := saveEnvFile(p.config); err != nil {
			ctx.Writer(fmt.Sprintf("✘ 保存失败: %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ %s 已更新\n", key))

	default:
		ctx.Writer("未知子命令，使用 /model help 查看用法\n")
	}

	return nil
}

func (p *AgentPlugin) showConfig(ctx *spec.Context) {
	keyDisplay := p.get(cfgAPIKey)
	if keyDisplay != "" {
		keyDisplay = "***"
	} else {
		keyDisplay = "(未设置)"
	}

	ctx.Writer(fmt.Sprintf("Endpoint:     %s\n", p.get(cfgEndpoint)))
	ctx.Writer(fmt.Sprintf("Model:        %s\n", p.get(cfgModel)))
	ctx.Writer(fmt.Sprintf("API Key:      %s\n", keyDisplay))
	ctx.Writer(fmt.Sprintf("Sandbox Mode: %s\n", p.get(cfgSandboxMode)))
}

// ---- /agent 命令 ----

func (p *AgentPlugin) handleAgent(ctx *spec.Context) error {
	input := strings.Join(ctx.Args, " ")
	if input == "" {
		ctx.Writer("用法: /agent <你的问题>\n")
		return nil
	}

	// 构建沙箱配置
	sandboxCfg := p.sandboxConfig()

	// 收集工具
	tools := p.collectTools(&sandboxCfg)

	// 创建 Provider
	provider := NewOpenAIProvider(p.get(cfgEndpoint), p.get(cfgAPIKey), p.get(cfgModel))
	agent := NewAgent(provider, tools, sandboxCfg)

	agentCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tokenCh, err := agent.Run(agentCtx, nil, input)
	if err != nil {
		ctx.Writer(fmt.Sprintf("✘ %v\n", err))
		return nil
	}

	for tok := range tokenCh {
		if tok.Done {
			break
		}
		ctx.Writer(tok.Content)
	}
	ctx.Writer("\n")

	return nil
}

func (p *AgentPlugin) sandboxConfig() sandbox.Config {
	mode := sandbox.Mode(p.get(cfgSandboxMode))

	cfg := sandbox.Config{
		Mode:           mode,
		DeniedPatterns: sandbox.MustCompile(sandbox.DefaultDeniedPatterns),
		RiskyPatterns:  sandbox.MustCompile(sandbox.DefaultRiskyPatterns),
	}

	// 获取工作目录
	if wd, err := os.Getwd(); err == nil {
		cfg.AllowedWorkDir = wd
	}

	// normal 模式下注入确认回调
	if mode == sandbox.ModeNormal {
		cfg.ConfirmationFn = p.confirmCommand
	}

	return cfg
}

// confirmCommand 提示用户确认高风险命令，返回 true 表示放行。
func (p *AgentPlugin) confirmCommand(cmd string) bool {
	p.mu.Lock()
	// 检查缓存
	h := sha256.Sum256([]byte(cmd))
	key := hex.EncodeToString(h[:])
	if p.approved[key] {
		p.mu.Unlock()
		return true
	}
	p.mu.Unlock()

	fmt.Fprintf(os.Stderr, "\n⚠️  高风险命令，确认执行？[y/N] %s\n> ", cmd)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)

	if line == "y" || line == "Y" || line == "yes" {
		p.mu.Lock()
		p.approved[key] = true
		p.mu.Unlock()
		return true
	}
	return false
}

func (p *AgentPlugin) collectTools(cfg *sandbox.Config) []Tool {
	var tools []Tool
	tools = append(tools, DefaultTools(cfg)...)

	for _, t := range p.hub.Tools() {
		tool := t
		tools = append(tools, Tool{
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  parseSchema(tool.Schema),
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				var buf strings.Builder
				err := p.hub.Eval(spec.NewContext(func(s string) { buf.WriteString(s) }, nil), "/"+tool.Name)
				if err != nil {
					return buf.String(), err
				}
				return strings.TrimSpace(buf.String()), nil
			},
		})
	}
	return tools
}

// ---- Config helpers ----

func (p *AgentPlugin) get(key string) string {
	if v, ok := p.config[key]; ok && v != "" {
		return v
	}
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaults[key]
}

func configDir() string {
	if d := os.Getenv("GOWORKER_CONFIG_DIR"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return filepath.Join(home, ".config", "goworker")
}

func envPath() string {
	return filepath.Join(configDir(), ".env")
}

func loadEnvFile() map[string]string {
	m := make(map[string]string)
	f, err := os.Open(envPath())
	if err != nil {
		return m
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			m[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return m
}

func saveEnvFile(config map[string]string) error {
	dir := configDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir config dir: %w", err)
	}

	f, err := os.Create(envPath())
	if err != nil {
		return fmt.Errorf("create env file: %w", err)
	}
	defer f.Close()

	for _, key := range []string{cfgEndpoint, cfgModel, cfgAPIKey, cfgSandboxMode} {
		if v, ok := config[key]; ok {
			fmt.Fprintf(f, "%s=%s\n", key, v)
		}
	}
	return nil
}

// ---- Util ----

func parseSchema(schema []byte) map[string]any {
	if len(schema) == 0 {
		return map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}
	}
	var m map[string]any
	json.Unmarshal(schema, &m) // best effort
	return m
}
