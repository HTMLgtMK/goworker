package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	hub          *spec.Hub
	config       map[string]string // in-memory config, overrides env
	conversation []Message         // 跨 /agent 调用的对话历史
}

func (p *AgentPlugin) Name() string { return "agent" }

func (p *AgentPlugin) Init(h *spec.Hub) error {
	p.hub = h
	p.config = loadEnvFile()

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

	// 创建 Provider 和 Agent
	provider := NewOpenAIProvider(p.get(cfgEndpoint), p.get(cfgAPIKey), p.get(cfgModel))
	agent := NewAgent(provider, tools, sandboxCfg)

	agentCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// decisions channel 用于 HITL 确认决策
	decisions := make(chan spec.HITLDecision, 1)

	tokenCh, msgCh, err := agent.Run(agentCtx, p.conversation, input, decisions)
	if err != nil {
		ctx.Writer(fmt.Sprintf("✘ %v\n", err))
		return nil
	}

	for tok := range tokenCh {
		if tok.Done {
			break
		}
		if tok.Type == TokenTypeInterrupt && tok.Interrupt != nil {
			decision := p.promptForDecision(ctx, tok.Interrupt)
			select {
			case decisions <- decision:
			case <-agentCtx.Done():
			}
			continue
		}
		ctx.Writer(tok.Content)
	}
	ctx.Writer("\n")

	// 保存对话历史（剔除首条 system prompt）
	messages := <-msgCh
	if len(messages) > 1 {
		p.conversation = messages[1:]
	}

	return nil
}

// promptForDecision 使用前端的 I/O 展示审批选项并获取用户决策。
func (p *AgentPlugin) promptForDecision(ctx *spec.Context, req *spec.InterruptRequest) spec.HITLDecision {
	ctx.Writer(fmt.Sprintf("\n⚠️  Risky %s: %s\n", req.ToolName, req.Command))
	ctx.Writer(fmt.Sprintf("  Reason: %s\n", req.RiskReason))
	ctx.Writer("[a]pprove, [e]dit, [r]eject, res[p]ond [a]: ")

	if ctx.ReadLine == nil {
		return spec.HITLDecision{
			InterruptID: req.ID,
			Type:        spec.DecisionReject,
		}
	}

	line, err := ctx.ReadLine()
	if err != nil {
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}
	}
	line = strings.TrimSpace(line)

	switch {
	case line == "" || line == "a" || line == "approve":
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionApprove}

	case line == "r" || line == "reject":
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}

	case line == "e" || line == "edit":
		ctx.Writer("  New command: ")
		edited, err := ctx.ReadLine()
		if err != nil {
			return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}
		}
		return spec.HITLDecision{
			InterruptID: req.ID,
			Type:        spec.DecisionEdit,
			Command:     strings.TrimSpace(edited),
		}

	case line == "p" || line == "respond":
		ctx.Writer("  Your instruction: ")
		msg, err := ctx.ReadLine()
		if err != nil {
			return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}
		}
		return spec.HITLDecision{
			InterruptID: req.ID,
			Type:        spec.DecisionRespond,
			Message:     strings.TrimSpace(msg),
		}

	default:
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}
	}
}

func (p *AgentPlugin) sandboxConfig() sandbox.Config {
	mode := sandbox.Mode(p.get(cfgSandboxMode))

	cfg := sandbox.Config{
		Mode:           mode,
		DeniedPatterns: sandbox.MustCompile(sandbox.DefaultDeniedPatterns),
		RiskyPatterns:  sandbox.MustCompile(sandbox.DefaultRiskyPatterns),
	}

	if wd, err := os.Getwd(); err == nil {
		cfg.AllowedWorkDir = wd
	}

	return cfg
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
				err := p.hub.Eval(spec.NewContext(func(s string) { buf.WriteString(s) }, nil, nil), "/"+tool.Name)
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
