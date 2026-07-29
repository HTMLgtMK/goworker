package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/middlewares"
	"github.com/tinguo/goworker/daemon/internal/sandbox"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

type AgentPlugin struct {
	hub          *spec.Hub
	conversation []core.Message // 跨 /agent 调用的对话历史
}

func (p *AgentPlugin) Name() string { return "agent" }

func (p *AgentPlugin) Init(h *spec.Hub) error {
	p.hub = h

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

	// 未匹配的任何命令都转发给 agent 处理
	h.SetFallbackHandler(p.handleAgent)

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

		cfg := p.hub.Config
		switch key {
		case "endpoint":
			cfg.LLM.Endpoint = val
		case "model":
			cfg.LLM.Model = val
		case "api_key":
			cfg.LLM.APIKey = val
		case "sandbox_mode":
			cfg.Sandbox.Mode = val
		default:
			ctx.Writer(fmt.Sprintf("未知配置项: %s（可用: endpoint, model, api_key, sandbox_mode）\n", key))
			return nil
		}

		if err := p.hub.SaveConfig(cfg); err != nil {
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
	cfg := p.hub.Config
	keyDisplay := cfg.LLM.APIKey
	if keyDisplay != "" {
		keyDisplay = "***"
	} else {
		keyDisplay = "(未设置)"
	}

	ctx.Writer(fmt.Sprintf("Endpoint:     %s\n", cfg.LLM.Endpoint))
	ctx.Writer(fmt.Sprintf("Model:        %s\n", cfg.LLM.Model))
	ctx.Writer(fmt.Sprintf("API Key:      %s\n", keyDisplay))
	ctx.Writer(fmt.Sprintf("Sandbox Mode: %s\n", cfg.Sandbox.Mode))
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

	// 创建 Provider、Middleware 和 Agent
	cfg := p.hub.Config
	provider := NewOpenAIProvider(cfg.LLM.Endpoint, cfg.LLM.APIKey, cfg.LLM.Model)
	decisions := make(chan spec.HITLDecision, 1)
	hitlMw := middlewares.NewHITLMiddleware(sandboxCfg, middlewares.NewChannelDecisionProvider(decisions))
	agent := NewAgent(provider, tools, []core.Middleware{hitlMw})

	agentCtx, cancel := context.WithTimeout(ctx.Ctx, 5*time.Minute)
	defer cancel()

	tokenCh, msgCh, err := agent.Run(agentCtx, p.conversation, input)
	if err != nil {
		ctx.Writer(fmt.Sprintf("✘ %v\n", err))
		return nil
	}

	for tok := range tokenCh {
		// 先输出内容再检查 Done — Done token 也可能带内容（如错误信息）
		if tok.Content != "" {
			kind, c := p.renderKind(tok)
			ctx.WriteToken(kind, c)
		}
		if tok.Done {
			break
		}
		if tok.Type == core.TokenTypeInterrupt && tok.Interrupt != nil {
			decision := p.promptForDecision(ctx, tok.Interrupt)
			select {
			case decisions <- decision:
			case <-agentCtx.Done():
			}
			continue
		}
	}
	ctx.Writer("\n")
	// 保存对话历史（剔除首条 system prompt）
	messages := <-msgCh
	if len(messages) > 1 {
		p.conversation = messages[1:]
	}

	return nil
}

// renderKind 将 core.Token 类型映射为 spec.RenderKind，剥离渲染逻辑。
func (p *AgentPlugin) renderKind(t core.Token) (spec.RenderKind, string) {
	switch t.Type {
	case core.TokenTypeToolCall:
		return spec.KindToolCall, t.Content
	case core.TokenTypeToolResult:
		return spec.KindToolResult, t.Content
	default:
		// TokenTypeText 及未知类型一律按 markdown 渲染
		return spec.KindText, t.Content
	}
}

// promptForDecision 使用前端的 I/O 展示审批选项并获取用户决策。
func (p *AgentPlugin) promptForDecision(ctx *spec.Context, req *spec.InterruptRequest) spec.HITLDecision {
	ctx.Writer(fmt.Sprintf("\n⚠ %s (%s)\n", req.Command, req.RiskReason))
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
	return *sandbox.NewFromConfig(&p.hub.Config.Sandbox)
}

func (p *AgentPlugin) collectTools(cfg *sandbox.Config) []core.Tool {
	var tools []core.Tool
	tools = append(tools, DefaultTools(cfg)...)

	for _, t := range p.hub.Tools() {
		tool := t
		tools = append(tools, core.Tool{
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  parseSchema(tool.Schema),
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				var buf strings.Builder
				err := p.hub.Eval(spec.NewContext(context.Background(), func(s string) { buf.WriteString(s) }, nil, nil), "/"+tool.Name)
				if err != nil {
					return buf.String(), err
				}
				return strings.TrimSpace(buf.String()), nil
			},
		})
	}
	return tools
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
