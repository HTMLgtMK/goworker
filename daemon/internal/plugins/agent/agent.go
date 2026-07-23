package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tinguo/goworker/daemon/internal/sandbox"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

const maxIterations = 15

var requestIDCounter atomic.Int64

// sandbox action result
const (
	sandboxExec  = iota // 执行 tool
	sandboxSkip         // 跳过此 tool call
	sandboxAbort        // 退出 goroutine
)

// Tool 是 Agent 可调用的工具。
type Tool struct {
	Name        string
	Description string
	Parameters  map[string]any // JSON Schema
	Execute     func(ctx context.Context, args map[string]any) (string, error)
}

// ToolSpec 返回 OpenAI 兼容的工具定义。
func (t Tool) ToolSpec() map[string]any {
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        t.Name,
			"description": t.Description,
			"parameters":  t.Parameters,
		},
	}
}

// DefaultTools 返回 Agent 的默认工具集。
// cfg 为沙箱配置，nil 表示不启用沙箱。
func DefaultTools(cfg *sandbox.Config) []Tool {
	return []Tool{
		{
			Name:        "bash",
			Description: "Execute a shell command. Returns stdout + stderr.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"command": map[string]any{"type": "string", "description": "shell command"},
					"timeout": map[string]any{"type": "number", "description": "timeout in seconds"},
				},
				"required": []string{"command"},
			},
			// NOTE: sandbox 检查不在 tool.Execute 里做，而是在 agent.Run 的 tool call 循环中统一处理。
			// 这样 sandbox 可以通过 interrupt token + decisions channel 与前端交互。
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				cmdStr, _ := args["command"].(string)
				if cmdStr == "" {
					return "", fmt.Errorf("bash: empty command")
				}
				timeout := 30
				if t, ok := args["timeout"].(float64); ok {
					timeout = int(t)
				}
				ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, "bash", "-c", cmdStr)
				if cfg != nil && cfg.AllowedWorkDir != "" {
					cmd.Dir = cfg.AllowedWorkDir
				}
				out, err := cmd.CombinedOutput()
				output := string(out)
				if err != nil {
					if ctx.Err() == context.DeadlineExceeded {
						return output + "\n[timed out]", nil
					}
					return output + "\n" + err.Error(), nil
				}
				return output, nil
			},
		},
		{
			Name:        "read_file",
			Description: "Read a file, with optional offset/limit for large files.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":   map[string]any{"type": "string", "description": "file path"},
					"offset": map[string]any{"type": "number", "description": "line offset"},
					"limit":  map[string]any{"type": "number", "description": "max lines"},
				},
				"required": []string{"path"},
			},
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				path, _ := args["path"].(string)
				if path == "" {
					return "", fmt.Errorf("read_file: empty path")
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return "", fmt.Errorf("read_file: %w", err)
				}
				lines := strings.Split(string(data), "\n")
				offset := 0
				if o, ok := args["offset"].(float64); ok {
					offset = int(o)
				}
				limit := len(lines)
				if l, ok := args["limit"].(float64); ok && int(l) > 0 {
					limit = offset + int(l)
				}
				if offset > len(lines) {
					offset = len(lines)
				}
				if limit > len(lines) {
					limit = len(lines)
				}
				if offset > 0 || limit < len(lines) {
					var b strings.Builder
					for i, line := range lines[offset:limit] {
						b.WriteString(fmt.Sprintf("%d\t%s\n", offset+i+1, line))
					}
					return b.String(), nil
				}
				return string(data), nil
			},
		},
		{
			Name:        "write_file",
			Description: "Write content to a file. Creates directories if needed.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":    map[string]any{"type": "string", "description": "file path"},
					"content": map[string]any{"type": "string", "description": "content to write"},
				},
				"required": []string{"path", "content"},
			},
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				path, _ := args["path"].(string)
				content, _ := args["content"].(string)
				if path == "" {
					return "", fmt.Errorf("write_file: empty path")
				}
				if idx := strings.LastIndex(path, "/"); idx > 0 {
					if err := os.MkdirAll(path[:idx], 0755); err != nil {
						return "", fmt.Errorf("write_file: mkdir: %w", err)
					}
				}
				if err := os.WriteFile(path, []byte(content), 0644); err != nil {
					return "", fmt.Errorf("write_file: %w", err)
				}
				return fmt.Sprintf("wrote %d bytes to %s", len(content), path), nil
			},
		},
	}
}

// ---- Agent (ReAct 循环) ----

// Agent 是一个可使用工具的 ReAct Agent。
type Agent struct {
	provider   Provider
	tools      []Tool
	toolMap    map[string]Tool
	sandboxCfg sandbox.Config
}

func NewAgent(provider Provider, tools []Tool, cfg sandbox.Config) *Agent {
	tm := make(map[string]Tool, len(tools))
	for _, t := range tools {
		tm[t.Name] = t
	}
	return &Agent{
		provider:   provider,
		tools:      tools,
		toolMap:    tm,
		sandboxCfg: cfg,
	}
}

// Run 执行 Agent 循环，返回 Token 流和最终消息历史。
// decisions 用于 HITL 确认决策。调用者必须 drain 到 tokenCh 关闭，然后读取 <-msgCh。
func (a *Agent) Run(ctx context.Context, history []Message, input string, decisions <-chan spec.HITLDecision) (<-chan Token, <-chan []Message, error) {
	messages := a.buildMessages(history, input)

	ch := make(chan Token)
	msgCh := make(chan []Message, 1)

	go func() {
		defer close(ch)
		defer func() { msgCh <- messages }()

		for iter := 0; iter < maxIterations; iter++ {
			req := &ChatRequest{
				Model:    a.provider.Model(),
				Messages: messages,
				Tools:    toolSpecs(a.tools),
			}

			resp, err := a.provider.Chat(ctx, req)
			if err != nil {
				sendToken(ctx, ch, Token{Type: TokenTypeText, Content: fmt.Sprintf("\n✘ agent error: %v", err), Done: true})
				return
			}
			if len(resp.Choices) == 0 {
				return
			}

			choice := resp.Choices[0]
			msg := choice.Message
			messages = append(messages, msg)

			// stream text
			if msg.Content != "" {
				sendToken(ctx, ch, Token{Type: TokenTypeText, Content: msg.Content})
			}

			// process tool calls
			if len(msg.ToolCalls) == 0 {
				break // final response
			}

			for _, tc := range msg.ToolCalls {
				if tc.Type != "function" {
					continue
				}

				tool, ok := a.toolMap[tc.Function.Name]
				if !ok {
					messages = append(messages, Message{
						Role: "tool", Content: fmt.Sprintf("unknown tool: %s", tc.Function.Name), ToolCallID: tc.ID,
					})
					continue
				}

				var args map[string]any
				if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
					messages = append(messages, Message{
						Role: "tool", Content: fmt.Sprintf("invalid args: %v", err), ToolCallID: tc.ID,
					})
					continue
				}

				// HITL 沙箱检查（仅对 bash 工具执行）
				if tool.Name == "bash" {
					switch a.handleSandbox(ctx, ch, decisions, &tc, args, &messages) {
					case sandboxSkip:
						continue
					case sandboxAbort:
						return
					}
					// sandboxExec: fall through
				}

				callStr := fmt.Sprintf("\n● %s(%s)", tool.Name, tc.Function.Arguments)
				sendToken(ctx, ch, Token{Type: TokenTypeToolCall, Content: callStr})

				toolCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				result, err := tool.Execute(toolCtx, args)
				cancel()
				if err != nil {
					result = fmt.Sprintf("error: %v", err)
				}

				resultPreview := result
				lines := strings.Split(result, "\n")
				if len(lines) > 4 {
					resultPreview = strings.Join(lines[:4], "\n") + "..."
				}
				if len(resultPreview) > 500 {
					resultPreview = resultPreview[:500] + "..."
				}
				resultStr := fmt.Sprintf("\n  ⎿  %s", strings.ReplaceAll(resultPreview, "\n", "\n  ⎿  "))
				sendToken(ctx, ch, Token{Type: TokenTypeToolResult, Content: resultStr})

				messages = append(messages, Message{
					Role: "tool", Content: result, ToolCallID: tc.ID,
				})
			}
		}

		sendToken(ctx, ch, Token{Type: TokenTypeText, Done: true})
	}()

	return ch, msgCh, nil
}

// handleSandbox 对 bash 命令执行沙箱检查并处理 HITL 确认交互。
// 返回 sandboxExec / sandboxSkip / sandboxAbort。
func (a *Agent) handleSandbox(
	ctx context.Context,
	ch chan<- Token,
	decisions <-chan spec.HITLDecision,
	tc *ToolCall,
	args map[string]any,
	messages *[]Message,
) int {
	cmdStr, _ := args["command"].(string)
	if cmdStr == "" {
		return sandboxExec
	}

	err := sandbox.Check(cmdStr, &a.sandboxCfg)
	if err == nil {
		return sandboxExec // 放行
	}

	// Denylist / Strict / ReadOnly 模式拒绝
	var needsConf *sandbox.NeedsConfirmationError
	if !errors.As(err, &needsConf) {
		sendToken(ctx, ch, Token{Type: TokenTypeToolCall, Content: fmt.Sprintf("\n⛔ %v", err)})
		*messages = append(*messages, Message{
			Role: "tool", Content: "⛔ " + err.Error(), ToolCallID: tc.ID,
		})
		return sandboxSkip
	}

	// ---- Normal 模式：触发 HITL interrupt ----

	req := &spec.InterruptRequest{
		ID:         fmt.Sprintf("req-%d", requestIDCounter.Add(1)),
		ToolName:   "bash",
		Command:    cmdStr,
		RiskReason: needsConf.Pattern,
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(30 * time.Second),
	}

	// 发送 interrupt token
	sendToken(ctx, ch, Token{Type: TokenTypeInterrupt, Interrupt: req})

	// 等待决策（阻塞 agent goroutine）
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		return sandboxAbort
	case <-timeout.C:
		sendToken(ctx, ch, Token{Type: TokenTypeText, Content: "\n⏰ confirmation timed out\n"})
		*messages = append(*messages, Message{
			Role: "tool", Content: "⏰ confirmation timed out", ToolCallID: tc.ID,
		})
		return sandboxSkip
	case d, ok := <-decisions:
		if !ok {
			return sandboxAbort
		}
		return a.applyDecision(ctx, d, ch, tc, args, messages, cmdStr)
	}
}

// applyDecision 根据用户决策处理后续动作。
func (a *Agent) applyDecision(
	ctx context.Context,
	d spec.HITLDecision,
	ch chan<- Token,
	tc *ToolCall,
	args map[string]any,
	messages *[]Message,
	originalCmd string,
) int {
	switch d.Type {
	case spec.DecisionApprove:
		return sandboxExec

	case spec.DecisionEdit:
		edited := strings.TrimSpace(d.Command)
		if edited == "" {
			edited = originalCmd
		}
		args["command"] = edited
		return sandboxExec

	case spec.DecisionReject:
		sendToken(ctx, ch, Token{Type: TokenTypeToolResult, Content: "\n  ⎿  ⛔ rejected by user"})
		*messages = append(*messages, Message{
			Role: "tool", Content: "⛔ rejected by user", ToolCallID: tc.ID,
		})
		return sandboxSkip

	case spec.DecisionRespond:
		msg := strings.TrimSpace(d.Message)
		if msg == "" {
			msg = "user declined to answer"
		}
		sendToken(ctx, ch, Token{Type: TokenTypeToolResult, Content: "\n  ⎿  💬 " + msg})
		*messages = append(*messages, Message{
			Role: "user", Content: msg,
		})
		return sandboxSkip

	default:
		return sandboxExec
	}
}

func (a *Agent) buildMessages(history []Message, input string) []Message {
	msgs := make([]Message, 0, len(history)+2)
	msgs = append(msgs, Message{Role: "system", Content: systemPrompt(a.tools)})
	msgs = append(msgs, history...)
	msgs = append(msgs, Message{Role: "user", Content: input})
	return msgs
}

func systemPrompt(tools []Tool) string {
	var b strings.Builder
	b.WriteString("You are a coding assistant with tool access.\n")
	b.WriteString("Use tools when you need to explore, run commands, or modify files.\n")
	b.WriteString("Think step by step. After getting tool results, continue reasoning.\n")
	b.WriteString("When you have enough info, provide a complete answer.\n\n")
	b.WriteString("Available tools:\n")
	for _, t := range tools {
		b.WriteString(fmt.Sprintf("- %s: %s\n", t.Name, t.Description))
	}
	b.WriteString("\nRespond naturally. Use tools when needed.")
	return b.String()
}

func toolSpecs(tools []Tool) []map[string]any {
	specs := make([]map[string]any, 0, len(tools))
	for _, t := range tools {
		specs = append(specs, t.ToolSpec())
	}
	return specs
}

func sendToken(ctx context.Context, ch chan<- Token, tok Token) {
	select {
	case ch <- tok:
	case <-ctx.Done():
	}
}
