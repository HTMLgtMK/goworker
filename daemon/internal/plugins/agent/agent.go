package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const maxIterations = 15

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
func DefaultTools() []Tool {
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
	provider Provider
	tools    []Tool
	toolMap  map[string]Tool
}

func NewAgent(provider Provider, tools []Tool) *Agent {
	tm := make(map[string]Tool, len(tools))
	for _, t := range tools {
		tm[t.Name] = t
	}
	return &Agent{
		provider: provider,
		tools:    tools,
		toolMap:  tm,
	}
}

// Run 执行 Agent 循环，返回 Token 流。调用者必须 drain 到 channel 关闭。
func (a *Agent) Run(ctx context.Context, history []Message, input string) (<-chan Token, error) {
	messages := a.buildMessages(history, input)

	ch := make(chan Token)
	go func() {
		defer close(ch)

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

	return ch, nil
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
