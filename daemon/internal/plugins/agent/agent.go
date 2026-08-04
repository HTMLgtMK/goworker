package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/sandbox"
)

const defaultMaxIterations = 15

// DefaultTools 返回 Agent 的默认工具集。
// cfg 为沙箱配置，nil 表示不启用沙箱。
func DefaultTools(cfg *sandbox.Config) []core.Tool {
	return []core.Tool{
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
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				// context 取消时 kill 整个进程组（含管道子进程），
				// exec.CommandContext 只 kill bash 本身，子进程变孤儿可能阻塞 CombinedOutput
				go func() {
					<-ctx.Done()
					if cmd.Process != nil {
						syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					}
				}()
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
				// os.ReadFile 是同步 syscall，不认 ctx —— goroutine + select 包装让取消
				// 能提前返回。极端（如 NFS 永久挂起）泄漏一个 goroutine，但比整个 ReAct
				// 循环无限挂住可接受。
				type readRes struct {
					data []byte
					err  error
				}
				resCh := make(chan readRes, 1)
				go func() {
					data, err := os.ReadFile(path)
					resCh <- readRes{data, err}
				}()
				var data []byte
				var err error
				select {
				case r := <-resCh:
					data, err = r.data, r.err
				case <-ctx.Done():
					return "", fmt.Errorf("read_file: %w", ctx.Err())
				}
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
				// os.WriteFile 是同步 syscall，不认 ctx —— goroutine + select 包装让取消可提前返回
				type writeRes struct{ err error }
				resCh := make(chan writeRes, 1)
				go func() {
					resCh <- writeRes{os.WriteFile(path, []byte(content), 0644)}
				}()
				select {
				case r := <-resCh:
					if r.err != nil {
						return "", fmt.Errorf("write_file: %w", r.err)
					}
				case <-ctx.Done():
					return "", fmt.Errorf("write_file: %w", ctx.Err())
				}
				return fmt.Sprintf("wrote %d bytes to %s", len(content), path), nil
			},
		},
	}
}

// ---- Agent (ReAct 循环) ----

// Agent 是一个可使用工具的 ReAct Agent。
type Agent struct {
	provider      core.Provider
	tools         []core.Tool
	toolMap       map[string]core.Tool
	middlewares   []core.Middleware
	maxIterations int

	OnIteration func() // 可选：每次 ReAct 循环前调用，用于 UI 反馈（status bar 迭代计数）
}

// Option 可配置 Agent 的可选行为。
type Option func(*Agent)

// WithMaxIterations 覆盖 ReAct 循环最大迭代数（0 或省略 = 默认 15）。
func WithMaxIterations(n int) Option {
	return func(a *Agent) {
		if n > 0 {
			a.maxIterations = n
		}
	}
}

func NewAgent(provider core.Provider, tools []core.Tool, mws []core.Middleware, opts ...Option) *Agent {
	tm := make(map[string]core.Tool, len(tools))
	for _, t := range tools {
		tm[t.Name] = t
	}
	a := &Agent{
		provider:      provider,
		tools:         tools,
		toolMap:       tm,
		middlewares:   mws,
		maxIterations: defaultMaxIterations,
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Run 执行 Agent 循环，返回 Token 流和最终消息历史。
// 调用者必须 drain 到 tokenCh 关闭，然后读取 <-msgCh。
// HITL 决策通过 middleware 的 DecisionProvider 处理，不在 Run 参数中传递。
func (a *Agent) Run(ctx context.Context, history []core.Message, input string) (<-chan core.Token, <-chan []core.Message, error) {
	a.fireMiddlewareEvent(&core.BeforeAgentEvent{Ctx: ctx, Input: input})
	messages := a.buildMessages(history, input)

	ch := make(chan core.Token)
	msgCh := make(chan []core.Message, 1)

	go func() {
		var runErr error
		finished := false
		defer close(ch)
		defer func() {
			a.fireMiddlewareEvent(&core.AfterAgentEvent{Ctx: ctx, History: messages, Err: runErr})
			msgCh <- messages
		}()

		for iter := 0; iter < a.maxIterations; iter++ {
			if a.OnIteration != nil {
				a.OnIteration()
			}
			bmEv := &core.BeforeModelEvent{Ctx: ctx, History: messages, Input: input}
			a.fireMiddlewareEvent(bmEv)
			// 压缩等 middleware 可能整体替换 History —— 发请求前回读，无替换时等价于原值
			messages = bmEv.History
			req := &core.ChatRequest{
				Model:    a.provider.Model(),
				Messages: messages,
				Tools:    toolSpecs(a.tools),
			}

			resp, err := a.provider.Chat(ctx, req)

			var usage *core.UsageInfo
			if resp != nil {
				usage = resp.Usage
			}
			// 用量统计由 usage middleware 观察 AfterModel 事件完成，Agent 核心不感知
			a.fireMiddlewareEvent(&core.AfterModelEvent{Ctx: ctx, History: messages, Err: err, Usage: usage})

			if err != nil {
				runErr = err
				messages = emitAgentNote(ch, messages, "⚠ [harness] ✘ agent error: "+err.Error(), true)
				return
			}
			if resp == nil {
				// 防御坏后端：err==nil 却返回 nil resp，解引用前拦下（goroutine 无 recover）
				runErr = fmt.Errorf("provider returned nil response")
				messages = emitAgentNote(ch, messages, "⚠ [harness] ✘ agent error: "+runErr.Error(), true)
				return
			}
			if len(resp.Choices) == 0 {
				runErr = fmt.Errorf("model returned empty response (backend unhealthy or malformed history)")
				messages = emitAgentNote(ch, messages, "⚠ [harness] ✘ agent error: "+runErr.Error(), true)
				return
			}

			choice := resp.Choices[0]
			msg := choice.Message

			// process tool calls — before text emit so we know which type
			if len(msg.ToolCalls) == 0 {
				// final response — will be markdown-rendered by frontend
				if msg.Content == "" {
					// 空 content + 无 tool_calls：畸形/异常响应（如 content_filter），
					// 别当正常 final 静默结束 —— 与 choices==0 同根因的另一条入口
					runErr = fmt.Errorf("model returned empty content without tool calls")
					messages = emitAgentNote(ch, messages, "⚠ [harness] ✘ agent error: "+runErr.Error(), true)
					return
				}
				messages = append(messages, msg)
				sendToken(ctx, ch, core.Token{Type: core.TokenTypeText, Content: msg.Content})
				finished = true
				break
			}
			messages = append(messages, msg)

			// intermediate thinking text
			if msg.Content != "" {
				sendToken(ctx, ch, core.Token{Type: core.TokenTypeText, Content: msg.Content})
			}

			for _, tc := range msg.ToolCalls {
				if tc.Type != "function" {
					// 非 function 类型也要回填 tool 响应，保持 assistant tool_call 配对完整 ——
					// 缺配对下轮被 OpenAI 兼容后端以 400/空 choices 拒，正是"莫名停止"诱因
					messages = append(messages, core.Message{
						Role: "tool", Content: fmt.Sprintf("unsupported tool call type: %s", tc.Type), ToolCallID: tc.ID,
					})
					continue
				}

				tool, ok := a.toolMap[tc.Function.Name]
				if !ok {
					messages = append(messages, core.Message{
						Role: "tool", Content: fmt.Sprintf("unknown tool: %s", tc.Function.Name), ToolCallID: tc.ID,
					})
					continue
				}

				var args map[string]any
				if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
					messages = append(messages, core.Message{
						Role: "tool", Content: fmt.Sprintf("invalid args: %v", err), ToolCallID: tc.ID,
					})
					continue
				}

				// BeforeTool — middleware 可修改 args 或设置 Aborted
				btEv := &core.BeforeToolEvent{
					Ctx: ctx, History: messages, Tool: &tc,
					TokenCh: ch,
					Args:    args,
				}
				a.fireMiddlewareEvent(btEv)
				if btEv.Aborted {
					messages = append(messages, btEv.ResponseMessages...)
					continue
				}

				sendToken(ctx, ch, core.Token{Type: core.TokenTypeToolCall, Content: fmt.Sprintf("%s(%s)", tool.Name, tc.Function.Arguments)})

				toolCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				result, execErr := tool.Execute(toolCtx, args)
				cancel()
				if execErr != nil {
					result = fmt.Sprintf("error: %v", execErr)
				}

				a.fireMiddlewareEvent(&core.AfterToolEvent{
					Ctx: ctx, History: messages, Tool: &tc, Err: execErr,
				})

				sendToken(ctx, ch, core.Token{Type: core.TokenTypeToolResult, Content: result})

				messages = append(messages, core.Message{
					Role: "tool", Content: result, ToolCallID: tc.ID,
				})
			}
		}

		if !finished {
			// 15 轮迭代耗尽但模型仍在调工具：明确提示 + 把"未完成"状态写回历史，
			// 用户知道不是卡死，下一轮对话也能接着干。必须强发 —— sendToken 在会话
			// 取消时会把提示吞掉，而耗尽场景正是取消的高发窗口。
			exhaustedMsg := fmt.Sprintf("⚠ [harness] 已达最大迭代次数 (%d)，未能得到最终答复。可 /compact 后继续，或把任务拆小。", a.maxIterations)
			messages = emitAgentNote(ch, messages, exhaustedMsg, false)
		}

		sendToken(ctx, ch, core.Token{Type: core.TokenTypeText, Done: true})
	}()

	return ch, msgCh, nil
}

// fireMiddlewareEvent 统一分发 middleware 事件到各 hook 接口。
// event 必须是 *core.{Before,After}{Agent,Model,Tool}Event 的指针。
func (a *Agent) fireMiddlewareEvent(event any) {
	for _, mw := range a.middlewares {
		switch ev := event.(type) {
		case *core.BeforeAgentEvent:
			if m, ok := mw.(core.BeforeAgent); ok {
				m.OnBeforeAgent(ev)
			}
		case *core.AfterAgentEvent:
			if m, ok := mw.(core.AfterAgent); ok {
				m.OnAfterAgent(ev)
			}
		case *core.BeforeModelEvent:
			if m, ok := mw.(core.BeforeModel); ok {
				m.OnBeforeModel(ev)
			}
		case *core.AfterModelEvent:
			if m, ok := mw.(core.AfterModel); ok {
				m.OnAfterModel(ev)
			}
		case *core.BeforeToolEvent:
			if m, ok := mw.(core.BeforeTool); ok {
				m.OnBeforeTool(ev)
			}
		case *core.AfterToolEvent:
			if m, ok := mw.(core.AfterTool); ok {
				m.OnAfterTool(ev)
			}
		}
	}
}

func (a *Agent) buildMessages(history []core.Message, input string) []core.Message {
	msgs := make([]core.Message, 0, len(history)+2)
	msgs = append(msgs, core.Message{Role: "system", Content: systemPrompt(a.tools)})
	msgs = append(msgs, history...)
	msgs = append(msgs, core.Message{Role: "user", Content: input})
	return msgs
}

func systemPrompt(tools []core.Tool) string {
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

func toolSpecs(tools []core.Tool) []map[string]any {
	specs := make([]map[string]any, 0, len(tools))
	for _, t := range tools {
		specs = append(specs, t.ToolSpec())
	}
	return specs
}

func sendToken(ctx context.Context, ch chan<- core.Token, tok core.Token) {
	select {
	case ch <- tok:
	case <-ctx.Done():
	}
}

// emitAgentNote 把一条运行期消息可靠送达调用方并写进历史。
// 不能用 sendToken：agentCtx 取消时它会走 <-ctx.Done() 分支把最该看到的信息吞掉。
// 调用方此刻正在 range tokenCh，channel 关闭前不会退出，阻塞发送最终必然成功
// （即使它暂时在渲染/HITL 决策中，完成后仍会回来接收）；不要用 default 静默丢弃，
// 错误恰恰是最该送达的。done 为 true 时该 token 携带终态标记。
// content 带 [harness] 前缀，明确这是系统注入的诊断而非模型自己的输出，避免污染
// 下轮喂回模型的历史。返回 append 后的历史，信息在下一轮对话里可见。
func emitAgentNote(ch chan<- core.Token, messages []core.Message, content string, done bool) []core.Message {
	ch <- core.Token{Type: core.TokenTypeText, Content: "\n" + content, Done: done}
	return append(messages, core.Message{Role: "assistant", Content: content})
}
