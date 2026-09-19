package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
)

// WithMaxIterations 覆盖 ReAct 循环最大迭代数（0 或省略 = 默认 MaxInt，即无上限）。
func WithMaxIterations(n int) Option {
	return func(a *Agent) {
		if n > 0 {
			a.maxIterations = n
		}
	}
}

func NewAgent(provider core.Provider, systemPrompt string, tools []core.Tool, mws []core.Middleware, opts ...Option) *Agent {
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
		systemPrompt:  systemPrompt,
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
			messages = stampMessageIDs(messages)
			msgCh <- messages
		}()

		for iter := 0; iter < a.maxIterations; iter++ {
			bmEv := &core.BeforeModelEvent{Ctx: ctx, Iteration: iter, History: messages, Input: input}
			a.fireMiddlewareEvent(bmEv)
			// 压缩等 middleware 可能整体替换 History —— 发请求前回读，无替换时等价于原值
			messages = bmEv.History
			req := &core.ChatRequest{
				Model:    a.provider.Model(),
				Messages: messages,
				Tools:    a.tools,
			}

			resp, streamed, err := a.chat(ctx, req, ch)

			var usage *core.UsageInfo
			if resp != nil {
				usage = resp.Usage
			}
			// 用量统计由 usage middleware 观察 AfterModel 事件完成，Agent 核心不感知
			a.fireMiddlewareEvent(&core.AfterModelEvent{Ctx: ctx, Iteration: iter, History: messages, Request: req, Response: resp, Err: err, Usage: usage})

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
				// 流式路径的 thinking/text 增量已在 chat() 透传，不再整段重发
				if !streamed {
					emitMessageTokens(ctx, ch, msg)
				}
				finished = true
				break
			}
			messages = append(messages, msg)
			if !streamed {
				emitMessageTokens(ctx, ch, msg)
			}

			for _, tc := range msg.ToolCalls {
				emitToken(ctx, ch, core.Token{
					Type:     core.TokenTypeToolCall,
					Content:  fmt.Sprintf("%s(%s)", tc.Function.Name, tc.Function.Arguments),
					ToolCall: tc,
				})

				result := ""
				tool, known := a.toolMap[tc.Function.Name]
				switch {
				case tc.Type != "function":
					result = fmt.Sprintf("unsupported tool call type: %s", tc.Type)
				case !known:
					result = fmt.Sprintf("unknown tool: %s", tc.Function.Name)
				default:
					var args map[string]any
					if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
						result = fmt.Sprintf("invalid args: %v", err)
					} else {
						btEv := &core.BeforeToolEvent{Ctx: ctx, Iteration: iter, History: messages, Tool: &tc, Emit: tokenEmitter{ch: ch}, Args: args}
						a.fireMiddlewareEvent(btEv)
						if btEv.Abort != nil {
							result = abortResult(btEv.Abort, tc.ID)
						} else {
							toolCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
							var execErr error
							result, execErr = tool.Execute(toolCtx, args)
							cancel()
							if execErr != nil {
								result = fmt.Sprintf("error: %v", execErr)
							}
							a.fireMiddlewareEvent(&core.AfterToolEvent{Ctx: ctx, Iteration: iter, History: messages, Tool: &tc, Args: args, Result: result, Err: execErr})
						}
					}
				}

				emitToken(ctx, ch, core.Token{Type: core.TokenTypeToolResult, Content: result, ToolCallID: tc.ID})
				messages = append(messages, core.Message{Role: "tool", Content: result, ToolCallID: tc.ID})
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

// chat 发起一次模型调用：优先走流式 —— thinking/text 增量即时透传给前端，
// tool_calls 分片由 provider 在流结束时重组为完整响应；provider 不支持流式
// （返回错误，如 anthropic 当前桩实现）则回退非流式。流式中断（ctx 取消）或
// 未收到重组响应时向上报错，由调用方走 agent error 通道。
// 返回的 streamed 表示响应内容是否已随流式增量透传（调用方据此跳过整段重发）。
func (a *Agent) chat(ctx context.Context, req *core.ChatRequest, ch chan<- core.Token) (*core.ChatResponse, bool, error) {
	stream, err := a.provider.ChatStream(ctx, req)
	if err != nil || stream == nil {
		// 不支持流式（含假后端/桩实现直接返回 nil channel）→ 回退非流式
		resp, err := a.provider.Chat(ctx, req)
		return resp, false, err
	}

	var final *core.ChatResponse
	for tok := range stream {
		if tok.Response != nil {
			final = tok.Response
			continue
		}
		// 剥掉增量上的 done 标记：provider 层的 done 表示"本轮模型响应结束"，
		// 与 session 层"Done 即整个运行结束"的语义冲突；done 只由 Run 收尾发出
		tok.Done = false
		sendToken(ctx, ch, tok)
	}
	if final != nil {
		return final, true, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	return nil, false, fmt.Errorf("stream ended without a complete response")
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
	sp := a.systemPrompt
	msgs = append(msgs, core.Message{Role: "system", Content: sp})
	msgs = append(msgs, history...)
	msgs = append(msgs, core.Message{Role: "user", Content: input})
	return msgs
}

type tokenEmitter struct {
	ch chan<- core.Token
}

func (e tokenEmitter) Emit(ctx context.Context, tok core.Token) {
	sendToken(ctx, e.ch, tok)
}

func emitMessageTokens(ctx context.Context, ch chan<- core.Token, message core.Message) {
	if message.Thinking.Text != "" {
		sendToken(ctx, ch, core.Token{Type: core.TokenTypeThinking, Content: message.Thinking.Text})
	}
	if message.Content != "" {
		sendToken(ctx, ch, core.Token{Type: core.TokenTypeText, Content: message.Content})
	}
}

func abortResult(abort *core.ToolAbort, toolCallID string) string {
	for _, message := range abort.Messages {
		if message.Role == "tool" && message.ToolCallID == toolCallID {
			return message.Content
		}
	}
	return "tool execution aborted"
}

// emitToken 发送工具生命周期 token（tool_call/tool_result），带取消保护。
// 语义边界：ctx 取消 ≠ 消费方弃读 —— 正常取消路径下消费方仍在 range tokenCh，
// 配对的 call/result 应继续送达（宽限窗口内阻塞发送必然成功）；消费方提前
// 弃读时宽限超时放弃，Run 不会被永久卡死。历史侧的 tool 消息配对由主循环
// 回填保证，不依赖 token 是否送达。
func emitToken(ctx context.Context, ch chan<- core.Token, tok core.Token) {
	select {
	case ch <- tok:
	case <-ctx.Done():
		select {
		case ch <- tok:
		case <-time.After(2 * time.Second):
		}
	}
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
