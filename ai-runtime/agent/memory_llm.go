package agent

import (
	"context"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-memory"
)

// llmAdapter 把 core.Provider 适配成本地 memory.LLM 接口。
// memory 模块零外部依赖，固化检查点（Checkpointer）通过这个最小接口发起 LLM 调用。
type llmAdapter struct {
	inner core.Provider
}

func (a *llmAdapter) Model() string { return a.inner.Model() }

func (a *llmAdapter) Chat(ctx context.Context, req *memory.ChatRequest) (*memory.ChatResponse, error) {
	coreReq := &core.ChatRequest{
		Model:    req.Model,
		Messages: toCoreMessages(req.Messages),
		JSONMode: req.JSONMode,
	}
	resp, err := a.inner.Chat(ctx, coreReq)
	if err != nil {
		return nil, err
	}
	out := &memory.ChatResponse{Choices: make([]memory.ResponseChoice, 0, len(resp.Choices))}
	for _, choice := range resp.Choices {
		out.Choices = append(out.Choices, memory.ResponseChoice{
			Message: memory.Message{Role: choice.Message.Role, Content: choice.Message.Content},
		})
	}
	return out, nil
}

// toCoreMessages 把 memory.Message 转成 core.Message（只带 role/content）。
func toCoreMessages(messages []memory.Message) []core.Message {
	out := make([]core.Message, 0, len(messages))
	for _, message := range messages {
		out = append(out, core.Message{Role: message.Role, Content: message.Content})
	}
	return out
}

// toMemoryMessages 把 core.Message 转成 memory.Message，只保留纯文本的
// user/assistant/system 消息。
//
// 过滤 tool 消息与带 tool_calls 的 assistant 消息，两个理由：
//  1. 固化是提炼任务/事实，tool 原始输出与工具调用声明是噪音，纯文本对话
//     （user 意图 + assistant 行动/结论）信息量足够，还省 token；
//  2. 残缺的 tool 结构（memory.Message 不携带 tool_call_id）会踩 OpenAI 的
//     严格校验 —— 400 "missing field tool_call_id"。主动过滤掉比补全字段干净。
func toMemoryMessages(messages []core.Message) []memory.Message {
	out := make([]memory.Message, 0, len(messages))
	for _, message := range messages {
		if message.Role == "tool" || len(message.ToolCalls) > 0 {
			continue
		}
		out = append(out, memory.Message{Role: message.Role, Content: message.Content})
	}
	return out
}
