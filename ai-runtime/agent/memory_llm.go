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

// toMemoryMessages keeps only plain user/assistant/system text. Tool messages and
// tool_calls are dropped as noise and because incomplete call structures fail strict
// provider validation.
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
