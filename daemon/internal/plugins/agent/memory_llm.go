package agent

import (
	"context"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/memory"
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
	}
	if req.JSONMode {
		// DeepSeek/OpenAI 兼容的 JSON 模式：配合 system 提示词里的 JSON 指示，
		// 保证固化输出是合法 JSON，而不是靠解析器事后擦屁股。
		coreReq.ResponseFormat = map[string]any{"type": "json_object"}
	}
	resp, err := a.inner.Chat(ctx, coreReq)
	if err != nil {
		return nil, err
	}
	out := &memory.ChatResponse{Choices: make([]memory.ResponseChoice, 0, len(resp.Choices))}
	for _, c := range resp.Choices {
		out.Choices = append(out.Choices, memory.ResponseChoice{
			Message: memory.Message{Role: c.Message.Role, Content: c.Message.Content},
		})
	}
	return out, nil
}

// toCoreMessages 把 memory.Message 转成 core.Message（只带 role/content）。
func toCoreMessages(msgs []memory.Message) []core.Message {
	out := make([]core.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, core.Message{Role: m.Role, Content: m.Content})
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
func toMemoryMessages(msgs []core.Message) []memory.Message {
	out := make([]memory.Message, 0, len(msgs))
	for _, m := range msgs {
		if m.Role == "tool" || len(m.ToolCalls) > 0 {
			continue
		}
		out = append(out, memory.Message{Role: m.Role, Content: m.Content})
	}
	return out
}
