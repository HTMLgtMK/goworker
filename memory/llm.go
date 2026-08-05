package memory

import "context"

// Message 是检查点对话的最小消息视图 —— 只有 role/content 参与固化。
// 不引用任何外部消息类型，保证模块零依赖。
type Message struct {
	Role    string
	Content string
}

// ChatRequest 是 LLM 非流式请求的最小面。Checkpointer 只需要 model + messages。
type ChatRequest struct {
	Model    string
	Messages []Message
	JSONMode bool // 要求后端以 JSON 模式输出（response_format=json_object），仅检查点使用
}

// ChatResponse 是 LLM 非流式响应的最小面。固化只消费 choices[0].message.content。
type ChatResponse struct {
	Choices []ResponseChoice
}

// ResponseChoice 是 ChatResponse 中的一个候选回答。
type ResponseChoice struct {
	Message Message
}

// LLM 是 Checkpointer 需要的模型面（consumer 定义的最小接口）。
// daemon 侧用适配器包装 core.Provider 注入；调用方自己保证 LLM 后端语义。
type LLM interface {
	Model() string
	Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error)
}
