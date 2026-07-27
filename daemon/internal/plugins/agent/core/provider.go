package core

import "context"

// Provider 是 LLM 后端的接口。
type Provider interface {
	Name() string
	Model() string
	Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error)
	ChatStream(ctx context.Context, req *ChatRequest) (<-chan Token, error)
}
