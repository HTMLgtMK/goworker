package agent

import (
	"context"
	"errors"

	"github.com/tinguo/goworker/ai-core/core"
)

// captureProvider 记录最后一次请求并返回最终答复。
type captureProvider struct {
	lastReq *core.ChatRequest
}

func (p *captureProvider) Name() string  { return "capture" }
func (p *captureProvider) Model() string { return "m" }
func (p *captureProvider) Chat(_ context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	p.lastReq = req
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "final"}}}}, nil
}
func (p *captureProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}
