package agent

import (
	"context"
	"errors"

	"github.com/tinguo/goworker/ai-core/core"
)

// captureProvider 记录最后一次请求并返回最终答复;chatErr 非 nil 时 Chat 直接
// 返回该错误(P2-1 取消态归一测试的错误注入点,普通错误与 ctx 错误均可注入)。
type captureProvider struct {
	lastReq  *core.ChatRequest
	response core.Message
	chatErr  error
}

func (p *captureProvider) Name() string  { return "capture" }
func (p *captureProvider) Model() string { return "m" }
func (p *captureProvider) Chat(_ context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	p.lastReq = req
	if p.chatErr != nil {
		return nil, p.chatErr
	}
	message := p.response
	if message.Role == "" {
		message = core.Message{Role: "assistant", Content: "final"}
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: message}}}, nil
}
func (p *captureProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}
