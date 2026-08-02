package agent

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
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

// replaceMW 模拟压缩中间件：整体替换即将发送的历史。
type replaceMW struct{}

func (replaceMW) Name() string { return "test-replace" }
func (replaceMW) OnBeforeModel(ev *core.BeforeModelEvent) *core.MiddlewareResponse {
	ev.History = []core.Message{{Role: "user", Content: "compacted"}}
	return nil
}

func TestAgent_SendsReplacedHistoryToChat(t *testing.T) {
	provider := &captureProvider{}
	a := NewAgent(provider, nil, []core.Middleware{replaceMW{}})

	tokenCh, msgCh, err := a.Run(context.Background(), nil, "hello")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}

	for tok := range tokenCh {
		if tok.Done {
			break
		}
	}

	// 发给模型的请求应使用替换后的历史，而不是原始 input
	if provider.lastReq == nil {
		t.Fatal("Chat not called")
	}
	want := []core.Message{{Role: "user", Content: "compacted"}}
	if !reflect.DeepEqual(provider.lastReq.Messages, want) {
		t.Errorf("request messages = %+v, want %+v", provider.lastReq.Messages, want)
	}

	<-msgCh
}
