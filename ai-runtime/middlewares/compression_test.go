package middlewares

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

// compressStubProvider 记录请求并返回固定内容，供 CompressionMiddleware 测试。
type compressStubProvider struct {
	called  bool
	lastReq *core.ChatRequest
	resp    string
	err     error
}

func (p *compressStubProvider) Name() string  { return "stub" }
func (p *compressStubProvider) Model() string { return "stub-model" }
func (p *compressStubProvider) Chat(_ context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	p.called = true
	p.lastReq = req
	if p.err != nil {
		return nil, p.err
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: p.resp}}}}, nil
}
func (p *compressStubProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// bigHistory 造一条估算值能超过阈值的消息历史。
func bigHistory() []core.Message {
	var msgs []core.Message
	for i := 0; i < 5; i++ {
		msgs = append(msgs, core.Message{Role: "user", Content: strings.Repeat("payload ", 20)})
	}
	return msgs
}

func TestCompressionMiddleware_CompressesAboveThreshold(t *testing.T) {
	stub := &compressStubProvider{resp: "SUMMARY"}
	mw := NewCompressionMiddleware(NewCompressor(stub, 2, false), 100, 0.8) // 阈值 80

	orig := bigHistory()
	ev := &core.BeforeModelEvent{Ctx: context.Background(), History: orig}
	mw.OnBeforeModel(ev)

	if !stub.called {
		t.Fatal("provider should be called when over threshold")
	}
	if len(ev.History) >= len(orig) {
		t.Errorf("history not shrunk: %d → %d", len(orig), len(ev.History))
	}
	// 首位应是摘要（system 角色），说明 History 已被压缩结果替换
	if ev.History[0].Role != "system" {
		t.Errorf("ev.History[0] = %+v, want summary message", ev.History[0])
	}
}

func TestCompressionMiddleware_SkipsBelowThreshold(t *testing.T) {
	stub := &compressStubProvider{}
	mw := NewCompressionMiddleware(NewCompressor(stub, 2, false), 100, 0.8)

	small := []core.Message{{Role: "user", Content: "hi"}}
	ev := &core.BeforeModelEvent{Ctx: context.Background(), History: small}
	mw.OnBeforeModel(ev)

	if stub.called {
		t.Error("provider should not be called when below threshold")
	}
	if len(ev.History) != 1 || ev.History[0].Content != "hi" {
		t.Error("History should stay untouched below threshold")
	}
}

func TestCompressionMiddleware_DisabledWithoutWindow(t *testing.T) {
	stub := &compressStubProvider{}
	mw := NewCompressionMiddleware(NewCompressor(stub, 2, false), 0, 0.8) // 没配窗口

	orig := bigHistory()
	ev := &core.BeforeModelEvent{Ctx: context.Background(), History: orig}
	mw.OnBeforeModel(ev)

	if stub.called {
		t.Error("should not compress when context window is 0")
	}
	if len(ev.History) != len(orig) {
		t.Error("History should stay untouched when disabled")
	}
}

func TestCompressionMiddleware_StopsAfterNoRelief(t *testing.T) {
	// 压缩后仍超阈值（keepLast 尾巴本身太大）→ 置 done，后续迭代不再重复压缩，防风暴
	stub := &compressStubProvider{resp: "SUMMARY"}
	mw := NewCompressionMiddleware(NewCompressor(stub, 2, false), 100, 0.8)

	orig := bigHistory()
	ev := &core.BeforeModelEvent{Ctx: context.Background(), History: orig}
	mw.OnBeforeModel(ev)
	if !stub.called {
		t.Fatal("first call should compress when over threshold")
	}
	// 压缩后 [SUMMARY + 最近 2 条大消息] 估算仍 >= 80 → done 置位
	if !mw.done {
		t.Fatal("done should be set when compression does not relieve the estimate")
	}

	// 下一轮迭代用压缩后的历史再来一次：必须不再压缩
	stub.called = false
	ev2 := &core.BeforeModelEvent{Ctx: context.Background(), History: ev.History}
	mw.OnBeforeModel(ev2)
	if stub.called {
		t.Error("should not re-compress after no relief (done guard)")
	}
}

func TestCompressionMiddleware_DegradesOnCompressError(t *testing.T) {
	stub := &compressStubProvider{err: errors.New("api down")}
	mw := NewCompressionMiddleware(NewCompressor(stub, 2, false), 100, 0.8)

	orig := bigHistory()
	ev := &core.BeforeModelEvent{Ctx: context.Background(), History: orig}
	mw.OnBeforeModel(ev)

	// 压缩失败不打断会话：原历史继续发
	if len(ev.History) != len(orig) {
		t.Error("History should stay original when compression fails")
	}
}
