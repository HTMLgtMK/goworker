package agent

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

type iterationSpy struct {
	fired *int
}

func (m iterationSpy) Name() string { return "iteration-spy" }

func (m iterationSpy) OnBeforeModel(*core.BeforeModelEvent) *core.MiddlewareResponse {
	(*m.fired)++
	return nil
}

type beforeToolIterationSpy struct {
	iterations *[]int
}

func (m beforeToolIterationSpy) Name() string { return "before-tool-iteration-spy" }

func (m beforeToolIterationSpy) OnBeforeTool(ev *core.BeforeToolEvent) *core.MiddlewareResponse {
	*m.iterations = append(*m.iterations, ev.Iteration)
	return nil
}

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

// errProvider 每次调用都返回错误。
type errProvider struct{}

func (errProvider) Name() string  { return "err" }
func (errProvider) Model() string { return "m" }
func (errProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("boom")
}
func (errProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// emptyProvider 返回空 choices —— 某些兼容后端在畸形历史时的表现。
type emptyProvider struct{}

func (emptyProvider) Name() string  { return "empty" }
func (emptyProvider) Model() string { return "m" }
func (emptyProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{}, nil
}
func (emptyProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// emptyContentProvider 返回单条 choice 但 content 为空、无 tool_calls。
type emptyContentProvider struct{}

func (emptyContentProvider) Name() string  { return "emptycontent" }
func (emptyContentProvider) Model() string { return "m" }
func (emptyContentProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{Choices: []core.ResponseChoice{{
		Message: core.Message{Role: "assistant", Content: ""},
	}}}, nil
}
func (emptyContentProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// nilRespProvider 返回 (nil, nil) —— 坏后端的极端形态。
type nilRespProvider struct{}

func (nilRespProvider) Name() string  { return "nilresp" }
func (nilRespProvider) Model() string { return "m" }
func (nilRespProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return nil, nil
}
func (nilRespProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// toolLoopProvider 前 count 次调用都返回 bash tool call，之后返回 final。
type toolLoopProvider struct {
	count int
	calls int
}

func (p *toolLoopProvider) Name() string  { return "loop" }
func (p *toolLoopProvider) Model() string { return "m" }
func (p *toolLoopProvider) Chat(_ context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	if p.calls < p.count {
		p.calls++
		return &core.ChatResponse{Choices: []core.ResponseChoice{{
			Message: core.Message{
				Role: "assistant",
				ToolCalls: []core.ToolCall{{
					ID: "c1", Type: "function",
					Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"echo hi"}`},
				}},
			},
		}}}, nil
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{
		Message: core.Message{Role: "assistant", Content: "final"},
	}}}, nil
}
func (p *toolLoopProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

type thinkingProvider struct {
	messages []core.Message
	calls    int
}

func (p *thinkingProvider) Name() string  { return "thinking" }
func (p *thinkingProvider) Model() string { return "m" }
func (p *thinkingProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	message := p.messages[p.calls]
	p.calls++
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: message}}}, nil
}
func (p *thinkingProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

func TestAgent_EmitsThinkingBeforeFinalContent(t *testing.T) {
	provider := &thinkingProvider{messages: []core.Message{{
		Role:     "assistant",
		Content:  "answer",
		Thinking: core.Thinking{Text: "reasoning"},
	}}}
	a := NewAgent(provider, "", nil, nil)

	tokenCh, msgCh, err := a.Run(context.Background(), nil, "question")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got []core.Token
	for token := range tokenCh {
		if !token.Done {
			got = append(got, token)
		}
	}
	want := []core.Token{
		{Type: core.TokenTypeThinking, Content: "reasoning"},
		{Type: core.TokenTypeText, Content: "answer"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tokens = %#v, want %#v", got, want)
	}
	messages := <-msgCh
	if gotThinking := messages[len(messages)-1].Thinking.Text; gotThinking != "reasoning" {
		t.Errorf("history thinking = %q, want reasoning", gotThinking)
	}
}

func TestAgent_EmitsThinkingBeforeToolCallAndFinalContent(t *testing.T) {
	provider := &thinkingProvider{messages: []core.Message{
		{
			Role:     "assistant",
			Content:  "using tool",
			Thinking: core.Thinking{Text: "tool reasoning"},
			ToolCalls: []core.ToolCall{{
				ID:   "c1",
				Type: "function",
				Function: core.ToolCallFunction{
					Name:      "bash",
					Arguments: `{"command":"echo hi"}`,
				},
			}},
		},
		{Role: "assistant", Content: "done", Thinking: core.Thinking{Text: "final reasoning"}},
	}}
	a := NewAgent(provider, "", testTools(), nil)

	tokenCh, msgCh, err := a.Run(context.Background(), nil, "question")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var gotTypes []string
	for token := range tokenCh {
		if !token.Done {
			gotTypes = append(gotTypes, token.Type)
		}
	}
	wantTypes := []string{
		core.TokenTypeThinking,
		core.TokenTypeText,
		core.TokenTypeToolCall,
		core.TokenTypeToolResult,
		core.TokenTypeThinking,
		core.TokenTypeText,
	}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Errorf("token types = %v, want %v", gotTypes, wantTypes)
	}
	<-msgCh
}

func TestAgent_CustomMaxIterationsRespected(t *testing.T) {
	a := NewAgent(&toolLoopProvider{count: 999}, "", testTools(), nil, WithMaxIterations(3))
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	var toolCalls int
	done := false
	for tok := range tokenCh {
		if tok.Type == core.TokenTypeToolCall {
			toolCalls++
		}
		if tok.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("token channel must end with Done")
	}
	// 3 轮耗尽：只跑 3 个 tool call，而不是默认 15
	if toolCalls != 3 {
		t.Errorf("tool calls = %d, want 3 (custom max iterations)", toolCalls)
	}
	msgs := <-msgCh
	// system + user + 3×(assistant tool_call + tool result) + assistant(unfinished)
	if want := 3 + 2*3; len(msgs) != want {
		t.Errorf("history msgs = %d, want %d", len(msgs), want)
	}
}

func TestAgent_IterationMiddlewareFiresPerIteration(t *testing.T) {
	fired := 0
	iterMw := iterationSpy{fired: &fired}
	// count=2：两轮 tool call + 一轮 final = 3 次迭代，每轮 OnBeforeModel fire 一次
	a := NewAgent(&toolLoopProvider{count: 2}, "", testTools(), []core.Middleware{iterMw})
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	for tok := range tokenCh {
		if tok.Done {
			break
		}
	}
	if fired != 3 {
		t.Errorf("iteration events = %d, want 3", fired)
	}
	<-msgCh
}

func TestAgent_BeforeToolEventCarriesIteration(t *testing.T) {
	iterations := []int{}
	mw := beforeToolIterationSpy{iterations: &iterations}
	// count=2：两轮 tool call，BeforeTool 应分别看到 iter 0 和 1。
	a := NewAgent(&toolLoopProvider{count: 2}, "", testTools(), []core.Middleware{mw})
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	for tok := range tokenCh {
		if tok.Done {
			break
		}
	}
	if !reflect.DeepEqual(iterations, []int{0, 1}) {
		t.Errorf("BeforeTool iterations = %v, want [0 1]", iterations)
	}
	<-msgCh
}

func TestAgent_ProviderErrorIsVisible(t *testing.T) {
	a := NewAgent(errProvider{}, "", nil, nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "hi")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	var sawErr bool
	for tok := range tokenCh {
		if strings.Contains(tok.Content, "✘ agent error") {
			sawErr = true
		}
		if tok.Done {
			break
		}
	}
	if !sawErr {
		t.Error("provider error must be emitted as a visible token, not swallowed")
	}
	msgs := <-msgCh
	if len(msgs) != 3 { // system + user(input) + assistant(err)
		t.Fatalf("history msgs = %d, want 3", len(msgs))
	}
	if !strings.Contains(msgs[2].Content, "✘ agent error") {
		t.Errorf("error should be recorded in history, got %q", msgs[2].Content)
	}
}

func TestAgent_EmptyChoicesIsVisible(t *testing.T) {
	a := NewAgent(emptyProvider{}, "", nil, nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "hi")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	var sawErr bool
	for tok := range tokenCh {
		if strings.Contains(tok.Content, "✘ agent error") {
			sawErr = true
		}
		if tok.Done {
			break
		}
	}
	if !sawErr {
		t.Error("empty choices must surface as an error, not silently return")
	}
	<-msgCh
}

func TestAgent_EmptyContentIsVisible(t *testing.T) {
	a := NewAgent(emptyContentProvider{}, "", nil, nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "hi")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	var sawErr bool
	for tok := range tokenCh {
		if strings.Contains(tok.Content, "✘ agent error") {
			sawErr = true
		}
		if tok.Done {
			break
		}
	}
	if !sawErr {
		t.Error("empty content without tool calls must surface as an error, not end silently")
	}
	<-msgCh
}

func TestAgent_NilResponseIsVisible(t *testing.T) {
	a := NewAgent(nilRespProvider{}, "", nil, nil)
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "hi")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	var sawErr bool
	for tok := range tokenCh {
		if strings.Contains(tok.Content, "✘ agent error") {
			sawErr = true
		}
		if tok.Done {
			break
		}
	}
	if !sawErr {
		t.Error("nil response must not panic and must surface an error")
	}
	<-msgCh
}

func TestAgent_MaxIterationsExhaustedStillFinishes(t *testing.T) {
	// 显式给个小上限触发耗尽：defaultMaxIterations 已是 MaxInt，靠默认值跑不出耗尽。
	const exhaustedIters = 5
	a := NewAgent(&toolLoopProvider{count: 999}, "", testTools(), nil, WithMaxIterations(exhaustedIters))
	tokenCh, msgCh, err := a.Run(context.Background(), nil, "do it")
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	var sawHint bool
	done := false
	for tok := range tokenCh {
		if strings.Contains(tok.Content, "已达最大迭代次数") {
			sawHint = true
		}
		if tok.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("token channel must end with Done after max iterations")
	}
	if !sawHint {
		t.Error("iteration exhaustion should emit a visible hint, not end silently")
	}
	msgs := <-msgCh
	// system + user(input) + exhaustedIters×(assistant tool_call + tool result) + assistant(unfinished note)
	if want := 3 + 2*exhaustedIters; len(msgs) != want {
		t.Errorf("history msgs = %d, want %d", len(msgs), want)
	}
	if last := msgs[len(msgs)-1]; !strings.Contains(last.Content, "已达最大迭代次数") {
		t.Errorf("last message should record unfinished state, got %q", last.Content)
	}
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
	a := NewAgent(provider, "test", nil, []core.Middleware{replaceMW{}})

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

// testTools 返回引擎测试用的最小工具集（DefaultTools 在 ai-runtime，引擎测试不依赖它）。
func testTools() []core.Tool {
	return []core.Tool{{
		Name:        "bash",
		Description: "execute a shell command",
		Parameters:  map[string]any{"type": "object"},
		Execute: func(ctx context.Context, args map[string]any) (string, error) {
			return "ok", nil
		},
	}}
}

// streamProvider 走流式路径：thinking/text 增量透传，流结束带重组响应。
type streamProvider struct {
	calls int
}

func (p *streamProvider) Name() string  { return "stream" }
func (p *streamProvider) Model() string { return "m" }
func (p *streamProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("stream provider should not fall back to Chat")
}
func (p *streamProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	p.calls++
	ch := make(chan core.Token, 4)
	ch <- core.Token{Type: core.TokenTypeThinking, Content: "ponder"}
	ch <- core.Token{Type: core.TokenTypeText, Content: "ans"}
	ch <- core.Token{Type: core.TokenTypeText, Content: "wer", Done: true}
	ch <- core.Token{Type: core.TokenTypeText, Done: true, Response: &core.ChatResponse{
		Choices: []core.ResponseChoice{{Message: core.Message{
			Role:     "assistant",
			Content:  "answer",
			Thinking: core.Thinking{Text: "ponder"},
		}}},
	}}
	close(ch)
	return ch, nil
}

func TestAgentRun_StreamsDeltasWithoutDuplication(t *testing.T) {
	provider := &streamProvider{}
	a := NewAgent(provider, "", nil, nil)

	tokenCh, msgCh, err := a.Run(context.Background(), nil, "question")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var text strings.Builder
	var thinking strings.Builder
	textDeltas := 0
	for tok := range tokenCh {
		switch tok.Type {
		case core.TokenTypeText:
			if tok.Content != "" {
				textDeltas++
				text.WriteString(tok.Content)
			}
		case core.TokenTypeThinking:
			thinking.WriteString(tok.Content)
		}
	}
	messages := <-msgCh

	if text.String() != "answer" || thinking.String() != "ponder" {
		t.Errorf("streamed text=%q thinking=%q", text.String(), thinking.String())
	}
	// 增量透传 2 段 text（"ans"+"wer"），重组响应不产生第二次整段重发
	if textDeltas != 2 {
		t.Errorf("text delta count = %d, want 2", textDeltas)
	}
	if len(messages) != 3 || messages[len(messages)-1].Content != "answer" {
		t.Errorf("history = %#v", messages)
	}
	if provider.calls != 1 {
		t.Errorf("provider calls = %d, want 1", provider.calls)
	}
}
