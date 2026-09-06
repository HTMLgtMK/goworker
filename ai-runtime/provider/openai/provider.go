package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/provider"
)

// Provider 兼容 OpenAI API（OpenAI, Ollama, vLLM, etc.）。
type Provider struct {
	provider.BaseProvider
	thinking provider.ThinkingOptions
}

type ProviderOptions func(*Provider)

func NewProvider(name, endpoint, apiKey, model string, client *http.Client, options ...ProviderOptions) *Provider {
	if endpoint == "" {
		endpoint = "http://localhost:8000/v1"
	}
	if model == "" {
		model = "gpt-4o"
	}
	if client == nil {
		panic("Provider requires a non-nil http.Client")
	}

	provider := &Provider{
		BaseProvider: *provider.NewBaseProvider(name, strings.TrimRight(endpoint, "/"), apiKey, model, client),
	}

	if len(options) > 0 {
		for _, opt := range options {
			opt(provider)
		}
	}

	return provider
}

func WithThinkingOptions(thinking provider.ThinkingOptions) ProviderOptions {
	return func(p *Provider) {
		p.thinking = thinking
	}
}

func (p *Provider) Chat(ctx context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	request := *req
	if request.Model == "" {
		request.Model = p.Model()
	}
	request.Stream = false

	wireReq := p.buildChatRequest(&request)
	body, err := json.Marshal(wireReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	// 调试观测：log.level=debug 时打印请求/响应全量 body。别打 Authorization 头（含密钥）。
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("llm.chat request", "url", p.Endpoint()+"/chat/completions",
			"model", request.Model, "messages", len(req.Messages), "body", string(body))
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.Endpoint()+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.ApiKey())

	resp, err := p.Client().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if slog.Default().Enabled(ctx, slog.LevelDebug) {
			slog.Debug("llm.chat response", "status", resp.StatusCode, "model", request.Model)
		}
		return nil, fmt.Errorf("API %d", resp.StatusCode)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("llm.chat response", "status", resp.StatusCode,
			"model", request.Model, "body", string(raw))
	}

	var wireResp OpenAIChatResponse
	if err := json.Unmarshal(raw, &wireResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return normalizeChatResponse(wireResp, p.Name()), nil
}

func (p *Provider) buildChatRequest(req *core.ChatRequest) OpenAIChatRequest {
	messages := make([]map[string]any, 0, len(req.Messages))
	for _, message := range req.Messages {
		wireMessage := map[string]any{
			"role":    message.Role,
			"content": message.Content,
		}
		if message.ToolCallID != "" {
			wireMessage["tool_call_id"] = message.ToolCallID
		}
		if len(message.ToolCalls) > 0 {
			wireMessage["tool_calls"] = message.ToolCalls
		}
		if raw := message.Custom[p.Name()]; len(raw) > 0 {
			replayCustom(wireMessage, raw)
		}
		messages = append(messages, wireMessage)
	}

	wireReq := OpenAIChatRequest{
		Model:          req.Model,
		Messages:       messages,
		Stream:         false,
		Tools:          toolSpec(req.Tools),
		ToolChoice:     openAIToolChoice(req.ToolChoice),
		ResponseFormat: openAIResponseFormat(req.JSONMode),
	}
	switch p.thinking.RequestMode {
	case runtimeconfig.ThinkingRequestEnable:
		wireReq.EnableThinking = true
	case runtimeconfig.ThinkingRequestEffort:
		wireReq.ReasoningEffort = string(p.thinking.Effort)
	}
	return wireReq
}

func toolSpec(definitions []core.Tool) []map[string]any {
	tools := make([]map[string]any, 0, len(definitions))
	for _, definition := range definitions {
		tools = append(tools, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        definition.Name,
				"description": definition.Description,
				"parameters":  definition.Parameters,
			},
		})
	}
	return tools
}

func openAIToolChoice(choice core.ToolChoice) any {
	switch choice {
	case core.ToolChoiceRequired:
		return "required"
	case core.ToolChoiceNone:
		return "none"
	default:
		return nil
	}
}

func openAIResponseFormat(jsonMode bool) any {
	if !jsonMode {
		return nil
	}
	return map[string]any{"type": "json_object"}
}

func replayCustom(message map[string]any, raw json.RawMessage) {
	var payload struct {
		Version int                        `json:"version"`
		Fields  map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil || payload.Version != 1 {
		return
	}
	for _, key := range []string{"reasoning_content", "reasoning", "reasoning_details"} {
		value, ok := payload.Fields[key]
		if ok && json.Valid(value) {
			message[key] = value
		}
	}
}

func normalizeChatResponse(wire OpenAIChatResponse, providerName string) *core.ChatResponse {
	choices := make([]core.ResponseChoice, 0, len(wire.Choices))
	for _, choice := range wire.Choices {
		message := choice.Message
		thinking := normalizeThinking(message)
		choices = append(choices, core.ResponseChoice{
			Index: choice.Index,
			Message: core.Message{
				Role:       message.Role,
				Content:    message.Content,
				ToolCallID: message.ToolCallID,
				ToolCalls:  message.ToolCalls,
				Thinking:   thinking,
				Custom:     openAICustom(message, providerName),
			},
			FinishReason: choice.FinishReason,
		})
	}
	return &core.ChatResponse{
		ID:      wire.ID,
		Object:  wire.Object,
		Created: wire.Created,
		Model:   wire.Model,
		Choices: choices,
		Usage:   wire.Usage,
	}
}

func openAICustom(message OpenAIResponseMessage, providerName string) map[string]json.RawMessage {
	fields := make(map[string]json.RawMessage, 3)
	for _, field := range []struct {
		name  string
		value json.RawMessage
	}{
		{name: "reasoning_content", value: message.ReasoningContent},
		{name: "reasoning", value: message.Reasoning},
		{name: "reasoning_details", value: message.ReasoningDetails},
	} {
		if len(field.value) > 0 {
			fields[field.name] = field.value
		}
	}
	if len(fields) == 0 {
		return nil
	}
	payload, err := json.Marshal(struct {
		Version int                        `json:"version"`
		Fields  map[string]json.RawMessage `json:"fields"`
	}{Version: 1, Fields: fields})
	if err != nil {
		return nil
	}
	return map[string]json.RawMessage{providerName: payload}
}

func normalizeThinking(message OpenAIResponseMessage) core.Thinking {
	fields := []struct {
		name  string
		value json.RawMessage
	}{
		{name: "reasoning_content", value: message.ReasoningContent},
		{name: "reasoning", value: message.Reasoning},
		{name: "reasoning_details", value: message.ReasoningDetails},
	}

	var text string
	for _, field := range fields {
		if text == "" {
			text = thinkingText(field.name, field.value)
		}
	}
	return core.Thinking{Text: text}
}

func thinkingText(name string, raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	if name != "reasoning_details" {
		var text string
		if json.Unmarshal(raw, &text) == nil {
			return strings.TrimSpace(text)
		}
		return ""
	}

	var details []map[string]json.RawMessage
	if json.Unmarshal(raw, &details) != nil {
		return ""
	}
	parts := make([]string, 0, len(details))
	for _, detail := range details {
		for _, key := range []string{"text", "content"} {
			var text string
			if json.Unmarshal(detail[key], &text) == nil && strings.TrimSpace(text) != "" {
				parts = append(parts, strings.TrimSpace(text))
				break
			}
		}
	}
	return strings.Join(parts, "\n")
}

func (p *Provider) ChatStream(ctx context.Context, req *core.ChatRequest) (<-chan core.Token, error) {
	request := *req
	if request.Model == "" {
		request.Model = p.Model()
	}
	request.Stream = true

	wireReq := p.buildChatRequest(&request)
	wireReq.Stream = true
	// 请求末尾的独立 usage chunk；不支持的兼容后端会忽略该字段
	wireReq.StreamOptions = &StreamOptions{IncludeUsage: true}
	body, err := json.Marshal(wireReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.Endpoint()+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.ApiKey())

	resp, err := p.Client().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("API %d", resp.StatusCode)
	}

	tokenCh := make(chan core.Token)
	go p.readSSE(ctx, resp.Body, tokenCh)
	return tokenCh, nil
}

// readSSE 解析 SSE 流。
//
// thinking/text 增量实时投递（token 直达前端做流式渲染）；tool_calls 分片按
// index 合并、不透传；流结束时把重组出的完整响应通过带 Response 的收尾 token
// 一次性交付，agent 用它回填历史并触发 AfterModel 中间件。
//
// done 语义保持与旧契约一致：finish_reason 所在 chunk 打 Done=true，代表
// "本轮模型响应结束"——不等于整个 agent 运行结束，也不代表会话结束。
// finish chunk 之后继续读到 [DONE]/EOF：include_usage 的 usage chunk 落在它后面。
func (p *Provider) readSSE(ctx context.Context, body io.ReadCloser, tokenCh chan<- core.Token) {
	defer body.Close()
	defer close(tokenCh)

	var (
		content strings.Builder
		rcText  strings.Builder // reasoning_content 增量
		rText   strings.Builder // reasoning 增量
		calls   []core.ToolCall // 按 Delta.Index 合并的 tool call 分片
		usage   *core.UsageInfo
	)
	acc := &streamAcc{content: &content, rcText: &rcText, rText: &rText, calls: &calls}
	// 投递失败（ctx 取消）时直接返回，让 close(tokenCh) 收尾
	send := func(tok core.Token) bool {
		select {
		case tokenCh <- tok:
			return true
		case <-ctx.Done():
			return false
		}
	}

	reader := bufio.NewReader(body)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			// 传输中断（连接重置等）：按失败处理，跳过末尾重组——
			// 绝不能把截断的半截响应当成功交付，那会污染会话历史
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "data:") {
			// 容忍 "data:"/"data: " 两种分隔（SSE 规范只要求冒号）
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "[DONE]" {
				break
			}
			var chunk core.StreamChunk
			if jsonErr := json.Unmarshal([]byte(data), &chunk); jsonErr == nil {
				if chunk.Usage != nil {
					usage = chunk.Usage
				}
				if len(chunk.Choices) > 0 {
					if !p.handleStreamDelta(chunk.Choices[0].Delta, acc, send) {
						return
					}
					if fr := chunk.Choices[0].FinishReason; fr != "" {
						acc.finish = fr
						if !send(core.Token{Type: core.TokenTypeText, Done: true}) {
							return
						}
					}
				}
			}
		}
		if err == io.EOF {
			// 流正常结束（部分后端不发 [DONE]），处理完最后一段后收尾
			break
		}
	}

	// 空流（无任何增量）不重组也不发收尾 token，保持旧行为
	if content.Len() == 0 && rcText.Len() == 0 && rText.Len() == 0 && len(calls) == 0 {
		return
	}

	thinkingText := rcText.String() + rText.String()
	msg := core.Message{Role: "assistant", Content: content.String()}
	// reasoning_details 是结构化数组，流式分片重组不出原始 RawMessage，故不重建
	// 该字段；reasoning_content/reasoning（字符串型）按 Custom 契约回放，保证
	// 工具轮次间的 thinking 回放与非流式路径一致。DeepSeek 等后端不要求回传
	// reasoning_content，收到该字段也会忽略，无兼容风险。
	if thinkingText != "" {
		msg.Thinking = core.Thinking{Text: thinkingText}
	}
	fields := make(map[string]json.RawMessage, 2)
	if rcText.Len() > 0 {
		if raw, err := json.Marshal(rcText.String()); err == nil {
			fields["reasoning_content"] = raw
		}
	}
	if rText.Len() > 0 {
		if raw, err := json.Marshal(rText.String()); err == nil {
			fields["reasoning"] = raw
		}
	}
	if len(fields) > 0 {
		payload, err := json.Marshal(struct {
			Version int                        `json:"version"`
			Fields  map[string]json.RawMessage `json:"fields"`
		}{Version: 1, Fields: fields})
		if err == nil {
			msg.Custom = map[string]json.RawMessage{p.Name(): payload}
		}
	}
	if len(calls) > 0 {
		// 过滤从未收到首片的空槽（如畸形分片从 index=1 开始），
		// 避免 agent 把空 tool call 回填进历史
		merged := make([]core.ToolCall, 0, len(calls))
		for _, c := range calls {
			if c.ID == "" && c.Function.Name == "" {
				continue
			}
			merged = append(merged, c)
		}
		if len(merged) > 0 {
			msg.ToolCalls = merged
		}
	}
	send(core.Token{
		Type: core.TokenTypeText,
		Done: true,
		Response: &core.ChatResponse{
			Choices: []core.ResponseChoice{{
				Message:      msg,
				FinishReason: acc.finish,
			}},
			Usage: usage,
		},
	})
}

// streamAcc 聚合单条 SSE 流的累积状态（readSSE 局部使用）。
type streamAcc struct {
	content *strings.Builder
	rcText  *strings.Builder
	rText   *strings.Builder
	calls   *[]core.ToolCall
	finish  string
}

// handleStreamDelta 处理单个 SSE 增量：thinking/text 实时投递，tool_calls
// 分片按 index 合并，finish_reason 记入 acc 并投递 done 标记。
// 返回 false 表示投递失败（ctx 取消），调用方须立即终止且不做末尾重组。
// 负 index 是畸形数据，跳过——readSSE 运行在独立 goroutine，越界 panic 会
// 带崩整个进程。
func (p *Provider) handleStreamDelta(delta core.Delta, acc *streamAcc, send func(core.Token) bool) bool {
	if delta.ReasoningContent != "" {
		acc.rcText.WriteString(delta.ReasoningContent)
		if !send(core.Token{Type: core.TokenTypeThinking, Content: delta.ReasoningContent}) {
			return false
		}
	}
	if delta.Reasoning != "" {
		acc.rText.WriteString(delta.Reasoning)
		if !send(core.Token{Type: core.TokenTypeThinking, Content: delta.Reasoning}) {
			return false
		}
	}
	if delta.Content != "" {
		acc.content.WriteString(delta.Content)
		if !send(core.Token{Type: core.TokenTypeText, Content: delta.Content}) {
			return false
		}
	}
	for _, dt := range delta.ToolCalls {
		if dt.Index < 0 {
			continue
		}
		for len(*acc.calls) <= dt.Index {
			*acc.calls = append(*acc.calls, core.ToolCall{})
		}
		merged := &(*acc.calls)[dt.Index]
		if dt.ID != "" {
			merged.ID = dt.ID
		}
		if dt.Type != "" {
			merged.Type = dt.Type
		}
		if dt.Function.Name != "" {
			merged.Function.Name = dt.Function.Name
		}
		merged.Function.Arguments += dt.Function.Arguments
	}
	return true
}
