package anthropic

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
	"github.com/tinguo/goworker/ai-runtime/provider"
)

const (
	anthropicVersion         = "2023-06-01"
	anthropicJSONInstruction = "Respond with valid JSON only."
	maxAnthropicResponseSize = 10 << 20
)

// Provider 适配 Anthropic Messages API 的非流式请求。
type Provider struct {
	provider.BaseProvider
	authType  AnthropicAuthType
	maxTokens int
}

type ProviderOption func(*Provider)

func WithAnthropicMaxTokens(maxTokens int) ProviderOption {
	return func(p *Provider) {
		if maxTokens > 0 {
			p.maxTokens = maxTokens
		}
	}
}

func NewProvider(name, endpoint, apiKey, model string, client *http.Client, authType AnthropicAuthType, options ...ProviderOption) *Provider {
	if name == "" {
		name = "anthropic"
	}
	if endpoint == "" {
		endpoint = "https://api.anthropic.com"
	}
	if model == "" {
		model = "claude-sonnet-4-6"
	}
	if authType == "" {
		authType = AnthropicAuthAPIKey
	}
	provider := &Provider{
		BaseProvider: *provider.NewBaseProvider(name, strings.TrimRight(endpoint, "/"), apiKey, model, client),
		authType:     authType,
		maxTokens:    8192,
	}
	for _, option := range options {
		option(provider)
	}
	return provider
}

func (p *Provider) Chat(ctx context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	wireReq, err := p.buildMessageRequest(req)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(wireReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint()+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("anthropic-version", anthropicVersion)
	switch p.authType {
	case AnthropicAuthBearer:
		httpReq.Header.Set("Authorization", "Bearer "+p.ApiKey())
	case AnthropicAuthAPIKey:
		httpReq.Header.Set("x-api-key", p.ApiKey())
	default:
		return nil, fmt.Errorf("unsupported Anthropic auth type %q", p.authType)
	}

	resp, err := p.Client().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if detail := provider.UpstreamErrorDetail(resp, p.Name()); detail != "" {
			return nil, fmt.Errorf("API %d: %s", resp.StatusCode, detail)
		}
		return nil, fmt.Errorf("API %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAnthropicResponseSize+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if len(data) > maxAnthropicResponseSize {
		return nil, fmt.Errorf("response exceeds %d byte limit", maxAnthropicResponseSize)
	}
	var wireResp anthropicMessageResponse
	if err := json.Unmarshal(data, &wireResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	response, err := normalizeAnthropicResponse(wireResp, p.Name())
	if err != nil {
		return nil, err
	}
	return response, nil
}

// ChatStream 走 SSE 流式请求（stream:true）：thinking/text 增量实时投递，
// tool_use 的 input_json_delta 按 content block 重组（不透传），流结束把完整
// 响应作为收尾 token 交付（agent 据此回填历史与触发 AfterModel）。
func (p *Provider) ChatStream(ctx context.Context, req *core.ChatRequest) (<-chan core.Token, error) {
	wireReq, err := p.buildMessageRequest(req)
	if err != nil {
		return nil, err
	}
	wireReq.Stream = true
	body, err := json.Marshal(wireReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint()+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("anthropic-version", anthropicVersion)
	switch p.authType {
	case AnthropicAuthBearer:
		httpReq.Header.Set("Authorization", "Bearer "+p.ApiKey())
	case AnthropicAuthAPIKey:
		httpReq.Header.Set("x-api-key", p.ApiKey())
	default:
		return nil, fmt.Errorf("unsupported Anthropic auth type %q", p.authType)
	}

	resp, err := p.Client().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		detail := provider.UpstreamErrorDetail(resp, p.Name())
		resp.Body.Close()
		if detail != "" {
			return nil, fmt.Errorf("API %d: %s", resp.StatusCode, detail)
		}
		return nil, fmt.Errorf("API %d", resp.StatusCode)
	}

	// 兼容代理可能无视 stream:true 直接回非流式 JSON（content-type 非 SSE）：
	// 按非流式解析并只投递收尾响应，避免流解析读不到事件被当成空流。
	ct := resp.Header.Get("Content-Type")
	isStream := strings.Contains(ct, "text/event-stream")
	if !isStream && strings.TrimSpace(strings.SplitN(ct, ";", 2)[0]) == "" {
		// 代理漏设 Content-Type：按响应首字节探测——SSE 以 data:/event: 开头，
		// JSON 对象以 { 开头。读到半截先包回 bufio 再把手（探测已读的字节不丢）。
		br := bufio.NewReader(resp.Body)
		if prefix, err := br.Peek(8); err == nil {
			trimmed := strings.TrimLeft(string(prefix), " \t\r\n")
			isStream = !strings.HasPrefix(trimmed, "{")
		}
		resp.Body = struct {
			io.Reader
			io.Closer
		}{br, resp.Body}
	}
	if !isStream {
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, maxAnthropicResponseSize+1))
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if len(data) > maxAnthropicResponseSize {
			return nil, fmt.Errorf("response exceeds %d byte limit", maxAnthropicResponseSize)
		}
		var wireResp anthropicMessageResponse
		if err := json.Unmarshal(data, &wireResp); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
		full, err := normalizeAnthropicResponse(wireResp, p.Name())
		if err != nil {
			return nil, err
		}
		tokenCh := make(chan core.Token, 1)
		tokenCh <- core.Token{Type: core.TokenTypeText, Done: true, Response: full}
		close(tokenCh)
		return tokenCh, nil
	}

	tokenCh := make(chan core.Token)
	go p.readSSE(ctx, resp.Body, tokenCh)
	return tokenCh, nil
}

// maxStreamBlocks 限制 index 位图上限，防负值 panic / 超大值吃内存。
const maxStreamBlocks = 1024

// anthropicStreamBlockState 是单个 content block 的增量累积状态。
type anthropicStreamBlockState struct {
	kind      string // thinking / text / tool_use
	text      strings.Builder
	thinking  strings.Builder
	signature string
	toolID    string
	toolName  string
	input     strings.Builder
}

// readSSE 解析 Anthropic Messages SSE：按 data.type 分派（event: 行不参与判别，
// 部分兼容代理只发 data:），thinking/text 增量即时投递，tool_use 分片重组，
// 流结束组装完整 ChatResponse 随收尾 token 交付。
func (p *Provider) readSSE(ctx context.Context, body io.ReadCloser, tokenCh chan<- core.Token) {
	defer body.Close()
	defer close(tokenCh)

	var (
		blocks     []*anthropicStreamBlockState
		msgID      string
		model      string
		stopReason string
		usage      anthropicUsage
		sawContent bool
	)
	send := func(tok core.Token) bool {
		select {
		case tokenCh <- tok:
			return true
		case <-ctx.Done():
			return false
		}
	}
	blockAt := func(index int) *anthropicStreamBlockState {
		// 越界夹到边界：宁可并进相邻块也不能 panic 或无界扩容。
		if index < 0 {
			index = 0
		} else if index >= maxStreamBlocks {
			index = maxStreamBlocks - 1
		}
		for len(blocks) <= index {
			blocks = append(blocks, &anthropicStreamBlockState{})
		}
		return blocks[index]
	}

	scanner := bufio.NewScanner(body)
	// 单行上限对齐非流式 10MB：超长 data 行交给 scanner 报 ErrTooLong，
	// 而不是 readSSE 无界 buff 一行。
	scanner.Buffer(make([]byte, 0, 64*1024), maxAnthropicResponseSize+1)
	var total int
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line := strings.TrimRight(scanner.Text(), "\r")
		total += len(line) + 1
		if total > maxAnthropicResponseSize {
			// 流式路径补齐非流式的同名上限：累计超限中止并丢弃重组，防无界累积。
			slog.Warn("anthropic: SSE stream exceeds size limit", "provider", p.Name(), "limit", maxAnthropicResponseSize)
			return
		}
		if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data != "" && data != "[DONE]" {
				var ev anthropicStreamEvent
				if json.Unmarshal([]byte(data), &ev) == nil {
					switch ev.Type {
					case "message_start":
						if ev.Message != nil {
							msgID = ev.Message.ID
							model = ev.Message.Model
							usage.InputTokens = ev.Message.Usage.InputTokens
							usage.CacheReadInputTokens = ev.Message.Usage.CacheReadInputTokens
							usage.CacheCreationInputTokens = ev.Message.Usage.CacheCreationInputTokens
							usage.OutputTokens = ev.Message.Usage.OutputTokens
						}
					case "content_block_start":
						if ev.ContentBlock != nil {
							sawContent = true
							b := blockAt(ev.Index)
							b.kind = ev.ContentBlock.Type
							b.toolID = ev.ContentBlock.ID
							b.toolName = ev.ContentBlock.Name
							if ev.ContentBlock.Text != "" {
								b.text.WriteString(ev.ContentBlock.Text)
							}
							if ev.ContentBlock.Thinking != "" {
								b.thinking.WriteString(ev.ContentBlock.Thinking)
							}
						}
					case "content_block_delta":
						if ev.Delta != nil {
							// 兼容省略 content_block_start 的代理：delta 也算看到内容，
							// 否则流结束的 !sawContent 会把重组响应整个丢掉。
							sawContent = true
							b := blockAt(ev.Index)
							switch ev.Delta.Type {
							case "text_delta":
								// 兼容省略 content_block_start 只发 delta 的代理：kind
								// 没被 start 赋值时按 delta 类型反推，否则增量实时投递了
								// 却写不进重建的 Content。
								if b.kind == "" {
									b.kind = "text"
								}
								if ev.Delta.Text != "" {
									b.text.WriteString(ev.Delta.Text)
									if !send(core.Token{Type: core.TokenTypeText, Content: ev.Delta.Text}) {
										return
									}
								}
							case "thinking_delta":
								if b.kind == "" {
									b.kind = "thinking"
								}
								if ev.Delta.Thinking != "" {
									b.thinking.WriteString(ev.Delta.Thinking)
									if !send(core.Token{Type: core.TokenTypeThinking, Content: ev.Delta.Thinking}) {
										return
									}
								}
							case "signature_delta":
								if b.kind == "" {
									b.kind = "thinking"
								}
								b.signature = ev.Delta.Signature
							case "input_json_delta":
								b.input.WriteString(ev.Delta.PartialJSON)
							}
						}
					case "message_delta":
						if ev.Delta != nil && ev.Delta.StopReason != "" {
							stopReason = ev.Delta.StopReason
						}
						if ev.Usage != nil {
							usage.OutputTokens = ev.Usage.OutputTokens
						}
					case "error":
						// 不重组半截响应，只落日志；带上上游的 type/message。
						errType, errMsg := "", ""
						if ev.Error != nil {
							errType, errMsg = ev.Error.Type, ev.Error.Message
						}
						slog.Warn("anthropic: SSE stream reported an error event",
							"provider", p.Name(), "type", errType, "message", errMsg)
						return
					}
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		// 传输中断或单行超长（bufio.ErrTooLong）：不重组半截响应（会污染会话历史），
		// 保留可观测日志让排查不靠猜。
		slog.Warn("anthropic: SSE stream read failed", "provider", p.Name(), "err", err)
		return
	}
	if !sawContent {
		// 空流（无任何 content block）不重组也不发收尾 token（同 openai 契约）
		return
	}

	// 组装完整响应：正文/思考拼接，tool_use 用累积的 input JSON。
	var content strings.Builder
	var thinkingParts []string
	var calls []core.ToolCall
	// 用 map 而不是带 omitempty 的 struct：thinking 块的 `thinking` 字段是上游
	// 反序列化的必填项——空思考 + 有 signature 时若被省略，整条历史回放 422
	// （实测 CC-Switch：`messages[N]: missing field thinking`）。
	rebuilt := make([]map[string]any, 0, len(blocks))
	for _, b := range blocks {
		switch b.kind {
		case "text":
			// 空文本块不回放：上游对空 content block 的校验不一致（部分网关 4xx）。
			if strings.TrimSpace(b.text.String()) == "" {
				continue
			}
			content.WriteString(b.text.String())
			rebuilt = append(rebuilt, map[string]any{"type": "text", "text": b.text.String()})
		case "thinking":
			// 无内容也无签名的思考块没有回放价值；有内容无签名的块只用于展示
			// （message.Thinking），不进 assistant_content——上游对缺签名的
			// thinking 块校验因网关而异，回放它风险大于收益。
			text := b.thinking.String()
			if text != "" {
				thinkingParts = append(thinkingParts, text)
			}
			if b.signature != "" {
				// thinking 字段无条件写出（空串也写）——上游必填。
				rebuilt = append(rebuilt, map[string]any{
					"type":      "thinking",
					"thinking":  text,
					"signature": b.signature,
				})
			}
		case "tool_use":
			args := strings.TrimSpace(b.input.String())
			// 上游要求 input 是 JSON object；拼接结果非对象时退化为空对象，
			// 免得一条畸形分片让整个历史回放持续报错。
			var inputObj map[string]any
			if args == "" || json.Unmarshal([]byte(args), &inputObj) != nil || inputObj == nil {
				args = "{}"
			}
			calls = append(calls, core.ToolCall{
				ID:   b.toolID,
				Type: "function",
				Function: core.ToolCallFunction{
					Name:      b.toolName,
					Arguments: args,
				},
			})
			rebuilt = append(rebuilt, map[string]any{
				"type":  "tool_use",
				"id":    b.toolID,
				"name":  b.toolName,
				"input": json.RawMessage(args),
			})
		}
	}
	if len(rebuilt) == 0 {
		// 全部块都被清理掉（空响应）：不重组，让 agent 报"流未给出完整响应"。
		return
	}

	message := core.Message{Role: "assistant", Content: content.String(), ToolCalls: calls}
	if len(thinkingParts) > 0 {
		message.Thinking.Text = strings.Join(thinkingParts, "\n")
	}
	// Custom 保留重组后的 assistant content（含 thinking signature），
	// 与非流式路径的 anthropicCustom 同形，保证后续轮次回放一致。
	if raw, marshalErr := json.Marshal(rebuilt); marshalErr == nil {
		message.Custom = anthropicCustom(raw, p.Name())
	}

	send(core.Token{
		Type: core.TokenTypeText,
		Done: true,
		Response: &core.ChatResponse{
			ID:     msgID,
			Object: "message",
			Model:  model,
			Choices: []core.ResponseChoice{{
				Message:      message,
				FinishReason: anthropicFinishReason(stopReason),
			}},
			Usage: &core.UsageInfo{
				PromptTokens:          usage.InputTokens,
				CompletionTokens:      usage.OutputTokens,
				PromptCacheHitTokens:  usage.CacheReadInputTokens,
				PromptCacheMissTokens: usage.CacheCreationInputTokens,
				TotalTokens:           usage.InputTokens + usage.OutputTokens,
			},
		},
	})
}

func (p *Provider) buildMessageRequest(req *core.ChatRequest) (anthropicMessageRequest, error) {
	model := req.Model
	if model == "" {
		model = p.Model()
	}
	system, messages, err := anthropicMessages(req.Messages, req.JSONMode, p.Name())
	if err != nil {
		return anthropicMessageRequest{}, err
	}
	return anthropicMessageRequest{
		Model:      model,
		MaxTokens:  p.maxTokens,
		System:     system,
		Messages:   messages,
		Tools:      anthropicTools(req.Tools),
		ToolChoice: anthropicToolChoice(req.ToolChoice),
	}, nil
}

func anthropicMessages(messages []core.Message, jsonMode bool, providerName string) ([]map[string]string, []anthropicWireMessage, error) {
	system := make([]map[string]string, 0, 1)
	wire := make([]anthropicWireMessage, 0, len(messages))
	for _, message := range messages {
		if message.Role == "system" {
			system = append(system, map[string]string{"type": "text", "text": message.Content})
			continue
		}
		blocks, err := anthropicBlocks(message, providerName)
		if err != nil {
			return nil, nil, err
		}
		role := message.Role
		if role == "tool" {
			role = "user"
		}
		if len(wire) > 0 && wire[len(wire)-1].Role == role {
			wire[len(wire)-1].Content = append(wire[len(wire)-1].Content, blocks...)
			continue
		}
		wire = append(wire, anthropicWireMessage{Role: role, Content: blocks})
	}
	if jsonMode {
		if len(system) == 0 {
			system = append(system, map[string]string{"type": "text", "text": anthropicJSONInstruction})
		} else {
			system[len(system)-1]["text"] += "\n\n" + anthropicJSONInstruction
		}
	}
	return system, wire, nil
}

func anthropicBlocks(message core.Message, providerName string) ([]json.RawMessage, error) {
	if message.Role == "tool" {
		return marshalAnthropicBlocks([]any{map[string]any{
			"type":        "tool_result",
			"tool_use_id": message.ToolCallID,
			"content":     message.Content,
		}})
	}
	if message.Role == "assistant" {
		if raw := message.Custom[providerName]; len(raw) > 0 {
			var payload struct {
				Version          int             `json:"version"`
				AssistantContent json.RawMessage `json:"assistant_content"`
			}
			if err := json.Unmarshal(raw, &payload); err == nil && payload.Version == 1 {
				var blocks []json.RawMessage
				if err := json.Unmarshal(payload.AssistantContent, &blocks); err == nil {
					// 历史里可能存着旧版重组写入的坏块（thinking 缺必填字段、
					// 空 text、非 object 的 tool input）：回放前统一清洗。
					return sanitizeAnthropicBlocks(blocks), nil
				}
			}
		}
	}
	blocks := make([]any, 0, len(message.ToolCalls)+1)
	if message.Content != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": message.Content})
	}
	for _, call := range message.ToolCalls {
		var input json.RawMessage
		if err := json.Unmarshal([]byte(call.Function.Arguments), &input); err != nil {
			return nil, fmt.Errorf("decode tool call %q input: %w", call.ID, err)
		}
		blocks = append(blocks, map[string]any{
			"type":  "tool_use",
			"id":    call.ID,
			"name":  call.Function.Name,
			"input": input,
		})
	}
	return marshalAnthropicBlocks(blocks)
}

// sanitizeAnthropicBlocks 清洗回放用的 assistant content：
//   - thinking 块补必填的 `thinking` 字段（旧数据缺它会让上游 422）
//   - 空 text 块丢弃
//   - tool_use 的 input 非 JSON object 时退化为 {}
//
// 未知类型的块原样保留（未来协议扩展不在这里丢数据）。
func sanitizeAnthropicBlocks(blocks []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(blocks))
	for _, item := range blocks {
		var block map[string]any
		if err := json.Unmarshal(item, &block); err != nil || block == nil {
			continue
		}
		kind, _ := block["type"].(string)
		touched := false
		switch kind {
		case "thinking":
			if _, ok := block["thinking"]; !ok {
				block["thinking"] = ""
				touched = true
			}
		case "text":
			if text, _ := block["text"].(string); strings.TrimSpace(text) == "" {
				continue
			}
		case "tool_use":
			if _, ok := block["input"].(map[string]any); !ok {
				block["input"] = map[string]any{}
				touched = true
			}
		}
		// 未修补的块原样保留（不重排字段顺序，避免无谓 diff）。
		if !touched {
			out = append(out, item)
			continue
		}
		data, err := json.Marshal(block)
		if err != nil {
			continue
		}
		out = append(out, data)
	}
	return out
}

func marshalAnthropicBlocks(blocks []any) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(blocks))
	for _, block := range blocks {
		data, err := json.Marshal(block)
		if err != nil {
			return nil, err
		}
		out = append(out, data)
	}
	return out, nil
}

func anthropicTools(source []core.Tool) []anthropicToolDefinition {
	definitions := make([]anthropicToolDefinition, 0, len(source))
	for _, tool := range source {
		definitions = append(definitions, anthropicToolDefinition{Name: tool.Name, Description: tool.Description, InputSchema: tool.Parameters})
	}
	return definitions
}

func anthropicToolChoice(choice core.ToolChoice) any {
	switch choice {
	case core.ToolChoiceRequired:
		return map[string]string{"type": "any"}
	case core.ToolChoiceNone:
		return map[string]string{"type": "none"}
	default:
		return nil
	}
}

func normalizeAnthropicResponse(response anthropicMessageResponse, providerName string) (*core.ChatResponse, error) {
	message := core.Message{Role: response.Role}
	var blocks []json.RawMessage
	if err := json.Unmarshal(response.Content, &blocks); err != nil {
		return nil, fmt.Errorf("decode response content: %w", err)
	}
	message.Custom = anthropicCustom(response.Content, providerName)
	thinking := make([]string, 0)
	for _, block := range blocks {
		var header struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(block, &header) != nil {
			continue
		}
		switch header.Type {
		case "text":
			var text struct {
				Text string `json:"text"`
			}
			if json.Unmarshal(block, &text) == nil {
				message.Content += text.Text
			}
		case "thinking":
			var thought struct {
				Thinking string `json:"thinking"`
			}
			if json.Unmarshal(block, &thought) == nil && thought.Thinking != "" {
				thinking = append(thinking, thought.Thinking)
			}
		case "tool_use":
			var tool struct {
				ID    string          `json:"id"`
				Name  string          `json:"name"`
				Input json.RawMessage `json:"input"`
			}
			if json.Unmarshal(block, &tool) == nil {
				message.ToolCalls = append(message.ToolCalls, core.ToolCall{ID: tool.ID, Type: "function", Function: core.ToolCallFunction{Name: tool.Name, Arguments: string(tool.Input)}})
			}
		}
	}
	message.Thinking.Text = strings.Join(thinking, "\n")
	return &core.ChatResponse{
		ID:      response.ID,
		Object:  response.Type,
		Model:   response.Model,
		Choices: []core.ResponseChoice{{Message: message, FinishReason: anthropicFinishReason(response.StopReason)}},
		Usage: &core.UsageInfo{
			PromptTokens:          response.Usage.InputTokens,
			CompletionTokens:      response.Usage.OutputTokens,
			PromptCacheHitTokens:  response.Usage.CacheReadInputTokens,
			PromptCacheMissTokens: response.Usage.CacheCreationInputTokens,
			TotalTokens:           response.Usage.InputTokens + response.Usage.OutputTokens,
		},
	}, nil
}

func anthropicCustom(content json.RawMessage, providerName string) map[string]json.RawMessage {
	payload, err := json.Marshal(struct {
		Version          int             `json:"version"`
		AssistantContent json.RawMessage `json:"assistant_content"`
	}{Version: 1, AssistantContent: content})
	if err != nil {
		return nil
	}
	return map[string]json.RawMessage{providerName: payload}
}

func anthropicFinishReason(stopReason string) string {
	switch stopReason {
	case "tool_use":
		return "tool_calls"
	case "max_tokens":
		return "length"
	case "end_turn":
		return "stop"
	default:
		return stopReason
	}
}
