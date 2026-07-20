package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Provider 是 LLM 后端的接口。
type Provider interface {
	Name() string
	Model() string
	Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error)
	ChatStream(ctx context.Context, req *ChatRequest) (<-chan Token, error)
}

// OpenAIProvider 兼容 OpenAI API（OpenAI, Ollama, vLLM, etc.）。
type OpenAIProvider struct {
	endpoint string
	apiKey   string
	model    string
	client   *http.Client
}

func NewOpenAIProvider(endpoint, apiKey, model string) *OpenAIProvider {
	if endpoint == "" {
		endpoint = "http://localhost:8000/v1"
	}
	if model == "" {
		model = "gpt-4o"
	}
	return &OpenAIProvider{
		endpoint: strings.TrimRight(endpoint, "/"),
		apiKey:   apiKey,
		model:    model,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (p *OpenAIProvider) Name() string  { return "openai" }
func (p *OpenAIProvider) Model() string { return p.model }

func (p *OpenAIProvider) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	if req.Model == "" {
		req.Model = p.model
	}
	req.Stream = false

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.endpoint+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("API %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var chatResp ChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &chatResp, nil
}

func (p *OpenAIProvider) ChatStream(ctx context.Context, req *ChatRequest) (<-chan Token, error) {
	if req.Model == "" {
		req.Model = p.model
	}
	req.Stream = true

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.endpoint+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		return nil, fmt.Errorf("API %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	tokenCh := make(chan Token)
	go p.readSSE(ctx, resp.Body, tokenCh)
	return tokenCh, nil
}

func (p *OpenAIProvider) readSSE(ctx context.Context, body io.ReadCloser, tokenCh chan<- Token) {
	defer body.Close()
	defer close(tokenCh)

	reader := bufio.NewReader(body)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")

		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			return
		}

		var chunk StreamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) == 0 {
			continue
		}

		content := chunk.Choices[0].Delta.Content
		finishReason := chunk.Choices[0].FinishReason

		select {
		case tokenCh <- Token{Type: TokenTypeText, Content: content, Done: finishReason != ""}:
		case <-ctx.Done():
			return
		}
		if finishReason != "" {
			return
		}
	}
}
