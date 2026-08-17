package agent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
)

// OpenAIProvider 兼容 OpenAI API（OpenAI, Ollama, vLLM, etc.）。
type OpenAIProvider struct {
	endpoint string
	apiKey   string
	model    string
	client   *http.Client
}

// defaultHTTPTimeout 是 HTTP 客户端总超时。检查点/压缩是非流式全量调用，
// 上下文一大（用户 config 里 compress_at 没开，会话无界累积）响应可能很慢，
// 30s 太紧，2min 起步。
const defaultHTTPTimeout = 2 * time.Minute

func NewOpenAIProvider(endpoint, apiKey, model string) *OpenAIProvider {
	if endpoint == "" {
		endpoint = "http://localhost:8000/v1"
	}
	if model == "" {
		model = "gpt-4o"
	}

	// GOWORKER_PROXY 显式指定抓包代理（如 whistle）时走它，否则回退
	// http.ProxyFromEnvironment：尊重系统 HTTP(S)_PROXY，没配就直连，
	// 行为跟不设置 Transport 的默认 http.Client 一致。
	// 抓包代理是 MITM，证书链必然校验不过，所以代理一旦显式配置就跳过
	// TLS 校验——只在这条调试路径生效，生产不设 GOWORKER_PROXY 保持严格校验。
	proxyFn := http.ProxyFromEnvironment
	tlsConfig := &tls.Config{}
	if proxyURL := strings.TrimSpace(os.Getenv("GOWORKER_PROXY")); proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			slog.Warn("GOWORKER_PROXY 解析失败，回退系统代理", "proxy", proxyURL, "err", err)
		} else {
			proxyFn = http.ProxyURL(u)
			tlsConfig = &tls.Config{InsecureSkipVerify: true}
		}
	}

	return &OpenAIProvider{
		endpoint: strings.TrimRight(endpoint, "/"),
		apiKey:   apiKey,
		model:    model,
		client: &http.Client{
			Timeout: defaultHTTPTimeout,
			Transport: &http.Transport{
				Proxy:           proxyFn,
				TLSClientConfig: tlsConfig,
			},
		},
	}
}

func (p *OpenAIProvider) Name() string  { return "openai" }
func (p *OpenAIProvider) Model() string { return p.model }

func (p *OpenAIProvider) Chat(ctx context.Context, req *core.ChatRequest) (*core.ChatResponse, error) {
	if req.Model == "" {
		req.Model = p.model
	}
	req.Stream = false

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	// 调试观测：log.level=debug 时打印请求/响应全量 body。别打 Authorization 头（含密钥）。
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("llm.chat request", "url", p.endpoint+"/chat/completions",
			"model", req.Model, "messages", len(req.Messages), "body", string(body))
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
		// 错误 body 只取前 1KB 兜底 —— 网关的 HTML 错误页/堆栈可能几 KB 起，
		// 全量读进内存再塞进错误消息，既占内存又刷用户终端。
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if slog.Default().Enabled(ctx, slog.LevelDebug) {
			slog.Debug("llm.chat response", "status", resp.StatusCode,
				"model", req.Model, "body", string(b))
		}
		return nil, fmt.Errorf("API %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("llm.chat response", "status", resp.StatusCode,
			"model", req.Model, "body", string(raw))
	}

	var chatResp core.ChatResponse
	if err := json.Unmarshal(raw, &chatResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &chatResp, nil
}

func (p *OpenAIProvider) ChatStream(ctx context.Context, req *core.ChatRequest) (<-chan core.Token, error) {
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

	tokenCh := make(chan core.Token)
	go p.readSSE(ctx, resp.Body, tokenCh)
	return tokenCh, nil
}

func (p *OpenAIProvider) readSSE(ctx context.Context, body io.ReadCloser, tokenCh chan<- core.Token) {
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

		var chunk core.StreamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) == 0 {
			continue
		}

		content := chunk.Choices[0].Delta.Content
		finishReason := chunk.Choices[0].FinishReason

		select {
		case tokenCh <- core.Token{Type: core.TokenTypeText, Content: content, Done: finishReason != ""}:
		case <-ctx.Done():
			return
		}
		if finishReason != "" {
			return
		}
	}
}
