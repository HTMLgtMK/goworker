package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

func TestNewOpenAIProvider_HTTPTimeout(t *testing.T) {
	p := NewOpenAIProvider("", "", "")
	if p.client.Timeout != defaultHTTPTimeout {
		t.Errorf("client timeout = %v, want %v", p.client.Timeout, defaultHTTPTimeout)
	}
}

func TestOpenAIProvider_ErrorBodyTruncated(t *testing.T) {
	// 非 200 响应 body 只取前 1KB 兜底，别让网关错误页全量进错误消息
	big := strings.Repeat("x", 10000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(big))
	}))
	defer srv.Close()

	p := NewOpenAIProvider(srv.URL, "key", "m")
	_, err := p.Chat(context.Background(), &core.ChatRequest{Model: "m"})
	if err == nil {
		t.Fatal("want error on 502")
	}
	if len(err.Error()) >= len(big) {
		t.Errorf("error message not bounded: len=%d, body=%d", len(err.Error()), len(big))
	}
	if !strings.Contains(err.Error(), "502") {
		t.Errorf("error should carry status code: %v", err)
	}
}
