package agent

import (
	"context"
	"testing"

	"github.com/tinguo/goworker/ai-memory"
)

func TestLLMAdapter_JSONModeSetsResponseFormat(t *testing.T) {
	inner := &captureProvider{}
	a := &llmAdapter{inner: inner}
	_, err := a.Chat(context.Background(), &memory.ChatRequest{
		Model:    "m",
		JSONMode: true,
		Messages: []memory.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	rf, ok := inner.lastReq.ResponseFormat.(map[string]any)
	if !ok || rf["type"] != "json_object" {
		t.Errorf("ResponseFormat = %v, want {\"type\":\"json_object\"}", inner.lastReq.ResponseFormat)
	}
}

func TestLLMAdapter_NoJSONModeLeavesResponseFormatNil(t *testing.T) {
	inner := &captureProvider{}
	a := &llmAdapter{inner: inner}
	if _, err := a.Chat(context.Background(), &memory.ChatRequest{Model: "m"}); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if inner.lastReq.ResponseFormat != nil {
		t.Errorf("ResponseFormat = %v, want nil (omitempty 序列化时省略)", inner.lastReq.ResponseFormat)
	}
}
