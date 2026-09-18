package agent

import (
	"context"
	"testing"

	"github.com/tinguo/goworker/ai-memory"
)

func TestLLMAdapter_JSONModePassesThrough(t *testing.T) {
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
	if !inner.lastReq.JSONMode {
		t.Error("JSONMode = false, want true")
	}
}

func TestLLMAdapter_NoJSONModePassesThrough(t *testing.T) {
	inner := &captureProvider{}
	a := &llmAdapter{inner: inner}
	if _, err := a.Chat(context.Background(), &memory.ChatRequest{Model: "m"}); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if inner.lastReq.JSONMode {
		t.Error("JSONMode = true, want false")
	}
}
