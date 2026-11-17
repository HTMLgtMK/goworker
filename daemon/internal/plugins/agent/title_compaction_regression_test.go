package service

import (
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

func TestGenerateTitleUsesOriginalFirstTurnAfterCompaction(t *testing.T) {
	p, store := newTitleTestPlugin(t)
	wireTitleControl(t, p, store)
	p.AcquireControl(store.SessionID(), "client")
	if err := store.Compact("title-user-1", "title-assistant-1", "summary"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit([]core.Message{{MsgID: "later-user", Role: "user", Content: "a different question"}}); err != nil {
		t.Fatal(err)
	}
	provider := &titleTestProvider{response: &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Content: "title"}}}}}
	p.cfg = runtimeconfig.Default()
	p.providerFactory = func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil }
	if _, err := p.GenerateSessionTitle(store.SessionID(), "client"); err != nil {
		t.Fatal(err)
	}
	prompt := provider.request.Messages[1].Content
	if !strings.Contains(prompt, "What is the answer?") || !strings.Contains(prompt, "Here it is.") || strings.Contains(prompt, "a different question") {
		t.Fatalf("title prompt = %q", prompt)
	}
}
