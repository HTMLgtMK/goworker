package middlewares

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
)

const (
	TitleGenerationTimeout = 15 * time.Second
	TitleInputLimitRunes   = 2000
	TitleMaxRunes          = 80
)

type TitleStore interface {
	Title() session.TitleState
	AppendTitle(title string) (session.TitleState, error)
}

type TitleMiddleware struct {
	store    TitleStore
	once     *sync.Once
	provider core.Provider
	cfg      *runtimeconfig.Config
	publish  func(event string, data any)
}

func NewTitleMiddleware(store TitleStore, once *sync.Once, provider core.Provider, cfg *runtimeconfig.Config, publish func(event string, data any)) *TitleMiddleware {
	return &TitleMiddleware{store: store, once: once, provider: provider, cfg: cfg, publish: publish}
}

func (m *TitleMiddleware) Name() string { return "title" }

func (m *TitleMiddleware) OnAfterAgent(ev *core.AfterAgentEvent) *core.MiddlewareResponse {
	if m.store == nil || m.once == nil || m.provider == nil || ev == nil {
		return nil
	}
	user, assistant, ok := firstTurnMaterial(ev.History)
	if !ok || m.store.Title().Title != "" {
		return nil
	}
	m.once.Do(func() { go m.generate(user, assistant) })
	return nil
}

func (m *TitleMiddleware) generate(user, assistant string) {
	ctx, cancel := context.WithTimeout(context.Background(), TitleGenerationTimeout)
	defer cancel()
	title, err := GenerateTitle(ctx, m.provider, m.cfg, user, assistant)
	if err != nil {
		slog.Warn("agent: session title generation failed", "err", err)
		return
	}
	state, err := m.store.AppendTitle(title)
	if err != nil {
		slog.Warn("agent: persist generated session title failed", "err", err)
		return
	}
	if m.publish != nil {
		m.publish(runtimeconfig.EventSessionTitleUpdated, session.TitleUpdated{Title: state.Title, TitleVersion: state.Version, Source: "generated"})
	}
}

func GenerateTitle(ctx context.Context, provider core.Provider, cfg *runtimeconfig.Config, user, assistant string) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("agent: title generation has no run config")
	}
	if provider == nil {
		return "", fmt.Errorf("agent: title provider is unavailable")
	}
	response, err := provider.Chat(ctx, &core.ChatRequest{
		Model: cfg.LLM.Current.Model,
		Messages: []core.Message{
			{Role: "system", Content: "Create a concise session title in the user's language. Return only the title, with no quotes or explanation. Keep it under 8 words."},
			{Role: "user", Content: "User message:\n" + limitTitleRunes(user, TitleInputLimitRunes) + "\n\nAssistant reply:\n" + limitTitleRunes(assistant, TitleInputLimitRunes)},
		},
	})
	if err != nil {
		return "", fmt.Errorf("agent: generate session title: %w", err)
	}
	if response == nil || len(response.Choices) == 0 {
		return "", fmt.Errorf("agent: title provider returned no choices")
	}
	title := strings.Trim(collapseSpaces(response.Choices[0].Message.Content), "\"'`“”‘’")
	title = strings.TrimSpace(title)
	if title == "" {
		return "", fmt.Errorf("agent: title provider returned an empty title")
	}
	return limitTitleRunes(title, TitleMaxRunes), nil
}

func firstTurnMaterial(history []core.Message) (firstUser, assistant string, ok bool) {
	userIndex := -1
	for i, msg := range history {
		if msg.Role == "user" && strings.TrimSpace(msg.Content) != "" {
			userIndex = i
			break
		}
	}
	if userIndex < 0 {
		return "", "", false
	}
	var replies []string
	for _, msg := range history[userIndex+1:] {
		if msg.Role == "user" {
			break
		}
		if msg.Role == "assistant" && strings.TrimSpace(msg.Content) != "" {
			replies = append(replies, msg.Content)
		}
	}
	return history[userIndex].Content, strings.Join(replies, "\n"), true
}

func collapseSpaces(s string) string { return strings.Join(strings.Fields(s), " ") }

func limitTitleRunes(value string, max int) string {
	runes := []rune(value)
	if max <= 0 || len(runes) <= max {
		return value
	}
	return string(runes[:max])
}
