package service

import (
	"context"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/session"
)

const (
	firstCommitTitleTimeout = 15 * time.Second
	generatedTitleMaxRunes  = 16
)

// makeFirstCommitTitle returns the per-runtime callback that generates and stores
// a title after the first persisted user turn. It is installed with the concrete
// Store in mountRuntime, not on the shared base deps, so each session writes to
// its own file.
func (p *AgentPlugin) makeFirstCommitTitle(store *session.Store) func(string, func(string)) {
	return func(firstUserText string, notify func(string)) {
		firstUserText = strings.TrimSpace(firstUserText)
		if firstUserText == "" || store == nil || p.deps.NewProvider == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), firstCommitTitleTimeout)
		defer cancel()

		provider, err := p.deps.NewProvider(p.cfg)
		if err != nil {
			slog.Warn("agent: title provider unavailable", "err", err)
			return
		}
		response, err := provider.Chat(ctx, &core.ChatRequest{
			Model: provider.Model(),
			Messages: []core.Message{
				{
					Role:    "system",
					Content: "Generate a concise title for this chat. Return only the title, no quotes, no prefix, no explanation. Keep it within 16 characters when possible.",
				},
				{Role: "user", Content: firstUserText},
			},
		})
		if err != nil {
			slog.Warn("agent: title generation failed", "err", err)
			return
		}
		if len(response.Choices) == 0 {
			slog.Warn("agent: title generation returned no choices")
			return
		}
		title := normalizeGeneratedTitle(response.Choices[0].Message.Content)
		if title == "" {
			return
		}
		if err := store.SetTitle(title); err != nil {
			slog.Warn("agent: persist generated title failed", "err", err)
			return
		}
		if notify != nil {
			notify(title)
		}
	}
}

// normalizeGeneratedTitle makes the LLM response safe for a one-line sidebar
// label. It deliberately does not attempt to translate or rewrite the content.
func normalizeGeneratedTitle(raw string) string {
	title := strings.TrimSpace(strings.SplitN(raw, "\n", 2)[0])
	title = strings.TrimSpace(title)
	title = strings.TrimPrefix(title, "Title:")
	title = strings.TrimSpace(title)
	title = strings.Trim(title, "\"'`「」『』")
	title = strings.Join(strings.Fields(title), " ")
	if title == "" {
		return ""
	}
	if utf8.RuneCountInString(title) > generatedTitleMaxRunes {
		runes := []rune(title)
		title = string(runes[:generatedTitleMaxRunes])
	}
	return title
}
