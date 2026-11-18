package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/middlewares"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

func (p *AgentPlugin) titleOnceFor(sessionID string) *sync.Once {
	once, _ := p.titleAttempts.LoadOrStore(sessionID, &sync.Once{})
	return once.(*sync.Once)
}

type generatedTitleStore struct {
	plugin    *AgentPlugin
	sessionID string
}

func (s *generatedTitleStore) Title() session.TitleState {
	p := s.plugin
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if p.inFlight.isClosed() {
		return session.TitleState{}
	}
	if store := p.storeFor(p.RouteSession(s.sessionID)); store != nil {
		return store.Title()
	}
	return session.TitleState{}
}

func (s *generatedTitleStore) AppendTitle(title string) (session.TitleState, error) {
	p := s.plugin
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if p.inFlight.isClosed() {
		return session.TitleState{}, errAgentStopping
	}
	if p.repo == nil {
		return session.TitleState{}, fmt.Errorf("agent: session persistence disabled")
	}

	store := p.storeFor(p.RouteSession(s.sessionID))
	owned := false
	if store == nil {
		if cached, ok := p.repo.Peek(s.sessionID); ok {
			store = cached
		} else {
			var err error
			store, err = p.repo.Open(s.sessionID)
			if err != nil {
				return session.TitleState{}, fmt.Errorf("agent: open session %s for generated title: %w", s.sessionID, err)
			}
			owned = true
		}
	}
	if owned {
		defer func() {
			if err := p.repo.CloseSession(s.sessionID); err != nil {
				slog.Warn("agent: release generated title store failed", "session", s.sessionID, "err", err)
			}
		}()
	}
	return store.AppendTitle(title)
}

func (p *AgentPlugin) GenerateSessionTitle(sessionID, clientID string) (string, error) {
	if p.repo == nil {
		return "", fmt.Errorf("agent: session persistence disabled")
	}
	if p.control == nil {
		return "", fmt.Errorf("agent: generate session title: control plane unavailable")
	}
	var firstUser, assistant string
	p.lifecycleMu.Lock()
	err := p.control.WithControl(sessionID, clientID, func() error {
		store, release, err := p.titleStoreFor(sessionID)
		if err != nil {
			return err
		}
		defer release()
		firstUser, assistant = titleMaterial(store.OriginalMessages())
		return nil
	})
	p.lifecycleMu.Unlock()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(firstUser) == "" {
		return "", fmt.Errorf("agent: session %q has no first user message to title", sessionID)
	}

	cfg := p.configSnapshot()
	if cfg == nil {
		return "", fmt.Errorf("agent: title configuration is unavailable")
	}
	if p.providerFactory == nil {
		return "", fmt.Errorf("agent: title provider is unavailable")
	}
	provider, err := p.providerFactory(cfg)
	if err != nil {
		return "", fmt.Errorf("agent: create title provider: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), middlewares.TitleGenerationTimeout)
	defer cancel()
	title, err := middlewares.GenerateTitle(ctx, provider, cfg, firstUser, assistant)
	if err != nil {
		return "", fmt.Errorf("agent: generate session title: %w", err)
	}
	if err := p.SetSessionTitle(sessionID, clientID, title); err != nil {
		return "", err
	}
	return title, nil
}

// titleStoreFor requires lifecycleMu and returns a release function for the acquired store.
func (p *AgentPlugin) titleStoreFor(sessionID string) (*session.Store, func(), error) {
	if p.inFlight.isClosed() {
		return nil, nil, errAgentStopping
	}
	rt := p.RouteSession(sessionID)
	if store := p.storeFor(rt); store != nil {
		return store, func() {}, nil
	}
	if store, ok := p.repo.Peek(sessionID); ok {
		return store, func() {}, nil
	}
	store, err := p.repo.Open(sessionID)
	if err != nil {
		if errors.Is(err, session.ErrSessionNotFound) {
			return nil, nil, boundSessionUnknownErr(sessionID)
		}
		return nil, nil, fmt.Errorf("agent: open session %s for title generation: %w", sessionID, err)
	}
	return store, func() {
		if err := p.repo.CloseSession(sessionID); err != nil {
			slog.Warn("agent: release title generation store failed", "session", sessionID, "err", err)
		}
	}, nil
}

func titleMaterial(messages []core.Message) (firstUser, assistant string) {
	first, ok := firstUserMessage(messages)
	if !ok {
		return "", ""
	}
	var replies []string
	seen := false
	for _, message := range messages {
		if message.Role == "user" {
			if seen {
				break
			}
			if strings.TrimSpace(message.Content) != "" {
				seen = true
			}
			continue
		}
		if seen && message.Role == "assistant" && strings.TrimSpace(message.Content) != "" {
			replies = append(replies, message.Content)
		}
	}
	return first, strings.Join(replies, "\n")
}

func firstUserMessage(messages []core.Message) (string, bool) {
	for _, message := range messages {
		if message.Role == "user" && strings.TrimSpace(message.Content) != "" {
			return message.Content, true
		}
	}
	return "", false
}

func (p *AgentPlugin) GetSessionTitle(sessionID string) (model.SessionTitleSnapshot, error) {
	if p.repo == nil {
		return model.SessionTitleSnapshot{}, fmt.Errorf("agent: session persistence disabled")
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	store, release, err := p.titleStoreFor(sessionID)
	if err != nil {
		return model.SessionTitleSnapshot{}, err
	}
	defer release()
	state, first := store.TitleSnapshot()
	title := state.Title
	if title == "" {
		title = truncateTitle(first)
	}
	return model.SessionTitleSnapshot{Title: title, TitleVersion: state.Version}, nil
}

func (p *AgentPlugin) SetSessionTitle(sessionID, clientID, title string) error {
	title = strings.TrimSpace(title)
	if title == "" {
		return fmt.Errorf("agent: session title is empty")
	}
	if len([]rune(title)) > middlewares.TitleMaxRunes {
		return fmt.Errorf("agent: session title exceeds %d characters", middlewares.TitleMaxRunes)
	}
	if p.repo == nil {
		return fmt.Errorf("agent: session persistence disabled")
	}
	if p.control == nil {
		return fmt.Errorf("agent: set session title: control plane unavailable")
	}

	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	st, release, err := p.titleStoreFor(sessionID)
	if err != nil {
		return err
	}
	defer release()
	var state session.TitleState
	if err := p.control.WithControl(sessionID, clientID, func() error {
		var err error
		state, err = st.AppendTitle(title)
		return err
	}); err != nil {
		return err
	}
	p.sessionEventPublisher(sessionID)(runtimeconfig.EventSessionTitleUpdated, session.TitleUpdated{Title: state.Title, TitleVersion: state.Version, Source: "manual"})
	return nil
}
