package session

import (
	"context"
	"errors"
	"fmt"

	ai_session "github.com/tinguo/goworker/ai-session"
)

// aiSessionCWDMetadataKey is reserved by the adapter for the declared workspace.
// A non-empty key creates a workspace record; on reads, that record wins over a
// conflicting metadata value. Workspace authorization remains a host concern.
const aiSessionCWDMetadataKey = "cwd"

// AIStorageAdapter adapts the existing JSONL Repository to ai-session.Storage.
// It is the only persistence bridge; repository files remain the source of truth.
// Metadata["cwd"] remains opaque metadata here: the host must validate it first,
// then explicitly append a workspace declaration through Repository/Store.
type AIStorageAdapter struct {
	repository *Repository
}

// NewAIStorageAdapter returns the ai-session Storage adapter for repository.
func NewAIStorageAdapter(repository *Repository) *AIStorageAdapter {
	return &AIStorageAdapter{repository: repository}
}

var _ ai_session.Storage = (*AIStorageAdapter)(nil)

func (a *AIStorageAdapter) Create(ctx context.Context, value ai_session.Session) error {
	if a == nil || a.repository == nil {
		return errors.New("session: ai-session storage requires a repository")
	}
	if ctx == nil {
		return errors.New("session: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := value.ID().Validate(); err != nil {
		return err
	}
	metadata := value.Metadata()
	_, err := a.repository.CreateWithID(ctx, value.ID().String(), CreateOptions{
		CreatedAt: value.CreatedAt(),
		UpdatedAt: value.UpdatedAt(),
		Metadata:  metadata,
	})
	if err != nil {
		if errors.Is(err, ErrSessionAlreadyExists) {
			return fmt.Errorf("ai-session: %w", ai_session.ErrAlreadyExists)
		}
		return fmt.Errorf("session: persist ai-session %q: %w", value.ID(), err)
	}
	if err := a.repository.CloseSession(value.ID().String()); err != nil {
		cleanupErr := a.repository.Delete(context.WithoutCancel(ctx), value.ID().String())
		return errors.Join(
			fmt.Errorf("session: close newly created ai-session %q: %w", value.ID(), err),
			wrapCleanupError(cleanupErr),
		)
	}
	return nil
}

func (a *AIStorageAdapter) Load(ctx context.Context, id ai_session.ID) (ai_session.Session, error) {
	if err := validateAIStorageRequest(ctx, a, id); err != nil {
		return ai_session.Session{}, err
	}
	store, err := a.repository.Open(id.String())
	if err != nil {
		return ai_session.Session{}, mapRepositoryError("load", id, err)
	}
	return aiSessionValue(id, store.Metadata(), store.Workspace())
}

func (a *AIStorageAdapter) Get(ctx context.Context, id ai_session.ID) (ai_session.Session, error) {
	if err := validateAIStorageRequest(ctx, a, id); err != nil {
		return ai_session.Session{}, err
	}
	items, err := a.repository.List(ctx)
	if err != nil {
		return ai_session.Session{}, fmt.Errorf("session: list for ai-session get %q: %w", id, err)
	}
	for _, item := range items {
		if item.SessionID == id.String() {
			return aiSessionValue(id, item, item.Workspace)
		}
	}
	return ai_session.Session{}, fmt.Errorf("ai-session: get %q: %w", id, ai_session.ErrNotFound)
}

func (a *AIStorageAdapter) List(ctx context.Context) ([]ai_session.Session, error) {
	if a == nil || a.repository == nil {
		return nil, errors.New("session: ai-session storage requires a repository")
	}
	if ctx == nil {
		return nil, errors.New("session: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items, err := a.repository.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("session: list ai-sessions: %w", err)
	}
	result := make([]ai_session.Session, 0, len(items))
	for _, item := range items {
		id, err := ai_session.ParseID(item.SessionID)
		if err != nil {
			return nil, fmt.Errorf("session: list returned invalid ai-session ID %q: %w", item.SessionID, err)
		}
		value, err := aiSessionValue(id, item, item.Workspace)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (a *AIStorageAdapter) Delete(ctx context.Context, id ai_session.ID) error {
	if err := validateAIStorageRequest(ctx, a, id); err != nil {
		return err
	}
	if err := a.repository.CloseSession(id.String()); err != nil {
		return fmt.Errorf("session: close ai-session %q before delete: %w", id, err)
	}
	if err := a.repository.Delete(ctx, id.String()); err != nil {
		return mapRepositoryError("delete", id, err)
	}
	return nil
}

func validateAIStorageRequest(ctx context.Context, adapter *AIStorageAdapter, id ai_session.ID) error {
	if adapter == nil || adapter.repository == nil {
		return errors.New("session: ai-session storage requires a repository")
	}
	if ctx == nil {
		return errors.New("session: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return id.Validate()
}

func aiSessionValue(id ai_session.ID, persisted Metadata, cwd string) (ai_session.Session, error) {
	if persisted.SessionID != id.String() {
		return ai_session.Session{}, fmt.Errorf("session: repository returned ID %q for %q", persisted.SessionID, id)
	}
	metadata := cloneStringMap(persisted.Values)
	if cwd != "" {
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[aiSessionCWDMetadataKey] = cwd
	}
	return ai_session.NewSession(id, persisted.CreatedAt, persisted.UpdatedAt, metadata)
}

func mapRepositoryError(operation string, id ai_session.ID, err error) error {
	if errors.Is(err, ErrSessionNotFound) {
		return fmt.Errorf("ai-session: %s %q: %w", operation, id, ai_session.ErrNotFound)
	}
	if errors.Is(err, ErrSessionAlreadyExists) {
		return fmt.Errorf("ai-session: %s %q: %w", operation, id, ai_session.ErrAlreadyExists)
	}
	return fmt.Errorf("session: %s ai-session %q: %w", operation, id, err)
}

func wrapCleanupError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("session: cleanup newly created ai-session: %w", err)
}
