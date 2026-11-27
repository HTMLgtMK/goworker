package session

import "time"

// Session is an immutable generic session identity and metadata snapshot.
// Its fields are private; Metadata always returns a defensive copy.
type Session struct {
	id        ID
	createdAt time.Time
	updatedAt time.Time
	metadata  map[string]string
}

// NewSession constructs a session value for storage adapters and callers that
// restore persisted snapshots. It never changes or remints the supplied ID.
func NewSession(id ID, createdAt, updatedAt time.Time, metadata map[string]string) (Session, error) {
	if err := id.Validate(); err != nil {
		return Session{}, err
	}
	return Session{
		id:        id,
		createdAt: createdAt,
		updatedAt: updatedAt,
		metadata:  cloneMetadata(metadata),
	}, nil
}

func (s Session) ID() ID                      { return s.id }
func (s Session) CreatedAt() time.Time        { return s.createdAt }
func (s Session) UpdatedAt() time.Time        { return s.updatedAt }
func (s Session) Metadata() map[string]string { return cloneMetadata(s.metadata) }

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	copy := make(map[string]string, len(metadata))
	for key, value := range metadata {
		copy[key] = value
	}
	return copy
}
