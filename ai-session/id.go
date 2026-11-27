package session

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ID is the stable, protocol-neutral identity of a session.
type ID string

const sessionPrefix = "session_"

var ErrInvalidID = errors.New("ai-session: invalid session ID")

// NewID generates a cryptographically random session identity in the same
// session_<lowercase hex> shape used by the existing persistent repository.
func NewID() (ID, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("ai-session: generate session ID: %w", err)
	}
	return ID(sessionPrefix + hex.EncodeToString(entropy[:])), nil
}

// ParseID validates and returns an existing canonical identity without
// changing its value. Both historical session_ and legacy_ IDs are accepted.
func ParseID(raw string) (ID, error) {
	id := ID(raw)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

// Validate rejects empty, path-like, or non-printable IDs without imposing a
// generator-specific format. Storage backends may impose additional restrictions.
func (id ID) Validate() error {
	raw := string(id)
	if raw == "" || raw == "." || raw == ".." || strings.TrimSpace(raw) != raw ||
		!utf8.ValidString(raw) || strings.ContainsAny(raw, `/\:*?"<>|`) || strings.IndexFunc(raw, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: ID must be a safe single path component", ErrInvalidID)
	}
	return nil
}

func (id ID) String() string { return string(id) }
