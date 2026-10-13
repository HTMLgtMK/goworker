package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimesession "github.com/tinguo/goworker/ai-runtime/session"
)

type sessionManagerDeps struct {
	Root       string
	Persistent bool
	Migrate    bool
	BuildDeps  func(id, cwd string, store *runtimesession.Store) runtimeagent.SessionDeps
}

type managedSession struct {
	id    string
	agent *runtimeagent.Session
	store *runtimesession.Store

	mu     sync.RWMutex
	status runtimesession.SessionStatus
	cwd    string
	cwdErr error

	runGate chan struct{}
}

func (s *managedSession) statusSnapshot() runtimesession.SessionStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *managedSession) cwdSnapshot() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cwd
}

func (s *managedSession) cwdError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cwdErr
}

func (s *managedSession) setCWD(cwd string) {
	s.mu.Lock()
	s.cwd = cwd
	s.cwdErr = nil
	s.mu.Unlock()
	s.agent.SetCWD(cwd)
}

func (s *managedSession) setCWDError(err error) {
	s.mu.Lock()
	s.cwdErr = err
	s.mu.Unlock()
}

func (s *managedSession) run(ctx context.Context, fn func(*runtimeagent.Session) error) error {
	select {
	case s.runGate <- struct{}{}:
		defer func() { <-s.runGate }()
	case <-ctx.Done():
		return ctx.Err()
	}

	s.mu.RLock()
	status := s.status
	s.mu.RUnlock()
	if status == runtimesession.SessionStatusArchived {
		return fmt.Errorf("session %q is archived", s.id)
	}
	return fn(s.agent)
}

func (s *managedSession) close() error {
	s.runGate <- struct{}{}
	defer func() { <-s.runGate }()
	if s.store == nil {
		return nil
	}
	return s.store.Close()
}

// SessionManager owns runtime and persistence state for independently addressable sessions.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*managedSession
	current  string
	deps     sessionManagerDeps
}

func newSessionManager(deps sessionManagerDeps) (*SessionManager, error) {
	if deps.BuildDeps == nil {
		return nil, fmt.Errorf("session manager: missing dependency builder")
	}
	m := &SessionManager{sessions: make(map[string]*managedSession), deps: deps}
	if deps.Persistent {
		if deps.Root == "" {
			return nil, fmt.Errorf("session manager: persistent root is empty")
		}
		if deps.Migrate {
			if err := runtimesession.MigrateLegacy(deps.Root); err != nil {
				return nil, err
			}
		}
		if err := m.discover(); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *SessionManager) discover() error {
	files, err := runtimesession.ListSessionFiles(m.deps.Root)
	if err != nil {
		return err
	}
	var firstLive string
	for _, file := range files {
		managed, err := m.open(file.ID, "", false)
		if err != nil {
			slog.Warn("session manager: skip invalid session file", "id", file.ID, "err", err)
			continue
		}
		if firstLive == "" && managed.statusSnapshot() == runtimesession.SessionStatusLive {
			firstLive = managed.id
		}
	}
	m.mu.Lock()
	m.current = firstLive
	m.mu.Unlock()
	return nil
}

func (m *SessionManager) Sessions() []*managedSession {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*managedSession, 0, len(m.sessions))
	for _, managed := range m.sessions {
		out = append(out, managed)
	}
	return out
}

func (m *SessionManager) Create(cwd string) (*managedSession, error) {
	for attempt := 0; attempt < 8; attempt++ {
		id, err := newManagedSessionID()
		if err != nil {
			return nil, err
		}
		if _, ok := m.lookup(id); ok {
			continue
		}
		return m.open(id, cwd, true)
	}
	return nil, fmt.Errorf("session manager: unable to allocate unique session id")
}

func (m *SessionManager) CreateWithID(id, cwd string) (*managedSession, error) {
	if err := runtimesession.ValidateSessionID(id); err != nil {
		return nil, fmt.Errorf("session manager: %w", err)
	}
	if existing, ok := m.lookup(id); ok {
		return existing, nil
	}
	return m.open(id, cwd, true)
}

func (m *SessionManager) Load(id, cwd string) (*managedSession, error) {
	if err := runtimesession.ValidateSessionID(id); err != nil {
		return nil, fmt.Errorf("session manager: %w", err)
	}
	if existing, ok := m.lookup(id); ok {
		if existing.statusSnapshot() == runtimesession.SessionStatusLive {
			if cwd != "" {
				existing.setCWD(cwd)
			}
			m.selectSession(id)
		}
		return existing, nil
	}
	managed, err := m.open(id, "", false)
	if err != nil {
		return nil, err
	}
	if managed.statusSnapshot() == runtimesession.SessionStatusLive {
		if cwd != "" {
			managed.setCWD(cwd)
		}
		m.selectSession(id)
	}
	return managed, nil
}

func (m *SessionManager) lookup(id string) (*managedSession, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

func (m *SessionManager) selectSession(id string) {
	m.mu.Lock()
	m.current = id
	m.mu.Unlock()
}

func (m *SessionManager) Current() (*managedSession, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == "" {
		return nil, false
	}
	s, ok := m.sessions[m.current]
	return s, ok
}

func (m *SessionManager) open(id, cwd string, create bool) (*managedSession, error) {
	var store *runtimesession.Store
	if m.deps.Persistent {
		path, err := runtimesession.SessionPath(m.deps.Root, id)
		if err != nil {
			return nil, err
		}
		if !create {
			if _, err := os.Stat(path); os.IsNotExist(err) {
				return nil, fmt.Errorf("session manager: session %q not found", id)
			} else if err != nil {
				return nil, fmt.Errorf("session manager: stat session %q: %w", id, err)
			}
		}
		store, err = runtimesession.OpenPath(path)
		if err != nil {
			return nil, err
		}
	}

	deps := m.deps.BuildDeps(id, cwd, store)
	agent := runtimeagent.NewSession(deps)
	managed := &managedSession{
		id:      id,
		agent:   agent,
		store:   store,
		status:  runtimesession.SessionStatusLive,
		cwd:     cwd,
		runGate: make(chan struct{}, 1),
	}
	if store != nil {
		meta, ok := store.SessionMeta()
		if ok {
			if meta.ID != id {
				_ = store.Close()
				return nil, fmt.Errorf("session manager: file identity %q does not match %q", meta.ID, id)
			}
			if meta.Status != runtimesession.SessionStatusLive && meta.Status != runtimesession.SessionStatusArchived {
				_ = store.Close()
				return nil, fmt.Errorf("session manager: session %q has invalid status %q", id, meta.Status)
			}
			managed.status = meta.Status
		} else if create {
			if err := store.SetSessionMeta(id, runtimesession.SessionStatusLive, time.Now()); err != nil {
				_ = store.Close()
				return nil, err
			}
		} else {
			_ = store.Close()
			return nil, fmt.Errorf("session manager: session %q has no metadata", id)
		}
	}

	m.mu.Lock()
	if existing, ok := m.sessions[id]; ok {
		m.mu.Unlock()
		_ = managed.close()
		return existing, nil
	}
	m.sessions[id] = managed
	if m.current == "" {
		m.current = id
	}
	m.mu.Unlock()
	return managed, nil
}

func (s *managedSession) archive() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == runtimesession.SessionStatusArchived {
		return nil
	}
	if s.store != nil {
		if err := s.store.SetSessionMeta(s.id, runtimesession.SessionStatusArchived, time.Time{}); err != nil {
			return err
		}
	}
	s.status = runtimesession.SessionStatusArchived
	return nil
}

func (s *managedSession) restoreLive() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store != nil {
		if err := s.store.SetSessionMeta(s.id, runtimesession.SessionStatusLive, time.Time{}); err != nil {
			return err
		}
	}
	s.status = runtimesession.SessionStatusLive
	return nil
}

func (m *SessionManager) Archive(ctx context.Context, id string) error {
	managed, ok := m.lookup(id)
	if !ok {
		return fmt.Errorf("session manager: unknown session %q", id)
	}
	select {
	case managed.runGate <- struct{}{}:
		defer func() { <-managed.runGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := managed.archive(); err != nil {
		return err
	}
	return nil
}

func (m *SessionManager) CloseAll() error {
	m.mu.Lock()
	sessions := make([]*managedSession, 0, len(m.sessions))
	for _, managed := range m.sessions {
		sessions = append(sessions, managed)
	}
	m.mu.Unlock()

	var firstErr error
	for _, managed := range sessions {
		if err := managed.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func newManagedSessionID() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("session manager: generate id: %w", err)
	}
	return "sess_" + hex.EncodeToString(raw[:]), nil
}
