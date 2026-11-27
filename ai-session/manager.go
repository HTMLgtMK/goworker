package session

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrAlreadyExists = errors.New("ai-session: session already exists")
	ErrNotFound      = errors.New("ai-session: session not found")
	ErrIDMismatch    = errors.New("ai-session: storage returned a different session ID")
	ErrDuplicate     = errors.New("ai-session: storage returned duplicate session IDs")
	ErrClosed        = errors.New("ai-session: manager is closed")
	ErrInProgress    = errors.New("ai-session: session lifecycle operation is in progress")
	ErrNilContext    = errors.New("ai-session: nil context")
)

const defaultMaxIDAttempts = 8

// Storage is the injected persistence boundary. Create must be atomic: when it
// returns an error, it must not have created the session. Delete is reserved
// for rolling back a Create that returned nil; callers must not use it to clean
// up failed Create calls whose ownership is unknown.
type Storage interface {
	Create(context.Context, Session) error
	Load(context.Context, ID) (Session, error)
	Get(context.Context, ID) (Session, error)
	List(context.Context) ([]Session, error)
	Delete(context.Context, ID) error
}

// CreateOptions contains only protocol- and business-neutral session metadata.
type CreateOptions struct {
	Metadata map[string]string
}

// Initializer attaches optional host-owned state to a staged session. It runs
// after persistence and before publication. If it fails after partially
// attaching state, it must return a rollback function along with the error. It
// may also return a rollback function on success; the manager uses it only if
// the lifecycle transaction is canceled before publication. It must not call
// List; List waits for in-flight lifecycle transactions. Successful attachment
// lifetime remains owned by the host, not this manager.
type Initializer func(context.Context, Session) (rollback func() error, err error)

type idGenerator func() (ID, error)

type managerConfig struct {
	generateID    idGenerator
	initializer   Initializer
	maxIDAttempts int
}

// ManagerOption configures generic manager behavior. Options are applied only
// during construction and are not retained as mutable caller-owned state.
type ManagerOption func(*managerConfig)

// WithIDGenerator replaces secure random ID generation. It is primarily useful
// for deterministic integration tests and must return IDs accepted by ID.Validate.
func WithIDGenerator(generator func() (ID, error)) ManagerOption {
	return func(config *managerConfig) { config.generateID = generator }
}

// WithInitializer installs a host hook that runs between persistence and
// publication for both Create and Load.
func WithInitializer(initializer Initializer) ManagerOption {
	return func(config *managerConfig) { config.initializer = initializer }
}

// WithMaxIDAttempts bounds collision retries during Create. Values below one
// are rejected by NewSessionManager.
func WithMaxIDAttempts(attempts int) ManagerOption {
	return func(config *managerConfig) { config.maxIDAttempts = attempts }
}

type operation struct {
	done chan struct{}
}

// SessionManager is the single in-process authority for session lifecycle.
// Create/Load publish only complete snapshots; Get/List are read operations.
type SessionManager struct {
	storage     Storage
	generateID  idGenerator
	initializer Initializer
	maxAttempts int

	mu         sync.Mutex
	visibility sync.RWMutex
	closed     bool
	registry   map[ID]Session
	operations map[ID]*operation
	inFlight   sync.WaitGroup
}

// NewSessionManager constructs a manager around the caller-owned Storage.
// Close never closes or deletes the injected storage.
func NewSessionManager(storage Storage, options ...ManagerOption) (*SessionManager, error) {
	if storage == nil {
		return nil, errors.New("ai-session: storage is required")
	}
	config := managerConfig{
		generateID:    NewID,
		maxIDAttempts: defaultMaxIDAttempts,
	}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.generateID == nil {
		return nil, errors.New("ai-session: ID generator is required")
	}
	if config.maxIDAttempts < 1 {
		return nil, errors.New("ai-session: max ID attempts must be positive")
	}
	return &SessionManager{
		storage:     storage,
		generateID:  config.generateID,
		initializer: config.initializer,
		maxAttempts: config.maxIDAttempts,
		registry:    make(map[ID]Session),
		operations:  make(map[ID]*operation),
	}, nil
}

// Create reserves a manager-generated ID, persists it, initializes host state,
// then publishes it. Only persistent ID collisions are retried; all other
// storage and initializer errors are returned with their original cause.
func (m *SessionManager) Create(ctx context.Context, options CreateOptions) (Session, error) {
	if err := validateContext(ctx); err != nil {
		return Session{}, err
	}
	options.Metadata = cloneMetadata(options.Metadata)
	m.visibility.RLock()
	defer m.visibility.RUnlock()
	for attempt := 0; attempt < m.maxAttempts; attempt++ {
		id, err := m.generateID()
		if err != nil {
			return Session{}, fmt.Errorf("ai-session: generate ID: %w", err)
		}
		if err := id.Validate(); err != nil {
			return Session{}, fmt.Errorf("ai-session: generator returned invalid ID: %w", err)
		}
		finish, err := m.reserve(ctx, id, false)
		if errors.Is(err, ErrAlreadyExists) {
			continue
		}
		if err != nil {
			return Session{}, err
		}

		createdAt := time.Now().UTC()
		session, err := NewSession(id, createdAt, createdAt, options.Metadata)
		if err != nil {
			finish()
			return Session{}, fmt.Errorf("ai-session: construct session: %w", err)
		}
		if err := m.storage.Create(ctx, session); err != nil {
			finish()
			if errors.Is(err, ErrAlreadyExists) {
				continue
			}
			return Session{}, fmt.Errorf("ai-session: create %q in storage: %w", id, err)
		}
		rollback, err := m.initialize(ctx, session)
		if err != nil {
			return Session{}, m.rollbackCreate(ctx, id, rollback, err, finish)
		}
		if err := ctx.Err(); err != nil {
			return Session{}, m.rollbackCreate(ctx, id, rollback, err, finish)
		}
		m.publish(id, session)
		finish()
		return session, nil
	}
	return Session{}, fmt.Errorf("ai-session: create exhausted %d ID attempts: %w", m.maxAttempts, ErrAlreadyExists)
}

// Load resolves exactly id from Storage and publishes it only after optional
// host initialization succeeds. Concurrent loads of one ID converge on the
// already published snapshot. Failed loads leave persistence untouched.
func (m *SessionManager) Load(ctx context.Context, id ID) (Session, error) {
	if err := validateContext(ctx); err != nil {
		return Session{}, err
	}
	if err := id.Validate(); err != nil {
		return Session{}, err
	}
	m.visibility.RLock()
	defer m.visibility.RUnlock()
	finish, err := m.reserve(ctx, id, true)
	if err != nil {
		return Session{}, err
	}
	if finish == nil {
		m.mu.Lock()
		loaded := m.registry[id]
		m.mu.Unlock()
		return loaded, nil
	}
	defer finish()

	loaded, err := m.storage.Load(ctx, id)
	if err != nil {
		return Session{}, fmt.Errorf("ai-session: load %q from storage: %w", id, err)
	}
	if loaded.ID() != id {
		return Session{}, fmt.Errorf("ai-session: load %q: %w: got %q", id, ErrIDMismatch, loaded.ID())
	}
	if err := loaded.ID().Validate(); err != nil {
		return Session{}, fmt.Errorf("ai-session: storage returned invalid ID for %q: %w", id, err)
	}
	loaded = cloneSession(loaded)
	rollback, initErr := m.initialize(ctx, loaded)
	if initErr != nil {
		cleanupErr := rollbackAttachment(id, rollback)
		return Session{}, errors.Join(
			fmt.Errorf("ai-session: initialize loaded session %q: %w", id, initErr),
			cleanupErr,
		)
	}
	if err := ctx.Err(); err != nil {
		return Session{}, errors.Join(
			fmt.Errorf("ai-session: load %q: %w", id, err),
			rollbackAttachment(id, rollback),
		)
	}
	m.publish(id, loaded)
	return loaded, nil
}

// Get returns the authoritative in-process snapshot when present, otherwise it
// performs a read-only Storage.Get. It never initializes or publishes a session.
func (m *SessionManager) Get(ctx context.Context, id ID) (Session, error) {
	if err := validateContext(ctx); err != nil {
		return Session{}, err
	}
	if err := id.Validate(); err != nil {
		return Session{}, err
	}
	m.visibility.RLock()
	defer m.visibility.RUnlock()
	finish, registered, err := m.reserveGet(id)
	if err != nil {
		return Session{}, err
	}
	if registered != nil {
		return cloneSession(*registered), nil
	}
	defer finish()

	got, err := m.storage.Get(ctx, id)
	if err != nil {
		return Session{}, fmt.Errorf("ai-session: get %q from storage: %w", id, err)
	}
	if got.ID() != id {
		return Session{}, fmt.Errorf("ai-session: get %q: %w: got %q", id, ErrIDMismatch, got.ID())
	}
	if err := got.ID().Validate(); err != nil {
		return Session{}, fmt.Errorf("ai-session: storage returned invalid ID for %q: %w", id, err)
	}
	return cloneSession(got), nil
}

// AbandonCreated removes only the exact immutable snapshot returned by Create.
// The visibility write lock fences Create, Load, Get, and List while storage and
// registry state are withdrawn as one manager transaction. It is for immediate
// rollback by the creator, not general session deletion. If Delete reports an error,
// storage is queried to distinguish an intact session from a delete that committed
// before its cleanup error was returned.
func (m *SessionManager) AbandonCreated(ctx context.Context, created Session) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	id := created.ID()
	if err := id.Validate(); err != nil {
		return err
	}

	// Admission and WaitGroup registration share m.mu with Close's transition to
	// closed, so Close cannot race Wait with a later Add.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	m.inFlight.Add(1)
	m.mu.Unlock()
	defer m.inFlight.Done()

	m.visibility.Lock()
	defer m.visibility.Unlock()

	m.mu.Lock()
	registered, ok := m.registry[id]
	m.mu.Unlock()
	if !ok || !sameSessionSnapshot(registered, created) {
		return fmt.Errorf("ai-session: abandon %q: %w", id, ErrNotFound)
	}
	if err := m.storage.Delete(ctx, id); err != nil {
		// A Storage may report a cleanup error after the durable row was already
		// removed. Reconcile before retaining the in-memory snapshot so retries
		// cannot leave a registered session with no persisted backing.
		persisted, lookupErr := m.storage.Get(context.WithoutCancel(ctx), id)
		if errors.Is(lookupErr, ErrNotFound) {
			m.mu.Lock()
			delete(m.registry, id)
			m.mu.Unlock()
			return nil
		}
		if lookupErr != nil {
			return errors.Join(
				fmt.Errorf("ai-session: abandon created session %q from storage: %w", id, err),
				fmt.Errorf("ai-session: verify session %q after delete failure: %w", id, lookupErr),
			)
		}
		if persisted.ID() != id {
			return fmt.Errorf("ai-session: abandon created session %q from storage: %w (verification returned ID %q)", id, err, persisted.ID())
		}
		return fmt.Errorf("ai-session: abandon created session %q from storage: %w", id, err)
	}
	m.mu.Lock()
	delete(m.registry, id)
	m.mu.Unlock()
	return nil
}

func sameSessionSnapshot(left, right Session) bool {
	if left.id != right.id || !left.createdAt.Equal(right.createdAt) || !left.updatedAt.Equal(right.updatedAt) || len(left.metadata) != len(right.metadata) {
		return false
	}
	for key, value := range left.metadata {
		if got, exists := right.metadata[key]; !exists || got != value {
			return false
		}
	}
	return true
}

// List returns a stable ID-sorted union of persisted sessions and the manager's
// registered snapshots. A storage error or malformed/duplicate record fails the
// whole query; partial results are never presented as complete.
func (m *SessionManager) List(ctx context.Context) ([]Session, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	m.visibility.Lock()
	defer m.visibility.Unlock()
	persisted, err := m.storage.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("ai-session: list storage: %w", err)
	}
	byID := make(map[ID]Session, len(persisted))
	for _, session := range persisted {
		id := session.ID()
		if err := id.Validate(); err != nil {
			return nil, fmt.Errorf("ai-session: list storage returned invalid session: %w", err)
		}
		if _, exists := byID[id]; exists {
			return nil, fmt.Errorf("ai-session: list storage: %w: %q", ErrDuplicate, id)
		}
		byID[id] = cloneSession(session)
	}

	m.mu.Lock()
	for id, session := range m.registry {
		byID[id] = cloneSession(session)
	}
	m.mu.Unlock()

	ids := make([]ID, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]Session, 0, len(ids))
	for _, id := range ids {
		result = append(result, byID[id])
	}
	return result, nil
}

// Close freezes lifecycle mutations and waits for transactions already in
// progress. Read-only Get/List remain available; persistence and successfully
// attached host resources remain caller-owned and are not deleted or closed.
func (m *SessionManager) Close() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.inFlight.Wait()
}

func (m *SessionManager) reserveGet(id ID) (func(), *Session, error) {
	m.mu.Lock()
	if registered, exists := m.registry[id]; exists {
		m.mu.Unlock()
		return nil, &registered, nil
	}
	if _, exists := m.operations[id]; exists {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("ai-session: get %q: %w", id, ErrInProgress)
	}
	current := &operation{done: make(chan struct{})}
	m.operations[id] = current
	m.mu.Unlock()

	var once sync.Once
	finish := func() {
		once.Do(func() {
			m.mu.Lock()
			delete(m.operations, id)
			close(current.done)
			m.mu.Unlock()
		})
	}
	return finish, nil, nil
}

func (m *SessionManager) reserve(ctx context.Context, id ID, returnRegistered bool) (func(), error) {
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, ErrClosed
		}
		if _, exists := m.registry[id]; exists {
			m.mu.Unlock()
			if returnRegistered {
				return nil, nil
			}
			return nil, ErrAlreadyExists
		}
		if current, exists := m.operations[id]; exists {
			done := current.done
			m.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, fmt.Errorf("ai-session: wait for session %q operation: %w", id, ctx.Err())
			}
		}
		current := &operation{done: make(chan struct{})}
		m.operations[id] = current
		m.inFlight.Add(1)
		m.mu.Unlock()

		var once sync.Once
		return func() {
			once.Do(func() {
				m.mu.Lock()
				delete(m.operations, id)
				close(current.done)
				m.mu.Unlock()
				m.inFlight.Done()
			})
		}, nil
	}
}

func (m *SessionManager) initialize(ctx context.Context, session Session) (func() error, error) {
	if m.initializer == nil {
		return nil, nil
	}
	rollback, err := m.initializer(ctx, cloneSession(session))
	if err != nil {
		return rollback, fmt.Errorf("host initializer: %w", err)
	}
	return rollback, nil
}

func (m *SessionManager) rollbackCreate(ctx context.Context, id ID, rollback func() error, cause error, finish func()) error {
	rollbackErr := rollbackAttachment(id, rollback)
	if err := m.storage.Delete(context.WithoutCancel(ctx), id); err != nil {
		rollbackErr = errors.Join(rollbackErr, fmt.Errorf("ai-session: rollback storage create for %q: %w", id, err))
	}
	finish()
	return errors.Join(fmt.Errorf("ai-session: initialize new session %q: %w", id, cause), rollbackErr)
}

func rollbackAttachment(id ID, rollback func() error) error {
	if rollback == nil {
		return nil
	}
	if err := rollback(); err != nil {
		return fmt.Errorf("ai-session: rollback host attachment for %q: %w", id, err)
	}
	return nil
}

func (m *SessionManager) publish(id ID, session Session) {
	m.mu.Lock()
	m.registry[id] = cloneSession(session)
	m.mu.Unlock()
}

func cloneSession(session Session) Session {
	session.metadata = cloneMetadata(session.metadata)
	return session
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("ai-session: context: %w", err)
	}
	return nil
}
