package session

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAbandonCreatedDeletesOnlyOwnedSnapshotAndRevokesRegistry(t *testing.T) {
	store := newMemoryStorage()
	manager := newManager(t, store, WithIDGenerator(func() (ID, error) { return testID(90), nil }))
	created, err := manager.Create(context.Background(), CreateOptions{Metadata: map[string]string{"origin": "rollback"}})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}

	unowned, err := NewSession(created.ID(), created.CreatedAt(), created.UpdatedAt(), map[string]string{"origin": "other"})
	if err != nil {
		t.Fatalf("NewSession(unowned): %v", err)
	}
	if err := manager.AbandonCreated(context.Background(), unowned); !errors.Is(err, ErrNotFound) {
		t.Fatalf("AbandonCreated(unowned) = %v, want ErrNotFound", err)
	}
	if _, ok := store.snapshot(created.ID()); !ok {
		t.Fatal("unowned snapshot removed persisted session")
	}

	if err := manager.AbandonCreated(context.Background(), created); err != nil {
		t.Fatalf("AbandonCreated(created): %v", err)
	}
	if _, ok := store.snapshot(created.ID()); ok {
		t.Fatal("persisted session remains after abandon")
	}
	if _, err := manager.Get(context.Background(), created.ID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get() after abandon = %v, want ErrNotFound", err)
	}
}

func TestAbandonCreatedDeleteFailureRetainsRegistryForRetry(t *testing.T) {
	store := newMemoryStorage()
	manager := newManager(t, store, WithIDGenerator(func() (ID, error) { return testID(91), nil }))
	created, err := manager.Create(context.Background(), CreateOptions{})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}

	deleteErr := errors.New("injected storage delete failure")
	store.mu.Lock()
	store.deleteErr = deleteErr
	store.mu.Unlock()
	if err := manager.AbandonCreated(context.Background(), created); !errors.Is(err, deleteErr) {
		t.Fatalf("AbandonCreated() = %v, want injected delete error", err)
	}
	if got, err := manager.Get(context.Background(), created.ID()); err != nil || got.ID() != created.ID() {
		t.Fatalf("Get() after failed abandon = (%v, %v), want created session", got.ID(), err)
	}
	if _, ok := store.snapshot(created.ID()); !ok {
		t.Fatal("storage delete failure removed persisted session")
	}

	store.mu.Lock()
	store.deleteErr = nil
	store.mu.Unlock()
	if err := manager.AbandonCreated(context.Background(), created); err != nil {
		t.Fatalf("AbandonCreated() retry: %v", err)
	}
	if _, err := manager.Get(context.Background(), created.ID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get() after retry = %v, want ErrNotFound", err)
	}
}

func TestAbandonCreatedReconcilesDeleteFailureAfterDurableRemoval(t *testing.T) {
	store := newMemoryStorage()
	deleteErr := errors.New("injected post-delete cleanup failure")
	manager := newManager(t, &deleteThenFailStorage{delegate: store, err: deleteErr}, WithIDGenerator(func() (ID, error) { return testID(92), nil }))
	created, err := manager.Create(context.Background(), CreateOptions{})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}

	if err := manager.AbandonCreated(context.Background(), created); err != nil {
		t.Fatalf("AbandonCreated() after committed delete: %v", err)
	}
	if _, err := manager.Get(context.Background(), created.ID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get() after committed delete = %v, want ErrNotFound", err)
	}
	if _, ok := store.snapshot(created.ID()); ok {
		t.Fatal("durable session remains after delete-then-error storage")
	}
}

type deleteThenFailStorage struct {
	delegate Storage
	err      error
}

func (s *deleteThenFailStorage) Create(ctx context.Context, value Session) error {
	return s.delegate.Create(ctx, value)
}

func (s *deleteThenFailStorage) Load(ctx context.Context, id ID) (Session, error) {
	return s.delegate.Load(ctx, id)
}

func (s *deleteThenFailStorage) Get(ctx context.Context, id ID) (Session, error) {
	return s.delegate.Get(ctx, id)
}

func (s *deleteThenFailStorage) List(ctx context.Context) ([]Session, error) {
	return s.delegate.List(ctx)
}

func (s *deleteThenFailStorage) Delete(ctx context.Context, id ID) error {
	if err := s.delegate.Delete(ctx, id); err != nil {
		return err
	}
	return s.err
}

func TestParseIDAcceptsPersistedSessionShapes(t *testing.T) {
	for _, raw := range []string{
		"session_0123456789abcdef",
		"session_0123456789abcdef0123456789abcdef",
		"legacy_0123456789abcdef",
		"other_0123456789ABCDEF",
		"session_abc",
		"session_",
	} {
		t.Run(raw, func(t *testing.T) {
			got, err := ParseID(raw)
			if err != nil {
				t.Fatalf("ParseID(%q): %v", raw, err)
			}
			if string(got) != raw {
				t.Fatalf("ParseID(%q) = %q", raw, got)
			}
		})
	}
}

func TestParseIDRejectsInvalidIdentityAndPath(t *testing.T) {
	for _, raw := range []string{
		"", ".", "..", "session_../escape", "session_a/b", "session_a\\b",
		" session_0123456789abcdef", "abc\x00def", "abc\ndef", "C:escape",
	} {
		t.Run(fmt.Sprintf("%q", raw), func(t *testing.T) {
			if _, err := ParseID(raw); !errors.Is(err, ErrInvalidID) {
				t.Fatalf("ParseID(%q) error = %v, want ErrInvalidID", raw, err)
			}
		})
	}
}

func TestNewIDProducesValidUniqueIDs(t *testing.T) {
	ids := make(map[ID]struct{}, 128)
	for range 128 {
		id, err := NewID()
		if err != nil {
			t.Fatalf("NewID(): %v", err)
		}
		if err := id.Validate(); err != nil {
			t.Fatalf("NewID() returned invalid ID %q: %v", id, err)
		}
		if _, exists := ids[id]; exists {
			t.Fatalf("NewID() returned duplicate %q", id)
		}
		ids[id] = struct{}{}
	}
}

func TestSessionCopiesMetadataAtBoundaries(t *testing.T) {
	input := map[string]string{"source": "test"}
	s, err := NewSession(testID(1), time.Unix(10, 0), time.Unix(20, 0), input)
	if err != nil {
		t.Fatalf("NewSession(): %v", err)
	}
	input["source"] = "mutated"
	gotMetadata := s.Metadata()
	if gotMetadata["source"] != "test" {
		t.Fatalf("metadata changed through constructor input: %v", gotMetadata)
	}
	gotMetadata["source"] = "mutated again"
	if s.Metadata()["source"] != "test" {
		t.Fatalf("metadata changed through accessor: %v", s.Metadata())
	}
	if s.ID() != testID(1) || !s.CreatedAt().Equal(time.Unix(10, 0)) || !s.UpdatedAt().Equal(time.Unix(20, 0)) {
		t.Fatalf("unexpected immutable session value: %+v", s)
	}
}

func TestCreatePersistsAndPublishesOnlyAfterInitializer(t *testing.T) {
	store := newMemoryStorage()
	id := testID(10)
	var manager *SessionManager
	initializerCalls := 0
	manager = newManager(t, store,
		WithIDGenerator(func() (ID, error) { return id, nil }),
		WithInitializer(func(ctx context.Context, created Session) (func() error, error) {
			initializerCalls++
			if _, ok := store.snapshot(created.ID()); !ok {
				t.Fatal("initializer ran before persistent create")
			}
			if _, err := manager.Get(context.Background(), created.ID()); err == nil {
				t.Fatal("session visible before initializer completed")
			}
			return nil, nil
		}),
	)

	got, err := manager.Create(context.Background(), CreateOptions{Metadata: map[string]string{"origin": "test"}})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	if got.ID() != id {
		t.Fatalf("Create() ID = %q, want %q", got.ID(), id)
	}
	if initializerCalls != 1 {
		t.Fatalf("initializer called %d times, want 1", initializerCalls)
	}
	registered, err := manager.Get(context.Background(), id)
	if err != nil || registered.ID() != got.ID() {
		t.Fatalf("Get(%q) = (%+v, %v), want created session", id, registered, err)
	}
	stored, ok := store.snapshot(id)
	if !ok || stored.Metadata()["origin"] != "test" {
		t.Fatalf("persisted session = (%+v, %t), want metadata", stored, ok)
	}
}

func TestCreateInitializerFailureRollsBackNewStoreAndAttachment(t *testing.T) {
	store := newMemoryStorage()
	id := testID(11)
	initErr := errors.New("host attachment failed")
	var detached atomic.Bool
	manager := newManager(t, store,
		WithIDGenerator(func() (ID, error) { return id, nil }),
		WithInitializer(func(context.Context, Session) (func() error, error) {
			return func() error { detached.Store(true); return nil }, initErr
		}),
	)

	got, err := manager.Create(context.Background(), CreateOptions{})
	if !errors.Is(err, initErr) {
		t.Fatalf("Create() error = %v, want wrapped initializer error", err)
	}
	if got.ID() != "" {
		t.Fatalf("failed Create() returned session %q", got.ID())
	}
	if _, ok := store.snapshot(id); ok {
		t.Fatal("failed create left a newly persisted session behind")
	}
	if _, err := manager.Get(context.Background(), id); err == nil {
		t.Fatal("failed create published the session")
	}
	if !detached.Load() {
		t.Fatal("initializer rollback was not called")
	}
	if store.deleteCount() != 1 {
		t.Fatalf("Delete called %d times, want exactly once", store.deleteCount())
	}
}

func TestCreateStorageErrorDoesNotDeletePreexistingSession(t *testing.T) {
	store := newMemoryStorage()
	preexisting := mustSession(t, testID(12), nil)
	store.seed(preexisting)
	manager := newManager(t, store,
		WithIDGenerator(func() (ID, error) { return preexisting.ID(), nil }),
		WithMaxIDAttempts(2),
	)

	_, err := manager.Create(context.Background(), CreateOptions{})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Create() error = %v, want ErrAlreadyExists", err)
	}
	persisted, ok := store.snapshot(preexisting.ID())
	if !ok || persisted.ID() != preexisting.ID() {
		t.Fatal("storage-create failure deleted the preexisting session")
	}
	if store.deleteCount() != 0 {
		t.Fatalf("Delete called after unowned create failure: %d", store.deleteCount())
	}
}

func TestCreateRetriesOnlyIdentityCollisions(t *testing.T) {
	store := newMemoryStorage()
	store.seed(mustSession(t, testID(20), nil))
	generated := []ID{testID(20), testID(21)}
	var next atomic.Int32
	manager := newManager(t, store, WithIDGenerator(func() (ID, error) {
		return generated[next.Add(1)-1], nil
	}))
	got, err := manager.Create(context.Background(), CreateOptions{})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	if got.ID() != testID(21) {
		t.Fatalf("created ID = %q, want collision retry ID %q", got.ID(), testID(21))
	}
	if store.deleteCount() != 0 {
		t.Fatalf("collision path deleted storage: Delete calls = %d", store.deleteCount())
	}
}

func TestLoadStagesAndPublishesExistingSession(t *testing.T) {
	store := newMemoryStorage()
	persisted := mustSession(t, testID(30), map[string]string{"source": "disk"})
	store.seed(persisted)
	var manager *SessionManager
	manager = newManager(t, store, WithInitializer(func(_ context.Context, loaded Session) (func() error, error) {
		if _, err := manager.Get(context.Background(), loaded.ID()); err == nil {
			t.Fatal("loaded session visible before initializer completed")
		}
		return nil, nil
	}))

	got, err := manager.Load(context.Background(), persisted.ID())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if got.ID() != persisted.ID() || got.Metadata()["source"] != "disk" {
		t.Fatalf("Load() = %+v, want persisted session", got)
	}
	registered, err := manager.Get(context.Background(), persisted.ID())
	if err != nil || registered.ID() != persisted.ID() {
		t.Fatalf("Load() did not publish session: (%+v, %v)", registered, err)
	}
}

func TestLoadInitializerFailureCanBeRetried(t *testing.T) {
	store := newMemoryStorage()
	persisted := mustSession(t, testID(31), nil)
	store.seed(persisted)
	initErr := errors.New("attachment failed")
	var calls atomic.Int32
	var detached atomic.Bool
	manager := newManager(t, store, WithInitializer(func(context.Context, Session) (func() error, error) {
		if calls.Add(1) == 1 {
			return func() error { detached.Store(true); return nil }, initErr
		}
		return nil, nil
	}))

	if _, err := manager.Load(context.Background(), persisted.ID()); !errors.Is(err, initErr) {
		t.Fatalf("first Load() error = %v, want initializer error", err)
	}
	if !detached.Load() {
		t.Fatal("failed load did not roll back the partial host attachment")
	}
	manager.mu.Lock()
	_, registered := manager.registry[persisted.ID()]
	manager.mu.Unlock()
	if registered {
		t.Fatal("failed load published a session")
	}
	got, err := manager.Load(context.Background(), persisted.ID())
	if err != nil || got.ID() != persisted.ID() {
		t.Fatalf("retry Load() = (%+v, %v), want successful load", got, err)
	}
	if _, ok := store.snapshot(persisted.ID()); !ok {
		t.Fatal("failed load removed the existing persisted session")
	}
	if store.deleteCount() != 0 {
		t.Fatalf("failed load deleted persisted data: Delete calls = %d", store.deleteCount())
	}
}

func TestConcurrentLoadConvergesOnOneRegisteredSession(t *testing.T) {
	store := newMemoryStorage()
	persisted := mustSession(t, testID(32), nil)
	store.seed(persisted)
	entered := make(chan struct{})
	release := make(chan struct{})
	var initCalls atomic.Int32
	manager := newManager(t, store, WithInitializer(func(context.Context, Session) (func() error, error) {
		if initCalls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return nil, nil
	}))

	const workers = 12
	results := make(chan Session, workers)
	errorsFound := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := manager.Load(context.Background(), persisted.ID())
			if err != nil {
				errorsFound <- err
				return
			}
			results <- s
		}()
	}
	<-entered
	close(release)
	wg.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent Load(): %v", err)
	}
	var gotID ID
	for got := range results {
		if gotID == "" {
			gotID = got.ID()
		}
		if got.ID() != gotID {
			t.Errorf("concurrent Load() returned different IDs %q and %q", gotID, got.ID())
		}
	}
	if initCalls.Load() != 1 || store.loadCount() != 1 {
		t.Fatalf("Load calls = %d, initializer calls = %d; both want 1", store.loadCount(), initCalls.Load())
	}
}

func TestConcurrentCreateUsesUniqueIDs(t *testing.T) {
	store := newMemoryStorage()
	var counter atomic.Uint64
	manager := newManager(t, store, WithIDGenerator(func() (ID, error) {
		return testID(counter.Add(1) + 100), nil
	}))

	const workers = 64
	ids := make(chan ID, workers)
	errorsFound := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := manager.Create(context.Background(), CreateOptions{})
			if err != nil {
				errorsFound <- err
				return
			}
			ids <- s.ID()
		}()
	}
	wg.Wait()
	close(ids)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent Create(): %v", err)
	}
	seen := make(map[ID]struct{}, workers)
	for id := range ids {
		if _, ok := seen[id]; ok {
			t.Errorf("duplicate ID %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != workers {
		t.Fatalf("created %d sessions, want %d", len(seen), workers)
	}
}

func TestGetListPropagateStorageErrorsAndReturnStableList(t *testing.T) {
	store := newMemoryStorage()
	first := mustSession(t, testID(41), nil)
	second := mustSession(t, testID(40), nil)
	store.seed(first)
	store.seed(second)
	manager := newManager(t, store)
	got, err := manager.List(context.Background())
	if err != nil {
		t.Fatalf("List(): %v", err)
	}
	ids := []ID{got[0].ID(), got[1].ID()}
	if !sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] < ids[j] }) {
		t.Fatalf("List() IDs not sorted: %v", ids)
	}
	if _, err := manager.Get(context.Background(), testID(99)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(missing) error = %v, want ErrNotFound", err)
	}
	store.getErr = errors.New("get failed")
	if _, err := manager.Get(context.Background(), testID(99)); !errors.Is(err, store.getErr) {
		t.Fatalf("Get() error = %v, want storage error", err)
	}
	store.getErr = nil
	store.listErr = errors.New("list failed")
	if _, err := manager.List(context.Background()); !errors.Is(err, store.listErr) {
		t.Fatalf("List() error = %v, want storage error", err)
	}
}

func TestGetRejectsStorageIDMismatch(t *testing.T) {
	store := newMemoryStorage()
	store.getOverride = mustSession(t, testID(52), nil)
	manager := newManager(t, store)
	if _, err := manager.Get(context.Background(), testID(51)); !errors.Is(err, ErrIDMismatch) {
		t.Fatalf("Get() error = %v, want ErrIDMismatch", err)
	}
}

func TestCloseWaitsForTransactionsAndLeavesQueriesAvailable(t *testing.T) {
	store := newMemoryStorage()
	id := testID(60)
	entered := make(chan struct{})
	release := make(chan struct{})
	manager := newManager(t, store,
		WithIDGenerator(func() (ID, error) { return id, nil }),
		WithInitializer(func(context.Context, Session) (func() error, error) {
			close(entered)
			<-release
			return nil, nil
		}),
	)
	createResult := make(chan error, 1)
	go func() {
		_, err := manager.Create(context.Background(), CreateOptions{})
		createResult <- err
	}()
	<-entered
	closed := make(chan struct{})
	go func() { manager.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while create transaction was in flight")
	default:
	}
	close(release)
	if err := <-createResult; err != nil {
		t.Fatalf("Create() during Close(): %v", err)
	}
	<-closed
	manager.Close() // idempotent
	if _, err := manager.Create(context.Background(), CreateOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Create() after Close() error = %v, want ErrClosed", err)
	}
	if _, err := manager.Load(context.Background(), id); !errors.Is(err, ErrClosed) {
		t.Fatalf("Load() after Close() error = %v, want ErrClosed", err)
	}
	if _, err := manager.Get(context.Background(), id); err != nil {
		t.Fatal("Get() should remain available after Close():", err)
	}
	if _, err := manager.List(context.Background()); err != nil {
		t.Fatalf("List() after Close(): %v", err)
	}
}

func TestAbandonCreatedCloseWaitsAndRejectsLateCall(t *testing.T) {
	store := &blockingDeleteStorage{
		delegate: newMemoryStorage(),
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	manager := newManager(t, store, WithIDGenerator(func() (ID, error) { return testID(61), nil }))
	created, err := manager.Create(context.Background(), CreateOptions{})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}

	var releaseOnce sync.Once
	releaseDelete := func() { releaseOnce.Do(func() { close(store.release) }) }
	defer releaseDelete()

	abandonResult := make(chan error, 1)
	go func() { abandonResult <- manager.AbandonCreated(context.Background(), created) }()
	<-store.entered

	closeStarted := make(chan struct{})
	closeDone := make(chan struct{})
	go func() {
		close(closeStarted)
		manager.Close()
		close(closeDone)
	}()
	<-closeStarted
	waitForManagerClosed(t, manager)

	// Delete remains blocked, so Close must remain in its in-flight wait.
	select {
	case <-closeDone:
		t.Fatal("Close returned while AbandonCreated was blocked in Storage.Delete")
	case <-time.After(50 * time.Millisecond):
	}

	lateResult := make(chan error, 1)
	go func() { lateResult <- manager.AbandonCreated(context.Background(), created) }()
	select {
	case err := <-lateResult:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("AbandonCreated() after Close began = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AbandonCreated() after Close began did not fail closed promptly")
	}

	releaseDelete()
	if err := <-abandonResult; err != nil {
		t.Fatalf("in-flight AbandonCreated() = %v", err)
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after in-flight AbandonCreated completed")
	}
	if err := manager.AbandonCreated(context.Background(), created); !errors.Is(err, ErrClosed) {
		t.Fatalf("AbandonCreated() after Close returned = %v, want ErrClosed", err)
	}
}

func waitForManagerClosed(t *testing.T, manager *SessionManager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		closed := manager.closed
		manager.mu.Unlock()
		if closed {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("Close did not mark manager closed")
}

type blockingDeleteStorage struct {
	delegate Storage
	entered  chan struct{}
	release  chan struct{}
}

func (s *blockingDeleteStorage) Create(ctx context.Context, session Session) error {
	return s.delegate.Create(ctx, session)
}

func (s *blockingDeleteStorage) Load(ctx context.Context, id ID) (Session, error) {
	return s.delegate.Load(ctx, id)
}

func (s *blockingDeleteStorage) Get(ctx context.Context, id ID) (Session, error) {
	return s.delegate.Get(ctx, id)
}

func (s *blockingDeleteStorage) List(ctx context.Context) ([]Session, error) {
	return s.delegate.List(ctx)
}

func (s *blockingDeleteStorage) Delete(ctx context.Context, id ID) error {
	close(s.entered)
	<-s.release
	return s.delegate.Delete(ctx, id)
}

func TestCreatePropagatesNonCollisionStorageFailureWithoutDeleting(t *testing.T) {
	store := newMemoryStorage()
	store.createErr = errors.New("disk unavailable")
	manager := newManager(t, store, WithIDGenerator(func() (ID, error) { return testID(13), nil }))

	_, err := manager.Create(context.Background(), CreateOptions{})
	if !errors.Is(err, store.createErr) {
		t.Fatalf("Create() error = %v, want storage error", err)
	}
	if store.deleteCount() != 0 {
		t.Fatalf("Delete called after Storage.Create error: %d", store.deleteCount())
	}
}

func TestCreateJoinsAttachmentAndStorageRollbackErrors(t *testing.T) {
	store := newMemoryStorage()
	storageRollbackErr := errors.New("delete failed")
	attachmentRollbackErr := errors.New("detach failed")
	store.deleteErr = storageRollbackErr
	manager := newManager(t, store,
		WithIDGenerator(func() (ID, error) { return testID(14), nil }),
		WithInitializer(func(context.Context, Session) (func() error, error) {
			return func() error { return attachmentRollbackErr }, errors.New("attach failed")
		}),
	)

	_, err := manager.Create(context.Background(), CreateOptions{})
	if !errors.Is(err, attachmentRollbackErr) || !errors.Is(err, storageRollbackErr) {
		t.Fatalf("Create() error = %v, want both rollback causes", err)
	}
	if _, ok := store.snapshot(testID(14)); !ok {
		t.Fatal("failed storage rollback unexpectedly removed the record")
	}
	manager.mu.Lock()
	_, registered := manager.registry[testID(14)]
	manager.mu.Unlock()
	if registered {
		t.Fatal("failed create published a session")
	}
}

func newManager(t *testing.T, store Storage, options ...ManagerOption) *SessionManager {
	t.Helper()
	manager, err := NewSessionManager(store, options...)
	if err != nil {
		t.Fatalf("NewSessionManager(): %v", err)
	}
	t.Cleanup(manager.Close)
	return manager
}

func testID(value uint64) ID {
	return ID(fmt.Sprintf("session_%016x", value))
}

func mustSession(t *testing.T, id ID, metadata map[string]string) Session {
	t.Helper()
	s, err := NewSession(id, time.Unix(1, 0).UTC(), time.Unix(2, 0).UTC(), metadata)
	if err != nil {
		t.Fatalf("NewSession(%q): %v", id, err)
	}
	return s
}

type memoryStorage struct {
	mu          sync.Mutex
	sessions    map[ID]Session
	createErr   error
	getErr      error
	loadErr     error
	listErr     error
	deleteErr   error
	getOverride Session
	loadCalls   int
	deleteCalls int
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{sessions: make(map[ID]Session)}
}

func (s *memoryStorage) Create(_ context.Context, session Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return s.createErr
	}
	if _, exists := s.sessions[session.ID()]; exists {
		return ErrAlreadyExists
	}
	s.sessions[session.ID()] = session
	return nil
}

func (s *memoryStorage) Load(_ context.Context, id ID) (Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadCalls++
	if s.loadErr != nil {
		return Session{}, s.loadErr
	}
	loaded, ok := s.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return loaded, nil
}

func (s *memoryStorage) Get(_ context.Context, id ID) (Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return Session{}, s.getErr
	}
	if s.getOverride.ID() != "" {
		return s.getOverride, nil
	}
	got, ok := s.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return got, nil
}

func (s *memoryStorage) List(_ context.Context) ([]Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		out = append(out, session)
	}
	return out, nil
}

func (s *memoryStorage) Delete(_ context.Context, id ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteCalls++
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.sessions, id)
	return nil
}

func (s *memoryStorage) seed(session Session) {
	s.mu.Lock()
	s.sessions[session.ID()] = session
	s.mu.Unlock()
}

func (s *memoryStorage) snapshot(id ID) (Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	got, ok := s.sessions[id]
	return got, ok
}

func (s *memoryStorage) loadCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadCalls
}

func (s *memoryStorage) deleteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteCalls
}
