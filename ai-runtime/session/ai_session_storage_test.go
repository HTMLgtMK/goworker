package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ai_session "github.com/tinguo/goworker/ai-session"
)

func TestAIStorageCreatePersistsSpecifiedIDAndMetadataWithoutDeclaringWorkspace(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(): %v", err)
	}
	defer repo.Close()

	storage := NewAIStorageAdapter(repo)
	id := ai_session.ID("session_0123456789abcdef0123456789abcdef")
	createdAt := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	updatedAt := createdAt.Add(3 * time.Hour)
	wantMetadata := map[string]string{"cwd": "/workspace/project", "origin": "test"}
	created, err := ai_session.NewSession(id, createdAt, updatedAt, wantMetadata)
	if err != nil {
		t.Fatalf("NewSession(): %v", err)
	}
	if err := storage.Create(ctx, created); err != nil {
		t.Fatalf("Storage.Create(): %v", err)
	}

	path := filepath.Join(root, string(id), string(id)+".jsonl")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("session data file missing at canonical ID path: %v", err)
	}
	stored, err := storage.Load(ctx, id)
	if err != nil {
		t.Fatalf("Storage.Load(): %v", err)
	}
	assertAISessionEqual(t, stored, created)

	got, err := storage.Get(ctx, id)
	if err != nil {
		t.Fatalf("Storage.Get(): %v", err)
	}
	assertAISessionEqual(t, got, created)

	listed, err := storage.List(ctx)
	if err != nil {
		t.Fatalf("Storage.List(): %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("Storage.List() returned %d sessions, want 1", len(listed))
	}
	assertAISessionEqual(t, listed[0], created)

	store, err := repo.Open(string(id))
	if err != nil {
		t.Fatalf("Repository.Open(): %v", err)
	}
	if got := store.Workspace(); got != "" {
		t.Fatalf("Storage.Create() persisted unvalidated workspace = %q, want none", got)
	}
	if got := store.Metadata().Values["cwd"]; got != wantMetadata["cwd"] {
		t.Fatalf("generic cwd metadata = %q, want %q", got, wantMetadata["cwd"])
	}
	// The host calls DeclareWorkspace only after its own CWD policy validation.
	if err := store.DeclareWorkspace(wantMetadata["cwd"]); err != nil {
		t.Fatalf("DeclareWorkspace() after validation: %v", err)
	}
}

func TestAIStorageIntegratesWithSessionManager(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(): %v", err)
	}
	defer repo.Close()

	id := ai_session.ID("session_2123456789abcdef0123456789abcdef")
	manager, err := ai_session.NewSessionManager(
		NewAIStorageAdapter(repo),
		ai_session.WithIDGenerator(func() (ai_session.ID, error) { return id, nil }),
	)
	if err != nil {
		t.Fatalf("NewSessionManager(): %v", err)
	}
	defer manager.Close()

	created, err := manager.Create(ctx, ai_session.CreateOptions{Metadata: map[string]string{
		"cwd":    "/manager/workspace",
		"origin": "manager-test",
	}})
	if err != nil {
		t.Fatalf("SessionManager.Create(): %v", err)
	}
	if created.ID() != id {
		t.Fatalf("SessionManager.Create() ID = %q, want %q", created.ID(), id)
	}
	persisted, err := NewAIStorageAdapter(repo).Load(ctx, id)
	if err != nil {
		t.Fatalf("Storage.Load(): %v", err)
	}
	assertAISessionEqual(t, persisted, created)
}

func TestAIStorageManagerInitializerFailureRemovesUnpublishedSession(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(): %v", err)
	}
	defer repo.Close()

	id := ai_session.ID("session_3123456789abcdef0123456789abcdef")
	initErr := errors.New("runtime attachment failed")
	manager, err := ai_session.NewSessionManager(
		NewAIStorageAdapter(repo),
		ai_session.WithIDGenerator(func() (ai_session.ID, error) { return id, nil }),
		ai_session.WithInitializer(func(context.Context, ai_session.Session) (func() error, error) {
			return nil, initErr
		}),
	)
	if err != nil {
		t.Fatalf("NewSessionManager(): %v", err)
	}
	defer manager.Close()

	if _, err := manager.Create(ctx, ai_session.CreateOptions{}); !errors.Is(err, initErr) {
		t.Fatalf("SessionManager.Create() error = %v, want initializer error", err)
	}
	if _, err := os.Stat(filepath.Join(root, id.String())); !os.IsNotExist(err) {
		t.Fatalf("failed manager create left session directory behind: stat error = %v", err)
	}
	if listed, err := NewAIStorageAdapter(repo).List(ctx); err != nil || len(listed) != 0 {
		t.Fatalf("Storage.List() after initializer failure = %v, %v; want no visible session", listed, err)
	}
}

func TestAIStorageCreateCollisionDoesNotDeleteExistingSession(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(): %v", err)
	}
	defer repo.Close()

	id := "session_0123456789abcdef0123456789abcdef"
	existing, err := repo.CreateWithID(ctx, id, CreateOptions{CWD: "/existing"})
	if err != nil {
		t.Fatalf("Repository.CreateWithID(): %v", err)
	}
	if _, err := existing.Commit(msgs("user", "keep me")); err != nil {
		t.Fatalf("Commit(): %v", err)
	}
	if err := repo.CloseSession(id); err != nil {
		t.Fatalf("CloseSession(): %v", err)
	}
	before, err := os.ReadFile(filepath.Join(root, id, id+".jsonl"))
	if err != nil {
		t.Fatalf("ReadFile(before): %v", err)
	}

	storage := NewAIStorageAdapter(repo)
	duplicate, err := ai_session.NewSession(ai_session.ID(id), time.Now(), time.Now(), nil)
	if err != nil {
		t.Fatalf("NewSession(): %v", err)
	}
	if err := storage.Create(ctx, duplicate); !errors.Is(err, ai_session.ErrAlreadyExists) {
		t.Fatalf("Storage.Create() error = %v, want ErrAlreadyExists", err)
	}
	after, err := os.ReadFile(filepath.Join(root, id, id+".jsonl"))
	if err != nil {
		t.Fatalf("ReadFile(after): %v", err)
	}
	if string(after) != string(before) {
		t.Fatalf("collision changed existing session data\nbefore: %s\nafter:  %s", before, after)
	}
	store, err := repo.Open(id)
	if err != nil {
		t.Fatalf("Repository.Open() after collision: %v", err)
	}
	if got := store.ActiveView(); len(got) != 1 || got[0].Content != "keep me" {
		t.Fatalf("existing conversation after collision = %v, want preserved message", got)
	}
}

func TestAIStorageReopensLegacyIDAndPreservesCurrentJSONLPath(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	id := "legacy_0123456789abcdef"
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll(): %v", err)
	}
	legacyJSONL := "{\"kind\":\"metadata\",\"session_id\":\"" + id + "\",\"created_at\":\"2024-01-02T03:04:05Z\"}\n" +
		"{\"kind\":\"workspace\",\"path\":\"/legacy/workspace\",\"created_at\":\"2024-01-02T03:04:06Z\"}\n"
	legacyPath := filepath.Join(dir, "current.jsonl")
	if err := os.WriteFile(legacyPath, []byte(legacyJSONL), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy JSONL): %v", err)
	}

	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(): %v", err)
	}
	storage := NewAIStorageAdapter(repo)
	loaded, err := storage.Load(ctx, ai_session.ID(id))
	if err != nil {
		t.Fatalf("Storage.Load(legacy ID): %v", err)
	}
	if loaded.ID().String() != id || loaded.Metadata()["cwd"] != "/legacy/workspace" {
		t.Fatalf("loaded legacy session = (%q, %v), want stable ID and workspace metadata", loaded.ID(), loaded.Metadata())
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("Repository.Close(): %v", err)
	}

	reopened, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(reopen): %v", err)
	}
	defer reopened.Close()
	listed, err := NewAIStorageAdapter(reopened).List(ctx)
	if err != nil {
		t.Fatalf("Storage.List() after reopen: %v", err)
	}
	if len(listed) != 1 || listed[0].ID().String() != id {
		t.Fatalf("Storage.List() after reopen = %v, want stable legacy ID %q", listed, id)
	}
	if _, err := os.Stat(legacyPath); err != nil {
		t.Fatalf("legacy current.jsonl path changed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, id+".jsonl")); !os.IsNotExist(err) {
		t.Fatalf("reopen migrated legacy path unexpectedly: stat error = %v", err)
	}
}

func TestCreateWithIDExplicitCWDOverridesReservedMetadataKey(t *testing.T) {
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(): %v", err)
	}
	defer repo.Close()

	id := "legacy_0123456789abcdef"
	_, err = repo.CreateWithID(context.Background(), id, CreateOptions{
		CWD:      "/explicit/workspace",
		Metadata: map[string]string{"cwd": "/metadata/workspace", "origin": "legacy"},
	})
	if err != nil {
		t.Fatalf("CreateWithID(): %v", err)
	}
	items, err := repo.List(context.Background())
	if err != nil {
		t.Fatalf("List(): %v", err)
	}
	if len(items) != 1 || items[0].SessionID != id {
		t.Fatalf("List() = %v, want stable legacy ID %q", items, id)
	}
	if items[0].Workspace != "/explicit/workspace" || items[0].Values["cwd"] != "/explicit/workspace" {
		t.Fatalf("cwd round-trip = workspace %q, metadata %q; explicit CWD must win", items[0].Workspace, items[0].Values["cwd"])
	}
	if items[0].Values["origin"] != "legacy" {
		t.Fatalf("metadata round-trip = %v, want origin preserved", items[0].Values)
	}
}

func TestRepositoryCreateWithIDOpenFailureRemovesOnlyOwnedDirectory(t *testing.T) {
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(): %v", err)
	}
	defer repo.Close()

	id := "session_1123456789abcdef0123456789abcdef"
	openErr := errors.New("injected open failure")
	_, err = repo.createWithID(context.Background(), id, CreateOptions{}, func(string) (*Store, error) {
		return nil, openErr
	})
	if !errors.Is(err, openErr) {
		t.Fatalf("CreateWithID() error = %v, want injected open failure", err)
	}
	if _, err := os.Stat(filepath.Join(root, id)); !os.IsNotExist(err) {
		t.Fatalf("failed create left its exclusively claimed directory: stat error = %v", err)
	}
}

func TestRepositoryCreateFailureSerializesCrossRepositoryOpen(t *testing.T) {
	root := t.TempDir()
	creator, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(creator): %v", err)
	}
	defer creator.Close()
	reader, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(reader): %v", err)
	}
	defer reader.Close()

	id := "session_4123456789abcdef0123456789abcdef"
	openErr := errors.New("injected open failure")
	seeded := make(chan struct{})
	continueCreate := make(chan struct{})
	var releaseCreateOnce sync.Once
	releaseCreate := func() { releaseCreateOnce.Do(func() { close(continueCreate) }) }
	defer releaseCreate()
	createResult := make(chan error, 1)
	go func() {
		_, createErr := creator.createWithID(context.Background(), id, CreateOptions{}, func(string) (*Store, error) {
			close(seeded)
			<-continueCreate
			return nil, openErr
		})
		createResult <- createErr
	}()
	<-seeded

	type openResult struct {
		store *Store
		err   error
	}
	readerStarted := make(chan struct{})
	readResult := make(chan openResult, 1)
	go func() {
		close(readerStarted)
		store, openErr := reader.Open(id)
		readResult <- openResult{store: store, err: openErr}
	}()
	<-readerStarted

	select {
	case got := <-readResult:
		releaseCreate()
		if got.store != nil {
			_ = reader.CloseSession(id)
		}
		t.Fatalf("Repository.Open() observed a session before create finished: store=%v err=%v", got.store, got.err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseCreate()
	if err := <-createResult; !errors.Is(err, openErr) {
		t.Fatalf("CreateWithID() error = %v, want injected open failure", err)
	}
	select {
	case got := <-readResult:
		if got.store != nil {
			_ = reader.CloseSession(id)
			t.Fatalf("Repository.Open() returned a Store after failed create cleanup")
		}
		if !errors.Is(got.err, ErrSessionNotFound) {
			t.Fatalf("Repository.Open() after rollback error = %v, want ErrSessionNotFound", got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Repository.Open() remained blocked after create rollback")
	}
	if _, err := os.Stat(filepath.Join(root, id)); !os.IsNotExist(err) {
		t.Fatalf("failed create left a visible directory: stat error = %v", err)
	}
}

func TestRepositoryDeleteDoesNotRemoveStoreHeldByAnotherRepository(t *testing.T) {
	root := t.TempDir()
	deleter, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(deleter): %v", err)
	}
	defer deleter.Close()
	owner, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(owner): %v", err)
	}
	defer owner.Close()

	id := "session_5123456789abcdef0123456789abcdef"
	if _, err := deleter.CreateWithID(context.Background(), id, CreateOptions{}); err != nil {
		t.Fatalf("CreateWithID(): %v", err)
	}
	if err := deleter.CloseSession(id); err != nil {
		t.Fatalf("CloseSession(): %v", err)
	}
	if _, err := owner.Open(id); err != nil {
		t.Fatalf("owner.Open(): %v", err)
	}
	if err := deleter.Delete(context.Background(), id); !errors.Is(err, ErrSessionInUse) {
		t.Fatalf("Delete() error = %v, want ErrSessionInUse", err)
	}
	if _, err := os.Stat(filepath.Join(root, id, id+".jsonl")); err != nil {
		t.Fatalf("Delete() removed a store held by another Repository: %v", err)
	}
}

type lockWaitContext struct {
	context.Context
	mu          sync.Mutex
	errCalls    int
	waitingLock chan struct{}
}

func newLockWaitContext() (*lockWaitContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &lockWaitContext{Context: ctx, waitingLock: make(chan struct{})}, cancel
}

func (c *lockWaitContext) Err() error {
	c.mu.Lock()
	c.errCalls++
	if c.errCalls == 3 {
		close(c.waitingLock)
	}
	c.mu.Unlock()
	return c.Context.Err()
}

func waitForRepositoryLockAttempt(ctx *lockWaitContext, cancel context.CancelFunc, unlock func()) bool {
	select {
	case <-ctx.waitingLock:
		return true
	case <-time.After(time.Second):
		cancel()
		unlock()
		return false
	}
}

func TestRepositoryCreateHonorsCancellationWhileWaitingForIDLock(t *testing.T) {
	root := t.TempDir()
	holder, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(holder): %v", err)
	}
	defer holder.Close()
	creator, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(creator): %v", err)
	}
	defer creator.Close()

	id := "session_6123456789abcdef0123456789abcdef"
	unlock, err := holder.lockSessionID(id)
	if err != nil {
		t.Fatalf("lockSessionID(): %v", err)
	}
	var unlockOnce sync.Once
	release := func() { unlockOnce.Do(unlock) }
	defer release()

	ctx, cancel := newLockWaitContext()
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := creator.CreateWithID(ctx, id, CreateOptions{})
		result <- err
	}()
	enteredLockWait := waitForRepositoryLockAttempt(ctx, cancel, release)
	cancel()
	release()

	if !enteredLockWait {
		t.Error("operation did not reach the contended repository lock")
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateWithID() error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(filepath.Join(root, id)); !os.IsNotExist(err) {
		t.Fatalf("canceled CreateWithID() created a session directory: %v", err)
	}
}

func TestRepositoryDeleteHonorsCancellationWhileWaitingForIDLock(t *testing.T) {
	root := t.TempDir()
	creator, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(creator): %v", err)
	}
	defer creator.Close()
	deleter, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(deleter): %v", err)
	}
	defer deleter.Close()

	id := "session_7123456789abcdef0123456789abcdef"
	if _, err := creator.CreateWithID(context.Background(), id, CreateOptions{}); err != nil {
		t.Fatalf("CreateWithID(): %v", err)
	}
	if err := creator.CloseSession(id); err != nil {
		t.Fatalf("CloseSession(): %v", err)
	}
	unlock, err := creator.lockSessionID(id)
	if err != nil {
		t.Fatalf("lockSessionID(): %v", err)
	}
	var unlockOnce sync.Once
	release := func() { unlockOnce.Do(unlock) }
	defer release()

	ctx, cancel := newLockWaitContext()
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- deleter.Delete(ctx, id) }()
	enteredLockWait := waitForRepositoryLockAttempt(ctx, cancel, release)
	cancel()
	release()

	if !enteredLockWait {
		t.Error("operation did not reach the contended repository lock")
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete() error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(filepath.Join(root, id, id+".jsonl")); err != nil {
		t.Fatalf("canceled Delete() removed the session data: %v", err)
	}
}

func TestAIStorageCreateFailureLeavesNoSessionDirectory(t *testing.T) {
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(): %v", err)
	}
	defer repo.Close()

	id := ai_session.ID("session_0123456789abcdef0123456789abcdef")
	created, err := ai_session.NewSession(id, time.Now(), time.Now(), nil)
	if err != nil {
		t.Fatalf("NewSession(): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := NewAIStorageAdapter(repo).Create(ctx, created); err == nil {
		t.Fatal("Storage.Create() with canceled context succeeded")
	}
	if _, err := os.Stat(filepath.Join(root, string(id))); !os.IsNotExist(err) {
		t.Fatalf("failed create left session directory behind: stat error = %v", err)
	}
}

func assertAISessionEqual(t *testing.T, got, want ai_session.Session) {
	t.Helper()
	if got.ID() != want.ID() || !got.CreatedAt().Equal(want.CreatedAt()) || !got.UpdatedAt().Equal(want.UpdatedAt()) {
		t.Fatalf("session identity/timestamps = (%q, %s, %s), want (%q, %s, %s)",
			got.ID(), got.CreatedAt(), got.UpdatedAt(), want.ID(), want.CreatedAt(), want.UpdatedAt())
	}
	gotMetadata, wantMetadata := got.Metadata(), want.Metadata()
	if len(gotMetadata) != len(wantMetadata) {
		t.Fatalf("session metadata = %v, want %v", gotMetadata, wantMetadata)
	}
	for key, value := range wantMetadata {
		if gotMetadata[key] != value {
			t.Fatalf("session metadata[%q] = %q, want %q", key, gotMetadata[key], value)
		}
	}
}
