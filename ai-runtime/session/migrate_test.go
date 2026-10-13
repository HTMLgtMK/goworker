package session

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
)

func TestMigrateLegacy_IsIdempotentAndPreservesLifecycle(t *testing.T) {
	root := t.TempDir()
	legacy, err := Open(root)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Commit([]core.Message{{MsgID: "m_current", Role: "user", Content: "current"}}); err != nil {
		t.Fatalf("commit current: %v", err)
	}
	if err := legacy.Archive(); err != nil {
		t.Fatalf("archive legacy: %v", err)
	}
	if _, err := legacy.Commit([]core.Message{{MsgID: "m_live", Role: "user", Content: "live"}}); err != nil {
		t.Fatalf("commit live: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	if err := MigrateLegacy(root); err != nil {
		t.Fatalf("MigrateLegacy: %v", err)
	}
	files, err := ListSessionFiles(root)
	if err != nil {
		t.Fatalf("ListSessionFiles: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("session files = %d, want 2", len(files))
	}
	sort.Slice(files, func(i, j int) bool { return files[i].ID < files[j].ID })
	for _, file := range files {
		st, err := OpenPath(file.Path)
		if err != nil {
			t.Fatalf("open migrated %s: %v", file.ID, err)
		}
		meta, ok := st.SessionMeta()
		if !ok || meta.ID != file.ID {
			t.Errorf("metadata for %s = %+v, ok=%v", file.ID, meta, ok)
		}
		if len(st.ActiveView()) != 1 {
			t.Errorf("history for %s = %v, want one message", file.ID, st.ActiveView())
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close migrated %s: %v", file.ID, err)
		}
	}

	if err := MigrateLegacy(root); err != nil {
		t.Fatalf("retry MigrateLegacy: %v", err)
	}
	again, err := ListSessionFiles(root)
	if err != nil {
		t.Fatalf("ListSessionFiles retry: %v", err)
	}
	if len(again) != len(files) {
		t.Fatalf("session files after retry = %d, want %d", len(again), len(files))
	}
}

func TestListSessionFiles_SkipsUnsafeNames(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "sessions")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir sessions: %v", err)
	}
	for _, name := range []string{".hidden.jsonl", "bad name.jsonl", "../not-a-session"} {
		path := filepath.Join(dir, name)
		if filepath.Dir(path) != dir {
			continue
		}
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	valid, err := SessionPath(root, "session-safe")
	if err != nil {
		t.Fatalf("SessionPath: %v", err)
	}
	if err := os.WriteFile(valid, nil, 0o600); err != nil {
		t.Fatalf("write valid: %v", err)
	}

	files, err := ListSessionFiles(root)
	if err != nil {
		t.Fatalf("ListSessionFiles: %v", err)
	}
	if len(files) != 1 || files[0].ID != "session-safe" {
		t.Fatalf("files = %+v, want only session-safe", files)
	}
}
func TestSessionPath_RejectsTraversal(t *testing.T) {
	for _, id := range []string{"", ".", "..", "../escape", `a\\b`, ".hidden"} {
		if _, err := SessionPath(t.TempDir(), id); err == nil {
			t.Errorf("SessionPath accepted unsafe id %q", id)
		}
	}
	path, err := SessionPath(t.TempDir(), "sess-safe_1")
	if err != nil {
		t.Fatalf("SessionPath safe id: %v", err)
	}
	if filepath.Base(path) != "sess-safe_1.jsonl" {
		t.Fatalf("path = %q", path)
	}
}

func TestMigrateLegacy_SkipsEmptyAndMalformedOnlyFiles(t *testing.T) {
	root := t.TempDir()
	archiveDir := filepath.Join(root, "archive")
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		t.Fatalf("mkdir archive: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "current.jsonl"), nil, 0o600); err != nil {
		t.Fatalf("write empty current: %v", err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, "empty.jsonl"), nil, 0o600); err != nil {
		t.Fatalf("write empty archive: %v", err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, "broken.jsonl"), []byte("not-json\n"), 0o600); err != nil {
		t.Fatalf("write malformed archive: %v", err)
	}

	if err := MigrateLegacy(root); err != nil {
		t.Fatalf("MigrateLegacy: %v", err)
	}
	files, err := ListSessionFiles(root)
	if err != nil {
		t.Fatalf("ListSessionFiles: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("migrated files = %+v, want none", files)
	}
}

func TestMigrateLegacy_PreservesValidRecordsWithMalformedLines(t *testing.T) {
	root := t.TempDir()
	legacy, err := Open(root)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Commit([]core.Message{{MsgID: "valid", Role: "user", Content: "keep me"}}); err != nil {
		t.Fatalf("commit legacy: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(root, "current.jsonl"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open legacy append: %v", err)
	}
	if _, err := f.WriteString("not-json\n"); err != nil {
		f.Close()
		t.Fatalf("append malformed record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close legacy append: %v", err)
	}

	if err := MigrateLegacy(root); err != nil {
		t.Fatalf("MigrateLegacy: %v", err)
	}
	files, err := ListSessionFiles(root)
	if err != nil || len(files) != 1 {
		t.Fatalf("migrated files = %+v, err=%v, want one", files, err)
	}
	st, err := OpenPath(files[0].Path)
	if err != nil {
		t.Fatalf("open migrated: %v", err)
	}
	defer st.Close()
	view := st.ActiveView()
	if len(view) != 1 || view[0].Content != "keep me" {
		t.Fatalf("migrated view = %+v, want valid message", view)
	}
}

func TestMigrateLegacy_ResolvesDestinationCollision(t *testing.T) {
	root := t.TempDir()
	archiveDir := filepath.Join(root, "archive")
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		t.Fatalf("mkdir archive: %v", err)
	}
	legacyPath := filepath.Join(archiveDir, "session-collision.jsonl")
	legacy, err := OpenPath(legacyPath)
	if err != nil {
		t.Fatalf("open legacy archive: %v", err)
	}
	if _, err := legacy.Commit([]core.Message{{MsgID: "legacy", Role: "user", Content: "legacy"}}); err != nil {
		t.Fatalf("commit legacy archive: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy archive: %v", err)
	}
	occupied, err := SessionPath(root, "session-collision")
	if err != nil {
		t.Fatalf("occupied path: %v", err)
	}
	occupiedStore, err := OpenPath(occupied)
	if err != nil {
		t.Fatalf("open occupied: %v", err)
	}
	if err := occupiedStore.SetSessionMeta("session-collision", SessionStatusLive, time.Now()); err != nil {
		occupiedStore.Close()
		t.Fatalf("set occupied metadata: %v", err)
	}
	if _, err := occupiedStore.Commit([]core.Message{{MsgID: "occupied", Role: "user", Content: "occupied"}}); err != nil {
		occupiedStore.Close()
		t.Fatalf("commit occupied: %v", err)
	}
	if err := occupiedStore.Close(); err != nil {
		t.Fatalf("close occupied: %v", err)
	}

	if err := MigrateLegacy(root); err != nil {
		t.Fatalf("MigrateLegacy: %v", err)
	}
	files, err := ListSessionFiles(root)
	if err != nil {
		t.Fatalf("ListSessionFiles: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("session files = %+v, want occupied plus migrated collision", files)
	}
	for _, file := range files {
		st, err := OpenPath(file.Path)
		if err != nil {
			t.Fatalf("open %s: %v", file.ID, err)
		}
		view := st.ActiveView()
		st.Close()
		if len(view) != 1 || (view[0].Content != "legacy" && view[0].Content != "occupied") {
			t.Fatalf("session %s view = %+v, want one preserved history", file.ID, view)
		}
	}
}
