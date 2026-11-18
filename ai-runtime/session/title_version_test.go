package session

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestTitleVersionPersistsAndAdvancesFromLegacy(t *testing.T) {
	dir := t.TempDir()
	legacy := []byte(`{"kind":"title","title":"legacy","created_at":"2024-01-01T00:00:00Z"}` + "\n")
	if err := os.WriteFile(filepath.Join(dir, "current.jsonl"), legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Title(); got.Title != "legacy" || got.Version != 0 {
		t.Fatalf("legacy title: %+v", got)
	}
	for i, title := range []string{"first", "second"} {
		got, err := st.AppendTitle(title)
		if err != nil || got.Title != title || got.Version != uint64(i+1) {
			t.Fatalf("AppendTitle(%q) = %+v, %v", title, got, err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if got := st.Title(); got.Title != "second" || got.Version != 2 {
		t.Fatalf("reopened title: %+v", got)
	}
	got, err := st.AppendTitle("third")
	if err != nil || got.Version != 3 {
		t.Fatalf("next title: %+v, %v", got, err)
	}
}

func TestTitleVersionOverflowAndFailedWrite(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	st.titleVersion = ^uint64(0)
	if _, err := st.AppendTitle("overflow"); err == nil {
		t.Fatal("version overflow accepted")
	}
	st.titleVersion = 0
	if err := st.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendTitle("failed"); err == nil {
		t.Fatal("closed file write succeeded")
	}
	st.file, err = os.OpenFile(st.path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	state, err := st.AppendTitle("next")
	if err != nil || state.Version != 2 {
		t.Fatalf("state = %+v, err = %v", state, err)
	}
}

func TestConcurrentTitleVersionsAreUnique(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const count = 20
	versions := make(chan uint64, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			state, err := st.AppendTitle("title")
			if err != nil {
				t.Errorf("AppendTitle: %v", err)
				return
			}
			versions <- state.Version
		}()
	}
	wg.Wait()
	close(versions)
	seen := make(map[uint64]bool)
	for version := range versions {
		seen[version] = true
	}
	if len(seen) != count || !seen[1] || !seen[count] {
		t.Fatalf("versions = %v", seen)
	}
}
