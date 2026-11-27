//go:build !unix

package session

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestUnsupportedPlatformRejectsRepositoryAndMigrationBeforeWrites(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sessions")
	if _, err := OpenRepository(root); !errors.Is(err, ErrLockUnavailable) {
		t.Fatalf("OpenRepository error = %v, want ErrLockUnavailable", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("OpenRepository created root: stat error = %v", err)
	}

	if err := os.MkdirAll(filepath.Join(root, "archive"), 0o700); err != nil {
		t.Fatalf("create legacy layout: %v", err)
	}
	legacyPath := filepath.Join(root, legacyDataFileName)
	legacy := []byte("legacy session data must remain untouched\n")
	if err := os.WriteFile(legacyPath, legacy, 0o600); err != nil {
		t.Fatalf("write legacy slot: %v", err)
	}

	if _, err := MigrateLegacyLayout(root); !errors.Is(err, ErrLockUnavailable) {
		t.Fatalf("MigrateLegacyLayout error = %v, want ErrLockUnavailable", err)
	}
	if got, err := os.ReadFile(legacyPath); err != nil || string(got) != string(legacy) {
		t.Fatalf("legacy slot changed: contents = %q, err = %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(root, "archive")); err != nil {
		t.Fatalf("migration removed archive directory: %v", err)
	}
}
