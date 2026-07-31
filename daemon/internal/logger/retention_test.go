package logger

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanupOld(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "app.2026-01-01T00-00-00.000.log")
	newer := filepath.Join(dir, "app.2026-07-30T00-00-00.000.log")
	active := filepath.Join(dir, "app.log")
	for _, f := range []string{old, newer, active} {
		if err := os.WriteFile(f, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	// 造 mtime：old 距今 48h（超龄），newer 距今 24h（未超龄）
	touch := func(path string, age time.Duration) {
		at := time.Now().Add(-age)
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
	}
	touch(old, 48*time.Hour)
	touch(newer, 24*time.Hour)

	removed, err := CleanupOld(dir, active, 36*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Error("old file still exists, want removed")
	}
	if _, err := os.Stat(newer); err != nil {
		t.Error("newer file removed, want kept")
	}
	if _, err := os.Stat(active); err != nil {
		t.Error("active file removed, want kept")
	}
}

func TestCleanupOld_OnlyActive(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "app.log")
	os.WriteFile(active, []byte("x"), 0644)

	// 即使 active 文件本身超龄，也绝不能删
	touch(active, 720*time.Hour)
	removed, err := CleanupOld(dir, active, 36*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0 (active must survive)", removed)
	}
	if _, err := os.Stat(active); err != nil {
		t.Error("active file gone, want kept")
	}
}

func TestCleanupOld_NonexistentDir(t *testing.T) {
	removed, err := CleanupOld(filepath.Join(t.TempDir(), "nope"), "", 36*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0", removed)
	}
}

// touch 把文件的 mtime 设为距今 age 之前。
func touch(path string, age time.Duration) {
	at := time.Now().Add(-age)
	if err := os.Chtimes(path, at, at); err != nil {
		panic(err)
	}
}
