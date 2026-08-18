package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetup_NoFile(t *testing.T) {
	l, err := Setup(Default())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.rotator != nil {
		t.Error("rotator should be nil when no file configured")
	}
	// 不炸、能写即可
	l.Info("smoke", "k", "v")
}

func TestSetup_WritesToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	l, err := Setup(Config{Level: "info", File: path})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	l.Info("hello", "request_id", "req-123")
	l.Warn("careful")

	b, _ := os.ReadFile(path)
	got := string(b)
	if !strings.Contains(got, "hello") || !strings.Contains(got, "req-123") {
		t.Fatalf("file missing structured fields: %q", got)
	}
	if !strings.Contains(got, "careful") {
		t.Fatalf("file missing warn line: %q", got)
	}
}

func TestSetup_LevelFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	l, _ := Setup(Config{Level: "error", File: path})
	defer l.Close()

	l.Debug("should-not-appear")
	l.Info("also-not")
	l.Error("boom", "code", 500)

	b, _ := os.ReadFile(path)
	got := string(b)
	if strings.Contains(got, "should-not-appear") || strings.Contains(got, "also-not") {
		t.Fatalf("level filter leaked lower logs: %q", got)
	}
	if !strings.Contains(got, "boom") {
		t.Fatalf("error line missing: %q", got)
	}
}

func TestSetup_DefaultsApplied(t *testing.T) {
	// 空 Level 用默认 info；空 File 不写文件
	l, err := Setup(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.rotator != nil {
		t.Error("empty File should not create rotator")
	}
}

func TestSetup_UnwritablePath(t *testing.T) {
	// dir 位置放一个普通文件，MkdirAll 无法在其下建目录 → 初始化失败
	blocker := filepath.Join(t.TempDir(), "afile")
	os.WriteFile(blocker, []byte("x"), 0644)
	path := filepath.Join(blocker, "sub", "app.log")

	if _, err := Setup(Config{File: path}); err == nil {
		t.Fatal("want error for unwritable log path, got nil")
	}
}

func TestSetup_InvalidLevelFallsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	l, err := Setup(Config{Level: "not-a-level", File: path})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Info("works") // 兜底 info 级，Info 必须能落盘
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "works") {
		t.Fatalf("fallback level rejected info line: %q", b)
	}
}
