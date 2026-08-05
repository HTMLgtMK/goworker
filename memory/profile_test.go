package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestProfile(t *testing.T, maxChars int) *Profile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "USER.md")
	return newProfile(path, maxChars)
}

func TestAddEntry(t *testing.T) {
	p := newTestProfile(t, 1500)
	res, err := p.AddEntry("prefers 中文 reply")
	if err != nil || res != "added" {
		t.Fatalf("AddEntry = %q, %v", res, err)
	}
	// 重复条目自动跳过
	if res, err := p.AddEntry("prefers 中文 reply"); err != nil || res != "duplicate, skipped" {
		t.Fatalf("duplicate AddEntry = %q, %v", res, err)
	}
	// 多行输入归一成单行
	if _, err := p.AddEntry("  a\n   b  "); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.Content(), "a b") {
		t.Fatalf("multi-line entry not normalized: %q", p.Content())
	}
	// 空条目报错
	if _, err := p.AddEntry("   "); err == nil {
		t.Fatal("empty entry should error")
	}
	// 原子落盘可读回
	raw, err := os.ReadFile(p.path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); got != strings.TrimSpace(p.Content()) {
		t.Fatalf("disk content mismatch: %q", got)
	}
}

func TestAddEntryCapacity(t *testing.T) {
	p := newTestProfile(t, 20)
	if _, err := p.AddEntry("short"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.AddEntry("this entry is way too long to fit the tiny budget"); err != ErrProfileFull {
		t.Fatalf("expect ErrProfileFull, got %v", err)
	}
}

func TestReplaceEntry(t *testing.T) {
	p := newTestProfile(t, 1500)
	p.AddEntry("prefers short replies")
	p.AddEntry("likes Go")

	res, err := p.ReplaceEntry("short replies", "prefers detailed replies with examples")
	if err != nil || res != "replaced" {
		t.Fatalf("ReplaceEntry = %q, %v", res, err)
	}
	if !strings.Contains(p.Content(), "detailed replies with examples") {
		t.Fatalf("replace failed: %q", p.Content())
	}
	if strings.Contains(p.Content(), "short replies") {
		t.Fatalf("old entry still present: %q", p.Content())
	}
}

func TestReplaceEntryAmbiguous(t *testing.T) {
	p := newTestProfile(t, 1500)
	p.AddEntry("likes Go")
	p.AddEntry("likes Go a lot")
	if _, err := p.ReplaceEntry("likes Go", "hates Go"); err == nil {
		t.Fatal("ambiguous old_text should error")
	}
	if _, err := p.ReplaceEntry("nonexistent", "x"); err == nil {
		t.Fatal("not-found old_text should error")
	}
}

func TestRemoveEntry(t *testing.T) {
	p := newTestProfile(t, 1500)
	p.AddEntry("keep me")
	p.AddEntry("drop me")
	res, err := p.RemoveEntry("drop me")
	if err != nil || res != "removed" {
		t.Fatalf("RemoveEntry = %q, %v", res, err)
	}
	if strings.Contains(p.Content(), "drop me") {
		t.Fatalf("entry not removed: %q", p.Content())
	}
	if !strings.Contains(p.Content(), "keep me") {
		t.Fatalf("sibling entry lost: %q", p.Content())
	}
	// 删空后 content 为空
	if _, err := p.RemoveEntry("keep me"); err != nil {
		t.Fatal(err)
	}
	if p.Content() != "" {
		t.Fatalf("expect empty content, got %q", p.Content())
	}
}

func TestRemoveEntryNotFound(t *testing.T) {
	p := newTestProfile(t, 1500)
	if _, err := p.RemoveEntry("nothing"); err == nil {
		t.Fatal("not-found old_text should error")
	}
}

func TestCapacity(t *testing.T) {
	p := newTestProfile(t, 10)
	if _, err := p.AddEntry("12345"); err != nil {
		t.Fatal(err)
	}
	used, max := p.Capacity()
	if max != 10 || used < 5 {
		t.Fatalf("Capacity = %d/%d, want >=5/10", used, max)
	}
}

func TestAddEntryPreservesHandEdit(t *testing.T) {
	p := newTestProfile(t, 1500)
	if _, err := p.AddEntry("original"); err != nil {
		t.Fatal(err)
	}
	// 模拟会话内手改：直接写文件加一条
	if err := os.WriteFile(p.path, []byte("original\nhand-edited"), 0o600); err != nil {
		t.Fatal(err)
	}
	// agent 写新条目，不应基于陈旧内存覆盖手改
	if _, err := p.AddEntry("agent-added"); err != nil {
		t.Fatal(err)
	}
	c := p.Content()
	if !strings.Contains(c, "hand-edited") {
		t.Fatalf("hand edit lost after agent write: %q", c)
	}
	if !strings.Contains(c, "agent-added") {
		t.Fatalf("agent entry missing: %q", c)
	}
}
