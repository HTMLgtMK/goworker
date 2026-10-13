package sandbox

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAudit_OpenRecordReadBack(t *testing.T) {
	dir := t.TempDir()
	lg, err := OpenAudit(dir)
	if err != nil {
		t.Fatalf("OpenAudit: %v", err)
	}
	defer lg.Close()

	entry := AuditEntry{
		Timestamp:      time.Now(),
		SessionID:      "sess-a",
		Command:        "rm -rf /tmp/x",
		RiskLevel:      "R3",
		Effects:        []string{"destructive"},
		Reasons:        []Reason{{Code: "write_command", Detail: "写操作命令"}},
		Source:         "heuristic",
		EngineDecision: "hitl",
		UserDecision:   "approve",
		Outcome:        "executed",
	}
	if err := lg.Record(entry); err != nil {
		t.Fatalf("Record: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	var got AuditEntry
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.SessionID != entry.SessionID || got.Command != entry.Command || got.RiskLevel != "R3" ||
		got.UserDecision != "approve" || got.EngineDecision != "hitl" {
		t.Errorf("readback mismatch: %+v", got)
	}
}

func TestAudit_AppendMultipleLines(t *testing.T) {
	dir := t.TempDir()
	lg, _ := OpenAudit(dir)
	defer lg.Close()

	for i := 0; i < 3; i++ {
		if err := lg.Record(AuditEntry{Timestamp: time.Now(), Command: "echo hi", RiskLevel: "R1"}); err != nil {
			t.Fatalf("Record %d: %v", i, err)
		}
	}
	b, _ := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		t.Errorf("lines = %d, want 3 (append, not overwrite)", len(lines))
	}
}

func TestAudit_ConcurrentSafe(t *testing.T) {
	dir := t.TempDir()
	lg, _ := OpenAudit(dir)
	defer lg.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lg.Record(AuditEntry{Timestamp: time.Now(), Command: "x", RiskLevel: "R1"})
		}()
	}
	wg.Wait()

	b, _ := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 20 {
		t.Errorf("lines = %d, want 20 (mutex must serialize)", len(lines))
	}
}

func TestAudit_ConcurrentLoggersShareWriteBoundary(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenAudit(dir)
	if err != nil {
		t.Fatalf("OpenAudit first: %v", err)
	}
	second, err := OpenAudit(dir)
	if err != nil {
		first.Close()
		t.Fatalf("OpenAudit second: %v", err)
	}
	defer first.Close()
	defer second.Close()

	const recordsPerLogger = 50
	var wg sync.WaitGroup
	for i, lg := range []*AuditLogger{first, second} {
		wg.Add(1)
		go func(session string, logger *AuditLogger) {
			defer wg.Done()
			for n := 0; n < recordsPerLogger; n++ {
				if err := logger.Record(AuditEntry{Timestamp: time.Now(), SessionID: session, Command: "echo hi"}); err != nil {
					t.Errorf("Record %s/%d: %v", session, n, err)
				}
			}
		}(string(rune('a'+i)), lg)
	}
	wg.Wait()

	b, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != recordsPerLogger*2 {
		t.Fatalf("lines = %d, want %d", len(lines), recordsPerLogger*2)
	}
	for i, line := range lines {
		var entry AuditEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("line %d is torn JSON: %v", i, err)
		}
		if entry.SessionID != "a" && entry.SessionID != "b" {
			t.Errorf("line %d session_id = %q, want a or b", i, entry.SessionID)
		}
	}
}
func TestAudit_CreatesNestedDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "audit")
	lg, err := OpenAudit(dir)
	if err != nil {
		t.Fatalf("OpenAudit nested: %v", err)
	}
	defer lg.Close()
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("nested dir not created: %v", err)
	}
}
