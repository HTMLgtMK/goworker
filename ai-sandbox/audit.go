package sandbox

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// OpenAudit 打开（必要时创建）审计文件。目录不存在则自动创建。
func OpenAudit(dir string) (*AuditLogger, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("audit mkdir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "audit.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("audit open: %w", err)
	}
	return &AuditLogger{f: f}, nil
}

// Record 追加一条记录并 fsync。
func (l *AuditLogger) Record(e AuditEntry) error {
	if l == nil || l.f == nil {
		return nil
	}
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("audit marshal: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("audit write: %w", err)
	}
	return l.f.Sync()
}

// Close 关闭底层文件。
func (l *AuditLogger) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
