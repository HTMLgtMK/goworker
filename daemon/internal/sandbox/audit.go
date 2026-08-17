package sandbox

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// AuditEntry 一条命令决策审计记录。UserDecision 是未来训练数据的标签（宪法 V）。
type AuditEntry struct {
	Timestamp      time.Time `json:"timestamp"`
	Command        string    `json:"command"`
	Cwd            string    `json:"cwd,omitempty"`
	RiskLevel      string    `json:"risk_level"`
	Effects        []string  `json:"effects"`
	Reasons        []Reason  `json:"reasons"`
	Source         string    `json:"source"`
	EngineDecision string    `json:"engine_decision"`         // allow | hitl | deny
	UserDecision   string    `json:"user_decision,omitempty"` // approve | edit | reject | respond（未走 HITL 时空）
	Outcome        string    `json:"outcome"`                 // executed | blocked | aborted
}

// AuditLogger 追加式 jsonl 审计记录器。mutex 串行化写 + fsync，进程崩溃不丢已确认记录。
// 路径语义：OpenAudit(dir) 写 dir/audit.jsonl。
type AuditLogger struct {
	mu sync.Mutex
	f  *os.File
}

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
