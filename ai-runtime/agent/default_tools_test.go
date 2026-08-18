package agent

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
)

func TestDefaultToolsReadWriteStillWork(t *testing.T) {
	tools := DefaultTools(nil)
	tm := make(map[string]core.Tool, len(tools))
	for _, tl := range tools {
		tm[tl.Name] = tl
	}
	path := filepath.Join(t.TempDir(), "a.txt")
	if _, err := tm["write_file"].Execute(context.Background(), map[string]any{"path": path, "content": "hello"}); err != nil {
		t.Fatalf("write_file: %v", err)
	}
	out, err := tm["read_file"].Execute(context.Background(), map[string]any{"path": path})
	if err != nil {
		t.Fatalf("read_file: %v", err)
	}
	if !strings.Contains(out, "hello") {
		t.Errorf("read_file output = %q, want contains hello", out)
	}
}

// TestReadFileNegativeOffset 回归：LLM 传负 offset 时不应 panic（slice 负索引）。
func TestReadFileNegativeOffset(t *testing.T) {
	tools := DefaultTools(nil)
	tm := make(map[string]core.Tool, len(tools))
	for _, tl := range tools {
		tm[tl.Name] = tl
	}
	path := filepath.Join(t.TempDir(), "multi.txt")
	content := "l1\nl2\nl3\nl4\nl5\n"
	if _, err := tm["write_file"].Execute(context.Background(), map[string]any{"path": path, "content": content}); err != nil {
		t.Fatalf("write_file: %v", err)
	}

	// 负 offset + 有效 limit 组合是原 panic 路径（lines[-3:5]）
	out, err := tm["read_file"].Execute(context.Background(), map[string]any{"path": path, "offset": -3, "limit": 5})
	if err != nil {
		t.Fatalf("read_file with negative offset: %v", err)
	}
	// 负 offset clamp 到 0，应返回带行号的前几行
	if !strings.Contains(out, "l1") {
		t.Errorf("output = %q, want l1 visible after negative offset clamp", out)
	}

	// 纯负 offset 也应兜底为全文
	out2, err := tm["read_file"].Execute(context.Background(), map[string]any{"path": path, "offset": -10})
	if err != nil {
		t.Fatalf("read_file with only negative offset: %v", err)
	}
	if !strings.Contains(out2, "l5") {
		t.Errorf("output = %q, want full content visible", out2)
	}
}
