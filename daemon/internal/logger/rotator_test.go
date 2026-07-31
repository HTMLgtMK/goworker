package logger

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRotatingWriter_RotatesOnSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	var rotations int
	w, err := NewRotatingWriter(path, 32, func() { rotations++ })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	first := []byte("01234567890123456789012345678901") // 正好 32 字节，写满
	w.Write(first)
	w.Write([]byte("A")) // 下次写触发轮转
	w.Write([]byte("B"))

	if rotations != 1 {
		t.Fatalf("want 1 rotation, got %d", rotations)
	}
	// 当前文件应只剩最后一次轮转后写入的内容
	cur, _ := os.ReadFile(path)
	if string(cur) != "AB" {
		t.Fatalf("current file = %q, want %q", cur, "AB")
	}

	files, _ := filepath.Glob(filepath.Join(dir, "app.*.log"))
	if len(files) != 1 {
		t.Fatalf("want 1 rotated file, got %d: %v", len(files), files)
	}
	rotated, _ := os.ReadFile(files[0])
	if string(rotated) != string(first) {
		t.Fatalf("rotated file = %q, want %q", rotated, first)
	}
}

func TestRotatingWriter_ContentPreserved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, _ := NewRotatingWriter(path, 16, nil)
	defer w.Close()

	var want strings.Builder
	chunks := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, c := range chunks {
		w.Write([]byte(c))
		want.WriteString(c)
	}

	got := joinAllFiles(t, dir)
	if got != want.String() {
		t.Fatalf("concatenated = %q, want %q", got, want.String())
	}
}

func TestRotatingWriter_MultipleRotations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	var rotations int
	w, _ := NewRotatingWriter(path, 8, func() { rotations++ })
	defer w.Close()

	for i := 0; i < 10; i++ {
		w.Write([]byte("12345678")) // 每段写满 8 字节
	}

	if rotations != 9 {
		t.Fatalf("want 9 rotations, got %d", rotations)
	}
	if got := joinAllFiles(t, dir); got != strings.Repeat("12345678", 10) {
		t.Fatalf("content mismatch after multiple rotations")
	}
}

// TestRotatingWriter_ConcurrentWrites 并发写不崩、字节总量不丢（配合 -race 验证竞态）。
func TestRotatingWriter_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	w, _ := NewRotatingWriter(path, 512, nil)
	defer w.Close()

	const goroutines, perG = 8, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				w.Write([]byte("hello\n"))
			}
		}()
	}
	wg.Wait()

	total := int64(goroutines * perG * len("hello\n"))
	var sum int64
	filepath.Walk(dir, func(_ string, fi os.FileInfo, _ error) error {
		if !fi.IsDir() {
			sum += fi.Size()
		}
		return nil
	})
	if sum != total {
		t.Fatalf("total bytes = %d, want %d", sum, total)
	}
	// 每段写入原子完成，内容应是无损的 "hello\n" 重复
	got := joinAllFiles(t, dir)
	if strings.Count(got, "hello\n") != goroutines*perG {
		t.Fatalf("lost or corrupted lines: got %d hello lines", strings.Count(got, "hello\n"))
	}
}

// joinAllFiles 按文件名排序拼接目录下所有日志文件。
func joinAllFiles(t *testing.T, dir string) string {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dir, "app*.log"))
	var sb strings.Builder
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		sb.Write(b)
	}
	return sb.String()
}
