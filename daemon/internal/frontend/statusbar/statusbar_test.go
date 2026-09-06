package statusbar

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// captureStderr 在 fn 执行期间重定向 os.Stderr，返回捕获到的输出。
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	w.Close()
	os.Stderr = old
	return <-done
}

// TestBar_StopSilentWhenNeverTicked Start 后从未 Tick（瞬时命令）Stop 不落任何行。
func TestBar_StopSilentWhenNeverTicked(t *testing.T) {
	bar := New()
	bar.Use(NewTestAddon())
	out := captureStderr(t, func() {
		bar.Start()
		bar.Stop()
	})
	if out != "" {
		t.Errorf("silent stop output = %q, want empty", out)
	}
	if bar.Active() {
		t.Error("bar should be inactive after Stop")
	}
}

// TestBar_StopCommitsAfterTick 至少 Tick 过一轮后 Stop 落永久行（✓ + 耗时）。
func TestBar_StopCommitsAfterTick(t *testing.T) {
	bar := New()
	bar.Use(NewTestAddon())
	ctx, cancel := context.WithCancel(context.Background())
	out := captureStderr(t, func() {
		bar.Start()
		go bar.Run(ctx, nil)
		time.Sleep(450 * time.Millisecond) // 等 ticker 触发 ≥1 轮（200ms 周期，留足调度余量）
		cancel()
		bar.Stop()
	})
	if !strings.Contains(out, "test") {
		t.Errorf("committed output = %q, want final render committed", out)
	}
}

// TestAddon 是可断言的最小 addon。
type TestAddon struct{ n int }

func NewTestAddon() *TestAddon                { return &TestAddon{} }
func (a *TestAddon) Name() string             { return "test" }
func (a *TestAddon) Tick(ctx context.Context) { a.n++ }
func (a *TestAddon) Render() string           { return "test" }
func (a *TestAddon) Reset()                   {}
