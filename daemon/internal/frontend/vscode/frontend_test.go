package vscode

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	dispatch "github.com/tinguo/goworker/ai-dispatch"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

func TestTokenToUpdate_PreservesStructuredToolLifecycle(t *testing.T) {
	tests := []struct {
		name  string
		token core.Token
		want  protocol.SessionUpdateBody
	}{
		{
			name:  "thinking",
			token: core.Token{Type: core.TokenTypeThinking, Content: "reasoning"},
			want: protocol.SessionUpdateBody{
				SessionUpdate: protocol.UpdateAgentThoughtChunk,
				Content:       &protocol.ContentBlock{Type: "text", Text: "reasoning"},
			},
		},
		{
			name:  "tool call",
			token: core.Token{Type: core.TokenTypeToolCall, Content: "read file", ToolCall: core.ToolCall{ID: "call_1"}},
			want: protocol.SessionUpdateBody{
				SessionUpdate: protocol.UpdateToolCall,
				ToolCallID:    "call_1",
				Title:         "read file",
				Status:        "pending",
			},
		},
		{
			name:  "empty tool result",
			token: core.Token{Type: core.TokenTypeToolResult, ToolCallID: "call_1"},
			want: protocol.SessionUpdateBody{
				SessionUpdate: protocol.UpdateToolCallUpdate,
				ToolCallID:    "call_1",
				Status:        "completed",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tokenToUpdate(tt.token); !sameUpdate(got, tt.want) {
				t.Errorf("tokenToUpdate(%+v) = %+v, want %+v", tt.token, got, tt.want)
			}
		})
	}
}

func TestFrontend_ServesPromptWithoutGivingTransportSessionToEvaluator(t *testing.T) {
	path := newSocketPath(t)
	var (
		mu      sync.Mutex
		inputs  []string
		updates []protocol.SessionUpdate
	)
	updated := make(chan struct{}, 6)
	frontend := New(path, func(ctx *plugin.Context, input string) error {
		mu.Lock()
		inputs = append(inputs, input)
		mu.Unlock()
		ctx.Writer("command result\n")
		ctx.EmitToken(core.Token{Type: core.TokenTypeToolCall, Content: "read file", ToolCall: core.ToolCall{ID: "call_1"}})
		ctx.EmitToken(core.Token{Type: core.TokenTypeToolResult, ToolCallID: "call_1"})
		return nil
	})
	if err := frontend.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = frontend.Stop() })

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	client := dispatch.NewClient("vscode", conn, conn.Close)
	t.Cleanup(func() { _ = client.Close() })
	client.SetCallbacks(dispatch.ClientCallbacks{OnUpdate: func(update protocol.SessionUpdate) {
		mu.Lock()
		updates = append(updates, update)
		mu.Unlock()
		updated <- struct{}{}
	}})

	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	firstSession, err := client.NewSession(context.Background(), "/first")
	if err != nil {
		t.Fatalf("NewSession first: %v", err)
	}
	if _, err := client.Prompt(context.Background(), firstSession, []protocol.ContentBlock{protocol.TextBlock("remember this")}); err != nil {
		t.Fatalf("Prompt first: %v", err)
	}
	secondSession, err := client.NewSession(context.Background(), "/second")
	if err != nil {
		t.Fatalf("NewSession second: %v", err)
	}
	if _, err := client.Prompt(context.Background(), secondSession, []protocol.ContentBlock{protocol.TextBlock("and this")}); err != nil {
		t.Fatalf("Prompt second: %v", err)
	}
	for range 6 {
		select {
		case <-updated:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for ACP updates")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := inputs, []string{"remember this", "and this"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("evaluator inputs = %#v, want %#v", got, want)
	}
	if len(updates) != 6 {
		t.Fatalf("updates = %+v, want six message and tool lifecycle updates", updates)
	}
	var toolUpdates []protocol.SessionUpdate
	for _, update := range updates {
		if update.Update.ToolCallID != "" {
			toolUpdates = append(toolUpdates, update)
		}
	}
	if len(toolUpdates) != 4 {
		t.Fatalf("tool updates = %+v, want four lifecycle updates", toolUpdates)
	}
	for index, update := range toolUpdates {
		if update.Update.ToolCallID != "call_1" {
			t.Errorf("tool update[%d].toolCallId = %q, want call_1", index, update.Update.ToolCallID)
		}
	}
}

func TestFrontend_CancelStopsOnlyTheActivePrompt(t *testing.T) {
	path := newSocketPath(t)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	frontend := New(path, func(ctx *plugin.Context, _ string) error {
		close(started)
		<-ctx.Ctx.Done()
		close(cancelled)
		return ctx.Ctx.Err()
	})
	if err := frontend.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = frontend.Stop() })

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	client := dispatch.NewClient("vscode", conn, conn.Close)
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sessionID, err := client.NewSession(context.Background(), "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	promptDone := make(chan error, 1)
	go func() {
		_, err := client.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("wait")})
		promptDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for prompt")
	}
	if err := client.Cancel(sessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("cancel did not reach evaluator")
	}
	select {
	case err := <-promptDone:
		// 取消经 ai-dispatch 协议层包装为 *RPCError{-32000, "context canceled"}，
		// 客户端拿到的是协议错误而非原始 context.Canceled，用 errors.As 断言。
		var rpcErr *protocol.RPCError
		if err == nil || !errors.As(err, &rpcErr) || !strings.Contains(rpcErr.Message, "context canceled") {
			t.Errorf("Prompt error = %v, want *protocol.RPCError mentioning \"context canceled\"", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled prompt did not return")
	}
}

func TestFrontend_StartRejectsDuplicateWithoutDroppingActiveSocket(t *testing.T) {
	path := newSocketPath(t)
	frontend := New(path, func(*plugin.Context, string) error { return nil })
	if err := frontend.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	t.Cleanup(func() { _ = frontend.Stop() })

	if err := frontend.Start(); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("second Start error = %v, want already started", err)
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial after duplicate Start: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close probe connection: %v", err)
	}
}

func TestFrontend_StartWaitsForStop(t *testing.T) {
	path := newSocketPath(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	frontend := New(path, func(*plugin.Context, string) error {
		close(entered)
		<-release
		return nil
	})
	if err := frontend.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = frontend.Stop() })

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	client := dispatch.NewClient("vscode", conn, conn.Close)
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sessionID, err := client.NewSession(context.Background(), "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	go func() {
		_, _ = client.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("block")})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("prompt did not enter evaluator")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- frontend.Stop() }()
	deadline := time.After(time.Second)
	for {
		frontend.mu.Lock()
		listenerStopped := frontend.listener == nil
		frontend.mu.Unlock()
		if listenerStopped {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Stop did not detach listener")
		case <-time.After(time.Millisecond):
		}
	}
	started := make(chan error, 1)
	go func() { started <- frontend.Start() }()
	select {
	case err := <-started:
		t.Fatalf("Start returned %v before Stop finished", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-started; err != nil {
		t.Fatalf("Start after Stop: %v", err)
	}
	if _, err := net.Dial("unix", path); err != nil {
		t.Fatalf("Dial after restart: %v", err)
	}
}

func TestFrontend_StartStopLifecycle(t *testing.T) {
	path := newSocketPath(t)
	frontend := New(path, func(*plugin.Context, string) error { return nil })
	if err := frontend.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("socket file after Start: %v", err)
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial after Start: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close probe connection: %v", err)
	}
	if err := frontend.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("socket file after Stop: %v, want removed", err)
	}
}

func TestFrontend_StartRefusesRegularFile(t *testing.T) {
	path := newSocketPath(t)
	if err := os.WriteFile(path, []byte("keep me"), 0o600); err != nil {
		t.Fatalf("write regular file: %v", err)
	}
	frontend := New(path, func(*plugin.Context, string) error { return nil })
	if err := frontend.Start(); err == nil || !strings.Contains(err.Error(), "refuse to remove non-socket") {
		t.Fatalf("Start error = %v, want non-socket refusal", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read regular file after Start: %v", err)
	}
	if string(content) != "keep me" {
		t.Errorf("regular file content = %q, want preserved", content)
	}
}

func TestFrontend_StartRemovesStaleSocket(t *testing.T) {
	path := newSocketPath(t)
	stale, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen stale socket: %v", err)
	}
	unixListener, ok := stale.(*net.UnixListener)
	if !ok {
		t.Fatal("Unix listener has unexpected type")
	}
	unixListener.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatalf("close stale socket: %v", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("stale socket path: %v", err)
	}

	frontend := New(path, func(*plugin.Context, string) error { return nil })
	if err := frontend.Start(); err != nil {
		t.Fatalf("Start on stale socket: %v", err)
	}
	t.Cleanup(func() { _ = frontend.Stop() })
	if _, err := net.Dial("unix", path); err != nil {
		t.Fatalf("Dial after stale-socket restart: %v", err)
	}
}

func newSocketPath(t *testing.T) string {
	t.Helper()
	path, err := os.CreateTemp("", "gw-acp-")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if err := path.Close(); err != nil {
		t.Fatalf("close socket placeholder: %v", err)
	}
	if err := os.Remove(path.Name()); err != nil {
		t.Fatalf("remove socket placeholder: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(path.Name()) })
	return path.Name()
}

func sameUpdate(got, want protocol.SessionUpdateBody) bool {
	if got.SessionUpdate != want.SessionUpdate || got.ToolCallID != want.ToolCallID || got.Title != want.Title || got.Status != want.Status {
		return false
	}
	if got.Content == nil || want.Content == nil {
		return got.Content == want.Content
	}
	return *got.Content == *want.Content
}
