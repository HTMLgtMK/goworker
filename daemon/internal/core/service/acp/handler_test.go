package acp

import (
	"context"
	"strings"
	"sync"
	"testing"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// recordingReporter 记录 Reporter 回流的事件，供断言输出内容。
type recordingReporter struct {
	mu      sync.Mutex
	chunks  []string
	updates []protocol.SessionUpdateBody
}

func (r *recordingReporter) Update(_ string, body protocol.SessionUpdateBody) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updates = append(r.updates, body)
}

func (r *recordingReporter) MessageChunk(_ string, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunks = append(r.chunks, text)
}

func (r *recordingReporter) RequestPermission(context.Context, string, protocol.PermissionRequest) (string, error) {
	return "", aiacp.ErrPermissionUnsupported
}

func TestFrontendHandlerRejectsUnknownSession(t *testing.T) {
	handler := &FrontendHandler{
		evaluate: func(ctx *model.Context, input string) error {
			t.Error("evaluate must not run for unknown sessions")
			return nil
		},
	}
	_, err := handler.Prompt(context.Background(), "nope", []protocol.ContentBlock{protocol.TextBlock("hi")}, &recordingReporter{})
	if err == nil {
		t.Fatal("Prompt for unknown session: want error")
	}
}

func TestFrontendHandlerRoutesPromptThroughEvaluator(t *testing.T) {
	var (
		mu       sync.Mutex
		gotInput string
	)
	reporter := &recordingReporter{}
	handler := &FrontendHandler{
		evaluate: func(ctx *model.Context, input string) error {
			mu.Lock()
			gotInput = input
			mu.Unlock()
			ctx.Writer("hello ")
			ctx.Writer("world")
			return nil
		},
	}
	if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp"}, nil); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	stop, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{
		protocol.TextBlock("hello"),
		protocol.TextBlock("world"),
	}, reporter)
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if stop != protocol.StopEndTurn {
		t.Fatalf("stop reason = %q, want %q", stop, protocol.StopEndTurn)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotInput != "hello\nworld" {
		t.Fatalf("evaluate input = %q, want %q", gotInput, "hello\nworld")
	}
	if joined := strings.Join(reporter.chunks, ""); joined != "hello world" {
		t.Fatalf("chunks joined = %q, want %q", joined, "hello world")
	}
}

func TestNewFrontendHandlerFactoryPerConnection(t *testing.T) {
	evaluate := func(ctx *model.Context, input string) error { return nil }
	factory := NewFrontendHandlerFactory(evaluate, nil)

	first, second := factory(), factory()
	if first == second {
		t.Fatal("factory returned the same handler instance for two connections")
	}
	if _, ok := first.(aiacp.SessionHandler); !ok {
		t.Fatal("factory product does not implement aiacp.SessionHandler")
	}
}
