package acp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
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

// fakeSessionSource 是脚本化的 model.SessionSource，供 adapter 测试注入。
type fakeSessionSource struct {
	mu              sync.Mutex
	currentID       string
	setSessionCalls []string
	reloadErr       error
	currentHistory  []model.SessionUpdate
	archived        map[string][]model.SessionUpdate
	list            []model.SessionSummary
	modes           *model.SessionModeState
}

func (f *fakeSessionSource) ListSessions() []model.SessionSummary {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.list
}

func (f *fakeSessionSource) CurrentSessionID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentID
}

func (f *fakeSessionSource) SetSession(sessionID, cwd string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setSessionCalls = append(f.setSessionCalls, sessionID+"|"+cwd)
}

func (f *fakeSessionSource) ReloadCurrentSession() error {
	return f.reloadErr
}

func (f *fakeSessionSource) CurrentSessionHistory() []model.SessionUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentHistory
}

func (f *fakeSessionSource) ArchivedSessionHistory(sessionID string) ([]model.SessionUpdate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if history, ok := f.archived[sessionID]; ok {
		return history, nil
	}
	return nil, fmt.Errorf("no archive for %q", sessionID)
}

func (f *fakeSessionSource) SessionModes() *model.SessionModeState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.modes
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
			ctx.Writer("\n") // 纯空白在围栏开启前必须被吞，不能造出空代码块
			ctx.Writer("world")
			return nil
		},
	}
	if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp"}, reporter); err != nil {
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
	joined := strings.Join(reporter.chunks, "")
	// 围栏开启前吞掉纯空白（首个 "hello " 即开栏）；开启后一律透传，
	// 输出应是「开栏 + hello + 换行 + world + 补换行 + 闭栏」。
	want := commandFence + "\nhello \nworld\n" + commandFence + "\n"
	if joined != want {
		t.Fatalf("chunks joined = %q, want %q", joined, want)
	}
}

func TestNewFrontendHandlerFactoryPerConnection(t *testing.T) {
	evaluate := func(ctx *model.Context, input string) error { return nil }
	factory := NewFrontendHandlerFactory(evaluate, nil, nil)

	first, second := factory(), factory()
	if first == second {
		t.Fatal("factory returned the same handler instance for two connections")
	}
	if _, ok := first.(aiacp.SessionHandler); !ok {
		t.Fatal("factory product does not implement aiacp.SessionHandler")
	}
}

func TestCreateSessionPushesCommands(t *testing.T) {
	reporter := &recordingReporter{}
	handler := &FrontendHandler{
		commands: func() []model.Command {
			return []model.Command{{Name: "/agent", Description: "agent", Aliases: []string{"/a"}}}
		},
	}
	if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{}, reporter); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if len(reporter.updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(reporter.updates))
	}
	body := reporter.updates[0]
	// 载荷整体经 Raw 透传，sessionUpdate 判别字段在 JSON 内部而非 Go 字段。
	if !strings.Contains(string(body.Raw), `"sessionUpdate":"available_commands_update"`) {
		t.Fatalf("raw payload missing discriminator: %s", body.Raw)
	}
	if !strings.Contains(string(body.Raw), `"name":"agent"`) || !strings.Contains(string(body.Raw), `"name":"a"`) {
		t.Fatalf("raw payload missing command/alias entries: %s", body.Raw)
	}
}

func TestLoadSessionCurrentReplaysHistory(t *testing.T) {
	sessions := &fakeSessionSource{
		currentID:      "s1",
		currentHistory: []model.SessionUpdate{{Kind: model.SessionUpdateAgentMessageChunk, Content: &model.SessionContent{Type: "text", Text: "hi"}}},
	}
	reporter := &recordingReporter{}
	handler := &FrontendHandler{
		evaluate: func(ctx *model.Context, input string) error { return nil },
		sessions: sessions,
	}

	if err := handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "s1", Cwd: "/tmp"}, reporter); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	// 重放的历史先于 load 响应写出；随后该 id 应可直接 prompt。
	stop, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("go on")}, reporter)
	if err != nil {
		t.Fatalf("Prompt after load: %v", err)
	}
	if stop != protocol.StopEndTurn {
		t.Fatalf("stop reason = %q, want %q", stop, protocol.StopEndTurn)
	}
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	if len(sessions.setSessionCalls) != 1 || sessions.setSessionCalls[0] != "s1|/tmp" {
		t.Fatalf("SetSession calls = %v, want [s1|/tmp]", sessions.setSessionCalls)
	}
}

func TestLoadSessionArchivedIsReadOnly(t *testing.T) {
	sessions := &fakeSessionSource{
		list: []model.SessionSummary{{SessionID: "old"}},
		archived: map[string][]model.SessionUpdate{
			"old": {{Kind: model.SessionUpdateUserMessageChunk, Content: &model.SessionContent{Type: "text", Text: "past"}}},
		},
	}
	reporter := &recordingReporter{}
	handler := &FrontendHandler{
		evaluate: func(ctx *model.Context, input string) error {
			t.Error("archived sessions are read-only; evaluate must not run")
			return nil
		},
		sessions: sessions,
	}

	if err := handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "old"}, reporter); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(reporter.updates) != 1 {
		t.Fatalf("updates = %d, want 1 replayed", len(reporter.updates))
	}
	// 归档会话 prompt 必须诚实报 read-only，而不是 unknown session。
	_, err := handler.Prompt(context.Background(), "old", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter)
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("Prompt archived = %v, want read-only error", err)
	}
}

func TestLoadSessionUnknownSession(t *testing.T) {
	handler := &FrontendHandler{sessions: &fakeSessionSource{currentID: "s1"}}
	err := handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "ghost"}, &recordingReporter{})
	if err == nil || !strings.Contains(err.Error(), "unknown session") {
		t.Fatalf("LoadSession ghost = %v, want unknown session error", err)
	}
}

func TestListSessionsAndModesMapDomain(t *testing.T) {
	sessions := &fakeSessionSource{
		list: []model.SessionSummary{{SessionID: "s1", Title: "t", UpdatedAt: "2026-01-01T00:00:00Z", Active: true}},
		modes: &model.SessionModeState{
			CurrentModeID:  "m1",
			AvailableModes: []model.SessionMode{{ID: "m1", Name: "mode one"}},
		},
	}
	handler := &FrontendHandler{sessions: sessions}

	infos, err := handler.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(infos) != 1 || infos[0].SessionID != "s1" || infos[0].Title != "t" {
		t.Fatalf("infos = %+v, want s1 mapped", infos)
	}

	modes := handler.SessionModes()
	if modes == nil || modes.CurrentModeID != "m1" || len(modes.AvailableModes) != 1 {
		t.Fatalf("modes = %+v, want m1 mapped", modes)
	}
}

func TestTokenToUpdateMapping(t *testing.T) {
	cases := []struct {
		name  string
		token core.Token
		want  protocol.SessionUpdateBody
	}{
		{
			name:  "thinking",
			token: core.Token{Type: core.TokenTypeThinking, Content: "hmm"},
			want: protocol.SessionUpdateBody{SessionUpdate: protocol.UpdateAgentThoughtChunk,
				Content: &protocol.ContentBlock{Type: "text", Text: "hmm"}},
		},
		{
			name:  "tool_call",
			token: core.Token{Type: core.TokenTypeToolCall, Content: "bash(ls)", ToolCall: core.ToolCall{ID: "t1"}},
			want: protocol.SessionUpdateBody{SessionUpdate: protocol.UpdateToolCall,
				ToolCallID: "t1", Title: "bash(ls)", Status: "pending"},
		},
		{
			name:  "tool_result",
			token: core.Token{Type: core.TokenTypeToolResult, Content: "ok", ToolCallID: "t1"},
			want: protocol.SessionUpdateBody{SessionUpdate: protocol.UpdateToolCallUpdate,
				ToolCallID: "t1", Title: "ok", Status: "completed"},
		},
		{
			name:  "text",
			token: core.Token{Type: core.TokenTypeText, Content: "hi"},
			want: protocol.SessionUpdateBody{SessionUpdate: protocol.UpdateAgentMessageChunk,
				Content: &protocol.ContentBlock{Type: "text", Text: "hi"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := TokenToUpdate(tc.token)
			if got.SessionUpdate != tc.want.SessionUpdate || got.ToolCallID != tc.want.ToolCallID ||
				got.Title != tc.want.Title || got.Status != tc.want.Status {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			if (got.Content == nil) != (tc.want.Content == nil) ||
				(got.Content != nil && *got.Content != *tc.want.Content) {
				t.Fatalf("content = %+v, want %+v", got.Content, tc.want.Content)
			}
		})
	}
}

// TestRememberClearsStaleReadOnlyMark 回归 MEDIUM-2：先以归档身份进入只读集合、
// 之后同一会话被恢复为当前会话再 load，旧的只读标记必须被清掉，prompt 不能被
// 永久误拒。
func TestRememberClearsStaleReadOnlyMark(t *testing.T) {
	handler := &FrontendHandler{
		evaluate: func(ctx *model.Context, input string) error { return nil },
		sessions: &fakeSessionSource{currentID: "old"},
	}
	handler.rememberReadOnly("old")
	if err := handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "old"}, &recordingReporter{}); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if _, err := handler.Prompt(context.Background(), "old", []protocol.ContentBlock{protocol.TextBlock("hi")}, &recordingReporter{}); err != nil {
		t.Fatalf("Prompt after restore: %v, want promptable", err)
	}
}
