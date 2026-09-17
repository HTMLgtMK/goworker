package vscode

import (
	"context"
	"encoding/json"
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

func TestAvailableCommandsUpdateBody(t *testing.T) {
	body := availableCommandsUpdateBody([]plugin.Command{
		{Name: "/help", Description: "显示帮助"},
		{Name: "/diagnose", Aliases: []string{"/diag"}, Description: "诊断"},
	})
	if body.Raw == nil {
		t.Fatalf("body = %+v, want Raw payload", body)
	}
	var payload availableCommandsUpdate
	if err := json.Unmarshal(body.Raw, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.SessionUpdate != protocol.UpdateAvailableCommands {
		t.Errorf("sessionUpdate = %q, want %q", payload.SessionUpdate, protocol.UpdateAvailableCommands)
	}
	got := payload.AvailableCommands
	want := []availableCommand{
		{Name: "help", Description: "显示帮助", Source: "daemon"},
		{Name: "diagnose", Description: "诊断", Source: "daemon"},
		{Name: "diag", Description: "(alias of /diagnose)", Source: "daemon"},
	}
	if len(got) != len(want) {
		t.Fatalf("availableCommands = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("availableCommands[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAvailableCommandsUpdateBody_EmptyListIsNonNull(t *testing.T) {
	body := availableCommandsUpdateBody(nil)
	if body.Raw == nil {
		t.Fatal("empty command list must still produce a Raw payload")
	}
	var payload availableCommandsUpdate
	if err := json.Unmarshal(body.Raw, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.AvailableCommands == nil {
		t.Fatalf("availableCommands = nil, want non-nil empty slice to clear stale UI state")
	}
	if len(payload.AvailableCommands) != 0 {
		t.Fatalf("availableCommands = %+v, want empty", payload.AvailableCommands)
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
	}, nil, nil)
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

func TestFrontend_EmitsAvailableCommandsOnSessionNew(t *testing.T) {
	path := newSocketPath(t)
	commands := []plugin.Command{
		{Name: "/help", Description: "显示帮助"},
		{Name: "/diagnose", Aliases: []string{"/diag"}, Description: "诊断"},
	}
	frontend := New(path, func(*plugin.Context, string) error { return nil }, func() []plugin.Command {
		return commands
	}, nil)
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

	var (
		mu      sync.Mutex
		updates []protocol.SessionUpdate
	)
	updated := make(chan struct{}, 1)
	client.SetCallbacks(dispatch.ClientCallbacks{OnUpdate: func(update protocol.SessionUpdate) {
		mu.Lock()
		updates = append(updates, update)
		mu.Unlock()
		updated <- struct{}{}
	}})

	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sessionID, err := client.NewSession(context.Background(), "/work")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	select {
	case <-updated:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for available_commands_update")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(updates) != 1 {
		t.Fatalf("updates = %+v, want exactly one available_commands_update", updates)
	}
	update := updates[0]
	if update.SessionID != sessionID {
		t.Errorf("update.sessionId = %q, want %q", update.SessionID, sessionID)
	}
	if update.Update.SessionUpdate != protocol.UpdateAvailableCommands {
		t.Errorf("sessionUpdate = %q, want %q", update.Update.SessionUpdate, protocol.UpdateAvailableCommands)
	}
	var payload availableCommandsUpdate
	if err := json.Unmarshal(update.Update.Raw, &payload); err != nil {
		t.Fatalf("unmarshal availableCommands payload: %v", err)
	}
	if len(payload.AvailableCommands) != 3 {
		t.Fatalf("availableCommands = %+v, want three entries (canonical + alias expanded)", payload.AvailableCommands)
	}
	names := make([]string, 0, len(payload.AvailableCommands))
	for _, cmd := range payload.AvailableCommands {
		names = append(names, cmd.Name)
	}
	want := []string{"help", "diagnose", "diag"}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("availableCommands[%d].name = %q, want %q", i, names[i], want[i])
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
	}, nil, nil)
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
	frontend := New(path, func(*plugin.Context, string) error { return nil }, nil, nil)
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
	}, nil, nil)
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
	frontend := New(path, func(*plugin.Context, string) error { return nil }, nil, nil)
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
	frontend := New(path, func(*plugin.Context, string) error { return nil }, nil, nil)
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

	frontend := New(path, func(*plugin.Context, string) error { return nil }, nil, nil)
	if err := frontend.Start(); err != nil {
		t.Fatalf("Start on stale socket: %v", err)
	}
	t.Cleanup(func() { _ = frontend.Stop() })
	if _, err := net.Dial("unix", path); err != nil {
		t.Fatalf("Dial after stale-socket restart: %v", err)
	}
}

// fakeSessionSource 是 SessionSource 的测试桩：注入当前会话 id、清单、重放历史
// 与 reload 错误。
type fakeSessionSource struct {
	current   string
	list      []protocol.SessionInfo
	history   []protocol.SessionUpdateBody
	reloaded  int
	reloadErr error
}

func (s *fakeSessionSource) ListSessions() []protocol.SessionInfo { return s.list }
func (s *fakeSessionSource) CurrentSessionID() string             { return s.current }
func (s *fakeSessionSource) ReloadCurrentSession() error {
	s.reloaded++
	return s.reloadErr
}
func (s *fakeSessionSource) CurrentSessionHistory() []protocol.SessionUpdateBody { return s.history }

// recordingReporter 记录 ingress 经 dispatch.Reporter 推送的全部更新（顺序即
// 推送顺序）。
type recordingReporter struct {
	mu      sync.Mutex
	updates []protocol.SessionUpdate
}

func (r *recordingReporter) Update(sessionID string, body protocol.SessionUpdateBody) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updates = append(r.updates, protocol.SessionUpdate{SessionID: sessionID, Update: body})
}

func (r *recordingReporter) MessageChunk(sessionID, text string) {
	r.Update(sessionID, protocol.SessionUpdateBody{
		SessionUpdate: protocol.UpdateAgentMessageChunk,
		Content:       &protocol.ContentBlock{Type: "text", Text: text},
	})
}

func (r *recordingReporter) all() []protocol.SessionUpdate {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]protocol.SessionUpdate(nil), r.updates...)
}

// dialACP 连接 socket 并返回已开始 Serve 的裸协议 Conn：session/list 与
// session/load 尚无 dispatch.Client 封装，直接走协议层断言 wire 行为。
func dialACP(t *testing.T, path string) *protocol.Conn {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	client := protocol.NewConn(conn)
	t.Cleanup(func() { _ = client.Close() })
	go func() { _ = client.Serve() }()
	return client
}

func TestFrontend_ServesSessionListAndAdvertisesLoadCapability(t *testing.T) {
	path := newSocketPath(t)
	source := &fakeSessionSource{
		current: "sess_live",
		list: []protocol.SessionInfo{
			{SessionID: "sess_live", Title: "当前会话", UpdatedAt: "2026-01-02T03:04:05Z"},
			{SessionID: "1735689600000000000", Title: "归档会话"},
		},
	}
	frontend := New(path, func(*plugin.Context, string) error { return nil }, nil, source)
	if err := frontend.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = frontend.Stop() })

	client := dialACP(t, path)
	var init protocol.InitializeResponse
	if err := client.Call(context.Background(), protocol.MethodInitialize,
		protocol.InitializeRequest{ProtocolVersion: protocol.Version}, &init); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if !init.AgentCapabilities.LoadSession {
		t.Errorf("LoadSession capability = false, want true (ingress 实现 SessionLoader)")
	}

	var list protocol.ListSessionsResponse
	if err := client.Call(context.Background(), protocol.MethodSessionList,
		protocol.ListSessionsRequest{Cwd: "/work"}, &list); err != nil {
		t.Fatalf("session/list: %v", err)
	}
	if len(list.Sessions) != len(source.list) || list.Sessions[0] != source.list[0] || list.Sessions[1] != source.list[1] {
		t.Errorf("sessions = %+v, want %+v", list.Sessions, source.list)
	}
	if list.NextCursor != "" {
		t.Errorf("nextCursor = %q, want empty", list.NextCursor)
	}
}

func TestFrontend_LoadSessionResumesOnlyTheCurrentSession(t *testing.T) {
	path := newSocketPath(t)
	source := &fakeSessionSource{
		current: "sess_live",
		list: []protocol.SessionInfo{
			{SessionID: "sess_live"},
			{SessionID: "1735689600000000000"},
		},
	}
	frontend := New(path, func(*plugin.Context, string) error { return nil }, nil, source)
	if err := frontend.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = frontend.Stop() })

	client := dialACP(t, path)
	if err := client.Call(context.Background(), protocol.MethodInitialize,
		protocol.InitializeRequest{ProtocolVersion: protocol.Version}, &protocol.InitializeResponse{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	// 当前会话：确认上下文已在 → 触发 ReloadCurrentSession 后成功
	var resp protocol.LoadSessionResponse
	if err := client.Call(context.Background(), protocol.MethodSessionLoad,
		protocol.LoadSessionRequest{SessionID: "sess_live", Cwd: "/work", McpServers: []map[string]any{}}, &resp); err != nil {
		t.Fatalf("session/load current: %v", err)
	}
	if source.reloaded != 1 {
		t.Errorf("reloaded = %d, want exactly one view refresh", source.reloaded)
	}

	// 归档会话：明确报错，不得假成功
	err := client.Call(context.Background(), protocol.MethodSessionLoad,
		protocol.LoadSessionRequest{SessionID: "1735689600000000000", Cwd: "/work"}, &resp)
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || !strings.Contains(rpcErr.Message, "archived session resume is not supported yet") {
		t.Errorf("archived load error = %v, want RPCError mentioning archived session resume", err)
	}
	if source.reloaded != 1 {
		t.Errorf("reloaded = %d after archived load, want unchanged", source.reloaded)
	}

	// 未知会话：点名报错
	err = client.Call(context.Background(), protocol.MethodSessionLoad,
		protocol.LoadSessionRequest{SessionID: "sess_nope", Cwd: "/work"}, &resp)
	if !errors.As(err, &rpcErr) || !strings.Contains(rpcErr.Message, "unknown session") {
		t.Errorf("unknown load error = %v, want RPCError mentioning unknown session", err)
	}
}

// TestIngress_LoadSessionReplaysCommandsThenHistory 断言 load 成功路径上 ingress
// 经 Reporter 的推送顺序：先 available_commands_update（斜杠补全），再逐条历史；
// 且 load 后本连接 known-session 检查放行（session/prompt 不再被拒）。
func TestIngress_LoadSessionReplaysCommandsThenHistory(t *testing.T) {
	h := &ingress{
		evaluate: func(*plugin.Context, string) error { return nil },
		commands: func() []plugin.Command {
			return []plugin.Command{{Name: "/help", Description: "帮助"}}
		},
		sessionSource: &fakeSessionSource{
			current: "sess_live",
			history: []protocol.SessionUpdateBody{
				{SessionUpdate: protocol.UpdateUserMessageChunk, Content: &protocol.ContentBlock{Type: "text", Text: "旧问题"}},
				{SessionUpdate: protocol.UpdateAgentMessageChunk, Content: &protocol.ContentBlock{Type: "text", Text: "旧回答"}},
			},
		},
		sessions: make(map[string]struct{}),
	}
	rep := &recordingReporter{}

	if err := h.LoadSession("sess_live", rep); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}

	updates := rep.all()
	if len(updates) != 3 {
		t.Fatalf("updates = %+v, want [commands, user, agent]", updates)
	}
	for _, update := range updates {
		if update.SessionID != "sess_live" {
			t.Errorf("update.sessionId = %q, want sess_live", update.SessionID)
		}
	}
	// 第一条：命令清单走 Raw 透传，判别值在载荷里
	var commands availableCommandsUpdate
	if err := json.Unmarshal(updates[0].Update.Raw, &commands); err != nil {
		t.Fatalf("unmarshal availableCommands payload: %v", err)
	}
	if commands.SessionUpdate != protocol.UpdateAvailableCommands || len(commands.AvailableCommands) != 1 || commands.AvailableCommands[0].Name != "help" {
		t.Errorf("first update payload = %+v, want available_commands_update with /help", commands)
	}
	// 第二、三条：历史按落盘顺序重放（user → agent）
	if updates[1].Update.SessionUpdate != protocol.UpdateUserMessageChunk || updates[1].Update.Content == nil || updates[1].Update.Content.Text != "旧问题" {
		t.Errorf("second update = %+v, want user_message_chunk 旧问题", updates[1])
	}
	if updates[2].Update.SessionUpdate != protocol.UpdateAgentMessageChunk || updates[2].Update.Content == nil || updates[2].Update.Content.Text != "旧回答" {
		t.Errorf("third update = %+v, want agent_message_chunk 旧回答", updates[2])
	}

	// load 不走 session/new：不 remember 则这里报 unknown transport session
	if _, err := h.Run(context.Background(), "sess_live", "新问题", rep); err != nil {
		t.Errorf("Run after load = %v, want known session (load 必须记住 sessionId)", err)
	}
}

// TestFrontend_LoadSessionReplaysHistoryAndEnablesPrompt 走完整 socket 路径：
// load 成功后客户端收到 1 条命令清单 + 全部历史通知（client 侧通知回调经 go
// 异步派发、到达顺序无保证，顺序断言在 TestIngress_LoadSessionReplaysCommandsThenHistory），
// 且 load 出的 sessionId 可直接 prompt（known-session 放行）。
func TestFrontend_LoadSessionReplaysHistoryAndEnablesPrompt(t *testing.T) {
	path := newSocketPath(t)
	source := &fakeSessionSource{
		current: "sess_live",
		list:    []protocol.SessionInfo{{SessionID: "sess_live", Title: "当前会话"}},
		history: []protocol.SessionUpdateBody{
			{SessionUpdate: protocol.UpdateUserMessageChunk, Content: &protocol.ContentBlock{Type: "text", Text: "旧问题"}},
			{SessionUpdate: protocol.UpdateAgentMessageChunk, Content: &protocol.ContentBlock{Type: "text", Text: "旧回答"}},
		},
	}
	evaluated := make(chan string, 1)
	frontend := New(path, func(_ *plugin.Context, input string) error {
		evaluated <- input
		return nil
	}, func() []plugin.Command { return []plugin.Command{{Name: "/help", Description: "帮助"}} }, source)
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

	var (
		mu      sync.Mutex
		updates []protocol.SessionUpdate
	)
	updated := make(chan struct{}, 4)
	client.SetCallbacks(dispatch.ClientCallbacks{OnUpdate: func(update protocol.SessionUpdate) {
		mu.Lock()
		updates = append(updates, update)
		mu.Unlock()
		updated <- struct{}{}
	}})
	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if _, err := client.SessionLoad(context.Background(), "sess_live", "/work"); err != nil {
		t.Fatalf("SessionLoad: %v", err)
	}
	for range 3 { // 1 commands + 2 history
		select {
		case <-updated:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for load replay updates, got %+v", updates)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(updates) != 3 {
		t.Fatalf("updates = %+v, want 3 (commands + user + agent)", updates)
	}
	var sawCommands, sawUser, sawAgent bool
	for _, update := range updates {
		switch update.Update.SessionUpdate {
		case protocol.UpdateAvailableCommands:
			sawCommands = true
		case protocol.UpdateUserMessageChunk:
			sawUser = update.Update.Content != nil && update.Update.Content.Text == "旧问题"
		case protocol.UpdateAgentMessageChunk:
			sawAgent = update.Update.Content != nil && update.Update.Content.Text == "旧回答"
		}
	}
	if !sawCommands || !sawUser || !sawAgent {
		t.Errorf("updates = %+v, want one commands update and both history chunks", updates)
	}

	// 修复回归：load 路径记住 sessionId 后，同连接 prompt 不再被拒
	if _, err := client.Prompt(context.Background(), "sess_live", []protocol.ContentBlock{protocol.TextBlock("新问题")}); err != nil {
		t.Fatalf("Prompt after load: %v", err)
	}
	select {
	case got := <-evaluated:
		if got != "新问题" {
			t.Errorf("evaluator input = %q, want 新问题", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for evaluator after load")
	}
}

func TestIngress_SessionCapabilitiesWithoutSource(t *testing.T) {
	h := &ingress{}
	if got := h.ListSessions(); got == nil || len(got) != 0 {
		t.Errorf("ListSessions = %#v, want non-nil empty", got)
	}
	err := h.LoadSession("sess_any", &recordingReporter{})
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || !strings.Contains(rpcErr.Message, "not supported") {
		t.Errorf("LoadSession = %v, want RPCError mentioning not supported", err)
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
