package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
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
// SetSession 无条件转移 active（不建模 cwd 校验——校验与「无效声明不顶掉」
// 语义归 AgentPlugin 侧测试）。
type fakeSessionSource struct {
	mu              sync.Mutex
	currentID       string
	activeID        string
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
	f.activeID = sessionID
}

func (f *fakeSessionSource) ActiveSessionID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.activeID
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

// capsReporter 在 recordingReporter 之上补齐 FrontendHandler 注入设备工具源
// 所需的两个通道：initialize 协商的 clientCapabilities 与 client-ward Call。
type capsReporter struct {
	recordingReporter
	caps protocol.ClientCapabilities
}

func (r *capsReporter) ClientCapabilities() protocol.ClientCapabilities { return r.caps }

// Call 模拟客户端应答 x-device/tools 探测：result 是 relay 层的未导出应答结构，
// 统一以 JSON 解码填充，不感知具体类型。
func (r *capsReporter) Call(_ context.Context, method string, _ any, result any) error {
	if method != acpworker.MethodDeviceTools {
		return fmt.Errorf("unexpected client-ward method %q", method)
	}
	const payload = `{"tools":[{"name":"vibrate","description":"buzz the device","schema":"{\"type\":\"object\"}","risk_level":"benign"}]}`
	return json.Unmarshal([]byte(payload), result)
}

// decodeCaps 从 JSON 构造 clientCapabilities（rawCapability 保留任意 key，
// 包括 x-device 这类不进标准面的扩展）。
func decodeCaps(t *testing.T, raw string) protocol.ClientCapabilities {
	t.Helper()
	var caps protocol.ClientCapabilities
	if err := json.Unmarshal([]byte(raw), &caps); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	return caps
}

// TestPromptInjectsDeviceToolSourceWhenCapable：对端声明 x-device 且提供 Call
// 通道时，Prompt 注入惰性工具源；调用即 probe，工具带 sys_ 前缀。
func TestPromptInjectsDeviceToolSourceWhenCapable(t *testing.T) {
	reporter := &capsReporter{caps: decodeCaps(t, `{"x-device": {"tools": true}}`)}
	var (
		mu      sync.Mutex
		toolSrc func(context.Context) []core.Tool
	)
	handler := &FrontendHandler{
		evaluate: func(ctx *model.Context, _ string) error {
			mu.Lock()
			toolSrc = ctx.ToolSource
			mu.Unlock()
			return nil
		},
	}
	if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{}, reporter); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	mu.Lock()
	ts := toolSrc
	mu.Unlock()
	if ts == nil {
		t.Fatal("Prompt did not inject ToolSource for x-device capable reporter")
	}
	tools := ts(context.Background())
	if len(tools) != 1 {
		t.Fatalf("tool source returned %d tools, want 1", len(tools))
	}
	if tools[0].Name != "sys_vibrate" {
		t.Fatalf("tool name = %q, want sys_vibrate", tools[0].Name)
	}
}

// TestPromptOmitsDeviceToolSourceWithoutCapability：能力门控的两个否定分支 ——
// caps 不含 x-device，或 reporter 没有反向 Call 通道，都必须零注入、零往返。
func TestPromptOmitsDeviceToolSourceWithoutCapability(t *testing.T) {
	tests := []struct {
		name     string
		reporter aiacp.Reporter
	}{
		{name: "caps without x-device", reporter: &capsReporter{caps: decodeCaps(t, `{"fs": {"readTextFile": true}}`)}},
		{name: "reporter without Call channel", reporter: &recordingReporter{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				mu      sync.Mutex
				toolSrc func(context.Context) []core.Tool
			)
			handler := &FrontendHandler{
				evaluate: func(ctx *model.Context, _ string) error {
					mu.Lock()
					toolSrc = ctx.ToolSource
					mu.Unlock()
					return nil
				},
			}
			if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{}, tc.reporter); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			if _, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, tc.reporter); err != nil {
				t.Fatalf("Prompt: %v", err)
			}

			mu.Lock()
			ts := toolSrc
			mu.Unlock()
			if ts != nil {
				t.Fatal("ToolSource must be nil without x-device capability")
			}
		})
	}
}

// TestPromptSupersededSessionGuard 单活动会话模型的诚实守卫：两条连接共享同一
// SessionSource（与装配层接线一致），后声明者顶掉先声明者。fake 的 SetSession
// 无条件转移 active，cwd 校验与「无效声明不顶掉」语义归 AgentPlugin 侧测试。
func TestPromptSupersededSessionGuard(t *testing.T) {
	newHandler := func(sessions *fakeSessionSource) *FrontendHandler {
		return &FrontendHandler{
			evaluate: func(ctx *model.Context, input string) error { return nil },
			sessions: sessions,
		}
	}
	reporter := &recordingReporter{}

	t.Run("same connection prompt passes", func(t *testing.T) {
		handler := newHandler(&fakeSessionSource{})
		if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp"}, reporter); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if _, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
			t.Fatalf("Prompt active session = %v, want silent guard", err)
		}
	})

	t.Run("second connection displaces first", func(t *testing.T) {
		sessions := &fakeSessionSource{}
		a, b := newHandler(sessions), newHandler(sessions)
		if err := a.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
			t.Fatalf("CreateSession s1: %v", err)
		}
		if err := b.CreateSession(context.Background(), "s2", protocol.NewSessionRequest{Cwd: "/tmp/b"}, reporter); err != nil {
			t.Fatalf("CreateSession s2: %v", err)
		}

		// A 的旧会话已被 B 的声明顶掉：prompt s1 诚实报 superseded，绝不静默串线执行
		_, err := a.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter)
		var rpcErr *protocol.RPCError
		if !errors.As(err, &rpcErr) {
			t.Fatalf("Prompt superseded = %v, want *protocol.RPCError", err)
		}
		if rpcErr.Code != -32000 {
			t.Errorf("code = %d, want -32000", rpcErr.Code)
		}
		if !strings.Contains(rpcErr.Message, `"s2"`) {
			t.Errorf("message = %q, want mention of current active session s2", rpcErr.Message)
		}

		// B 的活动会话照常 prompt
		if _, err := b.Prompt(context.Background(), "s2", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
			t.Fatalf("Prompt current active session = %v", err)
		}
	})

	t.Run("load current displaces", func(t *testing.T) {
		sessions := &fakeSessionSource{currentID: "s2"}
		handler := newHandler(sessions)
		if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
			t.Fatalf("CreateSession s1: %v", err)
		}
		// 对 s2 的 load-current 同样经 SetSession 声明，顶掉 s1 的活动身份
		if err := handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "s2", Cwd: "/tmp"}, reporter); err != nil {
			t.Fatalf("LoadSession s2: %v", err)
		}
		_, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter)
		var rpcErr *protocol.RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 || !strings.Contains(rpcErr.Message, `"s2"`) {
			t.Fatalf("Prompt s1 after load-current s2 = %v, want -32000 superseded error mentioning s2", err)
		}
	})

	t.Run("empty active skips guard", func(t *testing.T) {
		sessions := &fakeSessionSource{} // activeID 空 = 从未有 ACP 会话声明
		handler := newHandler(sessions)
		// 不走 CreateSession（那会经 SetSession 声明活动会话），直接记入可 prompt 集合
		handler.remember("s1")
		if _, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
			t.Fatalf("Prompt with empty active = %v, want guard skipped", err)
		}
	})
}
