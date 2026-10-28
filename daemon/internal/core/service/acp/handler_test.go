package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
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
		gate: NewSessionGate(),
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
	factory := NewFrontendHandlerFactory(evaluate, nil, nil, NewSessionGate())

	first, second := factory(), factory()
	if first == second {
		t.Fatal("factory returned the same handler instance for two connections")
	}
	if _, ok := first.(*FrontendHandler); !ok {
		t.Fatal("factory product is not a *FrontendHandler")
	}
	// 门禁被工厂闭包捕获：两连接的 handler 必须共享同一把门禁，否则 TOCTOU 守卫失效。
	if first.(*FrontendHandler).gate != second.(*FrontendHandler).gate {
		t.Fatal("handlers from one factory must share the session gate")
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
		gate: NewSessionGate(),
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
		gate:     NewSessionGate(),
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
		gate:     NewSessionGate(),
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

// binderSessionSource 在 fakeSessionSource 之上实现 model.ClientToolBinder
// （P5-9）：记录 handler 转发来的 connectionID 与 caller，工具源由测试注入。
// 真实 binder（AgentPlugin）的编排面在 plugins/agent 的集成测试；此处钉
// handler 的接线契约：能力门控在前 → binder 优先 → type-assert 失败回落 relay。
type binderSessionSource struct {
	fakeSessionSource
	mu       sync.Mutex
	lastConn string
	lastCall model.DeviceCaller
	source   func(context.Context) []core.Tool
}

func (b *binderSessionSource) ClientToolSource(connectionID string, caller model.DeviceCaller) func(context.Context) []core.Tool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastConn = connectionID
	b.lastCall = caller
	return b.source
}

func (b *binderSessionSource) consulted() (string, model.DeviceCaller) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastConn, b.lastCall
}

// TestPromptInjectsDeviceToolSourceWhenCapable：对端声明 x-device 且提供 Call
// 通道时，Prompt 注入惰性工具源。P5-9 两个后端形态：
//   - sessions 实现 model.ClientToolBinder → 走能力代理面（handler 的
//     connectionID 与 reporter 的 Call 通道转发给 binder，工具源即 binder 产出）；
//   - 未实现（纯 SessionSource 测试桩/无 sessions）→ 回落原始 relay 路径
//     （调用即 probe，工具带 sys_ 前缀）——测试桩兼容语义。
func TestPromptInjectsDeviceToolSourceWhenCapable(t *testing.T) {
	reporter := &capsReporter{caps: decodeCaps(t, `{"x-device": {"tools": true}}`)}

	t.Run("binder backend takes priority", func(t *testing.T) {
		binder := &binderSessionSource{
			source: func(context.Context) []core.Tool {
				// 哨兵工具：只有 binder 产出的源能返回它，证明 ToolSource 来自
				// binder 而非回落路径。
				return []core.Tool{{Name: "binder-sentinel", Metadata: map[string]string{"goworker.capability_wait": "true"}}}
			},
		}
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
			sessions:     binder,
			gate:         NewSessionGate(),
			connectionID: "conn-test",
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
			t.Fatal("Prompt did not inject ToolSource for x-device capable reporter with a binder backend")
		}
		tools := ts(context.Background())
		if len(tools) != 1 || tools[0].Name != "binder-sentinel" {
			t.Fatalf("tool source = %+v, want the binder-produced sentinel tool", tools)
		}
		if conn, caller := binder.consulted(); conn != "conn-test" || caller == nil {
			t.Fatalf("binder consulted with (conn=%q, caller=%v), want (\"conn-test\", non-nil reporter Call channel)", conn, caller)
		}
	})

	t.Run("raw relay fallback without binder", func(t *testing.T) {
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
			gate: NewSessionGate(),
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
	})
}

// TestPromptOmitsDeviceToolSourceWithoutCapability：能力门控的否定分支 ——
// caps 不含 x-device、reporter 没有反向 Call 通道、或后端是 binder 但 caps
// 未声明 x-device，都必须零注入、零往返；binder 路径同样先过 caps 门（未声明
// 的客户端 VS Code 等零变化，binder 不被征询）。
func TestPromptOmitsDeviceToolSourceWithoutCapability(t *testing.T) {
	notConsulted := &binderSessionSource{
		source: func(context.Context) []core.Tool {
			return []core.Tool{{Name: "binder-sentinel"}}
		},
	}
	tests := []struct {
		name     string
		reporter aiacp.Reporter
		sessions model.SessionSource
	}{
		{name: "caps without x-device", reporter: &capsReporter{caps: decodeCaps(t, `{"fs": {"readTextFile": true}}`)}},
		{name: "reporter without Call channel", reporter: &recordingReporter{}},
		{name: "binder backend but caps without x-device", reporter: &capsReporter{caps: decodeCaps(t, `{"fs": {"readTextFile": true}}`)}, sessions: notConsulted},
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
				sessions:     tc.sessions,
				gate:         NewSessionGate(),
				connectionID: "conn-omit",
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
			if conn, _ := notConsulted.consulted(); conn == "conn-omit" {
				t.Fatal("binder must not be consulted without x-device capability")
			}
		})
	}
}

// TestPromptSupersededSessionGuard 连接级 superseded 守卫(PR2 语义变更,总方案
// §六:会话选择属于调用入口):每连接只认自己的声明——其他连接的 session/new|load
// 不再顶掉本连接;本连接重声明后的旧 id 仍诚实报错。fake 的 SetSession 无条件
// 转移 active(共享源),cwd 校验与「无效声明不顶掉」语义归 AgentPlugin 侧测试。
func TestPromptSupersededSessionGuard(t *testing.T) {
	newHandler := func(sessions *fakeSessionSource) *FrontendHandler {
		return &FrontendHandler{
			evaluate: func(ctx *model.Context, input string) error { return nil },
			sessions: sessions,
			gate:     NewSessionGate(),
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

	t.Run("second connection does not displace first", func(t *testing.T) {
		// PR2 语义变更(原「second connection displaces first」反转为不顶掉):
		// 两条连接各自 session/new 后互不影响——A 的 prompt 不被 B 的声明拒绝,
		// 也不静默串到 B 的会话(Binding 连接级,各自路由)。
		sessions := &fakeSessionSource{currentID: "store-shared"}
		a, b := newHandler(sessions), newHandler(sessions)
		if err := a.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
			t.Fatalf("CreateSession s1: %v", err)
		}
		if err := b.CreateSession(context.Background(), "s2", protocol.NewSessionRequest{Cwd: "/tmp/b"}, reporter); err != nil {
			t.Fatalf("CreateSession s2: %v", err)
		}

		// 两条连接的 Binding 各自指向声明落地的会话(共享同一 current → 同值),
		// 实例独立(连接私有,§6.2 第 4 条:互不影响)。
		if got := a.binding.Get(); got != "store-shared" {
			t.Fatalf("a binding = %q, want store-shared (session/new 后 Set)", got)
		}
		if got := b.binding.Get(); got != "store-shared" {
			t.Fatalf("b binding = %q, want store-shared (session/new 后 Set)", got)
		}
		if a.active != "s1" || b.active != "s2" {
			t.Fatalf("active declarations = %q/%q, want s1/s2 (声明归连接)", a.active, b.active)
		}

		// A 的会话照常 prompt(B 的 session/new 不再顶掉它);evaluate 收到的 ctx
		// 携带 A 自己的 Binding(路由面唯一通道)。
		var aBinding *agentcontrol.SessionBinding
		a.evaluate = func(ctx *model.Context, input string) error {
			aBinding = agentcontrol.BindingFromCtx(ctx.Ctx)
			return nil
		}
		if _, err := a.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
			t.Fatalf("Prompt s1 after b's session/new = %v, want silent guard (PR2:声明归连接)", err)
		}
		if aBinding != &a.binding {
			t.Fatalf("evaluate ctx binding = %v, want &a.binding", aBinding)
		}

		// B 的活动会话照常 prompt
		if _, err := b.Prompt(context.Background(), "s2", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
			t.Fatalf("Prompt s2 = %v", err)
		}
	})

	t.Run("same connection redeclaration displaces", func(t *testing.T) {
		// 本连接重声明后的旧 id 诚实报 superseded:单连接行为与旧版一致。
		handler := newHandler(&fakeSessionSource{})
		if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
			t.Fatalf("CreateSession s1: %v", err)
		}
		if err := handler.CreateSession(context.Background(), "s2", protocol.NewSessionRequest{Cwd: "/tmp/b"}, reporter); err != nil {
			t.Fatalf("CreateSession s2: %v", err)
		}
		_, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter)
		var rpcErr *protocol.RPCError
		if !errors.As(err, &rpcErr) {
			t.Fatalf("Prompt stale s1 = %v, want *protocol.RPCError", err)
		}
		if rpcErr.Code != -32000 {
			t.Errorf("code = %d, want -32000", rpcErr.Code)
		}
		if !strings.Contains(rpcErr.Message, `"s2"`) {
			t.Errorf("message = %q, want mention of last declared s2", rpcErr.Message)
		}
	})

	t.Run("load current displaces and binds", func(t *testing.T) {
		sessions := &fakeSessionSource{currentID: "s2"}
		handler := newHandler(sessions)
		if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
			t.Fatalf("CreateSession s1: %v", err)
		}
		if got := handler.binding.Get(); got != "s2" {
			t.Fatalf("binding after session/new = %q, want s2 (声明落地会话的 store id)", got)
		}
		// 对 s2 的 load-current 同样经 SetSession 声明，顶掉 s1 的活动身份,
		// 并把 Binding 指向 s2
		if err := handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "s2", Cwd: "/tmp"}, reporter); err != nil {
			t.Fatalf("LoadSession s2: %v", err)
		}
		if got := handler.binding.Get(); got != "s2" {
			t.Fatalf("binding after load-current = %q, want s2", got)
		}
		_, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter)
		var rpcErr *protocol.RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 || !strings.Contains(rpcErr.Message, `"s2"`) {
			t.Fatalf("Prompt s1 after load-current s2 = %v, want -32000 superseded error mentioning s2", err)
		}
		if _, err := handler.Prompt(context.Background(), "s2", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
			t.Fatalf("Prompt s2 after load = %v", err)
		}
	})

	t.Run("archived load does not rebind", func(t *testing.T) {
		// 归档 load 是只读重放:Binding 不变(只读视图不是会话选择)。
		sessions := &fakeSessionSource{
			currentID: "cur",
			list:      []model.SessionSummary{{SessionID: "old"}},
			archived: map[string][]model.SessionUpdate{
				"old": {{Kind: model.SessionUpdateUserMessageChunk, Content: &model.SessionContent{Type: "text", Text: "past"}}},
			},
		}
		handler := newHandler(sessions)
		if err := handler.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp"}, reporter); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if err := handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "old"}, reporter); err != nil {
			t.Fatalf("LoadSession archived: %v", err)
		}
		if got := handler.binding.Get(); got != "cur" {
			t.Fatalf("binding after archived load = %q, want cur (unchanged)", got)
		}
	})

	t.Run("empty active skips guard", func(t *testing.T) {
		sessions := &fakeSessionSource{} // activeID 空 = 从未有 ACP 会话声明
		handler := newHandler(sessions)
		// 不走 CreateSession（那会进 SetSession 声明活动会话），直接记入可 prompt 集合
		handler.remember("s1")
		if _, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
			t.Fatalf("Prompt with empty active = %v, want guard skipped", err)
		}
	})
}

// blockedEval 是阻塞式 evaluate：进入即 close(started) 通知测试「prompt 已推进到
// evaluate（门禁已在其手上）」，随后阻塞在 release 上直到测试放行。这是构造
// 「确定性在飞 prompt」的关键积木。
type blockedEval struct {
	started chan struct{}
	release chan struct{}
}

func newBlockedEval() *blockedEval {
	return &blockedEval{started: make(chan struct{}), release: make(chan struct{})}
}

func (e *blockedEval) run(_ *model.Context, _ string) error {
	close(e.started)
	<-e.release
	return nil
}

// waitPrompt 在后台 goroutine 跑一轮 prompt，返回完成 err 的接收通道。
func waitPrompt(t *testing.T, h *FrontendHandler, sessionID string, reporter aiacp.Reporter) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := h.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter)
		done <- err
	}()
	return done
}

// TestPromptInFlightBlocksCreateSession 评审 P1 场景 1（TOCTOU 封闭）：A 的 prompt
// 在飞（持有门禁）时，B 的 session/new 必须收到 -32000 busy 而非把新 cwd 应用到
// 在飞会话上；A 释放后 B 重试成功，且 busy 拒绝不得留下任何副作用。
func TestPromptInFlightBlocksCreateSession(t *testing.T) {
	sessions := &fakeSessionSource{}
	gate := NewSessionGate()
	reporter := &recordingReporter{}
	newHandler := func(evaluate Evaluator) *FrontendHandler {
		return &FrontendHandler{evaluate: evaluate, sessions: sessions, gate: gate}
	}

	aEval := newBlockedEval()
	a := newHandler(aEval.run)
	if err := a.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
		t.Fatalf("CreateSession s1: %v", err)
	}
	// B 的 evaluate 一旦被调用即是失败：busy 场景里 B 的 prompt 根本不该提交
	b := newHandler(func(ctx *model.Context, _ string) error {
		t.Error("B's evaluate must not run in this scenario")
		return nil
	})

	aDone := waitPrompt(t, a, "s1", reporter)
	<-aEval.started // A 已持有门禁、卡在 evaluate 里

	// B 的 session/new：busy 诚实拒绝
	err := b.CreateSession(context.Background(), "s2", protocol.NewSessionRequest{Cwd: "/tmp/b"}, reporter)
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 || !strings.Contains(rpcErr.Message, "busy") {
		t.Fatalf("CreateSession while prompt in flight = %v, want -32000 busy error", err)
	}
	// busy 拒绝不得留下副作用：s2 不得被声明，活动会话仍是 s1
	sessions.mu.Lock()
	declared := append([]string(nil), sessions.setSessionCalls...)
	sessions.mu.Unlock()
	if len(declared) != 1 || declared[0] != "s1|/tmp/a" {
		t.Fatalf("SetSession calls = %v, want only s1's declaration (busy CreateSession must have no side effects)", declared)
	}

	// 放行 A：prompt 正常完成
	close(aEval.release)
	if err := <-aDone; err != nil {
		t.Fatalf("Prompt s1 = %v", err)
	}

	// A 结束后 B 重试成功
	if err := b.CreateSession(context.Background(), "s2", protocol.NewSessionRequest{Cwd: "/tmp/b"}, reporter); err != nil {
		t.Fatalf("CreateSession s2 after release = %v", err)
	}
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	if len(sessions.setSessionCalls) != 2 || sessions.setSessionCalls[1] != "s2|/tmp/b" {
		t.Fatalf("SetSession calls = %v, want s2 declared after retry", sessions.setSessionCalls)
	}
}

// TestPromptInFlightBlocksLoadCurrent 评审 P1 场景 2：prompt 在飞时对当前会话的
// session/load 报 busy；归档 load 是只读重放、不碰共享会话状态，不受门禁限制。
func TestPromptInFlightBlocksLoadCurrent(t *testing.T) {
	sessions := &fakeSessionSource{
		currentID: "s1",
		list:      []model.SessionSummary{{SessionID: "old"}},
		archived: map[string][]model.SessionUpdate{
			"old": {{Kind: model.SessionUpdateUserMessageChunk, Content: &model.SessionContent{Type: "text", Text: "past"}}},
		},
	}
	gate := NewSessionGate()
	reporter := &recordingReporter{}
	newHandler := func(evaluate Evaluator) *FrontendHandler {
		return &FrontendHandler{evaluate: evaluate, sessions: sessions, gate: gate}
	}

	aEval := newBlockedEval()
	a := newHandler(aEval.run)
	if err := a.CreateSession(context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
		t.Fatalf("CreateSession s1: %v", err)
	}
	b := newHandler(func(ctx *model.Context, _ string) error {
		t.Error("B's evaluate must not run in this scenario")
		return nil
	})

	aDone := waitPrompt(t, a, "s1", reporter)
	<-aEval.started

	// load-current 会 SetSession + reload，必须被 busy 拒绝
	err := b.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "s1", Cwd: "/tmp/b"}, reporter)
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 || !strings.Contains(rpcErr.Message, "busy") {
		t.Fatalf("LoadSession current while prompt in flight = %v, want -32000 busy error", err)
	}
	// 归档 load 只读：prompt 在飞也照常重放
	if err := b.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "old"}, reporter); err != nil {
		t.Fatalf("LoadSession archived while prompt in flight = %v, want read-only replay to bypass the gate", err)
	}

	// 放行 A 后，load-current 成功
	close(aEval.release)
	if err := <-aDone; err != nil {
		t.Fatalf("Prompt s1 = %v", err)
	}
	if err := b.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "s1", Cwd: "/tmp/b"}, reporter); err != nil {
		t.Fatalf("LoadSession current after release = %v", err)
	}
}

// TestPromptsSerializeAcrossConnections 门禁对 prompt 本身也是互斥的：A 的 prompt
// 在飞时 B 的 prompt（同一活动会话）排队等待，A 释放后 B 才开始执行。
func TestPromptsSerializeAcrossConnections(t *testing.T) {
	gate := NewSessionGate()
	reporter := &recordingReporter{}
	aEval, bEval := newBlockedEval(), newBlockedEval()
	a := &FrontendHandler{evaluate: aEval.run, gate: gate}
	b := &FrontendHandler{evaluate: bEval.run, gate: gate}
	// 同一活动会话的双连接：两侧都记入各自的可 prompt 集合（B 经 load-current 进入）
	a.remember("s1")
	b.remember("s1")

	aDone := waitPrompt(t, a, "s1", reporter)
	<-aEval.started

	bDone := waitPrompt(t, b, "s1", reporter)
	time.Sleep(50 * time.Millisecond) // 给调度器充分机会；B 仍不得越过门禁启动
	select {
	case <-bEval.started:
		t.Fatal("B's evaluate started while A still holds the gate")
	default:
	}

	close(aEval.release)
	if err := <-aDone; err != nil {
		t.Fatalf("Prompt A = %v", err)
	}
	<-bEval.started // A 释放后 B 才启动
	close(bEval.release)
	if err := <-bDone; err != nil {
		t.Fatalf("Prompt B = %v", err)
	}
}

// TestPromptCancelWhileQueued 连接断开（ctx 取消）时排队中的 prompt 必须立即以
// 错误退出而不是永远悬在门禁上；且 evaluate 不得再被调用。
func TestPromptCancelWhileQueued(t *testing.T) {
	gate := NewSessionGate()
	reporter := &recordingReporter{}
	aEval := newBlockedEval()
	a := &FrontendHandler{evaluate: aEval.run, gate: gate}
	var (
		bMu  sync.Mutex
		bRan bool
	)
	b := &FrontendHandler{
		evaluate: func(ctx *model.Context, _ string) error {
			bMu.Lock()
			bRan = true
			bMu.Unlock()
			return nil
		},
		gate: gate,
	}
	a.remember("s1")
	b.remember("s1")

	aDone := waitPrompt(t, a, "s1", reporter)
	<-aEval.started

	ctx, cancel := context.WithCancel(context.Background())
	bDone := make(chan error, 1)
	go func() {
		_, err := b.Prompt(ctx, "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter)
		bDone <- err
	}()
	time.Sleep(50 * time.Millisecond) // 让 B 真正排到门禁上，而非取消先于提交
	cancel()

	err := <-bDone
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 || !strings.Contains(rpcErr.Message, "gate") {
		t.Fatalf("Prompt canceled while queued = %v, want -32000 gate-canceled error", err)
	}

	// 放行 A：B 的 evaluate 必须从未运行
	close(aEval.release)
	if err := <-aDone; err != nil {
		t.Fatalf("Prompt A = %v", err)
	}
	bMu.Lock()
	defer bMu.Unlock()
	if bRan {
		t.Fatal("B's evaluate ran despite ctx cancellation while queued")
	}
}
