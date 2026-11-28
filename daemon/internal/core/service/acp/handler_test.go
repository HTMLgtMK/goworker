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
	"github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

func rememberPromptSession(handler *FrontendHandler, sessionID string) {
	handler.remember(sessionID)
	handler.binding.Set(sessionID)
}

func testCreateSession(handler *FrontendHandler, ctx context.Context, routeID string, req protocol.NewSessionRequest, reporter aiacp.Reporter) error {
	if handler.multi != nil {
		_, err := handler.CreateManagedSession(ctx, req, reporter)
		return err
	}
	// Unit tests that do not exercise session/new install an explicit route fixture.
	rememberPromptSession(handler, routeID)
	return nil
}

func createManagedSessionForTest(t *testing.T, handler *FrontendHandler, req protocol.NewSessionRequest, reporter aiacp.Reporter) string {
	t.Helper()
	id, err := handler.CreateManagedSession(context.Background(), req, reporter)
	if err != nil {
		t.Fatalf("createManagedSession: %v", err)
	}
	return id.String()
}

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
	readOnlyReason  error
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

func (f *fakeSessionSource) SessionReadOnlyReason(string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readOnlyReason
}

func (f *fakeSessionSource) ReloadCurrentSession() error {
	return f.reloadErr
}

func (f *fakeSessionSource) CurrentSessionHistory() []model.SessionUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentHistory
}

func (f *fakeSessionSource) ReloadAndReadCurrentSession(expectedID string) ([]model.SessionUpdate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.currentID != expectedID {
		return nil, fmt.Errorf("current session changed from %q to %q", expectedID, f.currentID)
	}
	if f.reloadErr != nil {
		return nil, f.reloadErr
	}
	return f.currentHistory, nil
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

type switchingLegacySessionSource struct {
	*fakeSessionSource
	reloadStarted  chan struct{}
	continueReload chan struct{}
}

func (s *switchingLegacySessionSource) ReloadCurrentSession() error {
	close(s.reloadStarted)
	<-s.continueReload
	return nil
}

func (s *switchingLegacySessionSource) ReloadAndReadCurrentSession(expectedID string) ([]model.SessionUpdate, error) {
	close(s.reloadStarted)
	<-s.continueReload
	s.fakeSessionSource.mu.Lock()
	defer s.fakeSessionSource.mu.Unlock()
	if s.fakeSessionSource.currentID != expectedID {
		return nil, fmt.Errorf("current session changed from %q to %q", expectedID, s.fakeSessionSource.currentID)
	}
	return s.fakeSessionSource.currentHistory, nil
}

type sequencedLegacySessionSource struct {
	fakeSessionSource
	muIDs sync.Mutex
	ids   []string
	seen  chan string
}

func (s *sequencedLegacySessionSource) CurrentSessionID() string {
	s.muIDs.Lock()
	if len(s.ids) == 0 {
		s.muIDs.Unlock()
		return ""
	}
	id := s.ids[0]
	s.ids = s.ids[1:]
	s.muIDs.Unlock()
	s.seen <- id
	return id
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
	if err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp"}, reporter); err != nil {
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

type lifecycleRaceSessionSource struct {
	fakeSessionSource
	createStarted chan struct{}
	loadStarted   chan struct{}
	finishCreate  chan struct{}
	finishLoad    chan struct{}
	muBindings    sync.Mutex
	bindings      map[string]string
	cleanupCount  int
	rollbackCount int
}

func (f *lifecycleRaceSessionSource) CreateBoundSession(string) (string, error) {
	close(f.createStarted)
	<-f.finishCreate
	return "runtime-session", nil
}

func (f *lifecycleRaceSessionSource) CreateBoundSessionWithRollback(cwd string) (string, func() error, error) {
	id, err := f.CreateBoundSession(cwd)
	if err != nil {
		return "", nil, err
	}
	return id, func() error {
		f.muBindings.Lock()
		f.rollbackCount++
		f.muBindings.Unlock()
		return nil
	}, nil
}

func (f *lifecycleRaceSessionSource) LoadBoundSession(string, string) error {
	close(f.loadStarted)
	<-f.finishLoad
	return nil
}

func (f *lifecycleRaceSessionSource) SessionHistory(string) ([]model.SessionUpdate, error) {
	return nil, nil
}

func (f *lifecycleRaceSessionSource) SessionArchived(string) bool         { return false }
func (f *lifecycleRaceSessionSource) ArchiveSession(string, string) error { return nil }

func (f *lifecycleRaceSessionSource) LoadBoundSessionForConnection(string, string, string) ([]model.SessionUpdate, error) {
	close(f.loadStarted)
	<-f.finishLoad
	return nil, nil
}

func (f *lifecycleRaceSessionSource) LoadBoundSessionForConnectionGuarded(_, _, connectionID string, allowBind func() bool) ([]model.SessionUpdate, bool, error) {
	close(f.loadStarted)
	<-f.finishLoad
	if !allowBind() {
		f.muBindings.Lock()
		f.cleanupCount++
		f.muBindings.Unlock()
		return nil, false, nil
	}
	f.BindConnectionSession(connectionID, "runtime-session")
	return nil, true, nil
}

func (f *lifecycleRaceSessionSource) BindConnectionSession(connectionID, sessionID string) {
	f.muBindings.Lock()
	defer f.muBindings.Unlock()
	if f.bindings == nil {
		f.bindings = make(map[string]string)
	}
	f.bindings[connectionID] = sessionID
}

func (f *lifecycleRaceSessionSource) BindConnectionSessionGuarded(connectionID, sessionID string, allowBind func() bool) bool {
	if !allowBind() {
		f.muBindings.Lock()
		f.cleanupCount++
		f.muBindings.Unlock()
		return false
	}
	f.BindConnectionSession(connectionID, sessionID)
	return true
}

func (f *lifecycleRaceSessionSource) ReleaseConnection(connectionID string) {
	f.muBindings.Lock()
	defer f.muBindings.Unlock()
	delete(f.bindings, connectionID)
}

func (f *lifecycleRaceSessionSource) binding(connectionID string) string {
	f.muBindings.Lock()
	defer f.muBindings.Unlock()
	return f.bindings[connectionID]
}

func (f *lifecycleRaceSessionSource) cleanupCalls() int {
	f.muBindings.Lock()
	defer f.muBindings.Unlock()
	return f.cleanupCount
}

func TestCreateSessionResponseAbortUsesConnectionScopedTransaction(t *testing.T) {
	source := &lifecycleRaceSessionSource{
		createStarted: make(chan struct{}), finishCreate: make(chan struct{}),
		loadStarted: make(chan struct{}), finishLoad: make(chan struct{}),
	}
	reporter := &rollbackRegistrarReporter{}
	handler := &FrontendHandler{sessions: source, multi: source, binder: source, gate: NewSessionGate(), connectionID: "conn"}
	result := make(chan error, 1)
	go func() {
		_, err := handler.CreateManagedSession(context.Background(), protocol.NewSessionRequest{}, reporter)
		result <- err
	}()
	<-source.createStarted
	close(source.finishCreate)
	if err := <-result; err != nil {
		t.Fatalf("CreateManagedSession: %v", err)
	}
	if reporter.abort == nil {
		t.Fatal("session creation response did not register its safe abort")
	}
	if err := reporter.abort(); err != nil {
		t.Fatalf("abort unsent response: %v", err)
	}
	if got := source.binding("conn"); got != "" {
		t.Fatalf("connection binding after abort = %q, want none", got)
	}
	if route, _, _ := handler.routeFor("runtime-session"); route != nil {
		t.Fatal("aborted session remained promptable")
	}
}

type rollbackRegistrarReporter struct {
	recordingReporter
	abort func() error
}

func (r *rollbackRegistrarReporter) SetSessionCreationAbort(abort func() error) {
	r.abort = abort
}

func (f *lifecycleRaceSessionSource) CreateBoundSessionWithRollbackForConnection(cwd, connectionID string) (string, func() error, error) {
	id, _, err := f.CreateBoundSessionWithRollback(cwd)
	if err != nil {
		return "", nil, err
	}
	return id, func() error {
		f.muBindings.Lock()
		defer f.muBindings.Unlock()
		delete(f.bindings, connectionID)
		f.rollbackCount++
		return nil
	}, nil
}

func TestCreateSessionAlreadyCanceledDoesNotStartCreation(t *testing.T) {
	source := &lifecycleRaceSessionSource{
		createStarted: make(chan struct{}), finishCreate: make(chan struct{}),
		loadStarted: make(chan struct{}), finishLoad: make(chan struct{}),
	}
	handler := &FrontendHandler{sessions: source, multi: source, binder: source, gate: NewSessionGate(), connectionID: "conn"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := handler.CreateManagedSession(ctx, protocol.NewSessionRequest{}, &recordingReporter{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateManagedSession error = %v, want context canceled", err)
	}
	select {
	case <-source.createStarted:
		t.Fatal("session creator ran despite an already-canceled request")
	default:
	}
}

func TestCreateSessionCancellationAfterBindingUsesSafeAbort(t *testing.T) {
	source := &lifecycleRaceSessionSource{
		createStarted: make(chan struct{}), finishCreate: make(chan struct{}),
		loadStarted: make(chan struct{}), finishLoad: make(chan struct{}),
	}
	close(source.finishCreate)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := &FrontendHandler{sessions: source, multi: source, binder: source, gate: NewSessionGate(), connectionID: "conn"}
	handler.selectionCommitHook = func(string) { cancel() }
	_, err := handler.CreateManagedSession(ctx, protocol.NewSessionRequest{}, &rollbackRegistrarReporter{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateManagedSession error = %v, want context canceled", err)
	}
	if got := source.binding("conn"); got != "" {
		t.Fatalf("connection binding after canceled create = %q, want none", got)
	}
	if route, _, _ := handler.routeFor("runtime-session"); route != nil {
		t.Fatal("canceled session remained promptable")
	}
	source.muBindings.Lock()
	rollbacks := source.rollbackCount
	source.muBindings.Unlock()
	if rollbacks != 1 {
		t.Fatalf("rollback calls = %d, want 1", rollbacks)
	}
}

func TestCreateSessionCanceledBeforeBindingRollsBack(t *testing.T) {
	source := &lifecycleRaceSessionSource{
		createStarted: make(chan struct{}), finishCreate: make(chan struct{}),
		loadStarted: make(chan struct{}), finishLoad: make(chan struct{}),
	}
	handler := &FrontendHandler{sessions: source, multi: source, binder: source, gate: NewSessionGate(), connectionID: "conn"}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := handler.CreateManagedSession(ctx, protocol.NewSessionRequest{}, &recordingReporter{})
		result <- err
	}()
	<-source.createStarted
	cancel()
	close(source.finishCreate)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("create error = %v, want context canceled", err)
	}
	if got := source.binding("conn"); got != "" {
		t.Fatalf("canceled create retained connection binding %q", got)
	}
	source.muBindings.Lock()
	rollbacks := source.rollbackCount
	source.muBindings.Unlock()
	if rollbacks != 1 {
		t.Fatalf("rollbacks = %d, want 1", rollbacks)
	}
}

func TestCreateSessionCloseWinsBeforeLifecycleRegistration(t *testing.T) {
	source := &lifecycleRaceSessionSource{
		createStarted: make(chan struct{}), finishCreate: make(chan struct{}),
		loadStarted: make(chan struct{}), finishLoad: make(chan struct{}),
	}
	handler := &FrontendHandler{sessions: source, multi: source, binder: source, gate: NewSessionGate(), connectionID: "conn"}
	result := make(chan error, 1)
	go func() {
		result <- testCreateSession(handler, context.Background(), "transport", protocol.NewSessionRequest{}, &recordingReporter{})
	}()
	<-source.createStarted
	handler.Close()
	close(source.finishCreate)
	if err := <-result; !errors.Is(err, protocol.ErrClosed) {
		t.Fatalf("CreateSession after Close = %v, want protocol.ErrClosed", err)
	}
	if got := source.binding("conn"); got != "" {
		t.Fatalf("connection binding after Close = %q, want none", got)
	}
	if got := source.cleanupCalls(); got != 1 {
		t.Fatalf("guarded cleanup calls = %d, want 1", got)
	}
	source.muBindings.Lock()
	rollbacks := source.rollbackCount
	source.muBindings.Unlock()
	if rollbacks != 1 {
		t.Fatalf("created-session rollback calls = %d, want 1", rollbacks)
	}
}

func TestLoadSessionCloseWinsBeforeLifecycleRegistration(t *testing.T) {
	source := &lifecycleRaceSessionSource{
		createStarted: make(chan struct{}), finishCreate: make(chan struct{}),
		loadStarted: make(chan struct{}), finishLoad: make(chan struct{}),
	}
	handler := &FrontendHandler{sessions: source, multi: source, binder: source, gate: NewSessionGate(), connectionID: "conn"}
	result := make(chan error, 1)
	go func() {
		result <- handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "runtime-session"}, &recordingReporter{})
	}()
	<-source.loadStarted
	handler.Close()
	close(source.finishLoad)
	if err := <-result; !errors.Is(err, protocol.ErrClosed) {
		t.Fatalf("LoadSession after Close = %v, want protocol.ErrClosed", err)
	}
	if got := source.binding("conn"); got != "" {
		t.Fatalf("connection binding after Close = %q, want none", got)
	}
	if got := source.cleanupCalls(); got != 1 {
		t.Fatalf("guarded cleanup calls = %d, want 1", got)
	}
}

func TestCreateSessionPushesCommands(t *testing.T) {
	reporter := &recordingReporter{}
	source := &multiRouteSessionSource{}
	handler := &FrontendHandler{
		commands: func() []model.Command {
			return []model.Command{{Name: "/agent", Description: "agent", Aliases: []string{"/a"}}}
		},
		sessions: source,
		multi:    source,
		gate:     NewSessionGate(),
	}
	handler.InitializeReady(context.Background(), reporter)
	if err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{}, reporter); err != nil {
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
	// Plan A2b1:load-current 不再声明 cwd(SetSession 调用已从声明路径整体移除,
	// cwd 声明归 BoundSessionSource;本 fake 无该能力走降级路径)。
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	if len(sessions.setSessionCalls) != 0 {
		t.Fatalf("SetSession calls = %v, want none (A2b1:声明面已切换到 BoundSessionSource)", sessions.setSessionCalls)
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

func TestMobileCapabilityScopesDefaultToolPolicyToConnection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		caps     string
		wantSkip bool
	}{
		{name: "mobile", caps: `{"mobile":true}`, wantSkip: true},
		{name: "ordinary", caps: `{}`, wantSkip: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reporter := &capsReporter{caps: decodeCaps(t, tc.caps)}
			var gotDisable bool
			handler := &FrontendHandler{
				evaluate: func(ctx *model.Context, _ string) error {
					gotDisable = ctx.ToolOptions.DisableDefaultTools
					return nil
				},
				gate: NewSessionGate(),
			}
			handler.InitializeReady(context.Background(), reporter)
			handler.remember("s1")
			if _, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
				t.Fatalf("Prompt: %v", err)
			}
			if gotDisable != tc.wantSkip {
				t.Fatalf("DisableDefaultTools = %v, want %v", gotDisable, tc.wantSkip)
			}
		})
	}
}

func TestPromptRejectsWorkspaceReadOnlySession(t *testing.T) {
	sessions := &fakeSessionSource{readOnlyReason: errors.New("workspace is unavailable")}
	handler := &FrontendHandler{
		evaluate: func(*model.Context, string) error {
			t.Fatal("workspace-read-only prompt reached evaluation")
			return nil
		},
		sessions:     sessions,
		promptRoutes: map[string]*sessionRoute{"historical": {}},
	}
	_, err := handler.Prompt(context.Background(), "historical", []protocol.ContentBlock{protocol.TextBlock("hello")}, &recordingReporter{})
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("Prompt on workspace-read-only session = %v, want read-only error", err)
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
		list: []model.SessionSummary{{SessionID: "s1", Title: "t", UpdatedAt: "2026-01-01T00:00:00Z", State: model.SessionStateActive}},
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

func TestLegacyLoadFailsClosedIfCurrentChangesDuringReload(t *testing.T) {
	base := &fakeSessionSource{
		currentID: "session-a",
		currentHistory: []model.SessionUpdate{{
			Kind:    model.SessionUpdateUserMessageChunk,
			Content: &model.SessionContent{Type: "text", Text: "history-a"},
		}},
	}
	source := &switchingLegacySessionSource{
		fakeSessionSource: base,
		reloadStarted:     make(chan struct{}),
		continueReload:    make(chan struct{}),
	}
	handler := &FrontendHandler{sessions: source, gate: NewSessionGate()}
	reporter := &recordingReporter{}
	result := make(chan error, 1)
	go func() {
		result <- handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "session-a"}, reporter)
	}()
	<-source.reloadStarted
	base.mu.Lock()
	base.currentID = "session-b"
	base.currentHistory = []model.SessionUpdate{{
		Kind:    model.SessionUpdateUserMessageChunk,
		Content: &model.SessionContent{Type: "text", Text: "history-b"},
	}}
	base.mu.Unlock()
	close(source.continueReload)

	if err := <-result; err == nil {
		t.Fatal("LoadSession succeeded without an atomic current-session snapshot")
	}
	if len(reporter.updates) != 0 {
		t.Fatalf("LoadSession replayed unverified history: %+v", reporter.updates)
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

// TestPromptInjectsDeviceToolSourceWhenCapable covers both backends: ClientToolBinder
// supplies a connection-scoped lazy source when available; otherwise the handler falls back
// to the legacy relay. Both require x-device capability and a reporter Call channel.
func TestPromptInjectsDeviceToolSourceWhenCapable(t *testing.T) {
	reporter := &capsReporter{caps: decodeCaps(t, `{"x-device": {"tools": true}}`)}

	t.Run("binder backend takes priority", func(t *testing.T) {
		binder := &binderSessionSource{
			fakeSessionSource: fakeSessionSource{currentID: "legacy-binder"},
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
		handler.InitializeReady(context.Background(), reporter)
		if err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{}, reporter); err != nil {
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
		handler.InitializeReady(context.Background(), reporter)
		if err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{}, reporter); err != nil {
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
		fakeSessionSource: fakeSessionSource{currentID: "legacy-omit"},
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
			if err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{}, tc.reporter); err != nil {
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
		handler := newHandler(&fakeSessionSource{currentID: "legacy-runtime"})
		if err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp"}, reporter); err != nil {
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
		if err := testCreateSession(a, context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
			t.Fatalf("CreateSession s1: %v", err)
		}
		if err := testCreateSession(b, context.Background(), "s2", protocol.NewSessionRequest{Cwd: "/tmp/b"}, reporter); err != nil {
			t.Fatalf("CreateSession s2: %v", err)
		}

		// 两条连接的 Binding:A2b1 降级语义(fake 无 BoundSessionSource)下
		// session/new 不产生稳定会话 id,Binding 保持空 = 回落 current 语义
		// (superseded 守卫只依赖 h.active,不受影响);实例独立(连接私有,
		// §6.2 第 4 条:互不影响)。
		if got := a.binding.Get(); got != "s1" {
			t.Fatalf("a binding = %q, want canonical session ID", got)
		}
		if got := b.binding.Get(); got != "s2" {
			t.Fatalf("b binding = %q, want canonical s2", got)
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
		if aBinding == nil || aBinding.Get() != "s1" || aBinding == &a.binding {
			t.Fatalf("evaluate ctx binding = %v, want an independent request binding for canonical s1", aBinding)
		}

		// B 的活动会话照常 prompt
		if _, err := b.Prompt(context.Background(), "s2", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
			t.Fatalf("Prompt s2 = %v", err)
		}
	})

	t.Run("legacy backend rejects a second transport without displacing the first", func(t *testing.T) {
		handler := newHandler(&fakeSessionSource{currentID: "legacy-runtime"})
		if err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
			t.Fatalf("CreateSession s1: %v", err)
		}
		if _, err := handler.commitSessionSelection("s2", true, true); err == nil {
			t.Fatal("legacy lifecycle binding unexpectedly accepted a second route")
		}
		if _, err := handler.Prompt(context.Background(), "s1", []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
			t.Fatalf("Prompt s1 after rejected declaration = %v", err)
		}
	})

	t.Run("load adds an independent runtime route", func(t *testing.T) {
		sessions := &multiRouteSessionSource{}
		handler := &FrontendHandler{evaluate: func(*model.Context, string) error { return nil }, sessions: sessions, multi: sessions, gate: NewSessionGate()}
		if err := testCreateSession(handler, context.Background(), "transport-a", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
			t.Fatalf("CreateSession A: %v", err)
		}
		if err := handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "runtime-loaded", Cwd: "/tmp"}, reporter); err != nil {
			t.Fatalf("LoadSession: %v", err)
		}
		for _, id := range []string{"runtime-1", "runtime-loaded"} {
			if _, err := handler.Prompt(context.Background(), id, []protocol.ContentBlock{protocol.TextBlock("hi")}, reporter); err != nil {
				t.Fatalf("Prompt %s after load = %v", id, err)
			}
		}
	})

	t.Run("archived load does not rebind", func(t *testing.T) {
		// 归档 load 是只读重放:Binding 不变(只读视图不是会话选择)。A2b1 降级
		// 语义:session/new 不设 Binding(fake 无多会话能力),归档 load 前后
		// 都保持空——「不变」的断言形态从「保持 cur」改为「保持空」。
		sessions := &fakeSessionSource{
			currentID: "cur",
			list:      []model.SessionSummary{{SessionID: "old"}},
			archived: map[string][]model.SessionUpdate{
				"old": {{Kind: model.SessionUpdateUserMessageChunk, Content: &model.SessionContent{Type: "text", Text: "past"}}},
			},
		}
		handler := newHandler(sessions)
		if err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp"}, reporter); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if got := handler.binding.Get(); got != "s1" {
			t.Fatalf("binding after session/new = %q, want canonical s1", got)
		}
		if err := handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "old"}, reporter); err != nil {
			t.Fatalf("LoadSession archived: %v", err)
		}
		if got := handler.binding.Get(); got != "s1" {
			t.Fatalf("binding after archived load = %q, want unchanged s1", got)
		}
	})

	t.Run("empty active skips guard", func(t *testing.T) {
		// A2b2:回落全局 ActiveSessionID 的分支已删除——h.active 为空 = 本连接
		// 从未声明,守卫直接放行,不再消费跨连接单值(plan A2「superseded 守卫
		// 删除空则回落」);路由由 Binding(空 → current 回落)决定。
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

func TestManagedCreateRollsBackWhenLifecycleBinderRejects(t *testing.T) {
	source := &multiRouteSessionSource{rejectBind: true}
	handler := &FrontendHandler{
		sessions: source,
		multi:    source,
		binder:   source,
		gate:     NewSessionGate(),
	}
	if _, err := handler.CreateManagedSession(context.Background(), protocol.NewSessionRequest{}, &recordingReporter{}); !errors.Is(err, protocol.ErrClosed) {
		t.Fatalf("createManagedSession with rejected binder = %v, want ErrClosed", err)
	}
	source.muRoute.Lock()
	defer source.muRoute.Unlock()
	if len(source.rolledBack) != 1 || source.rolledBack[0] != "runtime-1" {
		t.Fatalf("rolled back IDs = %v, want exactly [runtime-1]", source.rolledBack)
	}
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if len(handler.promptRoutes) != 0 {
		t.Fatalf("promptRoutes after rejected bind = %#v, want none", handler.promptRoutes)
	}
}

func TestManagedCreateAndLoadSerializeOnSharedGate(t *testing.T) {
	type result struct {
		runtimeID string
		err       error
	}
	source := &multiRouteSessionSource{}
	firstAtCommit := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	var hookOnce sync.Once
	handler := &FrontendHandler{
		sessions: source,
		multi:    source,
		gate:     NewSessionGate(),
		selectionCommitHook: func(_ string) {
			hookOnce.Do(func() {
				close(firstAtCommit)
				<-releaseFirst
			})
		},
	}
	defer release()
	createDone := make(chan error, 1)
	go func() {
		_, err := handler.CreateManagedSession(context.Background(), protocol.NewSessionRequest{}, &recordingReporter{})
		createDone <- err
	}()
	select {
	case <-firstAtCommit:
	case <-time.After(3 * time.Second):
		t.Fatal("create did not reach the post-create commit window")
	}

	loadDone := make(chan result, 1)
	go func() {
		runtimeID, err := handler.LoadSessionWithSessionID(context.Background(), protocol.LoadSessionRequest{SessionID: "runtime-current"}, &recordingReporter{})
		loadDone <- result{runtimeID: runtimeID, err: err}
	}()
	select {
	case got := <-loadDone:
		var rpcErr *protocol.RPCError
		if !errors.As(got.err, &rpcErr) || !strings.Contains(rpcErr.Message, "busy") {
			t.Fatalf("concurrent load = (%q, %v), want session busy", got.runtimeID, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("load did not observe the create lifecycle gate")
	}
	release()
	if err := <-createDone; err != nil {
		t.Fatalf("create after releasing commit hook = %v, want success", err)
	}
	if runtimeID, err := handler.LoadSessionWithSessionID(context.Background(), protocol.LoadSessionRequest{SessionID: "runtime-current"}, &recordingReporter{}); err != nil || runtimeID != "runtime-current" {
		t.Fatalf("load after create = (%q, %v), want (runtime-current, nil)", runtimeID, err)
	}
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if len(handler.promptRoutes) != 2 || handler.promptRoutes["runtime-current"] == nil || handler.promptRoutes["runtime-1"] == nil {
		t.Fatalf("promptRoutes after load/create = %#v, want runtime-current and runtime-1", handler.promptRoutes)
	}
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

// TestPromptInFlightDoesNotBlockCreateSession verifies session management uses a separate
// gate from prompt execution: creating B succeeds while A's prompt is blocked, without
// changing A's runtime.
func TestPromptInFlightDoesNotBlockCreateSession(t *testing.T) {
	sessions := &fakeSessionSource{currentID: "legacy-runtime"}
	reporter := &recordingReporter{}
	newHandler := func(evaluate Evaluator) *FrontendHandler {
		return &FrontendHandler{evaluate: evaluate, sessions: sessions, gate: NewSessionGate()}
	}

	aEval := newBlockedEval()
	a := newHandler(aEval.run)
	if err := testCreateSession(a, context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
		t.Fatalf("CreateSession s1: %v", err)
	}
	b := newHandler(func(ctx *model.Context, _ string) error { return nil })

	aDone := waitPrompt(t, a, "s1", reporter)
	<-aEval.started // A 的 prompt 已推进到 evaluate（在飞）

	// B 的 session/new：立即成功（不再 busy）——管理面与 prompt 面解耦。
	if err := testCreateSession(b, context.Background(), "s2", protocol.NewSessionRequest{Cwd: "/tmp/b"}, reporter); err != nil {
		t.Fatalf("CreateSession while prompt in flight = %v, want immediate success (A2b2:prompt 退出门禁)", err)
	}
	// 声明面零 source 调用（A2b1 降级语义不变：fake 无 BoundSessionSource，
	// 声明走降级路径不触碰 source）。
	sessions.mu.Lock()
	declared := append([]string(nil), sessions.setSessionCalls...)
	sessions.mu.Unlock()
	if len(declared) != 0 {
		t.Fatalf("SetSession calls = %v, want none (声明面已切换 BoundSessionSource)", declared)
	}

	// A 的 prompt 照常完成（B 的 new 不影响在飞执行）。
	close(aEval.release)
	if err := <-aDone; err != nil {
		t.Fatalf("Prompt s1 = %v", err)
	}
}

// TestPromptInFlightDoesNotBlockLoadCurrent（A2b2 语义翻转，取代评审 P1 场景 2
// 的 busy 断言）：prompt 在飞时 session/load 的可 prompt 分支立即成功——prompt
// 已退出全局门禁，load（降级路径仍走管理门禁 tryAcquire）在空闲门禁上立即通过；
// 归档 load 只读重放照旧不经门禁。
func TestPromptInFlightDoesNotBlockLoadCurrent(t *testing.T) {
	sessions := &fakeSessionSource{
		currentID: "s1",
		list:      []model.SessionSummary{{SessionID: "old"}},
		archived: map[string][]model.SessionUpdate{
			"old": {{Kind: model.SessionUpdateUserMessageChunk, Content: &model.SessionContent{Type: "text", Text: "past"}}},
		},
	}
	reporter := &recordingReporter{}
	newHandler := func(evaluate Evaluator) *FrontendHandler {
		return &FrontendHandler{evaluate: evaluate, sessions: sessions, gate: NewSessionGate()}
	}

	aEval := newBlockedEval()
	a := newHandler(aEval.run)
	if err := testCreateSession(a, context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp/a"}, reporter); err != nil {
		t.Fatalf("CreateSession s1: %v", err)
	}
	b := newHandler(func(ctx *model.Context, _ string) error { return nil })

	aDone := waitPrompt(t, a, "s1", reporter)
	<-aEval.started

	// load-current：立即成功（不再 busy——prompt 不持门禁，管理门禁空闲）。
	if err := b.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "s1", Cwd: "/tmp/b"}, reporter); err != nil {
		t.Fatalf("LoadSession current while prompt in flight = %v, want immediate success (A2b2:prompt 退出门禁)", err)
	}
	// 归档 load 只读：prompt 在飞也照常重放（不经门禁，语义不变）。
	if err := b.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: "old"}, reporter); err != nil {
		t.Fatalf("LoadSession archived while prompt in flight = %v, want read-only replay to bypass the gate", err)
	}

	// A 的 prompt 照常完成（B 的 load 只 reload 视图，不影响在飞执行）。
	close(aEval.release)
	if err := <-aDone; err != nil {
		t.Fatalf("Prompt s1 = %v", err)
	}
}

// TestPromptsRunConcurrentlyAcrossConnections verifies prompts on different connections are
// not serialized by the management gate. Same-session execution remains serialized by the
// plugin's per-runtime gate; this fake tests only the handler layer.
func TestPromptsRunConcurrentlyAcrossConnections(t *testing.T) {
	reporter := &recordingReporter{}
	aEval, bEval := newBlockedEval(), newBlockedEval()
	a := &FrontendHandler{evaluate: aEval.run, gate: NewSessionGate()}
	b := &FrontendHandler{evaluate: bEval.run, gate: NewSessionGate()}
	// 同一活动会话的双连接：两侧都记入各自的可 prompt 集合（B 经 load-current 进入）
	a.remember("s1")
	b.remember("s1")

	aDone := waitPrompt(t, a, "s1", reporter)
	<-aEval.started

	bDone := waitPrompt(t, b, "s1", reporter)
	// B 的 evaluate 必须启动（A 阻塞中）——旧版此处 B 排队于全局门禁、绝不启动。
	select {
	case <-bEval.started:
	case <-time.After(2 * time.Second):
		t.Fatal("B's evaluate did not start while A is in flight (A2b2:prompt 必须跨连接并行)")
	}

	close(aEval.release)
	if err := <-aDone; err != nil {
		t.Fatalf("Prompt A = %v", err)
	}
	close(bEval.release)
	if err := <-bDone; err != nil {
		t.Fatalf("Prompt B = %v", err)
	}
}

// Prompt queuing was removed from SessionGate: execution now uses per-runtime gates, and
// request cancellation flows through evaluate to AgentRun. The former queued-prompt test is
// obsolete; dispatchRequest exits when the handler returns.

// TestManagementGateSerializesManagementOps verifies a held management gate returns busy
// instead of queueing session/new; retry succeeds after release. Concurrent plugin-level
// serialization is covered by the integration suite.
func TestManagementGateSerializesManagementOps(t *testing.T) {
	source := &multiRouteSessionSource{}
	reporter := &recordingReporter{}
	handler := &FrontendHandler{
		evaluate: func(ctx *model.Context, _ string) error { return nil },
		sessions: source,
		multi:    source,
		gate:     NewSessionGate(),
	}

	// 模拟另一管理操作占住临界区；/new 必须在创建任何资源前返回 busy。
	if !handler.gate.tryAcquire() {
		t.Fatal("setup: idle gate must be acquirable")
	}
	err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp"}, reporter)
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 || !strings.Contains(rpcErr.Message, "busy") {
		t.Fatalf("CreateSession while management gate held = %v, want -32000 busy", err)
	}
	// busy 拒绝零副作用：在获取 gate 前不能创建后端 session。
	source.muRoute.Lock()
	if source.next != 0 || len(source.rolledBack) != 0 {
		source.muRoute.Unlock()
		t.Fatalf("busy create side effects: created=%d rolledBack=%v, want none", source.next, source.rolledBack)
	}
	source.muRoute.Unlock()

	// 释放后重试成功（瞬态错误的客户端契约）。
	handler.gate.release()
	if err := testCreateSession(handler, context.Background(), "s1", protocol.NewSessionRequest{Cwd: "/tmp"}, reporter); err != nil {
		t.Fatalf("CreateSession after gate release = %v, want success", err)
	}
	source.muRoute.Lock()
	defer source.muRoute.Unlock()
	if source.next != 1 {
		t.Fatalf("created sessions after retry = %d, want 1", source.next)
	}
}

type multiRouteSessionSource struct {
	fakeSessionSource
	muRoute    sync.Mutex
	next       int
	loaded     []string
	unbound    []string
	rolledBack []string
	rejectBind bool
}

func (s *multiRouteSessionSource) BindConnectionSession(string, string) {}

func (s *multiRouteSessionSource) UnbindConnectionSession(connectionID, sessionID string) {
	s.muRoute.Lock()
	s.unbound = append(s.unbound, connectionID+":"+sessionID)
	s.muRoute.Unlock()
}

func TestNewPromptDoesNotClearCanonicalSessionOrUnbindItsReference(t *testing.T) {
	sessions := &multiRouteSessionSource{}
	route := &sessionRoute{}
	handler := &FrontendHandler{
		evaluate: func(ctx *model.Context, _ string) error {
			if binding := agentcontrol.BindingFromCtx(ctx.Ctx); binding == nil || binding.Get() != "session-one" {
				t.Fatalf("request binding = %v, want session-one", binding)
			}
			return nil
		},
		sessions:     sessions,
		gate:         NewSessionGate(),
		connectionID: "connection-a",
		promptRoutes: map[string]*sessionRoute{
			"session-one": route,
		},
	}
	handler.binding.Set("session-one")

	if _, err := handler.Prompt(context.Background(), "session-one", []protocol.ContentBlock{protocol.TextBlock("/new")}, &recordingReporter{}); err != nil {
		t.Fatalf("Prompt /new on canonical session: %v", err)
	}
	if got := handler.promptRoutes["session-one"]; got != route {
		t.Fatal("prompt route for session-one changed")
	}
	if got := handler.binding.Get(); got != "session-one" {
		t.Fatalf("binding after prompt = %q, want session-one", got)
	}
	if got := len(sessions.unbound); got != 0 {
		t.Fatalf("unbound references = %v, want none", sessions.unbound)
	}
}

func (s *multiRouteSessionSource) CreateBoundSession(string) (string, error) {
	s.muRoute.Lock()
	defer s.muRoute.Unlock()
	s.next++
	return fmt.Sprintf("runtime-%d", s.next), nil
}

func (s *multiRouteSessionSource) CreateBoundSessionWithRollback(cwd string) (string, func() error, error) {
	id, err := s.CreateBoundSession(cwd)
	if err != nil {
		return "", nil, err
	}
	return id, func() error {
		s.muRoute.Lock()
		s.rolledBack = append(s.rolledBack, id)
		s.muRoute.Unlock()
		return nil
	}, nil
}

func (s *multiRouteSessionSource) BindConnectionSessionReferenceGuarded(_ string, _ string, allowBind func() bool) bool {
	return !s.rejectBind && allowBind != nil && allowBind()
}

func TestLoadUsesCanonicalSessionIDWithoutRuntimeRemapping(t *testing.T) {
	sessions := &multiRouteSessionSource{}
	handler := &FrontendHandler{
		sessions: sessions,
		multi:    sessions,
		gate:     NewSessionGate(),
		promptRoutes: map[string]*sessionRoute{
			"canonical-session": {},
		},
	}

	if _, err := handler.LoadSessionWithSessionID(context.Background(), protocol.LoadSessionRequest{SessionID: "canonical-session"}, &recordingReporter{}); err != nil {
		t.Fatalf("load canonical session: %v", err)
	}
	if got := sessions.loaded; len(got) != 1 || got[0] != "canonical-session" {
		t.Fatalf("loaded IDs = %v, want canonical-session without runtime remapping", got)
	}
}

func (s *multiRouteSessionSource) LoadBoundSession(sessionID, _ string) error {
	s.muRoute.Lock()
	s.loaded = append(s.loaded, sessionID)
	s.muRoute.Unlock()
	return nil
}

func (s *multiRouteSessionSource) SessionHistory(string) ([]model.SessionUpdate, error) {
	return nil, nil
}

func (s *multiRouteSessionSource) SessionArchived(string) bool         { return false }
func (s *multiRouteSessionSource) ArchiveSession(string, string) error { return nil }

type routingToolBinder struct {
	mu       sync.Mutex
	sessions []string
}

func (b *routingToolBinder) BindClientToolSpecs(string, string, []agent.ClientToolSpec, model.DeviceCaller) bool {
	return true
}

func (b *routingToolBinder) ClientToolSourceForSpecs(sessionID string, _ []agent.ClientToolSpec) func(context.Context) []core.Tool {
	b.mu.Lock()
	b.sessions = append(b.sessions, sessionID)
	b.mu.Unlock()
	return func(context.Context) []core.Tool {
		return []core.Tool{{Name: "tool-" + sessionID}}
	}
}

type multiRouteWithTools struct {
	*multiRouteSessionSource
	*routingToolBinder
}

func TestFrontendHandlerRoutesConcurrentTransportSessionsAndTools(t *testing.T) {
	source := &multiRouteSessionSource{fakeSessionSource: fakeSessionSource{currentID: "global-trap"}}
	tools := &routingToolBinder{}
	combined := &multiRouteWithTools{multiRouteSessionSource: source, routingToolBinder: tools}
	started := make(chan string, 2)
	release := make(chan struct{})
	observed := make(chan struct {
		transport string
		runtime   string
		tool      string
	}, 2)
	handler := &FrontendHandler{
		evaluate: func(ctx *model.Context, input string) error {
			binding := agentcontrol.BindingFromCtx(ctx.Ctx)
			if binding == nil {
				return errors.New("missing request binding")
			}
			runtimeID := binding.Get()
			toolList := ctx.ToolSource(context.Background())
			toolName := ""
			if len(toolList) > 0 {
				toolName = toolList[0].Name
			}
			started <- input
			observed <- struct {
				transport string
				runtime   string
				tool      string
			}{transport: input, runtime: runtimeID, tool: toolName}
			<-release
			return nil
		},
		sessions:          combined,
		multi:             combined,
		gate:              NewSessionGate(),
		connectionID:      "conn-multi",
		deviceCaller:      &capsReporter{},
		deviceDescriptors: []acpworker.DeviceToolDescriptor{{Name: "device-tool"}},
	}

	var runtimeA, runtimeB string
	runtimeA = createManagedSessionForTest(t, handler, protocol.NewSessionRequest{}, &recordingReporter{})
	runtimeB = createManagedSessionForTest(t, handler, protocol.NewSessionRequest{}, &recordingReporter{})
	if runtimeA == runtimeB {
		t.Fatalf("session/new runtime IDs = %q/%q, want distinct IDs", runtimeA, runtimeB)
	}

	prompt := func(transport string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, promptErr := handler.Prompt(context.Background(), transport, []protocol.ContentBlock{protocol.TextBlock(transport)}, &recordingReporter{})
			done <- promptErr
		}()
		return done
	}
	// ACP session/new exposes the canonical runtime ID; it is also the transport
	// routing key, so prompts must address the returned ID rather than a local alias.
	promptA := prompt(runtimeA)
	promptB := prompt(runtimeB)
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("prompts for different transport sessions did not run concurrently")
		}
	}

	got := map[string]struct{ runtime, tool string }{}
	for range 2 {
		item := <-observed
		got[item.transport] = struct{ runtime, tool string }{item.runtime, item.tool}
	}
	if route := got[runtimeA]; route.runtime != runtimeA || route.tool != "tool-"+runtimeA {
		t.Errorf("session %q routed to runtime/tool %q/%q, want %q/%q", runtimeA, route.runtime, route.tool, runtimeA, "tool-"+runtimeA)
	}
	if route := got[runtimeB]; route.runtime != runtimeB || route.tool != "tool-"+runtimeB {
		t.Errorf("session %q routed to runtime/tool %q/%q, want %q/%q", runtimeB, route.runtime, route.tool, runtimeB, "tool-"+runtimeB)
	}
	close(release)
	for _, done := range []<-chan error{promptA, promptB} {
		if err := <-done; err != nil {
			t.Errorf("Prompt: %v", err)
		}
	}

	tools.mu.Lock()
	defer tools.mu.Unlock()
	if len(tools.sessions) != 2 || tools.sessions[0] == tools.sessions[1] {
		t.Fatalf("device tool source sessions = %v, want one lookup for each runtime", tools.sessions)
	}
}

func TestFrontendHandlerLoadReturnsBoundRuntimeID(t *testing.T) {
	source := &multiRouteSessionSource{fakeSessionSource: fakeSessionSource{currentID: "global-trap"}}
	handler := NewFrontendHandlerFactory(func(*model.Context, string) error { return nil }, nil, source, NewSessionGate())().(*FrontendHandler)
	runtimeID, err := handler.LoadSessionWithSessionID(context.Background(), protocol.LoadSessionRequest{SessionID: "runtime-loaded"}, &recordingReporter{})
	if err != nil {
		t.Fatalf("LoadSessionWithSessionID: %v", err)
	}
	if runtimeID != "runtime-loaded" {
		t.Fatalf("loaded runtime ID = %q, want runtime-loaded", runtimeID)
	}
}
