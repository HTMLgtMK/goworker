package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	aisession "github.com/tinguo/goworker/ai-session"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	acpservice "github.com/tinguo/goworker/daemon/internal/core/service/acp"
)

// boundsession_test.go 是 Plan A2b1 的 handler×plugin 集成测试，覆盖四条场景：
// ① 两连接各自新建会话后 prompt 独立路由；② LoadSession 重开收口后的会话并
// 重放历史；③ 多连接共享会话双向路由；④ archived 会话 load 只读、binding 不动。
// 测试使用真实 handler/plugin 与 fixedProvider，零真实 LLM；断言从插件侧观测
// 绑定、注册与历史，不依赖 handler 包内私有字段。

// acpRecorder 是 aiacp.Reporter 的最小收集实现(acp 包 handler_test.go 的
// recordingReporter 同款形态;测试文件不可跨包引用,这里自带一份)。
type acpRecorder struct {
	mu      sync.Mutex
	updates []protocol.SessionUpdateBody
	chunks  []string
}

func (r *acpRecorder) Update(_ string, body protocol.SessionUpdateBody) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updates = append(r.updates, body)
}

func (r *acpRecorder) MessageChunk(_ string, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunks = append(r.chunks, text)
}

func (r *acpRecorder) RequestPermission(context.Context, string, protocol.PermissionRequest) (string, error) {
	return "", aiacp.ErrPermissionUnsupported
}

// updateTexts 抽出更新流里的文本载荷(重放历史的 user/agent chunk;命令补全的
// Raw 载荷没有 Content,天然被跳过)。
func (r *acpRecorder) updateTexts() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.updates))
	for _, u := range r.updates {
		if u.Content != nil {
			out = append(out, u.Content.Text)
		}
	}
	return out
}

// acpStack 是一条「连接」:handler + recorder。connectionID 由工厂分配,
// transport 会话 id(session/new 的服务端签发 id)由测试给定。
type acpStack struct {
	handler *acpservice.FrontendHandler
	rep     *acpRecorder
	p       *AgentPlugin
}

// newACPStack 构造真实插件 + 真实 FrontendHandler 的测试栈。每条连接一份
// handler(工厂形态,连接私有 binding/connectionID),共享同一插件与同一
// SessionGate(生产装配形态)。
func newACPStack(t *testing.T, pv core.Provider) (*AgentPlugin, func() *acpStack) {
	t.Helper()
	p := newPersistedPlugin(t, pv)
	evaluate := func(ctx *model.Context, input string) error {
		// Engine.Eval routes registered commands before falling back to /agent.
		ctx.Args = []string{input}
		if input == "/new" {
			return p.handleNew(ctx)
		}
		return p.handleAgent(ctx)
	}
	factory := acpservice.NewFrontendHandlerFactory(evaluate, nil, p, acpservice.NewSessionGate())
	return p, func() *acpStack {
		handler := factory().(*acpservice.FrontendHandler)
		rep := &acpRecorder{}
		// Direct handler tests have no wire initialize request; declare the same
		// no-device ready state that a capability-free initialize would produce.
		handler.InitializeReady(context.Background(), rep)
		return &acpStack{handler: handler, rep: rep, p: p}
	}
}

// registeredSessionIDs 快照 manager 注册表的 runtime id 集合(集合差断言的基准)。
func registeredSessionIDs(p *AgentPlugin) map[string]bool {
	out := make(map[string]bool)
	for _, rt := range p.manager.List() {
		out[rt.ID] = true
	}
	return out
}

// newestSessionID 返回 after 相对 before 新增的注册 id(恰一个——CreateSession
// 的注册面;既有 id 重复注册会被 Register 拒绝,不产生新条目)。
func newestSessionID(t *testing.T, before, after map[string]bool) string {
	t.Helper()
	fresh := ""
	for id := range after {
		if !before[id] {
			if fresh != "" {
				t.Fatalf("more than one new registration: %q and %q", fresh, id)
			}
			fresh = id
		}
	}
	if fresh == "" {
		t.Fatal("no new registration after CreateSession")
	}
	return fresh
}

// create calls the lifecycle adapter and returns the one canonical ID used on every surface.
func (s *acpStack) create(t *testing.T, _ string) string {
	t.Helper()
	id, err := s.handler.CreateManagedSession(context.Background(), protocol.NewSessionRequest{}, s.rep)
	if err != nil {
		t.Fatalf("CreateManagedSession: %v", err)
	}
	return id.String()
}

func TestACPPromptNewRequiresStandardSessionNew(t *testing.T) {
	p, newStack := newACPStack(t, fixedProvider{content: "ack"})
	stack := newStack()
	sessionID := stack.create(t, "ignored-transport")
	currentID := p.CurrentSessionID()
	before := registeredSessionIDs(p)

	_, err := stack.handler.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("/new")}, stack.rep)
	if err == nil || !strings.Contains(err.Error(), "session/new") {
		t.Fatalf("ACP prompt /new error = %v, want explicit session/new guidance", err)
	}
	if got := p.CurrentSessionID(); got != currentID {
		t.Fatalf("current session after ACP /new = %q, want unchanged %q", got, currentID)
	}
	after := registeredSessionIDs(p)
	if len(after) != len(before) {
		t.Fatalf("registered session count after ACP /new = %d, want unchanged %d", len(after), len(before))
	}
	for id := range before {
		if !after[id] {
			t.Fatalf("session %q was removed by ACP /new", id)
		}
	}
	if p.manager.Get(sessionID) == nil {
		t.Fatalf("ACP /new removed old session %q from runtime manager", sessionID)
	}
	if _, err := stack.handler.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock("still usable")}, stack.rep); err != nil {
		t.Fatalf("prompt old session after rejected ACP /new: %v", err)
	}
}

func TestAbandonCreatedBoundSessionRemovesAllRegistriesAndStore(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ack"})
	id, rollback, err := p.CreateBoundSessionWithRollback("")
	if err != nil {
		t.Fatalf("CreateBoundSessionWithRollback: %v", err)
	}
	if p.manager.Get(id) == nil {
		t.Fatalf("runtime %q was not registered before rollback", id)
	}
	managedID, err := aisession.ParseID(id)
	if err != nil {
		t.Fatalf("ParseID(%q): %v", id, err)
	}
	if _, err := p.aiSessions.Get(context.Background(), managedID); err != nil {
		t.Fatalf("ai-session registry missing created session: %v", err)
	}

	if err := rollback(); err != nil {
		t.Fatalf("rollback created session: %v", err)
	}
	if p.manager.Get(id) != nil {
		t.Fatalf("runtime %q remains registered after rollback", id)
	}
	if _, err := p.aiSessions.Get(context.Background(), managedID); !errors.Is(err, aisession.ErrNotFound) {
		t.Fatalf("ai-session Get after rollback = %v, want ErrNotFound", err)
	}
	stored, err := p.repo.List(context.Background())
	if err != nil {
		t.Fatalf("Repository.List after rollback: %v", err)
	}
	for _, item := range stored {
		if item.SessionID == id {
			t.Fatalf("persistent session %q remains after rollback", id)
		}
	}
}

func TestAbandonCreatedBoundSessionRestoresRuntimeAfterDeleteFailure(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ack"})
	storage := &failDeleteOnceStorage{delegate: session.NewAIStorageAdapter(p.repo), err: errors.New("injected storage delete failure")}
	manager, err := aisession.NewSessionManager(storage, aisession.WithInitializer(p.initializeAISession))
	if err != nil {
		t.Fatalf("NewSessionManager: %v", err)
	}
	p.aiSessions = manager

	id, rollback, err := p.CreateBoundSessionWithRollback("")
	if err != nil {
		t.Fatalf("CreateBoundSessionWithRollback: %v", err)
	}
	runtime := p.manager.Get(id)
	if runtime == nil {
		t.Fatalf("runtime %q was not registered before rollback", id)
	}
	managedID, err := aisession.ParseID(id)
	if err != nil {
		t.Fatalf("ParseID(%q): %v", id, err)
	}

	if err := rollback(); err == nil || !strings.Contains(err.Error(), "injected storage delete failure") {
		t.Fatalf("rollback with storage failure = %v, want injected failure", err)
	}
	if got := p.manager.Get(id); got != runtime {
		t.Fatalf("runtime after failed rollback = %p, want original runtime %p", got, runtime)
	}
	if runtime.Retired() {
		t.Fatal("runtime remains retired after failed storage deletion")
	}
	if _, err := p.aiSessions.Get(context.Background(), managedID); err != nil {
		t.Fatalf("ai-session registry after failed rollback: %v", err)
	}
	if _, err := p.repo.Open(id); err != nil {
		t.Fatalf("persistent store after failed rollback: %v", err)
	}

	if err := rollback(); err != nil {
		t.Fatalf("rollback retry: %v", err)
	}
	if p.manager.Get(id) != nil {
		t.Fatalf("runtime %q remains registered after successful retry", id)
	}
	if _, err := p.aiSessions.Get(context.Background(), managedID); !errors.Is(err, aisession.ErrNotFound) {
		t.Fatalf("ai-session after successful retry = %v, want ErrNotFound", err)
	}
}

type failDeleteOnceStorage struct {
	delegate aisession.Storage
	mu       sync.Mutex
	err      error
	failed   bool
}

func (s *failDeleteOnceStorage) Create(ctx context.Context, value aisession.Session) error {
	return s.delegate.Create(ctx, value)
}

func (s *failDeleteOnceStorage) Load(ctx context.Context, id aisession.ID) (aisession.Session, error) {
	return s.delegate.Load(ctx, id)
}

func (s *failDeleteOnceStorage) Get(ctx context.Context, id aisession.ID) (aisession.Session, error) {
	return s.delegate.Get(ctx, id)
}

func (s *failDeleteOnceStorage) List(ctx context.Context) ([]aisession.Session, error) {
	return s.delegate.List(ctx)
}

func (s *failDeleteOnceStorage) Delete(ctx context.Context, id aisession.ID) error {
	s.mu.Lock()
	if !s.failed {
		s.failed = true
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	return s.delegate.Delete(ctx, id)
}

func TestAbandonCreatedBoundSessionRefusesConnectionReference(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ack"})
	id, rollback, err := p.CreateBoundSessionWithRollback("")
	if err != nil {
		t.Fatalf("CreateBoundSessionWithRollback: %v", err)
	}
	p.BindConnectionSessionReference("another-connection", id)

	if err := rollback(); err == nil || !strings.Contains(err.Error(), "connection references remain") {
		t.Fatalf("rollback with another connection reference = %v, want refusal", err)
	}
	if p.manager.Get(id) == nil {
		t.Fatalf("referenced runtime %q was removed", id)
	}
	managedID, err := aisession.ParseID(id)
	if err != nil {
		t.Fatalf("ParseID(%q): %v", id, err)
	}
	if _, err := p.aiSessions.Get(context.Background(), managedID); err != nil {
		t.Fatalf("referenced ai-session disappeared: %v", err)
	}
	if _, err := p.repo.Open(id); err != nil {
		t.Fatalf("referenced persistent store disappeared: %v", err)
	}
	p.UnbindConnectionSession("another-connection", id)
}

// load 走 handler.LoadSession。
func (s *acpStack) load(t *testing.T, sessionID string) {
	t.Helper()
	if err := s.handler.LoadSession(context.Background(), protocol.LoadSessionRequest{SessionID: sessionID}, s.rep); err != nil {
		t.Fatalf("LoadSession(%s): %v", sessionID, err)
	}
}

// prompt 走 handler.Prompt(标准 ACP;sessionID = transport id 或 load 后的
// store id,与 PR2 的「load 换轨活动身份」语义一致)。
func (s *acpStack) prompt(t *testing.T, sessionID, input string) {
	t.Helper()
	_, err := s.handler.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock(input)}, s.rep)
	if err != nil {
		t.Fatalf("Prompt(%s, %q): %v", sessionID, input, err)
	}
}

// sessionHistory 是 SessionHistory 的测试形态(t.Fatal 化错误)。
func sessionHistory(t *testing.T, p *AgentPlugin, sessionID string) []model.SessionUpdate {
	t.Helper()
	updates, err := p.SessionHistory(sessionID)
	if err != nil {
		t.Fatalf("SessionHistory(%s): %v", sessionID, err)
	}
	return updates
}

// TestACPConnectionsCreateDistinctSessions(场景 ①):两连接各 session/new →
// 各自绑定**不同**新会话;current 不动(A2b1 语义:新会话只是本连接的路由目标);
// prompt 各自路由正确、历史互不串扰。
func TestACPConnectionsCreateDistinctSessions(t *testing.T) {
	p, newStack := newACPStack(t, fixedProvider{content: "ack"})
	currentID := p.CurrentSessionID() // Init 创建的首会话 = current(CLI 便利指针)

	conn1, conn2 := newStack(), newStack()
	sid1 := conn1.create(t, "ignored-conn1")
	sid2 := conn2.create(t, "ignored-conn2")

	// 各自绑定不同新会话:store 稳定 id(注册表里的新条目),与 transport id
	// 无关。
	if sid1 == "" || sid2 == "" {
		t.Fatalf("created session ids = %q / %q, want non-empty store ids", sid1, sid2)
	}
	if sid1 == sid2 {
		t.Fatalf("two connections bound the same session %q (真新建必须产出不同会话)", sid1)
	}
	if sid1 == currentID || sid2 == currentID {
		t.Fatal("created sessions must be fresh store ids, not the CLI current session")
	}

	// current 不动:session/new 不换轨(plan A2「current 收缩为 CLI/stdin 便利
	// 指针」;旧语义在此会易主,断言逐条对齐为新语义)。
	if got := p.CurrentSessionID(); got != currentID {
		t.Fatalf("CurrentSessionID after two session/new = %q, want %q unchanged (A2b1:创建不动 current)", got, currentID)
	}

	// prompt 各自路由(此步 SessionGate 仍串行,跨会话不并行——A2b2 收缩):
	// 执行链经连接级 Binding 路由到各自会话,历史互不串扰。
	conn1.prompt(t, sid1, "hello-one")
	conn2.prompt(t, sid2, "hello-two")

	conv1 := p.manager.Get(sid1).Session().Conversation()
	if len(conv1) == 0 || conv1[0].Role != "user" || conv1[0].Content != "hello-one" {
		t.Fatalf("session one conversation = %v, want the bound prompt", conv1)
	}
	for _, m := range conv1 {
		if m.Content == "hello-two" {
			t.Fatalf("session one got session two's input (历史串扰): %v", conv1)
		}
	}
	conv2 := p.manager.Get(sid2).Session().Conversation()
	if len(conv2) == 0 || conv2[0].Content != "hello-two" {
		t.Fatalf("session two conversation = %v, want its own prompt", conv2)
	}

	// 历史重放面(BoundSessionSource.SessionHistory)按会话隔离。
	if !historyHasUserText(sessionHistory(t, p, sid1), "hello-one") || historyHasUserText(sessionHistory(t, p, sid1), "hello-two") {
		t.Fatalf("history(one) = %+v, want only its own round", sessionHistory(t, p, sid1))
	}
	if !historyHasUserText(sessionHistory(t, p, sid2), "hello-two") || historyHasUserText(sessionHistory(t, p, sid2), "hello-one") {
		t.Fatalf("history(two) = %+v, want only its own round", sessionHistory(t, p, sid2))
	}

	// 连接 1 新建另一会话不会丢弃它此前加载的 session 1 引用；显式 /new
	// 才精确解除被替换会话，断连则一次清掉该连接的所有引用。
	sid3 := conn1.create(t, "transport-conn3")
	if sid3 == sid1 {
		t.Fatalf("conn1 re-create reused session %q", sid1)
	}
	if p.manager.Get(sid1) == nil {
		t.Fatal("session one must remain registered while conn1 still references it")
	}
	if got := p.control.SessionBindings(sid1); len(got) != 1 {
		t.Fatalf("session one bindings after conn1 creates another session = %v, want conn1's reference", got)
	}
	if p.manager.Get(sid2) == nil {
		t.Fatal("session two must survive conn1's create (another connection still references it)")
	}
}

// TestACPLoadSessionReopensReleasedSession(场景 ②):/new 收口(A2a 生命周期:
// 发起连接的绑定摘除 → 引用归零 → manager.Remove + repo.CloseSession 逐出缓存)
// 后的会话 → session/load 重开 → 拿活实例、历史重放、prompt 路由到重开会话。
func TestACPLoadSessionReopensReleasedSession(t *testing.T) {
	p, newStack := newACPStack(t, fixedProvider{content: "ack"})
	conn := newStack()

	// 会话 X:真新建 + prompt 写历史(落 store + checkpoint)。
	sidX := conn.create(t, "ignored-transport-x")
	conn.prompt(t, sidX, "question-x")
	rtX1 := p.manager.Get(sidX)
	if rtX1 == nil {
		t.Fatal("session X runtime missing after create+prompt")
	}
	oldStore := p.storeFor(rtX1)
	if oldStore == nil {
		t.Fatal("session X store missing before release")
	}

	// Closing the last connection reference exercises the same runtime/store teardown
	// without relying on the now-rejected ACP prompt /new shortcut.
	bound := p.control.SessionBindings(sidX)
	if len(bound) != 1 {
		t.Fatalf("setup: bindings(%s) = %v, want exactly the creating connection", sidX, bound)
	}
	p.ReleaseConnection(bound[0])
	if p.manager.Get(sidX) != nil {
		t.Fatal("session X must be released after /new teardown (引用归零收口)")
	}

	// session/load 重开:不在册但持久化 → repo.Open 拿活实例(flock 空闲;逐出
	// 后重开走慢路径)→ 新 runtime 注册 → 历史重放。
	conn.load(t, sidX)
	rtX2 := p.manager.Get(sidX)
	if rtX2 == nil {
		t.Fatal("reopened session must be registered")
	}
	if rtX2 == rtX1 {
		t.Fatal("reopen must build a fresh runtime, not resurrect the released one")
	}
	newStore := p.storeFor(rtX2)
	if newStore == nil || newStore == oldStore {
		t.Fatalf("reopened store = %v, want a live instance distinct from the closed one (A2.1①:CloseSession 逐出后重开)", newStore)
	}

	// 历史重放:重放更新里含 question-x(load 响应前的重放面)。
	if !replayHasText(conn.rep.updates, "question-x") {
		t.Fatalf("replayed updates = %+v, want the persisted round (历史重放)", conn.rep.updateTexts())
	}

	// prompt 路由到重开会话:load 换轨活动身份后,store id 即本连接的活动声明;
	// 重开拿的是活实例,可继续写(数据在原 store 上累积)。
	conn.prompt(t, sidX, "after-reopen")
	if !historyHasUserText(sessionHistory(t, p, sidX), "after-reopen") {
		t.Fatalf("reopened session history = %+v, want the post-reopen round routed into it", sessionHistory(t, p, sidX))
	}
}

// TestACPLoadSessionSharesSessionAcrossConnections(场景 ③):A 绑会话 X,
// B load 同一 X → 双绑定,prompt 双方都可路由;A 改绑后 X 因 B 的引用保留。
func TestACPLoadSessionSharesSessionAcrossConnections(t *testing.T) {
	p, newStack := newACPStack(t, fixedProvider{content: "ack"})
	connA, connB := newStack(), newStack()

	// A 真新建 X 并写一轮历史。
	sidX := connA.create(t, "ignored-transport-a")
	connA.prompt(t, sidX, "from-a")

	// B load 同一 X(未绑过):命中已注册 runtime → 只 reload 视图 + 改绑登记
	// (共享会话,不重建);历史重放把 A 的轮次带给 B。
	connB.load(t, sidX)
	bindings := p.control.SessionBindings(sidX)
	if len(bindings) != 2 {
		t.Fatalf("SessionBindings(%s) = %v, want both connections (双绑定)", sidX, bindings)
	}
	if !replayHasText(connB.rep.updates, "from-a") {
		t.Fatalf("connB replayed updates = %+v, want A's round", connB.rep.updateTexts())
	}

	// 双方 prompt 都可路由到 X(此步仍串行执行,A2b2 收缩后并行)。
	connA.prompt(t, sidX, "again-from-a")
	connB.prompt(t, sidX, "from-b")
	conv := p.manager.Get(sidX).Session().Conversation()
	for _, want := range []string{"from-a", "again-from-a", "from-b"} {
		if !conversationHasUserText(conv, want) {
			t.Fatalf("shared session conversation = %v, want %q routed from either connection", conv, want)
		}
	}

	// A 创建另一个会话后仍持有 X 的生命周期引用；B 也仍绑定 X，因此会话
	// 保留且可继续 prompt。路由选择已换轨，但历史引用不会因选择器变化而消失。
	connA.create(t, "transport-c")
	if p.manager.Get(sidX) == nil {
		t.Fatal("shared session must survive A creating another session")
	}
	if got := p.control.SessionBindings(sidX); len(got) != 2 {
		t.Fatalf("SessionBindings after A creates another session = %v, want both references", got)
	}
	connB.prompt(t, sidX, "still-here")
	if !historyHasUserText(sessionHistory(t, p, sidX), "still-here") {
		t.Fatal("shared session history must grow after the other connection left")
	}
}

// TestACPLoadArchivedSessionReadOnly(场景 ④):archived 终态会话 load → 只读
// 重放、binding 不动、不注册 runtime、prompt 诚实拒绝。
func TestACPLoadArchivedSessionReadOnly(t *testing.T) {
	p, newStack := newACPStack(t, fixedProvider{content: "ack"})
	conn := newStack()

	// 先绑一个活会话(binding 非空,验证归档 load 不改绑)。
	liveID := conn.create(t, "transport-live")
	if liveID == "" {
		t.Fatal("live binding missing")
	}

	// 造 archived 会话(Plan A1 原位终态:MarkArchived 不搬文件,数据文件停在
	// per-id 目录)——repo.Create → 写历史 → 落终态标记 → 收口释放。
	st, err := p.repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatalf("repo.Create(archived fixture): %v", err)
	}
	archID := st.SessionID()
	if _, err := st.Commit([]core.Message{{Role: "user", Content: "archived-past", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	if err := st.MarkArchived(); err != nil {
		t.Fatalf("MarkArchived: %v", err)
	}
	if err := p.repo.CloseSession(archID); err != nil {
		t.Fatalf("release archived fixture store: %v", err)
	}

	// load archived:repo 层探测(SessionArchived)命中终态 → 只读重放。
	conn.load(t, archID)
	if !replayHasText(conn.rep.updates, "archived-past") {
		t.Fatalf("archived replay = %+v, want the persisted round (只读重放)", conn.rep.updateTexts())
	}
	// binding 不动(只读视图不是会话选择):活会话的绑定登记原样、archived 会话
	// 零登记;runtime 不注册。
	if got := p.control.SessionBindings(liveID); len(got) != 1 {
		t.Fatalf("live session bindings after archived load = %v, want the connection retained", got)
	}
	if got := p.control.SessionBindings(archID); len(got) != 0 {
		t.Fatalf("archived session bindings = %v, want none (只读视图不改绑)", got)
	}
	if p.manager.Get(archID) != nil {
		t.Fatal("archived session must not be reopened into a runtime")
	}

	// prompt 诚实报 read-only(而非 unknown 或静默续写)。
	_, err = conn.handler.Prompt(context.Background(), archID, []protocol.ContentBlock{protocol.TextBlock("hi")}, conn.rep)
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("Prompt(archived) = %v, want read-only error", err)
	}

	// 只读重放不常驻句柄:快照后 store 已释放,repo 可再次打开(A2b1 扩展的
	// ArchivedSessionHistory repo 分支的释放语义),终态标记跨重启(此处跨
	// 重开)依然为真——A7 设计点 4 的持久事实源。
	reopened, err := p.repo.Open(archID)
	if err != nil {
		t.Fatalf("repo.Open(archived) after replay: %v", err)
	}
	if !reopened.Archived() {
		t.Fatal("archived marker must survive reopen")
	}
	_ = reopened.Close()
}

// replayHasText 报告 handler 下发的更新流(wire 形态)里是否有文本载荷 == want
// (重放历史的 user/agent chunk 都走 Content;命令补全的 Raw 载荷无 Content,
// 天然被跳过)。
func replayHasText(updates []protocol.SessionUpdateBody, want string) bool {
	for _, u := range updates {
		if u.Content != nil && u.Content.Text == want {
			return true
		}
	}
	return false
}

// historyHasUserText 报告重放更新序列里是否有 user chunk 文本 == want。
func historyHasUserText(updates []model.SessionUpdate, want string) bool {
	for _, u := range updates {
		if u.Kind == model.SessionUpdateUserMessageChunk && u.Content != nil && u.Content.Text == want {
			return true
		}
	}
	return false
}

// conversationHasUserText 报告会话快照里是否有 user 消息文本 == want。
func conversationHasUserText(conv []core.Message, want string) bool {
	for _, m := range conv {
		if m.Role == "user" && m.Content == want {
			return true
		}
	}
	return false
}

// ── A2b2 并行验收（plan A6：两连接并行 prompt 互不阻塞、在飞 prompt 不阻塞
// 管理操作；执行互斥下沉 per-runtime gate 后的跨会话并行形态）────────────────

// promptAsync 在后台 goroutine 跑一轮 prompt，返回完成 err 的接收通道
// （waitPrompt 的 acpStack 形态；并行断言需要非阻塞观察面）。
func (s *acpStack) promptAsync(t *testing.T, sessionID, input string) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := s.handler.Prompt(context.Background(), sessionID, []protocol.ContentBlock{protocol.TextBlock(input)}, s.rep)
		done <- err
	}()
	return done
}

// TestACPCrossSessionPromptsRunInParallel(任务书验收①)验证跨会话并行：连接 1 的
// prompt 阻塞在 provider（handleAgent → manager.StartRun → Session.Run →
// provider.Chat），连接 2 的 prompt 在此期间正常完成。拆除 prompt 全局门禁前，
// 连接 2 会排队在 handler.Prompt 的 gate.acquire 上；拆除后每个会话持有各自的
// runtime gate（withSession → AcquireRun），两者互不阻塞。
func TestACPCrossSessionPromptsRunInParallel(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p, newStack := newACPStack(t, fixedProvider{content: "ack"})
	currentID := p.CurrentSessionID()
	conn1, conn2 := newStack(), newStack()

	// 逐会话注入 provider 工厂:mountRuntime 以创建时刻的 p.deps 为底座(值拷贝
	// 换绑,CreateBoundSession 内部冻结),conn1 的会话绑阻塞 provider、conn2 的
	// 绑立即完成 provider——并行断言的两个角色。
	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) { return gated, nil }
	sid1 := conn1.create(t, "transport-1")
	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) { return fixedProvider{content: "ack"}, nil }
	sid2 := conn2.create(t, "transport-2")
	if sid1 == sid2 || sid1 == currentID || sid2 == currentID {
		t.Fatalf("created sessions = %q/%q, want two fresh distinct ids", sid1, sid2)
	}

	// 连接 1 的长 prompt:阻塞在 provider(started 关闭 = 已推进到模型调用)。
	p1 := conn1.promptAsync(t, sid1, "slow-one")
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("conn1 prompt did not reach the gated provider in time")
	}

	// 连接 2 的 prompt:必须在 A 阻塞期间完成(旧全局门禁下此处排队到 A 释放)。
	p2 := conn2.promptAsync(t, sid2, "fast-two")
	select {
	case err := <-p2:
		if err != nil {
			t.Fatalf("conn2 prompt while conn1 in flight: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("conn2 prompt did not finish while conn1 is in flight (A2b2:跨会话必须并行)")
	}
	if conv2 := p.manager.Get(sid2).Session().Conversation(); len(conv2) == 0 || conv2[0].Content != "fast-two" {
		t.Fatalf("session two conversation = %v, want its own round", conv2)
	}

	// A 仍在飞:放行后收敛。
	select {
	case <-gated.release:
	default:
	}
	close(gated.release)
	if err := <-p1; err != nil {
		t.Fatalf("conn1 prompt = %v", err)
	}
	if conv1 := p.manager.Get(sid1).Session().Conversation(); len(conv1) == 0 || conv1[0].Content != "slow-one" {
		t.Fatalf("session one conversation = %v, want its own round", conv1)
	}
}

// TestACPCreateSessionImmediateWhilePromptInFlight(任务书验收②):prompt 在飞时
// 另一连接的 session/new 立即成功——管理面与 prompt 面解耦(A2b2 拆除 prompt
// 全局门禁;拆除前 new 会收到 -32000 busy 拒绝)。
func TestACPCreateSessionImmediateWhilePromptInFlight(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p, newStack := newACPStack(t, fixedProvider{content: "ack"})
	conn1, conn2 := newStack(), newStack()

	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) { return gated, nil }
	sid1 := conn1.create(t, "transport-1")

	p1 := conn1.promptAsync(t, sid1, "long-running")
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("conn1 prompt did not reach the gated provider in time")
	}

	// A 在飞时 B 的 session/new:立即返回(时限断言的失败即「门禁仍被 prompt
	// 攥住」的回归)。
	created := make(chan string, 1)
	go func() {
		created <- conn2.create(t, "transport-2")
	}()
	select {
	case sid2 := <-created:
		if sid2 == "" || sid2 == sid1 {
			t.Fatalf("created session = %q, want a fresh id", sid2)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session/new did not return while a prompt is in flight (A2b2:管理操作不得被在飞 prompt 阻塞)")
	}

	close(gated.release)
	if err := <-p1; err != nil {
		t.Fatalf("conn1 prompt = %v", err)
	}
}

// TestACPManagementOpsConcurrentCreateSerializes(任务书验收③)验证两条连接并发
// session/new：管理操作在容量 1 的全局门禁上互斥，满时以非阻塞 busy 拒绝而非
// 排队，测试按客户端契约重试。断言两者最终都成功、绑定不同新会话、注册表恰好
// 新增两条，证明并发突变全序且无竞态损坏；门禁确定性由 handler 层测试覆盖。
func TestACPManagementOpsConcurrentCreateSerializes(t *testing.T) {
	p, newStack := newACPStack(t, fixedProvider{content: "ack"})
	conn1, conn2 := newStack(), newStack()
	before := registeredSessionIDs(p)

	// createWithRetry 建模客户端契约:busy 是瞬态错误,稍后重试直至成功(2s 上限)。
	// 新会话 id 不从连接 Binding 读取(handler 的连接私有字段,跨包不可达)——
	// 并发下两连接的新增会话由 wg 汇合后的注册表差集给出(恰两条,见下)。
	createWithRetry := func(s *acpStack, transportID string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			_, err := s.handler.CreateManagedSession(context.Background(), protocol.NewSessionRequest{}, s.rep)
			if err == nil {
				return
			}
			var rpcErr *protocol.RPCError
			if !errors.As(err, &rpcErr) || !strings.Contains(rpcErr.Message, "busy") {
				t.Fatalf("CreateSession(%s): %v", transportID, err)
			}
			if time.Now().After(deadline) {
				t.Fatalf("CreateSession(%s) still busy after retry window: %v", transportID, err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); createWithRetry(conn1, "transport-c1") }()
	go func() { defer wg.Done(); createWithRetry(conn2, "transport-c2") }()
	wg.Wait()

	// 注册表差集 = 恰两条新会话(并发下互斥保证突变全序,不丢不重);每条新会话
	// 恰有一条连接绑定(各连接的改绑各落各的)。
	var fresh []string
	after := registeredSessionIDs(p)
	if len(after) != len(before)+2 {
		t.Fatalf("registry = %d entries, want %d (exactly two new sessions)", len(after), len(before)+2)
	}
	for id := range after {
		if !before[id] {
			fresh = append(fresh, id)
		}
	}
	if len(fresh) != 2 {
		t.Fatalf("fresh sessions = %v, want exactly two (并发 create 的注册表突变必须全序收敛)", fresh)
	}
	for _, id := range fresh {
		if got := p.control.SessionBindings(id); len(got) != 1 {
			t.Fatalf("bindings(%s) = %v, want exactly one connection", id, got)
		}
	}
}

func TestConnectionScopedSessionRollbackPreservesOtherReferences(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "ack"})
	creator, ok := any(p).(model.ConnectionScopedTransactionalBoundSessionCreator)
	if !ok {
		t.Fatal("AgentPlugin does not implement connection-scoped transactional creation")
	}

	id, rollback, err := creator.CreateBoundSessionWithRollbackForConnection("", "creator")
	if err != nil {
		t.Fatalf("CreateBoundSessionWithRollbackForConnection: %v", err)
	}
	if got := p.control.SessionBindings(id); len(got) != 1 || got[0] != "creator" {
		t.Fatalf("initial bindings = %v, want creator reference", got)
	}
	p.BindConnectionSessionReference("observer", id)

	if err := rollback(); err == nil || !strings.Contains(err.Error(), "connection references remain") {
		t.Fatalf("rollback with another connection reference = %v, want refusal", err)
	}
	if got := p.control.SessionBindings(id); len(got) != 1 || got[0] != "observer" {
		t.Fatalf("bindings after refusal = %v, want only observer reference", got)
	}
	if p.manager.Get(id) == nil {
		t.Fatalf("runtime %q was removed despite another connection reference", id)
	}
	managedID, err := aisession.ParseID(id)
	if err != nil {
		t.Fatalf("ParseID(%q): %v", id, err)
	}
	if _, err := p.aiSessions.Get(context.Background(), managedID); err != nil {
		t.Fatalf("created ai-session disappeared despite another reference: %v", err)
	}

	p.UnbindConnectionSession("observer", id)
	if err := rollback(); err != nil {
		t.Fatalf("retry rollback after final reference was removed: %v", err)
	}
	if p.manager.Get(id) != nil {
		t.Fatalf("runtime %q remains after successful retry", id)
	}
	if _, err := p.aiSessions.Get(context.Background(), managedID); !errors.Is(err, aisession.ErrNotFound) {
		t.Fatalf("ai-session after rollback = %v, want ErrNotFound", err)
	}
}

func TestConnectionScopedSessionRollbackRefusesActiveRunAndCanRetry(t *testing.T) {
	provider := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, fixedProvider{content: "ack"})
	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) { return provider, nil }
	id, rollback, err := p.CreateBoundSessionWithRollbackForConnection("", "creator")
	if err != nil {
		t.Fatalf("CreateBoundSessionWithRollbackForConnection: %v", err)
	}

	handle, err := p.manager.StartRun(id, runtimeagent.RunRequest{Input: "run while rollback races"}, runtimeagent.RunCallbacks{})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	select {
	case <-provider.started:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not reach provider")
	}

	if err := rollback(); err == nil || !strings.Contains(err.Error(), "runtime is active") {
		t.Fatalf("rollback during active run = %v, want active-run refusal", err)
	}
	if got := p.control.SessionBindings(id); len(got) != 0 {
		t.Fatalf("provisional creator reference after rollback attempt = %v, want removed", got)
	}
	if p.manager.Get(id) == nil {
		t.Fatalf("runtime %q was removed while its run was active", id)
	}
	managedID, err := aisession.ParseID(id)
	if err != nil {
		t.Fatalf("ParseID(%q): %v", id, err)
	}
	if _, err := p.aiSessions.Get(context.Background(), managedID); err != nil {
		t.Fatalf("ai-session disappeared while its run was active: %v", err)
	}
	stored, err := p.repo.List(context.Background())
	if err != nil {
		t.Fatalf("Repository.List during active run: %v", err)
	}
	persisted := false
	for _, item := range stored {
		if item.SessionID == id {
			persisted = true
			break
		}
	}
	if !persisted {
		t.Fatalf("persistent session %q was deleted while its run was active", id)
	}

	close(provider.release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := handle.Wait(ctx); err != nil {
		t.Fatalf("wait for active run: %v", err)
	}
	if err := rollback(); err != nil {
		t.Fatalf("retry rollback after run completion: %v", err)
	}
	if p.manager.Get(id) != nil {
		t.Fatalf("runtime %q remains after successful rollback", id)
	}
	if _, err := p.aiSessions.Get(context.Background(), managedID); !errors.Is(err, aisession.ErrNotFound) {
		t.Fatalf("ai-session after rollback = %v, want ErrNotFound", err)
	}
}
