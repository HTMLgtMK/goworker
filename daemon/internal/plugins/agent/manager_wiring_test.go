package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// P0-4 接线测试：plugin 的会话持有从「裸 Session 指针」升级为「manager 注册的
// SessionRuntime」，current 为唯一解析点。锁死的语义（P5-5 起收口反转了旧的
// 「旧 runtime 保留在册」）：/new（经 startSession）后 current 指向新 runtime，
// 旧 runtime 在 handleNew 尾部（CheckpointSync 完成后）注销出 manager——归档
// 固化窗口内它仍在册，窗口结束即收口（注册表不再随 /new 无界累积）；
// current 切换后 withSession 的互斥落在新 runtime 上（解析与互斥同源）。

// TestCurrentSessionSwapsOnNew 模拟 /new：current 换到新 runtime，旧 runtime 在
// 收口后不再可 Get。走真实 startSession 装配首会话（memory/持久化/MCP 全关，零
// 外部依赖）——这是唯一一次注册；随后真实 handleNew → startSession 重建，会话
// 切换后的 current/manager 状态即生产接线形态。
func TestCurrentSessionSwapsOnNew(t *testing.T) {
	p := NewPlugin(&runtimeconfig.Config{}, runtimeconfig.Paths{})
	p.startSession()
	old := p.currentSession()
	if old == nil {
		t.Fatal("no current runtime after startSession")
	}
	oldSession := old.Session()
	if oldSession == nil {
		t.Fatal("current runtime has no bound session after startSession")
	}
	if got := p.manager.Get(old.ID); got != old {
		t.Fatalf("manager.Get(%q) = %v, want the initially registered runtime", old.ID, got)
	}

	ctx, buf := newContext()
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}
	if !strings.Contains(buf.String(), "New session started") {
		t.Errorf("handleNew output = %q, want success message", buf.String())
	}

	fresh := p.currentSession()
	if fresh == nil {
		t.Fatal("no current runtime after /new")
	}
	if fresh == old {
		t.Fatal("current still points at the pre-/new runtime")
	}
	if fresh.Session() == nil || fresh.Session() == oldSession {
		t.Fatal("fresh runtime did not bind a new session object")
	}

	// P5-5 收口：旧 runtime 已注销出 manager。纯内存（无 store）下新旧 runtime
	// 同 id "current"、startSession 的 Register 冲突旧条目保留，handleNew 尾部
	// Remove("current") 移除的正是旧条目——新 runtime 本就不在册，Get 返回 nil。
	if got := p.manager.Get(old.ID); got != nil {
		t.Fatalf("manager.Get(%q) = %v, want nil after /new (P5-5 removed the old runtime)", old.ID, got)
	}
}

// TestGateFollowsCurrent 验证「解析与互斥同源」：current 切换后，withSession 的
// 互斥落在新 runtime 的 gate 上——旧 gate 被占住不再阻塞命令，新 gate 被占住
// 则命令排队等待。
func TestGateFollowsCurrent(t *testing.T) {
	p := newGatedPlugin()
	old := p.currentSession()
	if old == nil {
		t.Fatal("no current runtime after construction")
	}

	// 模拟 /new 的换轨：current 切到新 runtime（id 取不同值以便 Register 成功，
	// gate 跟随 current 的语义与 id 无关）
	fresh := runtimeagent.NewSessionRuntime("current-2", nil)
	if err := p.manager.Register(fresh); err != nil {
		t.Fatalf("register fresh runtime: %v", err)
	}
	p.current.Store(fresh)

	// 1) 旧 runtime 的 gate 被占住：withSession 不受它阻塞（互斥已随 current 迁移）
	releaseOld, err := old.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("hold old gate: %v", err)
	}
	defer releaseOld()

	first := make(chan error, 1)
	go func() {
		first <- p.withSession(context.Background(), func(context.Context) error { return nil })
	}()
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("withSession should not serialize on the superseded runtime: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("withSession blocked on the old runtime's gate")
	}

	// 2) 新 runtime 的 gate 被占住：withSession 排队等待，release 后进入
	releaseFresh, err := fresh.AcquireRun(context.Background())
	if err != nil {
		t.Fatalf("hold fresh gate: %v", err)
	}
	second := make(chan error, 1)
	go func() {
		second <- p.withSession(context.Background(), func(context.Context) error { return nil })
	}()
	select {
	case err := <-second:
		t.Fatalf("withSession entered while fresh gate held: %v", err)
	case <-time.After(50 * time.Millisecond):
		// 预期：仍阻塞在新 runtime 的 gate 上
	}
	releaseFresh()
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("withSession after fresh release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("withSession did not enter after fresh gate release")
	}
}

// ---------------------------------------------------------------------------
// P5-1:WithRunContext 注入 sessionGateKey 重入标记 —— 死锁回归的集成证明
// (总方案 §6 风险 2:P5-6/P5-7 的 StartRun 接线以本语义为顺序硬前置)。
// ---------------------------------------------------------------------------

// toolCallProvider 第 1 轮返回对指定工具的调用、第 2 轮返回最终文本:驱动
// 「后台 Run → 模型发起工具调用 → 工具 Execute → 嵌套 Eval」的完整链路。
// ChatStream 恒报错(回退非流式,与 gatedProvider 同型)。
type toolCallProvider struct {
	toolName string
	mu       sync.Mutex
	calls    int
}

func (p *toolCallProvider) Name() string  { return "toolcall" }
func (p *toolCallProvider) Model() string { return "m" }

func (p *toolCallProvider) Chat(_ context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.mu.Lock()
	i := p.calls
	p.calls++
	p.mu.Unlock()
	if i == 0 {
		return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{
			Role: "assistant",
			ToolCalls: []core.ToolCall{{
				ID:       "call-1",
				Type:     "function",
				Function: core.ToolCallFunction{Name: p.toolName, Arguments: `{}`},
			}},
		}}}}, nil
	}
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "nested eval done"}}}}, nil
}

func (p *toolCallProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// TestStartRunCarriesGateReentryMarker(P5-1):NewPlugin 装配的 manager 携带
// sessionGateKey 重入标记 → StartRun 后台 Run 的 execute 持有会话 gate → run 内
// hub 工具 Execute(collectTools 的 p.hub.Eval 路径,tools.go:34-42)嵌套
// Eval("/usage") → withSession 判 owner == p 直接放行 → 命令正常返回、run 正常
// 完成。若无标记,嵌套 withSession 会对同一 runtime gate 二次 AcquireRun——永久
// 死锁,直到 ai-core 的 toolCtx 60s 超时才把错误塞回工具结果;测试以 10s Wait
// 上界先行失败,回归显式暴露。
//
// hub 装配说明(plan「真实 hub 装配(newAgentPlugin 形态)」的落地形态):完整
// Engine 装配内嵌生产 provider 工厂(acpworker.ProviderFactory),与挂起式 stub
// provider 编排冲突——按 plan 预留的调整空间,采用 newAgentPlugin 形态 + 真实
// 命令注册面:经真实 Init(hub) 注册全部命令(hub.RegisterCommand 收录真实
// handler),hub.Eval 按 Engine.Eval 同型规则(engine.go:264-285:解析 → 查表 →
// 填 ctx.Args → Handler)分发。嵌套链与生产逐环一致:
// 工具 Execute → hub.Eval("/usage") → handleUsage → withSession(标记放行)。
func TestStartRunCarriesGateReentryMarker(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Session.Enabled = false // 纯内存,不碰真实会话目录
	cfg.Memory.Enabled = false

	commands := map[string]model.Command{}
	var hubTools []model.Tool
	hub := &model.Hub{
		Config:             cfg,
		RegisterCommand:    func(cmd model.Command) error { commands[cmd.Name] = cmd; return nil },
		RegisterTool:       func(tool model.Tool) error { hubTools = append(hubTools, tool); return nil },
		Tools:              func() []model.Tool { return append([]model.Tool(nil), hubTools...) },
		SetFallbackHandler: func(func(*model.Context) error) {},
		RegisterCapability: func(string, any) error { return nil },
		// Engine.Eval 同型分发(engine.go:264-285)。
		Eval: func(ctx *model.Context, input string) error {
			parts := strings.Fields(input)
			if len(parts) == 0 {
				return fmt.Errorf("empty input")
			}
			cmd, ok := commands[parts[0]]
			if !ok {
				return fmt.Errorf("unknown command: %q", parts[0])
			}
			ctx.Args = parts[1:]
			return cmd.Handler(ctx)
		},
	}

	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Init 的 startSession 用生产 provider 工厂(acpworker.ProviderFactory)建了
	// "current" runtime;StartRun 经 manager.Get 按 id 寻址,同 id 重建会撞旧条目
	// ——换桩 provider 后以独立 id 注册 stub 会话并接管 current(refreshSession
	// 同款形态:Register 进 manager + current.Store)。
	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) {
		return &toolCallProvider{toolName: "usage"}, nil
	}
	rt := runtimeagent.NewSessionRuntime("gate-marker", runtimeagent.NewSession(p.deps))
	if err := p.manager.Register(rt); err != nil {
		t.Fatalf("register stub runtime: %v", err)
	}
	p.current.Store(rt)

	// 注册与 /usage 命令同名的 hub 工具:collectTools(tools.go:27-43)把它包成
	// agent 工具,Execute → p.hub.Eval("/usage") —— 与生产嵌套路径逐字一致。
	if err := hub.RegisterTool(model.Tool{Name: "usage", Description: "show session usage"}); err != nil {
		t.Fatalf("register hub tool: %v", err)
	}

	h, err := p.manager.StartRun(rt.ID, runtimeagent.RunRequest{Input: "q"}, runtimeagent.RunCallbacks{
		Write: func(string) {},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// 核心断言①:run 在上界内到达终态(不死锁)。标记缺失时嵌套 withSession 对
	// 同一 gate 二次 AcquireRun → 永久阻塞至 toolCtx 60s 超时,10s 上界必先到。
	done := make(chan struct{})
	var res runtimeagent.RunResult
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, _ = h.Wait(ctx)
	}()
	select {
	case <-done:
	case <-time.After(12 * time.Second):
		t.Fatal(`run did not finish in 12s — nested hub.Eval("/usage") deadlocked on the session gate (sessionGateKey marker missing?)`)
	}
	if res.State != runtimeagent.RunCompleted {
		t.Fatalf("run state = %s (err=%v), want RunCompleted", res.State, res.Err)
	}

	// 核心断言②:嵌套 Eval 正常返回——工具结果(hub.Eval 的 Writer 输出)进入
	// conversation 的 tool 消息,内容是 /usage 的真实渲染(usage 记账在第 1 轮
	// AfterModel 已落账,handleUsage 写出 "usage: N model calls")。
	var toolResult string
	for _, msg := range p.boundSession().Conversation() {
		if msg.Role == "tool" {
			toolResult = msg.Content
		}
	}
	if toolResult == "" || strings.HasPrefix(toolResult, "error:") {
		t.Fatalf("tool result = %q, want a successful /usage output(嵌套 Eval 未正常返回)", toolResult)
	}
	if !strings.Contains(toolResult, "usage:") {
		t.Fatalf("tool result = %q, want /usage rendering (usage: ...)", toolResult)
	}
}

// ---------------------------------------------------------------------------
// P5-5:/new 收口 —— 旧 runtime 注销出 manager + 旧 store Close(遗留①)。
// ---------------------------------------------------------------------------

// TestNewRemovesOldRuntimeAndClosesStore(P5-5,持久化启用):真实 legacy 挂载 +
// handleNew → ① manager.Get(旧 id) == nil(注册表不再随 /new 累积旧 runtime);
// ② 旧 store 已 Close——判据用「写入必须失败」:handleNew 里 Archive 会重开旧
// store 的句柄(store.go Archive),若收口漏了 Close,Commit 必然成功、断言即
// 失败;已关闭实例 file/lock 均为 nil,Commit 走 appendRecord 对 nil file 的
// Write → 错误返回。再补一条 Close 幂等断言(对已关闭实例再 Close 仍 nil)。
// ③ 新会话照常:current 换轨、新 runtime 在册、新 store 独立可写。
func TestNewRemovesOldRuntimeAndClosesStore(t *testing.T) {
	root := t.TempDir()
	oldID := seedLegacySession(t, root, "旧会话的问题")

	cfg := &runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: root}}
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	p.startSession() // 首周期:root 下有 legacy 槽 → 原地挂载(维持旧版启动行为)
	t.Cleanup(func() { _ = p.Stop() })

	oldRT := p.currentSession()
	if oldRT == nil {
		t.Fatal("no current runtime after startSession")
	}
	if oldRT.ID != oldID {
		t.Fatalf("current runtime id = %q, want mounted legacy id %q", oldRT.ID, oldID)
	}
	oldStore := p.currentStore()
	if oldStore == nil {
		t.Fatal("no current store after startSession (legacy mount failed)")
	}

	ctx, buf := newContext()
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}
	if !strings.Contains(buf.String(), "New session started") {
		t.Errorf("handleNew output = %q, want success message", buf.String())
	}

	// ① 旧 runtime 注销出 manager:注册表不再累积(勘察 K:此前 daemon 对
	// manager.Remove 零调用,旧 runtime 随 /new 无界累积)。
	if got := p.manager.Get(oldID); got != nil {
		t.Fatalf("manager.Get(%q) = %v, want nil after /new (P5-5 removed the old runtime)", oldID, got)
	}

	// ② 旧 store 已 Close:先验证「写入失败」(可靠判据),再验证 Close 幂等。
	if _, err := oldStore.Commit([]core.Message{{Role: "user", Content: "post-close", MsgID: session.NewMsgID()}}); err == nil {
		t.Fatal("Commit into old store succeeded — /new did not close the old store")
	}
	if err := oldStore.Close(); err != nil {
		t.Fatalf("second Close on old store = %v, want nil (Close 幂等, store.go:177-199)", err)
	}

	// ③ 新会话照常:current 换轨到新 runtime(在册),新 store 独立可写。
	fresh := p.currentSession()
	if fresh == nil || fresh == oldRT {
		t.Fatal("current did not move to a new runtime after /new")
	}
	if got := p.manager.Get(fresh.ID); got != fresh {
		t.Fatalf("manager.Get(%q) = %v, want the fresh runtime registered", fresh.ID, got)
	}
	freshStore := p.currentStore()
	if freshStore == nil || freshStore == oldStore {
		t.Fatal("current store did not move to a new instance after /new")
	}
	if freshStore.SessionID() == oldID {
		t.Fatalf("fresh store id %q equals old id", freshStore.SessionID())
	}
	if _, err := freshStore.Commit([]core.Message{{Role: "user", Content: "新会话的问题", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit into fresh store: %v", err)
	}
}
