package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
)

// binding_test.go 是 PR2(统一 Session 路由,总方案 §六/§十)的插件侧测试:
// AgentPlugin 的 binding 形态解析点(sessionRuntimeIn/boundSessionIn/currentStoreIn)
// 的路由语义——Binding 优先(命中非 current 的已注册 runtime,CR4)、空 Binding
// 回落 current(无绑定调用方默认)、悬空 Binding(/new 收口摘除的旧会话)诚实
// 失败;以及 /new 收口「只摘发起方的 binding」。

// TestBindingRoutesPromptToBoundSession:current 已是 B 时,Binding→A 的连接
// prompt(handleAgent)路由到 A 的 runtime/store,B 的会话不受影响。
func TestBindingRoutesPromptToBoundSession(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "echo-A"})
	sidA := p.CurrentSessionID()
	sidB := newPersistedSession(t, p, fixedProvider{content: "echo-B"})
	if sidA == sidB {
		t.Fatal("fixture produced identical session ids")
	}

	// 连接级 Binding 指向 A(非 current)。
	b := &agentcontrol.SessionBinding{}
	b.Set(sidA)
	ctx := agentcontrol.WithBinding(context.Background(), b)

	// 解析点:binding 优先命中 A 的 runtime(注册表权威,CR4),不是 current(B)。
	rt, err := p.sessionRuntimeIn(ctx)
	if err != nil {
		t.Fatalf("sessionRuntimeIn(binding→A): %v", err)
	}
	if rt != p.manager.Get(sidA) {
		t.Fatal("sessionRuntimeIn(binding→A) must resolve session A's runtime")
	}
	if rt == p.currentSession() {
		t.Fatal("sessionRuntimeIn(binding→A) must not fall back to current (B)")
	}

	// 取数点:storeFor 按被解析 runtime 取 A 自己的 store(repo 实例唯一缓存)。
	stA := p.currentStoreIn(ctx)
	if stA == nil || stA.SessionID() != sidA {
		t.Fatalf("currentStoreIn(binding→A) = %v, want session A's store", stA)
	}

	// prompt(handleAgent)落到 A:会话本体收到 user 消息,B 无消息。
	mctx, _ := newContext("hello-from-bound")
	mctx.Ctx = ctx
	if err := p.handleAgent(mctx); err != nil {
		t.Fatalf("handleAgent(binding→A): %v", err)
	}
	convA := p.manager.Get(sidA).Session().Conversation()
	if len(convA) == 0 || convA[0].Role != "user" || convA[0].Content != "hello-from-bound" {
		t.Fatalf("session A conversation = %v, want the bound prompt", convA)
	}
	if convB := p.currentSession().Session().Conversation(); len(convB) != 0 {
		t.Fatalf("session B conversation = %v, want empty (prompt must not leak to current)", convB)
	}
}

// TestBindingDanglingSessionFailsHonest:Binding 指向已被收口(注销出 manager)
// 的会话 → 解析点显式错误(ErrSessionNotFound),handleAgent 诚实失败,绝不
// 静默回落 current(那会把请求串到别的会话的工作区)。
func TestBindingDanglingSessionFailsHonest(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "x"})
	sidA := p.CurrentSessionID()
	sidB := newPersistedSession(t, p, fixedProvider{content: "x"})

	// 模拟 /new 收口对旧 id 的注销(P5-5:manager.Remove)。
	p.manager.Remove(sidA)

	b := &agentcontrol.SessionBinding{}
	b.Set(sidA)
	ctx := agentcontrol.WithBinding(context.Background(), b)

	rt, err := p.sessionRuntimeIn(ctx)
	if rt != nil || err == nil {
		t.Fatalf("sessionRuntimeIn(dangling) = %v, %v; want nil, ErrSessionNotFound", rt, err)
	}
	if !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("sessionRuntimeIn(dangling) err = %v, want wrapping ErrSessionNotFound", err)
	}

	mctx, _ := newContext("hi")
	mctx.Ctx = ctx
	err = p.handleAgent(mctx)
	if err == nil || !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("handleAgent(dangling binding) = %v, want honest session-not-found", err)
	}

	// 空 Binding 的连接与无 Binding 调用方不受牵连:仍回落 current 语义(B)。
	rtEmpty, errEmpty := p.sessionRuntimeIn(agentcontrol.WithBinding(context.Background(), &agentcontrol.SessionBinding{}))
	if errEmpty != nil || rtEmpty != p.currentSession() {
		t.Fatalf("sessionRuntimeIn(empty binding) = %v, %v; want current", rtEmpty, errEmpty)
	}
	rtNoBinding, errNoBinding := p.sessionRuntimeIn(context.Background())
	if errNoBinding != nil || rtNoBinding == nil {
		t.Fatal("sessionRuntimeIn(no binding) = nil, want current runtime")
	}
	if p.CurrentSessionID() != sidB {
		t.Fatalf("CurrentSessionID = %q, want %q (fallback intact)", p.CurrentSessionID(), sidB)
	}
}

// TestHandleNewDetachesOnlyInitiatorBinding:两条连接的 Binding 同指 A,连接 1
// 发起 /new → TEARDOWN 语义不变(归档/换轨/收口),但只摘发起方 b1;连接 2 的
// b2 原样保留,此后指向被收口会话、解析诚实失败;b1 回落 current 落到新会话。
func TestHandleNewDetachesOnlyInitiatorBinding(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "x"})
	sidA := p.CurrentSessionID()

	b1 := &agentcontrol.SessionBinding{}
	b1.Set(sidA) // 连接 1(发起方)
	b2 := &agentcontrol.SessionBinding{}
	b2.Set(sidA) // 连接 2(旁观者,Binding 连接私有,§6.2 第 4 条)

	mctx, _ := newContext()
	mctx.Ctx = agentcontrol.WithBinding(context.Background(), b1)
	if err := p.handleNew(mctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}

	// TEARDOWN 语义不变:current 换到新会话,A 的 runtime 已收口注销(P5-5)。
	sidNew := p.CurrentSessionID()
	if sidNew == sidA || sidNew == "" {
		t.Fatalf("CurrentSessionID after /new = %q, want a fresh session id", sidNew)
	}
	if p.manager.Get(sidA) != nil {
		t.Fatal("session A runtime still registered after /new teardown")
	}

	// 只摘发起方:发起方 Binding 清空(回落 current 语义,落到新会话);连接 2
	// 的 Binding 原样保留。
	if got := b1.Get(); got != "" {
		t.Fatalf("initiator binding = %q, want empty (detached)", got)
	}
	if got := b2.Get(); got != sidA {
		t.Fatalf("observer binding = %q, want %q (untouched)", got, sidA)
	}

	// 发起方的下一次解析回落 current → 新会话。
	rt1, err := p.sessionRuntimeIn(agentcontrol.WithBinding(context.Background(), b1))
	if err != nil || rt1 != p.currentSession() {
		t.Fatalf("initiator re-resolve = %v, %v; want current runtime", rt1, err)
	}

	// 旁观者指向被收口的会话 → 诚实失败(不静默串到新会话)。
	rt2, err := p.sessionRuntimeIn(agentcontrol.WithBinding(context.Background(), b2))
	if rt2 != nil || !errors.Is(err, runtimeagent.ErrSessionNotFound) {
		t.Fatalf("observer re-resolve = %v, %v; want honest session-not-found", rt2, err)
	}

	// 无 Binding(CLI)的 /new 不受牵连:照常作用于 current。
	mctx2, buf := newContext()
	if err := p.handleNew(mctx2); err != nil {
		t.Fatalf("handleNew(no binding): %v", err)
	}
	if !strings.Contains(buf.String(), "New session started") {
		t.Fatalf("CLI /new output = %q", buf.String())
	}
	if got := p.CurrentSessionID(); got == sidNew {
		t.Fatalf("CLI /new did not advance current (%q)", got)
	}
}
