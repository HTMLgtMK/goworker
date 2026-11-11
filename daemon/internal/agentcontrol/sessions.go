package agentcontrol

import (
	"context"
	"fmt"
	"sync"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// sessions.go 是会话寻址与会话级资源的窄接口:Service 不持有「当前会话」选择
// 状态(会话选择属于调用入口,不存在 Daemon 全局事实源),也不直接持有随会话
// 边界重建的 journal 资源——两者经宿主注入的窄接口解析,接口定义放在
// agentcontrol(而不是让 Service 反向依赖 plugin)。

// SessionBinding is one connection's session selector, not a control lease or event
// subscription. Standard ACP requests use it, while extension methods name a session; an
// invalid non-empty binding is an error. Lifecycle reference counts live in BindingRegistry.
type SessionBinding struct {
	mu        sync.RWMutex
	sessionID string
}

// Set 记录本连接选择的会话(runtime id)。空串 = 清除选择(/new 收口摘发起方
// Binding 的形态),此后 Get 返回空、解析回落 current 语义。线程安全:请求
// goroutine(声明)与 prompt 执行链(解析)并发访问。
func (b *SessionBinding) Set(id string) {
	b.mu.Lock()
	b.sessionID = id
	b.mu.Unlock()
}

// Get 返回本连接当前选择的会话 id;空 = 未选择/已清除(回落 current 语义)。
func (b *SessionBinding) Get() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.sessionID
}

// bindingContextKey 是 SessionBinding 的 ctx 载体键(私有):Transport Adapter
// 把本连接的 Binding 挂上请求 ctx,经 Engine.Eval 直通命令 handler(ctx.Ctx
// 原样传递,engine.go Eval),插件的 binding 形态解析点据此取回。这是
// 「标准 ACP 会话解析优先读 Binding」的唯一通道——命令链本身没有会话参数。
type bindingContextKey struct{}

// WithBinding 把连接的 SessionBinding 挂上请求 ctx。b 为 nil 时不包装(等价
// 无绑定调用方,回落 current 语义)。
func WithBinding(ctx context.Context, b *SessionBinding) context.Context {
	if b == nil {
		return ctx
	}
	return context.WithValue(ctx, bindingContextKey{}, b)
}

// BindingFromCtx 取回请求携带的连接级 SessionBinding;nil = 无绑定调用方
// (CLI 命令、stdio、测试、后台 Run 的嵌套命令),消费方回落 current 语义。
func BindingFromCtx(ctx context.Context) *SessionBinding {
	b, _ := ctx.Value(bindingContextKey{}).(*SessionBinding)
	return b
}

// clientIDContextKey carries the connection ID alongside SessionBinding. It is consumed by
// /new cleanup to unbind the connection from BindingRegistry: SessionBinding has no client
// identity, while that ledger is keyed by connection ID.
type clientIDContextKey struct{}

// WithClientID 把连接句柄挂上请求 ctx。clientID 为空串时不包装(等价无连接身份
// 的调用方,CLI/stdio/测试)。
func WithClientID(ctx context.Context, clientID string) context.Context {
	if clientID == "" {
		return ctx
	}
	return context.WithValue(ctx, clientIDContextKey{}, clientID)
}

// ClientIDFromCtx 取回请求携带的连接句柄;空 = 无连接身份的调用方(CLI 命令、
// stdio、测试),/new 收口据此跳过绑定登记摘除(无登记可摘)。
func ClientIDFromCtx(ctx context.Context) string {
	id, _ := ctx.Value(clientIDContextKey{}).(string)
	return id
}

// SessionRouter resolves a caller's session ID to its authoritative runtime.
// The host checks the current runtime first, then the registered-session map;
// unresolved IDs return nil rather than silently falling back to current.
type SessionRouter interface {
	RouteSession(sessionID string) *runtimeagent.SessionRuntime
}

// RunStatusJournal exposes the read-only journal queries used by RunStatus.
// Implementations must report the routed session's journal, not the current session's.
type RunStatusJournal interface {
	LastRunState(runID string) (session.RunStateRecord, bool)
	SeqRange() (earliest, latest uint64, ok bool, err error)
}

// JournalSource resolves the journal belonging to the routed runtime.
// Return a true nil when no journal exists; RunStatus then uses its in-memory fallback.
type JournalSource interface {
	RunJournal(rt *runtimeagent.SessionRuntime) RunStatusJournal
}

// sessionNotFoundErr 是「sessionID 未定位到 runtime」的统一错误(原 plugin
// runcontrol.go 同名 helper 迁入):包裹 manager 的 ErrSessionNotFound sentinel
// (errors.Is 可判,协议面映射 -32000)。覆盖三种形态:任意未注册 id、/new 已
// 收口的旧 id(诚实失败)、无活动 runtime(Init 未完成/失败回滚)。
func sessionNotFoundErr(sessionID string) error {
	return fmt.Errorf("agent: session %q: %w", sessionID, runtimeagent.ErrSessionNotFound)
}

// route 经宿主统一解析点寻址(NewService 的 router 不可为 nil;解析语义见
// SessionRouter)。
func (s *Service) route(sessionID string) *runtimeagent.SessionRuntime {
	return s.router.RouteSession(sessionID)
}
