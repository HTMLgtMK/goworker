package agentcontrol

import (
	"context"
	"fmt"
	"sync"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// sessions.go 是会话寻址与会话级资源的窄接口(总方案 §三/§六):Service 不持有
// 「当前会话」选择状态(会话选择属于调用入口,不存在 Daemon 全局事实源),也
// 不直接持有随会话边界重建的 journal 资源——两者经宿主注入的窄接口解析,接口
// 定义按方案要求放在 agentcontrol(而不是让 Service 反向依赖 plugin)。

// SessionBinding 是连接级会话选择器(总方案 §6.1,PR2):由每条客户端连接
// (ACP FrontendHandler;CLI 上下文同型)持有一个实例,记录「本连接当前选择的
// 会话」。标准 ACP 方法(session/new|load|prompt)使用当前连接的 Binding;
// x-goworker/* 扩展方法显式携带 sessionId、不经 Binding(CR4 的多会话路由不
// 变);无 Binding 的调用方(CLI 命令、stdio、测试)回落 current 语义——插件
// 的 current atomic 保留为「无 Binding 调用方的默认」。
//
// 它不是 Session、不是 Run、也不是控制权(总方案 §6.2)——只是一个纯粹的会话
// 选择器,不持有任何会话状态。对 Binding 的重新选择(Set)只修改本连接自己的
// 选择状态,四条纪律(§6.2 的四条「不应该」逐条落注释,评审与使用方据此对照):
//
//  1. 不删除被放弃的会话——会话生命周期归 SessionManager 与宿主命令(/new 的
//     TEARDOWN),重选/清除 Binding 绝不触发删除;
//  2. 不取消该会话的后台 Run——Run 生命周期归 RunSupervisor,与连接解耦
//     (P2P3 §6:断连不取消;重选同样不取消);
//  3. 不自动获得新选择会话的 control lease——控制权经 attach(mode=control)
//     显式获取(§七:会话选择、事件订阅、控制权获取是三个独立操作);
//  4. 不影响其他连接的 Binding——Binding 连接私有,不存在全局 Binding 注册表,
//     Service 与插件都不持有跨连接的 Binding 事实源(§二不变量 6)。
//
// 不变量(PR2 收口):Binding 指向的会话必须是已注册 runtime。写入点
// (session/new|load-current 的声明成功后)只 Set 统一解析点可路由的 runtime
// id(store 的稳定 SessionID;纯内存降级无稳定 id,Binding 保持空 = 回落
// current 语义)。/new 收口摘除旧 runtime 后,仍指向它的 Binding 解析为 nil,
// 由消费方诚实失败(不静默回落 current——那会把请求串到别的会话的工作区)。
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

// SessionRouter 是会话寻址的窄接口(宿主注入):把调用方寻址的 sessionID 解析为
// 权威 runtime。plugin 侧实现 = 原 sessionRuntime 统一解析点原样迁移(current
// 优先 + manager.Get 注册表权威),解析序:
//  1. current 优先:sessionID 命中当前会话的身份(runtime id,含纯内存降级的
//     空串寻址形态)→ 取 current runtime——同 id 重建窗口(纯内存 /new、测试
//     fixture 的 refreshSession)下注册表可能仍挂旧条目,current 才是权威路由
//     目标。持久化模式下空串不匹配任何 store id → 落到第 2 步 → nil(诚实
//     失败,不把「未声明」静默当成 current)。
//  2. 注册表权威:覆盖非 current 的 runtime(/new 后保留在册的旧 runtime、
//     多会话注册);nil → 调用方按 ErrSessionNotFound 语义诚实失败。
type SessionRouter interface {
	RouteSession(sessionID string) *runtimeagent.SessionRuntime
}

// RunStatusJournal 是 RunStatus 的 journal 权威分支所需的只读查询最小面:
// LastRunState 读 runs/<runID>.jsonl 末条 state 记录,SeqRange 取磁盘游标范围。
// *session.RunJournalStore 天然满足;接口收敛在此而非直接引具体类型,是
// 「消费侧最小面」先例(agent.RunStateJournal/ConversationStore 同型),宿主的
// 只读实例构造细节(current 会话走 deps 单例、跨会话路由走 repo 只读实例)由
// JournalSource 承担,Service 不感知。
type RunStatusJournal interface {
	LastRunState(runID string) (session.RunStateRecord, bool)
	SeqRange() (earliest, latest uint64, ok bool, err error)
}

// JournalSource 是被路由会话的 journal 解析窄接口(宿主注入):返回 rt 自己会话
// 的 journal——journal 必须取被路由会话的事实源,不能拿当前会话的 journal 冒充。
// nil 返回 = 无 journal(纯内存/持久化未启用/仓库已收尾),RunStatus 的 journal
// 权威分支跳过、幂等表成为唯一进程内观测面。返回值必须是真 nil 接口或非 nil
// 实现,不允许 typed-nil(接口持 nil 指针会让 nil 判断失效,agent spec.go:96
// 的 Store/RunJournal 纪律同源)。
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
