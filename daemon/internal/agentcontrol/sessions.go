package agentcontrol

import (
	"fmt"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
)

// sessions.go 是会话寻址与会话级资源的窄接口(总方案 §三/§六):Service 不持有
// 「当前会话」选择状态(会话选择属于调用入口,不存在 Daemon 全局事实源),也
// 不直接持有随会话边界重建的 journal 资源——两者经宿主注入的窄接口解析,接口
// 定义按方案要求放在 agentcontrol(而不是让 Service 反向依赖 plugin)。

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
