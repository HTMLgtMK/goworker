package agent

import (
	"sync"

	"github.com/tinguo/goworker/ai-core/core"
)

// runErrorCapture 捕获本轮 ReAct 的运行期错误(AC-1:provider 错误经流式出口
// 不再记 RunCompleted;ai-core 零改动的消费侧修正)。
//
// ai-core 的错误通道现状(ai-core/agent/agent.go):
//   - provider 错误 4 处入口(:82 chat 失败、:88 nil response、:93 空 choices、
//     :107 空 content)全部经 emitAgentNote(:309-312)转成 note token
//     (TokenTypeText + Done=true)并 append 进 messages 历史 —— 错误走流式出口;
//   - Agent.Run 的同步 error 返回恒 nil(:175),Session.Run 的 agent.Run 失败
//     分支对 provider stub 不可达(P2-1 实测,run_test.go 留痕);
//   - runErr 的唯一结构化出口是 agent goroutine defer 里的
//     AfterAgentEvent{Err}(:55)。
//
// 因此 capture 作为每 Run 一实例的中间件挂链尾(Session.buildMiddlewareChain),
// OnAfterAgent 把 ev.Err 记下,Session.Run 在 <-msgCh 之后经 captured() 读出并
// 映射终态(core.AfterAgent = Middleware + OnAfterAgent(*AfterAgentEvent)
// *MiddlewareResponse,ai-core/core/spec.go:286-289)。
//
// happens-before 论证(plan AC-1):capture 的写入发生在 agent goroutine 的
// defer 内 fireMiddlewareEvent(:54-58)——该调用先于同一 defer 的
// `msgCh <- messages`(:57);Session.Run 只在 `messages := <-msgCh` 返回之后读
// captured(),channel 收发建立 happens-before,锁不参与正确性。mu 仅为防御与
// -race 明示:写者唯一(agent goroutine,fireMiddlewareEvent 串行分发,
// :213-241),读者唯一(Session.Run)。次序依赖由 run_test.go 的 AC-1 断言钉死
// ——若未来重构把 msgCh 发送挪出 defer 或挪到 fire 之前,capture 将读不到
// 错误,provider 失败的 run 被静默记回 RunCompleted:Persisted==true 与终态
// 断言在该回归下失败报警(plan §6 风险 1)。
type runErrorCapture struct {
	mu  sync.Mutex
	err error
}

// Name 实现 core.Middleware(链内标识,与状态无关)。
func (c *runErrorCapture) Name() string { return "runErrorCapture" }

// OnAfterAgent 捕获本轮错误。每 Run 恰好触发一次(agent goroutine defer);
// ev.Err == nil(正常定稿/迭代耗尽)不写入 —— 迭代耗尽的 ⚠ 提示走 note 通道
// (agent.go:164-170),不改变终态语义。响应恒空:capture 只观察,不干预。
func (c *runErrorCapture) OnAfterAgent(ev *core.AfterAgentEvent) *core.MiddlewareResponse {
	if ev.Err != nil {
		c.mu.Lock()
		c.err = ev.Err
		c.mu.Unlock()
	}
	return &core.MiddlewareResponse{}
}

// captured 返回已捕获的错误快照(nil = 本轮无运行期错误)。调用点在
// Session.Run 的 <-msgCh 之后(可见性依据见类型注释的 happens-before 论证)。
func (c *runErrorCapture) captured() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}
