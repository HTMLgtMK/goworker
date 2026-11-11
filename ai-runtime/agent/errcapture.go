package agent

import (
	"sync"

	"github.com/tinguo/goworker/ai-core/core"
)

// runErrorCapture 捕获 ai-core 通过 AfterAgentEvent.Err 报告的本轮运行期错误。
// Session.Run 在收到 msgCh 后读取快照；该通道收发建立 happens-before，保证错误可见。
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
