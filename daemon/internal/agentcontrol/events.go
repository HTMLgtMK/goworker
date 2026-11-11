package agentcontrol

import (
	"context"
	"fmt"

	"github.com/tinguo/goworker/ai-runtime/session"
)

// Event subscription and lease acquisition are separate operations. ACP coordinates
// both for control attaches, so a failed subscription cannot mutate the lease.

// EventSubscription exposes the read-only event stream and its cancellation handle.
// Consumers observe closure when the context, subscriber, or hub terminates.
type EventSubscription interface {
	Events() <-chan session.SessionEvent
	Close() error
}

// RunSnapshot 是活动 Run 的扁平快照(AttachResult.CurrentRun 的形状):
// 取 AgentRun 按 wire 需要的最小面(RunID/State 字符串),不透传 runtime 句柄
// ——与 PendingApprovalInfo 的「扁平化不泄漏 agent 包结构」纪律同型(方向相反:
// 那处是 agent 结构不进 model,这处是 runtime 观测面收敛为订阅契约的一部分)。
type RunSnapshot struct {
	RunID string
	State string
}

// AttachResult captures the event cursor, replay integrity, active run, and subscription
// observed for one routed session. A nil CurrentRun means no active run.
type AttachResult struct {
	LatestSeq       uint64
	ReplayIntegrity string
	CurrentRun      *RunSnapshot
	Subscription    EventSubscription
}

// hubSubscription adapts the SDK hub channel and cancellation function to
// EventSubscription.
type hubSubscription struct {
	ch     <-chan session.SessionEvent
	cancel context.CancelFunc
}

// 编译期断言:hub 通道适配满足订阅契约(签名漂移在编译期暴露,EventJournal
// 断言先例 event_hub.go:76)。
var _ EventSubscription = (*hubSubscription)(nil)

// Events 返回订阅的只读投递通道(hub.Subscribe 的返回原样转交)。
func (s *hubSubscription) Events() <-chan session.SessionEvent { return s.ch }

// Close 取消底层 SDK 订阅:hub 泵退出并关闭投递通道,订阅自注册表摘除
// (event_hub.go pumpEvents 的 defer 路径)。幂等(context.CancelFunc 幂等),
// 恒返回 nil(cancel 无错误面;hub 侧断开经通道关闭可观测,不经本方法返回)。
func (s *hubSubscription) Close() error {
	s.cancel()
	return nil
}

// SubscribeEvents creates a session event subscription independent of the request context.
// It routes the session, initializes the hub, subscribes after afterSeq, and snapshots state.
func (s *Service) SubscribeEvents(sessionID string, afterSeq uint64) (AttachResult, error) {
	rt := s.route(sessionID)
	if rt == nil {
		return AttachResult{}, sessionNotFoundErr(sessionID)
	}
	hub, err := rt.EnsureHub()
	if err != nil {
		return AttachResult{}, fmt.Errorf("agent: session %q: ensure event hub: %w", sessionID, err)
	}
	if hub == nil {
		return AttachResult{}, fmt.Errorf("agent: event stream not enabled for session %q (persistence disabled)", sessionID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := hub.Subscribe(ctx, afterSeq)
	if err != nil {
		cancel()
		return AttachResult{}, err
	}
	res := AttachResult{
		LatestSeq:       hub.LatestSeq(),
		ReplayIntegrity: hub.ReplayIntegrity(),
		Subscription:    &hubSubscription{ch: ch, cancel: cancel},
	}
	if run := rt.Active(); run != nil {
		res.CurrentRun = &RunSnapshot{RunID: run.ID, State: string(run.State())}
	}
	return res, nil
}
