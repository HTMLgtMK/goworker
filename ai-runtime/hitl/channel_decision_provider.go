package hitl

import (
	"context"
	"time"
)

func NewChannelDecisionProvider(decisions <-chan Decision) DecisionProvider {
	return &ChannelDecisionProvider{decisions: decisions}
}

func (p *ChannelDecisionProvider) GetDecision(ctx context.Context, req *InterruptRequest) Decision {
	// 超时窗口以请求的 ExpiresAt 为唯一事实来源（从创建时刻算起，涵盖 token 发送、
	// 前端渲染、用户确认全链路）；ExpiresAt 缺失时兜底用 DefaultTimeout。
	var timeout *time.Timer
	switch {
	case req.ExpiresAt.IsZero():
		timeout = time.NewTimer(DefaultTimeout)
	case time.Until(req.ExpiresAt) <= 0:
		// 已过期：不再等，按宪法直接拒绝。迟到决策本来就无效——前端已按这个时间关闭。
		return Decision{InterruptID: req.ID, Type: DecisionReject}
	default:
		timeout = time.NewTimer(time.Until(req.ExpiresAt))
	}
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		return Decision{InterruptID: req.ID, Type: DecisionReject}
	case <-timeout.C:
		return Decision{InterruptID: req.ID, Type: DecisionReject}
	case d, ok := <-p.decisions:
		if !ok {
			return Decision{InterruptID: req.ID, Type: DecisionReject}
		}
		return d
	}
}
