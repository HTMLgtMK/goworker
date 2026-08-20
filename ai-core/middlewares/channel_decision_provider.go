package middlewares

import (
	"context"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-core/spec"
)

func NewChannelDecisionProvider(decisions <-chan spec.HITLDecision) core.DecisionProvider {
	return &ChannelDecisionProvider{decisions: decisions}
}

func (p *ChannelDecisionProvider) GetDecision(ctx context.Context, req *spec.InterruptRequest) spec.HITLDecision {
	// 超时窗口以请求的 ExpiresAt 为唯一事实来源（从创建时刻算起，涵盖 token 发送、
	// 前端渲染、用户确认全链路）；ExpiresAt 缺失时兜底用 spec.DefaultHITLTimeout。
	var timeout *time.Timer
	switch {
	case req.ExpiresAt.IsZero():
		timeout = time.NewTimer(spec.DefaultHITLTimeout)
	case time.Until(req.ExpiresAt) <= 0:
		// 已过期：不再等，按宪法直接拒绝。迟到决策本来就无效——前端已按这个时间关闭。
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}
	default:
		timeout = time.NewTimer(time.Until(req.ExpiresAt))
	}
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}
	case <-timeout.C:
		return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}
	case d, ok := <-p.decisions:
		if !ok {
			return spec.HITLDecision{InterruptID: req.ID, Type: spec.DecisionReject}
		}
		return d
	}
}
