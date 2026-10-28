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

	// 陈旧决策排空（P4 评审 issue1，确定性时序缺陷）：decisions 是本 run 共用
	// 的容量 1 缓冲（agent/session.go:149），上一轮 interrupt 的迟到决策会在
	// 其中残留——后台路径 decideFunc 的 ApprovalManager.Wait 与本函数对同一
	// ExpiresAt 各起计时器，本函数超时返回后 Wait 的超时 reject 才入缓冲、
	// 无人消费；sync 路径调用方 Decide 在窗口关闭后的迟到响应同理。不排空的话，
	// 同 run 下一个 interrupt 的 select 立即吃到上一轮的决策：第二次审批用户
	// 还没看到就被自动拒绝并级联（每次超时毒化下一个 interrupt）。因此循环
	// 逐条核对 InterruptID：仅消费匹配本请求的决策，不匹配的丢弃后继续等
	// （ctx/超时/新决策），绝不把上一轮的决策算到本轮头上。循环有界：每轮
	// select 要么返回、要么消费缓冲一条，投递方每 interrupt 至多投递一次
	// （agent/session.go:280-283），排空后即回到阻塞等待。
	for {
		select {
		case <-ctx.Done():
			return Decision{InterruptID: req.ID, Type: DecisionReject}
		case <-timeout.C:
			return Decision{InterruptID: req.ID, Type: DecisionReject}
		case d, ok := <-p.decisions:
			if !ok {
				return Decision{InterruptID: req.ID, Type: DecisionReject}
			}
			if d.InterruptID != req.ID {
				// 上一轮 interrupt 的陈旧决策（迟到的 reject/approve）：排空丢弃。
				continue
			}
			return d
		}
	}
}
