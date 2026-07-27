package middlewares

import (
	"context"
	"time"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// ChannelDecisionProvider 包装 channel 实现 core.DecisionProvider。
// 插件侧的 promptForDecision 将用户决策写入 channel，这里收割。
type ChannelDecisionProvider struct {
	decisions <-chan spec.HITLDecision
}

func NewChannelDecisionProvider(decisions <-chan spec.HITLDecision) core.DecisionProvider {
	return &ChannelDecisionProvider{decisions: decisions}
}

const hitlTimeout = 30 * time.Second

func (p *ChannelDecisionProvider) GetDecision(ctx context.Context, req *spec.InterruptRequest) spec.HITLDecision {
	timeout := time.NewTimer(hitlTimeout)
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
