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
