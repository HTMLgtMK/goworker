package agentcontrol

import (
	"errors"
	"fmt"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// approval.go 是审批授权直转面。Service 经 SessionManager 查询 SDK 记录的审批归属并提交决策，
// 不直接触碰 ApprovalManager 内部状态。

// ErrApprovalOwnership 是审批越权 sentinel:决策声明的 sessionID 与服务端记录的
// 审批归属会话不一致。授权以服务端
// 记录为准、绝不信任客户端声明——归属在 SDK 审批管理面 Wait 注册时落账
// (SessionID+RunID 非连接),即便调用方真持有其他会话的 control lease 也不放行。
var ErrApprovalOwnership = errors.New("approval ownership mismatch: the approval belongs to a different session")

// ResolveApproval atomically fences approval delivery with the session lease and
// verifies the approval's server-recorded session owner before forwarding the decision.
func (s *Service) ResolveApproval(sessionID, clientID, requestID string, decision hitl.Decision) error {
	rt := s.route(sessionID)
	if rt == nil {
		return sessionNotFoundErr(sessionID)
	}
	return s.leases.WithControl(sessionID, clientID, func() error {
		owner, ok := s.sessions.ApprovalOwner(requestID)
		if !ok {
			return fmt.Errorf("agent: approval %q: %w", requestID, runtimeagent.ErrApprovalUnknown)
		}
		if owner != rt.ID {
			return fmt.Errorf("agent: approval %q belongs to session %q, not %q: %w", requestID, owner, rt.ID, ErrApprovalOwnership)
		}
		return s.sessions.ResolveApproval(requestID, decision)
	})
}
