package agentcontrol

import (
	"errors"
	"fmt"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// approval.go 是审批授权的直转面(总方案 §四,职责迁移表「Plugin.ResolveApproval
// → agentcontrol.Service 连同 Session 授权一起迁移」):Owner 查询 + 归属校验 +
// Resolve 提交从 plugin runcontrol.go 迁入,ApprovalManager 的访问经 Service 持有
// 的 runtimeagent.SessionManager(manager.ApprovalOwner/ResolveApproval 是 SDK 的
// 授权查询/提交入口,Service 不直接触碰 ApprovalManager 内部状态)。

// ErrApprovalOwnership 是审批越权 sentinel(第二轮评审 C1 修复,原 runcontrol.go
// 迁入):决策声明的 sessionID 与服务端记录的审批归属会话不一致。授权以服务端
// 记录为准、绝不信任客户端声明——归属在 SDK 审批管理面 Wait 注册时落账
// (SessionID+RunID 非连接),即便调用方真持有其他会话的 control lease 也不放行
// (跨会话审批越权是 C1 的攻击面:持有任意会话 lease 的连接曾可应答他会话的
// 审批)。
var ErrApprovalOwnership = errors.New("approval ownership mismatch: the approval belongs to a different session")

// ResolveApproval 投递一笔审批决策(C1 修复的归属三段校验 + 第三轮评审 I4
// fencing 的五步序,原 runcontrol.go 迁入;DE2-I1 后锁粒度=会话,推翻旧契约
// 「requestID 全局寻址、无需会话校验」——control lease 已按会话隔离(I3),审批
// 授权必须绑定目标会话,否则持任意会话 lease 的连接可应答他会话的审批):
//  1. 路由:RouteSession 按 sessionID 定位 runtime(与审批列表同一寻址模型:
//     内部比较用 rt.ID 而非调用方原串——审批的 SessionID 归属是 StartRun 传入
//     的 runtime id,纯内存降级下为 "current" 而调用方寻址空串),nil →
//     sessionNotFoundErr;
//  2. lease(fencing):本会话 lease 临界区内校验 clientID 持有该会话的租约,
//     未持有 → ErrControlLeaseLost(接管 Acquire 同锁,校验后到提交间不可能被
//     接管);
//  3. 归属:sessions.ApprovalOwner(requestID) 查服务端记录的真实归属,不存在
//     (从未注册/已过宽限窗被清扫)→ 包裹 ErrApprovalUnknown 的明确错误;
//  4. 越权:归属会话 != rt.ID → 包裹 ErrApprovalOwnership 的越权错误(「服务端
//     记录为准,不信客户端声明」);
//  5. 匹配 → 直转 sessions.ResolveApproval:首个有效决策获胜,迟到/重复 →
//     ErrApprovalResolved(sentinel 语义见 SDK approval.go),错误原样上抛由
//     协议面映射 -32000。②③④⑤ 同临界区:归属校验与提交之间无 TOCTOU 窗口。
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
