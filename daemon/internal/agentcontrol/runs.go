package agentcontrol

import (
	"fmt"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
)

// 控制操作的路由、fencing 和幂等流程见 ../../docs/README.md。

// InternalRunner 是受信任的本地宿主入口，不执行连接 lease 校验。
// 接口分离仅限制静态方法集，不构成运行时授权边界；协议控制操作必须走 fencing。
type InternalRunner interface {
	StartRunSync(sessionID string, req runtimeagent.RunRequest, cb runtimeagent.RunCallbacks) (runtimeagent.RunHandle, error)
	CancelRunSync(sessionID, runID string) error
}

// InternalRunner 编译期断言:Service 是该面的唯一实现方,方法集或签名漂移即
// 编译失败。
var _ InternalRunner = (*Service)(nil)

// ensureRegistered restores the runtime registration before submission. It avoids
// reviving retired runtimes and replaces stale entries for a reused runtime ID.
func (s *Service) ensureRegistered(rt *runtimeagent.SessionRuntime) {
	if got := s.sessions.Get(rt.ID); got == rt {
		return
	}
	if rt.Retired() {
		return
	}
	s.sessions.Remove(rt.ID)
	_ = s.sessions.Register(rt)
}

// StartRun 通过会话 lease 提交后台 Run。非空 requestID 在会话内按输入去重；
// 重试输入不一致返回 ErrRequestConflict，只有首次提交的 cb 生效。
// 新提交返回接受时状态；幂等命中返回已有 Run 的当前状态。
func (s *Service) StartRun(sessionID, clientID, input, requestID string, cb runtimeagent.RunCallbacks) (string, string, error) {
	rt := s.route(sessionID)
	if rt == nil {
		return "", "", sessionNotFoundErr(sessionID)
	}
	var key RequestKey
	if requestID != "" {
		key = RequestKey{SessionID: rt.ID, RequestID: requestID}
	}

	var runID, state string
	err := s.leases.WithControl(sessionID, clientID, func() error {
		h, fresh, submitErr := s.requests.Submit(key, input, func() (runtimeagent.RunHandle, error) {
			s.ensureRegistered(rt)
			return s.sessions.StartRun(rt.ID, runtimeagent.RunRequest{Input: input}, cb)
		})
		if submitErr == nil {
			// Fresh submissions return the deterministic submit state; a fast provider may
			// finish before the response is encoded. Replayed idempotent submissions return
			// the live handle state because the caller asks for the existing run's status.
			if fresh {
				runID, state = h.RunID, string(h.SubmitState())
			} else {
				runID, state = h.RunID, string(h.State())
			}
		}
		return submitErr
	})
	if err != nil {
		return "", "", err
	}
	return runID, state, nil
}

// StartRunSync submits an internal synchronous run without connection fencing or
// idempotency. It routes the session, restores registration, and delegates all run
// admission, quotas, journaling, events, and finalization to SessionManager.
func (s *Service) StartRunSync(sessionID string, req runtimeagent.RunRequest, cb runtimeagent.RunCallbacks) (runtimeagent.RunHandle, error) {
	rt := s.route(sessionID)
	if rt == nil {
		return runtimeagent.RunHandle{}, sessionNotFoundErr(sessionID)
	}
	s.ensureRegistered(rt)
	return s.sessions.StartRun(rt.ID, req, cb)
}

// CancelRunSync cancels an internal synchronous run without connection fencing.
// A missing or already-finished run is reported by SessionManager unchanged.
func (s *Service) CancelRunSync(sessionID, runID string) error {
	rt := s.route(sessionID)
	if rt == nil {
		return sessionNotFoundErr(sessionID)
	}
	s.ensureRegistered(rt)
	return s.sessions.CancelRun(rt.ID, runID)
}

// CancelRun routes by session and atomically fences cancellation with the lease
// check, so a former holder cannot submit after takeover.
func (s *Service) CancelRun(sessionID, clientID, runID string) error {
	rt := s.route(sessionID)
	if rt == nil {
		return sessionNotFoundErr(sessionID)
	}
	return s.leases.WithControl(sessionID, clientID, func() error {
		s.ensureRegistered(rt)
		return s.sessions.CancelRun(rt.ID, runID)
	})
}

// RunStatus observes the routed session without requiring a control lease. It
// prefers the active run, then that session's journal, then the in-memory registry.
func (s *Service) RunStatus(sessionID, runID string) (string, uint64, string, error) {
	rt := s.route(sessionID)
	if rt == nil {
		return "", 0, runtimeagent.ReplayIntegrityComplete, sessionNotFoundErr(sessionID)
	}

	replayIntegrity := runtimeagent.ReplayIntegrityComplete
	if hub := rt.Hub(); hub != nil {
		replayIntegrity = hub.ReplayIntegrity()
	}

	// ① active 命中(在跑/排队)→ 实时快照。
	if run := rt.Active(); run != nil && run.ID == runID {
		latest := uint64(0)
		if hub := rt.Hub(); hub != nil {
			latest = hub.LatestSeq()
		}
		return string(run.State()), latest, replayIntegrity, nil
	}

	// ② journal 权威(被路由会话自己的事实源;存在即权威,miss 即未找到)。
	if s.journals != nil {
		if js := s.journals.RunJournal(rt); js != nil {
			rec, ok := js.LastRunState(runID)
			if !ok {
				return "", 0, replayIntegrity, fmt.Errorf("agent: run %q: %w", runID, runtimeagent.ErrRunNotFound)
			}
			_, latest, _, err := js.SeqRange()
			if err != nil {
				// 扫描失败显式上抛,不把读取失败静默降级为 latest=0(游标是
				// 「仅描述」,描述不可信时诚实报错优于编造一个看似合法的 0)。
				return "", 0, replayIntegrity, fmt.Errorf("agent: run %q: seq range: %w", runID, err)
			}
			return rec.State, latest, replayIntegrity, nil
		}
	}

	// ③ 幂等表反查(仅纯内存)。
	if h, ok := s.requests.byRun(rt.ID, runID); ok {
		latest := uint64(0)
		if hub := rt.Hub(); hub != nil {
			latest = hub.LatestSeq()
		}
		return string(h.State()), latest, replayIntegrity, nil
	}
	return "", 0, replayIntegrity, fmt.Errorf("agent: run %q: %w", runID, runtimeagent.ErrRunNotFound)
}
